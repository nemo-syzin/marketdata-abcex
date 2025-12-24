#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import glob
import json
import logging
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlsplit

import httpx
from playwright.async_api import async_playwright, Page, Frame, Locator

# ───────────────────────── CONFIG ─────────────────────────

ABCEX_URL = os.getenv("ABCEX_URL", "https://abcex.io/client/spot/USDTRUB")
STATE_PATH = os.getenv("STATE_PATH", "abcex_state.json")

SOURCE = os.getenv("SOURCE", "abcex")
SYMBOL = os.getenv("SYMBOL", "USDT/RUB")

LIMIT = int(os.getenv("LIMIT", "120"))
POLL_SEC = float(os.getenv("POLL_SEC", "2.0"))

ABCEX_EMAIL = os.getenv("ABCEX_EMAIL", "")
ABCEX_PASSWORD = os.getenv("ABCEX_PASSWORD", "")

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "exchange_trades")

GOTO_TIMEOUT_MS = int(os.getenv("GOTO_TIMEOUT_MS", "60000"))
WAIT_TRADES_TIMEOUT_MS = int(os.getenv("WAIT_TRADES_TIMEOUT_MS", "45000"))

# Санити-фильтры (подкручиваются ENV)
PRICE_MIN = Decimal(os.getenv("PRICE_MIN", "50"))      # USDT/RUB
PRICE_MAX = Decimal(os.getenv("PRICE_MAX", "200"))     # USDT/RUB
VOL_USDT_MIN = Decimal(os.getenv("VOL_USDT_MIN", "0.00000001"))
VOL_USDT_MAX = Decimal(os.getenv("VOL_USDT_MAX", "500000"))  # защита от мусора

# Подсказка для выбора "что из двух чисел является ценой"
PRICE_HINT = Decimal(os.getenv("PRICE_HINT", "80"))

# Render cache path
BROWSERS_ROOT = os.getenv("PLAYWRIGHT_BROWSERS_PATH", "/opt/render/.cache/ms-playwright")
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = BROWSERS_ROOT

# ───────────────────────── LOGGING ─────────────────────────

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
log = logging.getLogger("abcex")

# ───────────────────────── INSTALL ─────────────────────────

def _env_without_proxies() -> Dict[str, str]:
    env = dict(os.environ)
    for k in [
        "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY",
        "http_proxy", "https_proxy", "all_proxy", "no_proxy",
    ]:
        env.pop(k, None)
    env["PLAYWRIGHT_BROWSERS_PATH"] = BROWSERS_ROOT
    return env

def _headless_shell_exists() -> bool:
    pattern = os.path.join(BROWSERS_ROOT, "chromium_headless_shell-*", "chrome-linux", "headless_shell")
    return len(glob.glob(pattern)) > 0

def _chromium_exists() -> bool:
    pattern = os.path.join(BROWSERS_ROOT, "chromium-*", "chrome-linux", "chrome")
    return len(glob.glob(pattern)) > 0

def ensure_playwright_browsers(force: bool = False) -> None:
    if not force and _chromium_exists() and _headless_shell_exists():
        log.info("Playwright browsers already present.")
        return

    max_attempts = 4
    for attempt in range(1, max_attempts + 1):
        log.warning(
            "Installing Playwright browsers (runtime) to %s ... force=%s attempt=%s/%s",
            BROWSERS_ROOT, force, attempt, max_attempts
        )
        r = subprocess.run(
            [sys.executable, "-m", "playwright", "install", "chromium", "chromium-headless-shell"],
            check=False,
            capture_output=True,
            text=True,
            env=_env_without_proxies(),
            timeout=25 * 60,
        )
        log.warning("playwright install returncode=%s", r.returncode)
        if r.stdout:
            log.warning("playwright install STDOUT (tail):\n%s", r.stdout[-2000:])
        if r.stderr:
            log.warning("playwright install STDERR (tail):\n%s", r.stderr[-2000:])

        if r.returncode == 0 and _chromium_exists() and _headless_shell_exists():
            log.warning("Install verification: chromium=True headless_shell=True")
            return

        if attempt < max_attempts:
            sleep_s = 6 * attempt
            log.warning("Install not complete yet. Sleeping %ss and retrying...", sleep_s)
            time.sleep(sleep_s)

    raise RuntimeError("playwright install failed after retries")

def _should_force_install(err: Exception) -> bool:
    s = str(err)
    return (
        "Executable doesn't exist" in s
        or "playwright install" in s
        or "chromium_headless_shell" in s
        or ("ms-playwright" in s and "doesn't exist" in s)
    )

# ───────────────────────── PROXY ─────────────────────────

def _get_proxy_url() -> Optional[str]:
    return os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY") or None

def _parse_proxy_for_playwright(proxy_url: str) -> Dict[str, Any]:
    u = urlsplit(proxy_url)
    if not u.scheme or not u.hostname or not u.port:
        return {"server": proxy_url}
    out: Dict[str, Any] = {"server": f"{u.scheme}://{u.hostname}:{u.port}"}
    if u.username:
        out["username"] = u.username
    if u.password:
        out["password"] = u.password
    return out

# ───────────────────────── NUMBERS / TIME ─────────────────────────

TIME_RE = re.compile(r"\b(\d{1,2}:\d{2}(?::\d{2})?)\b")
STRICT_TIME_RE = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d$")

def _q8(x: Decimal) -> Decimal:
    return x.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)

def _to_decimal_num(text: str) -> Decimal:
    t = (text or "").strip().replace("\xa0", " ")
    t = re.sub(r"[^0-9\s\.,]", "", t)
    t = t.replace(" ", "")
    if not t:
        raise InvalidOperation("empty numeric")
    if "," in t and "." in t:
        t = t.replace(",", "")  # ',' как разделитель тысяч
    else:
        t = t.replace(",", ".")  # ',' как десятичный
    return Decimal(t)

def _normalize_time_to_hhmmss(text: str) -> Optional[str]:
    m = TIME_RE.search((text or "").replace("\xa0", " "))
    if not m:
        return None
    raw = m.group(1)
    parts = raw.split(":")
    if len(parts) == 2:
        hh, mm = parts
        return f"{hh.zfill(2)}:{mm}:00"
    hh, mm, ss = parts
    return f"{hh.zfill(2)}:{mm}:{ss.zfill(2)}"

def _is_valid_time_hhmmss(tt: str) -> bool:
    return bool(tt and STRICT_TIME_RE.match(tt))

def _price_ok(p: Decimal) -> bool:
    return (p >= PRICE_MIN) and (p <= PRICE_MAX)

def _vol_ok(v: Decimal) -> bool:
    return (v >= VOL_USDT_MIN) and (v <= VOL_USDT_MAX)

# ───────────────────────── DEBUG ARTIFACTS ─────────────────────────

async def save_debug(page: Page, tag: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    png = f"debug_{tag}_{ts}.png"
    html = f"debug_{tag}_{ts}.html"

    try:
        # full_page=False снижает шанс зависания на шрифтах
        await page.screenshot(path=png, full_page=False, timeout=8_000)
        log.warning("Saved debug screenshot: %s", png)
    except Exception as e:
        log.warning("Could not save screenshot: %s", e)

    try:
        content = await page.content()
        with open(html, "w", encoding="utf-8") as f:
            f.write(content)
        log.warning("Saved debug html: %s", html)
    except Exception as e:
        log.warning("Could not save html: %s", e)

# ───────────────────────── PAGE HELPERS (frames-aware) ─────────────────────────

async def accept_cookies_if_any(page: Page) -> None:
    for txt in ["Принять", "Согласен", "Я согласен", "Accept", "I agree"]:
        try:
            btn = page.locator(f"text={txt}")
            if await btn.count() > 0 and await btn.first.is_visible():
                log.info("Cookies banner: clicking '%s'...", txt)
                await btn.first.click(timeout=5_000)
                await page.wait_for_timeout(500)
                return
        except Exception:
            pass

async def _find_visible_in_all_frames(page: Page, selectors: List[str], per_selector_scan: int = 10) -> Optional[Tuple[Locator, str]]:
    """
    Ищем первый видимый элемент среди всех frames/page.
    Возвращаем (locator, frame_url).
    """
    frames: List[Frame] = page.frames  # включает main frame
    for fr in frames:
        for sel in selectors:
            try:
                loc = fr.locator(sel)
                cnt = await loc.count()
                if cnt <= 0:
                    continue
                for i in range(min(cnt, per_selector_scan)):
                    cand = loc.nth(i)
                    try:
                        if await cand.is_visible():
                            return cand, fr.url
                    except Exception:
                        continue
            except Exception:
                continue
    return None

async def _is_login_visible(page: Page) -> bool:
    found = await _find_visible_in_all_frames(page, [
        "input[type='password']",
        "input[autocomplete='current-password']",
        "input[name*='pass' i]",
    ])
    return found is not None

async def _open_login_modal_if_needed(page: Page) -> None:
    if await _is_login_visible(page):
        return
    found = await _find_visible_in_all_frames(page, [
        "button:has-text('Войти')",
        "a:has-text('Войти')",
        "text=Войти",
        "button:has-text('Login')",
        "a:has-text('Login')",
        "text=Login",
        "button:has-text('Sign in')",
        "a:has-text('Sign in')",
        "text=Sign in",
    ])
    if not found:
        return
    btn, fr_url = found
    try:
        await btn.click(timeout=7_000)
        log.info("Opened login UI (frame=%s).", fr_url)
        await page.wait_for_timeout(800)
    except Exception:
        pass

async def login_if_needed(page: Page, email: str, password: str) -> None:
    await _open_login_modal_if_needed(page)

    if not await _is_login_visible(page):
        log.info("Login not required (no password field detected).")
        return

    if not email or not password:
        raise RuntimeError("Login form detected, but ABCEX_EMAIL/ABCEX_PASSWORD are not set.")

    log.info("Login detected. Performing sign-in ...")

    email_found = await _find_visible_in_all_frames(page, [
        "input[type='email']",
        "input[autocomplete='username']",
        "input[name*='mail' i]",
        "input[name*='login' i]",
        "input[placeholder*='mail' i]",
        "input[placeholder*='email' i]",
        "input[placeholder*='почта' i]",
    ])
    pw_found = await _find_visible_in_all_frames(page, [
        "input[type='password']",
        "input[autocomplete='current-password']",
        "input[name*='pass' i]",
    ])

    if not email_found or not pw_found:
        await save_debug(page, "login_fields_not_found")
        raise RuntimeError("Не смог найти поля email/password (включая frames).")

    email_loc, email_fr = email_found
    pw_loc, pw_fr = pw_found

    await email_loc.fill(email, timeout=10_000)
    await pw_loc.fill(password, timeout=10_000)

    submit_found = await _find_visible_in_all_frames(page, [
        "button[type='submit']",
        "button:has-text('Войти')",
        "button:has-text('Вход')",
        "button:has-text('Login')",
        "button:has-text('Sign in')",
    ])
    if submit_found:
        btn, fr_url = submit_found
        try:
            await btn.click(timeout=10_000)
            log.info("Clicked submit (frame=%s).", fr_url)
        except Exception:
            try:
                await page.keyboard.press("Enter")
            except Exception:
                pass
    else:
        try:
            await page.keyboard.press("Enter")
        except Exception:
            pass

    await page.wait_for_timeout(1_000)

    for _ in range(80):
        if not await _is_login_visible(page):
            log.info("Login successful (password field disappeared).")
            return
        await page.wait_for_timeout(400)

    await save_debug(page, "login_failed")
    raise RuntimeError("Логин не прошёл (форма логина всё ещё видна).")

async def click_trades_tab_best_effort(page: Page) -> None:
    # Пытаемся включить вкладку "Сделки/Trades" — тоже через frames
    candidates = ["Сделки", "Trades", "Order history", "История", "Последние сделки"]
    selectors: List[str] = []
    for t in candidates:
        selectors += [
            f"[role='tab']:has-text('{t}')",
            f"button:has-text('{t}')",
            f"a:has-text('{t}')",
            f"text={t}",
        ]
    found = await _find_visible_in_all_frames(page, selectors)
    if not found:
        return
    el, fr_url = found
    try:
        await el.click(timeout=8_000)
        log.info("Clicked trades tab (frame=%s).", fr_url)
        await page.wait_for_timeout(800)
    except Exception:
        pass

# ───────────────────────── PARSING (frame-aware) ─────────────────────────

JS_TRIPLETS = r"""
(limit) => {
  const timeRe = /\b(\d{1,2}:\d{2}(?::\d{2})?)\b/;

  const pickRoot = () => {
    const roots = [
      document.querySelector("[role='tabpanel'][data-state='active']"),
      document.querySelector("[role='tabpanel']:not([hidden])"),
      document.querySelector("[role='tabpanel']"),
      document.querySelector("main"),
      document.body
    ].filter(Boolean);
    return roots[0] || document.body;
  };

  const root = pickRoot();
  const out = [];
  const seen = new Set();

  const divs = Array.from(root.querySelectorAll("div")).slice(0, 9000);

  for (const div of divs) {
    // основной формат, который у тебя уже работал: p[data-size='xs']
    const ps = div.querySelectorAll("p[data-size='xs']");
    if (!ps || ps.length < 3) continue;

    const texts = Array.from(ps).slice(0, 6).map(p => (p.textContent || "").trim()).filter(Boolean);
    if (texts.length < 3) continue;

    for (let i = 0; i < texts.length; i++) {
      const m = texts[i].match(timeRe);
      if (!m) continue;
      const timeTxt = m[1];

      const nums = texts
        .filter((_, idx) => idx !== i)
        .filter(x => /^[0-9]/.test(x));

      if (nums.length < 2) continue;

      const a = nums[0];
      const b = nums[1];

      const key = `${timeTxt}|${a}|${b}`;
      if (seen.has(key)) continue;
      seen.add(key);

      out.push({ a, b, time_raw: timeTxt });
      if (out.length >= limit) return out;
    }
  }
  return out;
}
"""

async def _extract_raw_triplets_from_frame(fr: Frame, limit: int) -> List[Dict[str, str]]:
    try:
        return await fr.evaluate(JS_TRIPLETS, limit)
    except Exception:
        return []

async def _extract_raw_triplets_best_target(page: Page, limit: int) -> Tuple[List[Dict[str, str]], str]:
    best: List[Dict[str, str]] = []
    best_where = "page"

    # main page
    try:
        raw_page = await page.evaluate(JS_TRIPLETS, limit)
        if len(raw_page) > len(best):
            best = raw_page
            best_where = "page"
    except Exception:
        pass

    # frames
    for fr in page.frames:
        raw = await _extract_raw_triplets_from_frame(fr, limit)
        if len(raw) > len(best):
            best = raw
            best_where = fr.url or "frame"

    return best, best_where

def _candidate_score(price: Decimal, hint: Decimal) -> Decimal:
    return abs(price - hint)

def _apply_price_scaling_if_needed(p: Decimal) -> List[Decimal]:
    """
    Если цена выглядит как 86836 вместо 86.836 — пробуем делить.
    Возвращаем список кандидатов (включая исходный).
    """
    outs = [p]
    if p <= 0:
        return outs
    for div in (Decimal("10"), Decimal("100"), Decimal("1000"), Decimal("10000")):
        outs.append(p / div)
    return outs

def _try_parse_trade(a: str, b: str, time_raw: str, price_hint: Decimal) -> Optional[Tuple[Decimal, Decimal, str]]:
    tt = _normalize_time_to_hhmmss(time_raw)
    if not tt or not _is_valid_time_hhmmss(tt):
        return None

    def parse_num(x: str) -> Optional[Decimal]:
        try:
            return _q8(_to_decimal_num(x))
        except Exception:
            return None

    da = parse_num(a)
    db = parse_num(b)
    if da is None or db is None:
        return None

    # Два варианта: price=a qty=b или наоборот.
    # Для каждого варианта пробуем еще "масштабирование" цены.
    candidates: List[Tuple[Decimal, Decimal]] = []

    for p_raw, q_raw in ((da, db), (db, da)):
        for p in _apply_price_scaling_if_needed(p_raw):
            p = _q8(p)
            q = _q8(q_raw)
            if p <= 0 or q <= 0:
                continue
            if not _vol_ok(q):
                continue
            if not _price_ok(p):
                continue
            candidates.append((p, q))

    if not candidates:
        return None

    # Выбор лучшего по близости к price_hint
    best = min(candidates, key=lambda pq: _candidate_score(pq[0], price_hint))
    return best[0], best[1], tt

async def extract_trades(page: Page, limit: int, price_hint: Decimal) -> Tuple[List[Dict[str, Any]], Decimal, str]:
    raw, where = await _extract_raw_triplets_best_target(page, limit=limit)
    log.info("Raw triplets: %d (best_target=%s)", len(raw), where)

    rows: List[Dict[str, Any]] = []
    prices: List[Decimal] = []
    rejected = 0

    for r in raw:
        a = str(r.get("a") or "").strip()
        b = str(r.get("b") or "").strip()
        tr = str(r.get("time_raw") or "").strip()

        parsed = _try_parse_trade(a, b, tr, price_hint=price_hint)
        if not parsed:
            rejected += 1
            continue

        price, vol_usdt, tt = parsed
        vol_rub = _q8(price * vol_usdt)

        rows.append({
            "source": SOURCE,
            "symbol": SYMBOL,
            "price": str(price),
            "volume_usdt": str(vol_usdt),
            "volume_rub": str(vol_rub),
            "trade_time": tt,
        })
        prices.append(price)

    if rejected:
        log.info("Rejected rows (failed parse/sanity): %d", rejected)

    # Обновим hint по медиане цен, если есть
    new_hint = price_hint
    if prices:
        prices_sorted = sorted(prices)
        new_hint = prices_sorted[len(prices_sorted) // 2]

    return rows, new_hint, where

async def wait_trades_parsable(page: Page, timeout_ms: int, min_rows: int, price_hint: Decimal) -> Tuple[Decimal, str]:
    start = time.time()
    last_n = -1
    last_where = "unknown"
    hint = price_hint

    while (time.time() - start) * 1000 < timeout_ms:
        try:
            probe, hint2, where = await extract_trades(page, limit=50, price_hint=hint)
            n = len(probe)
            last_where = where
            hint = hint2
            if n != last_n:
                log.info("Trades parsable probe: %d (where=%s)", n, where)
                last_n = n
            if n >= min_rows:
                return hint, where
        except Exception:
            pass

        await page.wait_for_timeout(900)

    await save_debug(page, "trades_not_parsable")
    raise RuntimeError(f"Не дождался парсабельных сделок (time + 2 numbers). last_where={last_where}")

# ───────────────────────── SUPABASE ─────────────────────────

@dataclass(frozen=True)
class TradeKey:
    source: str
    symbol: str
    trade_time: str
    price: str
    volume_usdt: str

def trade_key(t: Dict[str, Any]) -> TradeKey:
    return TradeKey(
        source=str(t.get("source", "")),
        symbol=str(t.get("symbol", "")),
        trade_time=str(t.get("trade_time", "")),
        price=str(t.get("price", "")),
        volume_usdt=str(t.get("volume_usdt", "")),
    )

async def supabase_upsert_trades(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    if not SUPABASE_URL or not SUPABASE_KEY or not SUPABASE_TABLE:
        log.warning("Supabase env is not fully set; skipping DB write.")
        return

    url = (
        SUPABASE_URL.rstrip("/")
        + f"/rest/v1/{SUPABASE_TABLE}"
        + "?on_conflict=source,symbol,trade_time,price,volume_usdt"
    )
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }

    async with httpx.AsyncClient(timeout=35.0, headers=headers, trust_env=True) as client:
        resp = await client.post(url, content=json.dumps(rows, ensure_ascii=False).encode("utf-8"))
        if resp.status_code in (200, 201, 204):
            log.info("Supabase upsert OK: %s trades", len(rows))
            return
        log.error("Supabase write failed: %s %s", resp.status_code, resp.text[:2000])

# ───────────────────────── CORE ─────────────────────────

async def _goto_stable(page: Page, url: str) -> None:
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=GOTO_TIMEOUT_MS)
    except Exception as e:
        log.warning("goto failed (%s). retry with reload...", e)
        await page.reload(wait_until="domcontentloaded", timeout=GOTO_TIMEOUT_MS)

    try:
        await page.wait_for_load_state("networkidle", timeout=20_000)
    except Exception:
        pass

    await page.wait_for_timeout(1_200)

async def run_once(price_hint: Decimal) -> Tuple[List[Dict[str, Any]], Decimal]:
    proxy_url = _get_proxy_url()
    pw_proxy = _parse_proxy_for_playwright(proxy_url) if proxy_url else None

    async with async_playwright() as p:
        browser = None
        for attempt in (1, 2, 3):
            try:
                kwargs: Dict[str, Any] = {
                    "headless": True,
                    "args": [
                        "--disable-blink-features=AutomationControlled",
                        "--no-sandbox",
                        "--disable-dev-shm-usage",
                    ],
                }
                if pw_proxy:
                    kwargs["proxy"] = pw_proxy

                browser = await p.chromium.launch(**kwargs)
                break
            except Exception as e:
                if _should_force_install(e):
                    log.warning("Chromium launch failed; forcing install and retry... attempt=%s", attempt)
                    ensure_playwright_browsers(force=True)
                    await asyncio.sleep(2.0 * attempt)
                    continue
                raise

        if browser is None:
            raise RuntimeError("Cannot launch browser after retries.")

        storage_state = STATE_PATH if os.path.exists(STATE_PATH) else None
        if storage_state:
            log.info("Using saved session state: %s", storage_state)

        context = await browser.new_context(
            viewport={"width": 1440, "height": 810},
            locale="ru-RU",
            timezone_id="Europe/Moscow",
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            storage_state=storage_state,
        )

        page = await context.new_page()
        page.set_default_timeout(20_000)
        page.set_default_navigation_timeout(GOTO_TIMEOUT_MS)

        try:
            log.info("Opening ABCEX: %s", ABCEX_URL)
            await _goto_stable(page, ABCEX_URL)

            await accept_cookies_if_any(page)
            await login_if_needed(page, ABCEX_EMAIL, ABCEX_PASSWORD)

            # После логина UI может перекинуть — фиксируемся на нужной странице
            await _goto_stable(page, ABCEX_URL)

            try:
                await context.storage_state(path=STATE_PATH)
                log.info("Saved session state to %s", STATE_PATH)
            except Exception as e:
                log.warning("Could not save storage state: %s", e)

            await click_trades_tab_best_effort(page)

            # Дожидаемся парсинга (включая frames) и одновременно уточняем price_hint
            price_hint2, where = await wait_trades_parsable(
                page, timeout_ms=WAIT_TRADES_TIMEOUT_MS, min_rows=5, price_hint=price_hint
            )
            log.info("Trades are parsable (where=%s). price_hint=%s", where, price_hint2)

            trades, price_hint3, where2 = await extract_trades(page, limit=LIMIT, price_hint=price_hint2)
            log.info("Parsed trades (validated): %d (where=%s)", len(trades), where2)

            if not trades:
                await save_debug(page, "parsed_zero_trades")

            return trades, price_hint3

        except Exception as e:
            await save_debug(page, "cycle_error")
            raise e

        finally:
            try:
                await page.close()
            except Exception:
                pass
            try:
                await context.close()
            except Exception:
                pass
            try:
                await browser.close()
            except Exception:
                pass

async def main() -> None:
    ensure_playwright_browsers(force=False)

    seen: Dict[TradeKey, float] = {}
    seen_ttl = 60 * 30  # 30 минут

    price_hint = PRICE_HINT

    while True:
        try:
            trades, price_hint = await run_once(price_hint=price_hint)

            now = time.time()
            for k, ts in list(seen.items()):
                if now - ts > seen_ttl:
                    del seen[k]

            fresh: List[Dict[str, Any]] = []
            for t in trades:
                k = trade_key(t)
                if k in seen:
                    continue
                seen[k] = now
                fresh.append(t)

            if fresh:
                await supabase_upsert_trades(fresh)
            else:
                log.info("No new trades after in-memory dedup.")

        except Exception as e:
            log.error("Cycle error: %s", e)
            if _should_force_install(e):
                try:
                    ensure_playwright_browsers(force=True)
                except Exception as ie:
                    log.error("Forced install failed: %s", ie)
            await asyncio.sleep(4.0)

        await asyncio.sleep(POLL_SEC)

if __name__ == "__main__":
    asyncio.run(main())
