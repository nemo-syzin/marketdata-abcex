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
from playwright.async_api import async_playwright, Page

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

# Защита от мусора: допустимый диапазон цены (настрой через ENV при необходимости)
PRICE_MIN = Decimal(os.getenv("PRICE_MIN", "50"))   # для USDT/RUB разумно 50
PRICE_MAX = Decimal(os.getenv("PRICE_MAX", "200"))  # для USDT/RUB разумно 200

# Render cache path
BROWSERS_ROOT = os.getenv("PLAYWRIGHT_BROWSERS_PATH", "/opt/render/.cache/ms-playwright")
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = BROWSERS_ROOT

# ───────────────────────── LOGGING ─────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
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
STRICT_TIME_RE = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d$")  # 00:00:00 .. 23:59:59

def _q8(x: Decimal) -> Decimal:
    return x.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)

def _to_decimal_num(text: str) -> Decimal:
    """
    Грязестойкий парсер чисел:
    - убирает валюты/буквы/символы типа ₽, USDT
    - поддерживает '191 889,47' / '191889.47' / '14,964.9824'
    """
    t = (text or "").strip().replace("\xa0", " ")
    t = re.sub(r"[^0-9\s\.,]", "", t)
    t = t.replace(" ", "")
    if not t:
        raise InvalidOperation("empty numeric")
    if "," in t and "." in t:
        # если есть и "," и ".", почти всегда "," = тысячные
        t = t.replace(",", "")
    else:
        t = t.replace(",", ".")
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

# ───────────────────────── DEBUG ARTIFACTS ─────────────────────────

async def save_debug(page: Page, tag: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    png = f"debug_{tag}_{ts}.png"
    html = f"debug_{tag}_{ts}.html"

    try:
        # уменьшаем шанс таймаута на шрифтах
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

# ───────────────────────── PAGE ACTIONS ─────────────────────────

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

async def _is_login_visible(page: Page) -> bool:
    # проще и надежнее: достаточно увидеть input[type=password] хоть где-то
    try:
        pw = page.locator("input[type='password']")
        return (await pw.count() > 0) and (await pw.first.is_visible())
    except Exception:
        return False

async def _open_login_modal_if_needed(page: Page) -> None:
    if await _is_login_visible(page):
        return
    for sel in [
        "button:has-text('Войти')",
        "a:has-text('Войти')",
        "text=Войти",
        "button:has-text('Login')",
        "a:has-text('Login')",
        "text=Login",
        "button:has-text('Sign in')",
        "a:has-text('Sign in')",
        "text=Sign in",
    ]:
        try:
            btn = page.locator(sel)
            if await btn.count() > 0 and await btn.first.is_visible():
                await btn.first.click(timeout=7_000)
                await page.wait_for_timeout(800)
                return
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

    # email field: берём первый видимый input email/text
    email_loc = page.locator("input[type='email'], input[autocomplete='username'], input[name*='mail' i], input[name*='login' i]").first
    pw_loc = page.locator("input[type='password'], input[autocomplete='current-password']").first

    if not await email_loc.is_visible() or not await pw_loc.is_visible():
        await save_debug(page, "login_fields_not_visible")
        raise RuntimeError("Не смог найти/увидеть поля email/password.")

    await email_loc.fill(email, timeout=10_000)
    await pw_loc.fill(password, timeout=10_000)

    submit = page.locator("button[type='submit'], button:has-text('Войти'), button:has-text('Login'), button:has-text('Sign in')").first
    try:
        if await submit.is_visible():
            await submit.click(timeout=10_000)
        else:
            await page.keyboard.press("Enter")
    except Exception:
        try:
            await page.keyboard.press("Enter")
        except Exception:
            pass

    await page.wait_for_timeout(1_000)

    for _ in range(60):
        if not await _is_login_visible(page):
            log.info("Login successful (password field disappeared).")
            return
        await page.wait_for_timeout(400)

    await save_debug(page, "login_failed")
    raise RuntimeError("Логин не прошёл (форма логина всё ещё видна).")

async def click_trades_tab_best_effort(page: Page) -> None:
    # пробуем вкладки/кнопки “Сделки/Trades/Order history”
    candidates = ["Сделки", "Trades", "Order history", "История", "Последние сделки"]
    for t in candidates:
        for sel in [
            f"[role='tab']:has-text('{t}')",
            f"button:has-text('{t}')",
            f"a:has-text('{t}')",
            f"text={t}",
        ]:
            try:
                el = page.locator(sel).first
                if await el.count() > 0 and await el.is_visible():
                    await el.click(timeout=8_000)
                    await page.wait_for_timeout(800)
                    return
            except Exception:
                continue

# ───────────────────────── PARSING ─────────────────────────

async def _extract_raw_trade_triplets(page: Page, limit: int) -> List[Dict[str, str]]:
    """
    Достаём "триплеты" из активной панели: два числа + время.
    Не привязываемся к id orderHistory — потому что он ломается.
    """
    js = r"""
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

      // Находим div, внутри которых есть p[data-size=xs] (как в твоей версии),
      // но НЕ предполагаем порядок: время может быть в любом из 3.
      const nodes = Array.from(root.querySelectorAll("div"))
        .map(div => {
          const ps = div.querySelectorAll("p[data-size='xs']");
          if (!ps || ps.length < 3) return null;

          // берём первые 3, но если их больше — всё равно часто первые три являются строкой
          const texts = Array.from(ps).slice(0, 4).map(p => (p.textContent || "").trim()).filter(Boolean);
          if (texts.length < 3) return null;

          // ищем любую комбинацию: одно поле время, два поля начинаются с цифры
          for (let i = 0; i < texts.length; i++) {
            if (!timeRe.test(texts[i])) continue;
            const timeTxt = (texts[i].match(timeRe) || [])[1];
            const nums = texts.filter((_, idx) => idx !== i);
            // оставляем только те, что похожи на числа
            const numLike = nums.filter(x => /^[0-9]/.test(x));
            if (numLike.length < 2) continue;
            return { a: numLike[0], b: numLike[1], time_raw: timeTxt };
          }
          return null;
        })
        .filter(Boolean);

      // дедуп + лимит
      const out = [];
      const seen = new Set();
      for (const r of nodes) {
        const key = `${r.time_raw}|${r.a}|${r.b}`;
        if (seen.has(key)) continue;
        seen.add(key);
        out.push(r);
        if (out.length >= limit) break;
      }
      return out;
    }
    """
    return await page.evaluate(js, limit)

def _try_parse_trade(a: str, b: str, time_raw: str) -> Optional[Tuple[Decimal, Decimal, str]]:
    """
    Возвращаем (price, vol_usdt, hh:mm:ss) либо None.
    Порядок (a,b) может быть (price, qty) или (qty, price) — выбираем по sanity диапазону цены.
    """
    tt = _normalize_time_to_hhmmss(time_raw)
    if not tt or not _is_valid_time_hhmmss(tt):
        return None

    def parse_pair(p_txt: str, q_txt: str) -> Optional[Tuple[Decimal, Decimal]]:
        try:
            p = _q8(_to_decimal_num(p_txt))
            q = _q8(_to_decimal_num(q_txt))
            if p <= 0 or q <= 0:
                return None
            return p, q
        except Exception:
            return None

    # 1) прямой порядок
    r1 = parse_pair(a, b)
    if r1 and _price_ok(r1[0]):
        return r1[0], r1[1], tt

    # 2) обратный порядок
    r2 = parse_pair(b, a)
    if r2 and _price_ok(r2[0]):
        return r2[0], r2[1], tt

    # 3) иногда UI отдаёт "цену" со сдвигом (например, *1000) — пробуем нормализацию
    #    НО применяем только если после деления цена попадает в диапазон.
    for base in (r1, r2):
        if not base:
            continue
        p, q = base
        if p > PRICE_MAX:
            for div in (Decimal("10"), Decimal("100"), Decimal("1000"), Decimal("10000")):
                pp = p / div
                if _price_ok(pp):
                    return _q8(pp), q, tt

    return None

async def extract_trades(page: Page, limit: int) -> List[Dict[str, Any]]:
    raw = await _extract_raw_trade_triplets(page, limit=limit)

    rows: List[Dict[str, Any]] = []
    rejected = 0

    for r in raw:
        a = str(r.get("a") or "").strip()
        b = str(r.get("b") or "").strip()
        tr = str(r.get("time_raw") or "").strip()

        parsed = _try_parse_trade(a, b, tr)
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

    if rejected:
        log.info("Rejected rows (failed sanity/parse): %s", rejected)

    return rows

async def wait_trades_parsable(page: Page, timeout_ms: int, min_rows: int = 5) -> None:
    start = time.time()
    last_n = -1
    while (time.time() - start) * 1000 < timeout_ms:
        try:
            probe = await extract_trades(page, limit=50)
            n = len(probe)
            if n != last_n:
                log.info("Trades parsable probe: %d", n)
                last_n = n
            if n >= min_rows:
                return
        except Exception:
            pass
        await page.wait_for_timeout(900)

    await save_debug(page, "trades_not_parsable")
    raise RuntimeError("Не дождался парсабельных сделок (time + 2 numbers) в активной панели.")

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
    # Playwright иногда “подвисает” на goto — добавим fallback reload
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=GOTO_TIMEOUT_MS)
    except Exception as e:
        log.warning("goto failed (%s). retry with reload...", e)
        try:
            await page.reload(wait_until="domcontentloaded", timeout=GOTO_TIMEOUT_MS)
        except Exception:
            raise
    try:
        await page.wait_for_load_state("networkidle", timeout=20_000)
    except Exception:
        pass
    await page.wait_for_timeout(1_200)

async def run_once() -> List[Dict[str, Any]]:
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

            # иногда после логина уводит/перерисовывает — возвращаемся на нужный URL
            await _goto_stable(page, ABCEX_URL)

            try:
                await context.storage_state(path=STATE_PATH)
                log.info("Saved session state to %s", STATE_PATH)
            except Exception as e:
                log.warning("Could not save storage state: %s", e)

            await click_trades_tab_best_effort(page)

            # ждём, пока сделки реально начнут парситься
            await wait_trades_parsable(page, timeout_ms=WAIT_TRADES_TIMEOUT_MS, min_rows=5)

            trades = await extract_trades(page, limit=LIMIT)
            log.info("Parsed trades (validated): %d", len(trades))

            if not trades:
                await save_debug(page, "parsed_zero_trades")

            return trades

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

    while True:
        try:
            trades = await run_once()

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
