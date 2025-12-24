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
from playwright.async_api import async_playwright, Page, Frame, Locator, Response


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

# sanity для USDT/RUB
PRICE_MIN = Decimal(os.getenv("PRICE_MIN", "50"))
PRICE_MAX = Decimal(os.getenv("PRICE_MAX", "200"))

# Диагностика в логах
DIAG = os.getenv("DIAG", "1") == "1"
DIAG_TEXT_CHARS = int(os.getenv("DIAG_TEXT_CHARS", "1400"))
DIAG_MAX_SAMPLES = int(os.getenv("DIAG_MAX_SAMPLES", "6"))

DEBUG_DIR = os.getenv("DEBUG_DIR", "/tmp")

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
STRICT_TIME_RE = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d(?::[0-5]\d)?$")

def _q8(x: Decimal) -> Decimal:
    return x.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)

def _to_decimal_num(text: str) -> Decimal:
    t = (text or "").strip().replace("\xa0", " ")
    t = re.sub(r"[^0-9\s\.,]", "", t)
    t = t.replace(" ", "")
    if not t:
        raise InvalidOperation("empty numeric")
    if "," in t and "." in t:
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

def _is_valid_time(tt: str) -> bool:
    return bool(tt and STRICT_TIME_RE.match(tt))

def _price_ok(p: Decimal) -> bool:
    return (p >= PRICE_MIN) and (p <= PRICE_MAX)


# ───────────────────────── DEBUG HELPERS ─────────────────────────

async def save_debug(page: Page, tag: str) -> None:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    png_path = os.path.join(DEBUG_DIR, f"abcex_{tag}_{ts}.png")
    html_path = os.path.join(DEBUG_DIR, f"abcex_{tag}_{ts}.html")

    try:
        await page.screenshot(path=png_path, full_page=False, timeout=15_000)
        log.warning("Saved debug screenshot: %s", png_path)
    except Exception as e:
        log.warning("Could not save screenshot: %s", e)

    try:
        content = await page.content()
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(content)
        log.warning("Saved debug html: %s", html_path)
    except Exception as e:
        log.warning("Could not save html: %s", e)


# ───────────────────────── DIAG (LOG ONLY) ─────────────────────────

_JS_DIAG = r"""
(args) => {
  const maxChars = args.maxChars ?? 1400;
  const maxSamples = args.maxSamples ?? 6;

  const timeRe = /\b(\d{1,2}:\d{2}(?::\d{2})?)\b/;
  const numRe = /(?:^|[^\d])(\d[\d\s.,]*\d|\d)(?:[^\d]|$)/g;

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
  const rootText = (root.innerText || "").replace(/\u00a0/g, " ").trim();
  const textSnippet = rootText.slice(0, maxChars);

  const lines = rootText.split("\n").map(s => s.trim()).filter(Boolean);

  let timeLinesCount = 0;
  const candidates = [];
  for (const line of lines) {
    if (!timeRe.test(line)) continue;
    timeLinesCount++;
    const nums = (line.match(numRe) || []).map(x => x.replace(/^[^\d]+|[^\d]+$/g, ""));
    if (nums.length >= 2 && candidates.length < maxSamples) {
      candidates.push(line.length > 220 ? line.slice(0, 220) : line);
    }
  }

  const tabpanels = Array.from(document.querySelectorAll("[role='tabpanel']")).slice(0, 8).map(tp => ({
    hidden: tp.hasAttribute("hidden"),
    data_state: tp.getAttribute("data-state"),
    id: tp.getAttribute("id"),
    text: (tp.innerText || "").replace(/\u00a0/g, " ").trim().slice(0, 180)
  }));

  return {
    rootTag: root.tagName,
    rootTextLen: rootText.length,
    timeLinesCount,
    candidates,
    tabpanels,
    textSnippet
  };
}
"""

async def log_diag(page: Page, tag: str) -> None:
    if not DIAG:
        return

    try:
        frames = page.frames
        log.warning("DIAG[%s] url=%s frames=%d", tag, page.url, len(frames))
        for i, fr in enumerate(frames[:10]):
            try:
                log.warning("DIAG[%s] frame[%d] url=%s", tag, i, fr.url)
            except Exception:
                pass
    except Exception as e:
        log.warning("DIAG[%s] frames failed: %s", tag, e)

    try:
        out = await page.evaluate(_JS_DIAG, {"maxChars": DIAG_TEXT_CHARS, "maxSamples": DIAG_MAX_SAMPLES})
        log.warning(
            "DIAG[%s] root=%s rootTextLen=%s timeLines=%s",
            tag, out.get("rootTag"), out.get("rootTextLen"), out.get("timeLinesCount")
        )
        for i, tp in enumerate(out.get("tabpanels") or []):
            log.warning(
                "DIAG[%s] tabpanel[%d] hidden=%s state=%s id=%s text=%s",
                tag, i, tp.get("hidden"), tp.get("data_state"), tp.get("id"), tp.get("text")
            )
        for i, s in enumerate(out.get("candidates") or []):
            log.warning("DIAG[%s] candidateLine[%d]=%s", tag, i, s)
    except Exception as e:
        log.warning("DIAG[%s] evaluate failed: %s", tag, e)


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
            btn = page.locator(sel).first
            if await btn.count() > 0 and await btn.is_visible():
                await btn.click(timeout=7_000)
                await page.wait_for_timeout(900)
                return
        except Exception:
            pass

async def login_if_needed(page: Page, email: str, password: str) -> None:
    await _open_login_modal_if_needed(page)

    if not await _is_login_visible(page):
        log.info("Login not required (no password field detected).")
        return

    if not email or not password:
        await save_debug(page, "need_credentials")
        raise RuntimeError("Login form detected, but ABCEX_EMAIL/ABCEX_PASSWORD are not set.")

    log.info("Login detected. Performing sign-in ...")

    email_loc = page.locator(
        "input[type='email'], input[autocomplete='username'], input[name*='mail' i], input[name*='login' i]"
    ).first
    pw_loc = page.locator("input[type='password'], input[autocomplete='current-password']").first

    if not await email_loc.is_visible() or not await pw_loc.is_visible():
        await save_debug(page, "login_fields_not_visible")
        raise RuntimeError("Не смог найти/увидеть поля email/password.")

    await email_loc.fill(email, timeout=10_000)
    await pw_loc.fill(password, timeout=10_000)

    submit = page.locator(
        "button[type='submit'], button:has-text('Войти'), button:has-text('Login'), button:has-text('Sign in')"
    ).first
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

    await page.wait_for_timeout(1_200)

    for _ in range(80):
        if not await _is_login_visible(page):
            log.info("Login successful (password field disappeared).")
            return
        await page.wait_for_timeout(400)

    await save_debug(page, "login_failed")
    raise RuntimeError("Логин не прошёл (форма логина всё ещё видна). Возможны 2FA/капча/иной флоу.")

async def click_trades_tab_best_effort(page: Page) -> None:
    candidates = ["Сделки", "Trades", "Order history", "История", "Последние сделки", "Recent trades"]
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
                    await el.click(timeout=10_000)
                    await page.wait_for_timeout(900)
                    log.info("Clicked trades tab by '%s'.", t)
                    return
            except Exception:
                continue

async def _goto_stable(page: Page, url: str) -> None:
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=GOTO_TIMEOUT_MS)
    except Exception:
        await page.reload(wait_until="domcontentloaded", timeout=GOTO_TIMEOUT_MS)

    try:
        await page.wait_for_load_state("networkidle", timeout=20_000)
    except Exception:
        pass
    await page.wait_for_timeout(1_200)


# ───────────────────────── PANEL PARSER (как в бек-версии) ─────────────────────────

@dataclass(frozen=True)
class TradeRow:
    price: Decimal
    qty: Decimal
    trade_time: str
    side: Optional[str]  # buy/sell/None
    price_raw: str
    qty_raw: str

async def find_best_trades_panel(page: Page) -> Optional[Locator]:
    """
    1) пытаемся найти panel-orderHistory
    2) если нет — ищем любую видимую tabpanel, где есть HH:MM:SS и строки (price/qty/time)
    """
    # 1) привычный id
    try:
        panel = page.locator("div[role='tabpanel'][id*='panel-orderHistory']")
        if await panel.count() > 0:
            for i in range(await panel.count()):
                p = panel.nth(i)
                try:
                    if await p.is_visible():
                        return p
                except Exception:
                    continue
    except Exception:
        pass

    # 2) эвристика по любому tabpanel
    tabpanels = page.locator("div[role='tabpanel']")
    try:
        n = await tabpanels.count()
    except Exception:
        n = 0

    js_has_rows = r"""
    (root) => {
      const isTime = (s) => /^\d{2}:\d{2}:\d{2}$/.test((s||'').trim());
      const isNum  = (s) => /^[0-9][0-9\s\u00A0.,]*$/.test((s||'').trim());

      // ищем строки как: div > p,p,p где last = time
      const divs = Array.from(root.querySelectorAll('div'));
      let hits = 0;

      for (const g of divs) {
        const ps = Array.from(g.querySelectorAll(':scope > p'));
        if (ps.length < 3) continue;
        const t0 = (ps[0].textContent||'').trim();
        const t1 = (ps[1].textContent||'').trim();
        const t2 = (ps[2].textContent||'').trim();
        if (!isTime(t2)) continue;
        if (!isNum(t0) || !isNum(t1)) continue;
        hits++;
        if (hits >= 5) return true;
      }
      return false;
    }
    """

    for i in range(min(n, 12)):
        p = tabpanels.nth(i)
        try:
            if not await p.is_visible():
                continue
            h = await p.element_handle()
            if not h:
                continue
            ok = await h.evaluate(js_has_rows)
            if ok:
                return p
        except Exception:
            continue

    return None

async def extract_trades_from_panel(panel: Locator, limit: int) -> List[TradeRow]:
    """
    Парсим строго из панели: ищем блоки div, где прямые дети — p,p,p (price/qty/time)
    """
    handle = await panel.element_handle()
    if handle is None:
        return []

    js_extract = r"""
    (root, limit) => {
      const isTime = (s) => /^\d{2}:\d{2}:\d{2}$/.test((s||'').trim());
      const isNum  = (s) => /^[0-9][0-9\s\u00A0.,]*$/.test((s||'').trim());

      const out = [];
      const divs = Array.from(root.querySelectorAll('div'));

      for (const g of divs) {
        const ps = Array.from(g.querySelectorAll(':scope > p'));
        if (ps.length < 3) continue;

        const t0 = (ps[0].textContent||'').trim();
        const t1 = (ps[1].textContent||'').trim();
        const t2 = (ps[2].textContent||'').trim();

        if (!isTime(t2)) continue;
        if (!isNum(t0) || !isNum(t1)) continue;

        const style0 = (ps[0].getAttribute('style') || '').toLowerCase();
        let side = null;
        if (style0.includes('green')) side = 'buy';
        if (style0.includes('red')) side = 'sell';

        out.push({ price_raw: t0, qty_raw: t1, time: t2, side });
        if (out.length >= limit) break;
      }

      return out;
    }
    """

    raw_rows: List[Dict[str, Any]] = await handle.evaluate(js_extract, limit)

    rows: List[TradeRow] = []
    for r in raw_rows:
        try:
            price_raw = str(r.get("price_raw", "")).strip()
            qty_raw = str(r.get("qty_raw", "")).strip()
            tt = str(r.get("time", "")).strip()
            side = r.get("side", None)

            if not price_raw or not qty_raw or not tt:
                continue
            if not _is_valid_time(tt):
                continue

            price = _q8(_to_decimal_num(price_raw))
            qty = _q8(_to_decimal_num(qty_raw))

            if qty <= 0 or not _price_ok(price):
                continue

            rows.append(TradeRow(
                price=price,
                qty=qty,
                trade_time=tt,
                side=side if side in ("buy", "sell") else None,
                price_raw=price_raw,
                qty_raw=qty_raw,
            ))
        except Exception:
            continue

    return rows


# ───────────────────────── API SNIFF (fallback) ─────────────────────────

@dataclass
class SniffState:
    last_hits: List[Dict[str, Any]]
    hit_url: Optional[str]
    seen_urls: Dict[str, int]

def _looks_like_trades_json(obj: Any) -> bool:
    if isinstance(obj, list) and obj and isinstance(obj[0], dict):
        keys = {k.lower() for k in obj[0].keys()}
        return (("price" in keys or "p" in keys) and (("amount" in keys) or ("qty" in keys) or ("volume" in keys) or ("q" in keys))) and (("time" in keys) or ("ts" in keys) or ("timestamp" in keys) or ("t" in keys))
    if isinstance(obj, dict):
        for v in obj.values():
            if _looks_like_trades_json(v):
                return True
    return False

def _extract_trades_from_obj(obj: Any, limit: int) -> List[Dict[str, Any]]:
    def find_list(x: Any) -> Optional[List[dict]]:
        if isinstance(x, list) and x and isinstance(x[0], dict) and _looks_like_trades_json(x):
            return x  # type: ignore
        if isinstance(x, dict):
            for v in x.values():
                res = find_list(v)
                if res is not None:
                    return res
        return None

    lst = find_list(obj)
    if not lst:
        return []

    out: List[Dict[str, Any]] = []
    for it in lst[:limit]:
        if not isinstance(it, dict):
            continue

        price_raw = it.get("price", it.get("p"))
        qty_raw = it.get("amount", it.get("qty", it.get("volume", it.get("q"))))
        t_raw = it.get("time", it.get("ts", it.get("timestamp", it.get("t"))))

        if price_raw is None or qty_raw is None or t_raw is None:
            continue

        try:
            price = _q8(_to_decimal_num(str(price_raw)))
            qty = _q8(_to_decimal_num(str(qty_raw)))
            if qty <= 0 or not _price_ok(price):
                continue
        except Exception:
            continue

        # время: epoch -> HH:MM:SS, иначе пробуем вытащить HH:MM:SS
        trade_time = None
        try:
            if isinstance(t_raw, (int, float)) or (isinstance(t_raw, str) and str(t_raw).isdigit()):
                tv = int(t_raw)
                if tv > 10_000_000_000:  # ms
                    tv = tv // 1000
                trade_time = time.strftime("%H:%M:%S", time.gmtime(tv))
            else:
                trade_time = _normalize_time_to_hhmmss(str(t_raw))
        except Exception:
            trade_time = None

        if not trade_time:
            trade_time = str(t_raw)[:32]

        vol_rub = _q8(price * qty)
        observed_at = datetime.now(timezone.utc).isoformat()

        out.append({
            "source": SOURCE,
            "symbol": SYMBOL,
            "price": str(price),
            "volume_usdt": str(qty),
            "volume_rub": str(vol_rub),
            "trade_time": trade_time,
            "observed_at": observed_at,
        })

    return out

def _safe_task(coro: Any) -> None:
    t = asyncio.create_task(coro)
    def _done(_t: asyncio.Task) -> None:
        try:
            _t.exception()
        except Exception:
            pass
    t.add_done_callback(_done)

async def install_response_sniffer(page: Page, state: SniffState) -> None:
    async def handle_response(resp: Response) -> None:
        try:
            url = resp.url
            ct = (resp.headers.get("content-type") or "").lower()
            if "abcex.io" not in url:
                return
            if "application/json" not in ct and "text/json" not in ct:
                return

            state.seen_urls[url] = state.seen_urls.get(url, 0) + 1

            body = await resp.body()
            if not body or len(body) > 2_000_000:
                return

            try:
                obj = json.loads(body.decode("utf-8", errors="ignore"))
            except Exception:
                return

            if not _looks_like_trades_json(obj):
                return

            rows = _extract_trades_from_obj(obj, limit=LIMIT)
            if rows:
                state.last_hits = rows
                state.hit_url = url
                log.warning("API HIT: trades detected from %s | rows=%d", url, len(rows))
                for i, r in enumerate(rows[:3]):
                    log.warning("API SAMPLE[%d]: %s", i, r)

        except Exception:
            return

    def on_response(resp: Response) -> None:
        _safe_task(handle_response(resp))

    page.on("response", on_response)


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


# ───────────────────────── CORE SCRAPE ─────────────────────────

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

        sniff = SniffState(last_hits=[], hit_url=None, seen_urls={})
        await install_response_sniffer(page, sniff)

        try:
            log.info("Opening ABCEX: %s", ABCEX_URL)
            await _goto_stable(page, ABCEX_URL)
            await accept_cookies_if_any(page)
            await login_if_needed(page, ABCEX_EMAIL, ABCEX_PASSWORD)

            # после логина — вернуться на spot
            await _goto_stable(page, ABCEX_URL)

            try:
                await context.storage_state(path=STATE_PATH)
                log.info("Saved session state to %s", STATE_PATH)
            except Exception as e:
                log.warning("Could not save storage state: %s", e)

            await click_trades_tab_best_effort(page)
            await page.wait_for_timeout(1200)
            await log_diag(page, "after_click_trades_tab")

            # 1) пробуем панельный парсер (как в бек-версии)
            panel = await find_best_trades_panel(page)
            if panel is not None:
                rows_panel = await extract_trades_from_panel(panel, limit=LIMIT)
                if rows_panel:
                    observed_at = datetime.now(timezone.utc).isoformat()
                    out: List[Dict[str, Any]] = []
                    for r in rows_panel:
                        vol_rub = _q8(r.price * r.qty)
                        out.append({
                            "source": SOURCE,
                            "symbol": SYMBOL,
                            "price": str(r.price),
                            "volume_usdt": str(r.qty),
                            "volume_rub": str(vol_rub),
                            "trade_time": r.trade_time,
                            "side": r.side,
                            "observed_at": observed_at,
                        })
                    log.info("Parsed trades from panel: %d", len(out))
                    return out
                else:
                    log.warning("Panel found, but parsed 0 rows.")
            else:
                log.warning("No suitable tabpanel found for trades parsing.")

            # 2) ждём немного: возможно API отдаст trades
            start = time.time()
            while (time.time() - start) * 1000 < WAIT_TRADES_TIMEOUT_MS:
                if sniff.last_hits:
                    log.warning("Using API trades from: %s", sniff.hit_url)
                    return sniff.last_hits
                await page.wait_for_timeout(900)

            # 3) финальная диагностика: топ JSON endpoints
            if sniff.seen_urls:
                top = sorted(sniff.seen_urls.items(), key=lambda x: x[1], reverse=True)[:10]
                log.warning("Top JSON endpoints (count):")
                for u, c in top:
                    log.warning("  %s  (%d)", u, c)

            await save_debug(page, "no_trades")
            raise RuntimeError("Не смог получить сделки: panel parser=0 и API sniff=0.")

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


# ───────────────────────── MAIN LOOP ─────────────────────────

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
                # контрольный лог
                for i, t in enumerate(fresh[:3]):
                    log.info("Fresh trade[%d]: %s", i, t)

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
