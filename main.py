#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ABCEX (abcex.io) spot trades scraper for Render:
- Runtime installs Playwright browsers (chromium + chromium-headless-shell) with cooldown
- Optional outbound proxy for BOTH install downloads and browser traffic
- Robust navigation (commit -> domcontentloaded best-effort), resource blocking
- Login best-effort (if form visible), storage_state persistence
- Parses trades strictly from panel-orderHistory (same DOM logic as your local prototype)
- Upserts to Supabase (kenig_rates by default)

ENV (minimal):
  SUPABASE_URL
  SUPABASE_SERVICE_ROLE_KEY  (or SUPABASE_KEY)

Optional:
  ABCEX_URL (default: https://abcex.io/client/spot/USDTRUB)
  ABCEX_EMAIL / ABCEX_PASSWORD
  PROXY_URL (or HTTP_PROXY/HTTPS_PROXY)
  PLAYWRIGHT_BROWSERS_PATH (default: /opt/render/.cache/ms-playwright)
  STATE_PATH (default: /opt/render/.cache/abcex_state.json)

  SUPABASE_TABLE (default: kenig_rates)
  SUPABASE_ON_CONFLICT (default: source,symbol)
  COL_SOURCE/COL_SYMBOL/COL_RATE/COL_UPDATED_AT/COL_PAYLOAD
"""

import asyncio
import json
import logging
import os
import random
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import unquote, urlparse

import certifi
import httpx
from playwright.async_api import async_playwright, Browser, BrowserContext, Locator, Page

# ───────────────────────── CONFIG ─────────────────────────

ABCEX_URL = os.getenv("ABCEX_URL", "https://abcex.io/client/spot/USDTRUB")
SOURCE = os.getenv("SOURCE", "abcex")
SYMBOL = os.getenv("SYMBOL", "USDT/RUB")

ABCEX_EMAIL = os.getenv("ABCEX_EMAIL", "")
ABCEX_PASSWORD = os.getenv("ABCEX_PASSWORD", "")

MAX_TRADES = int(os.getenv("MAX_TRADES", "30"))
LIMIT = int(os.getenv("LIMIT", str(MAX_TRADES)))  # alias
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "10"))

# Proxy (recommended on Render if ABCEX blocks DC IPs)
PROXY_URL = (os.getenv("PROXY_URL", "") or os.getenv("HTTPS_PROXY", "") or os.getenv("HTTP_PROXY", "")).strip()
USE_PROXY_FOR_BROWSER = os.getenv("USE_PROXY_FOR_BROWSER", "1") == "1"

# Playwright cache (writable on Render)
PLAYWRIGHT_BROWSERS_PATH = os.getenv("PLAYWRIGHT_BROWSERS_PATH", "/opt/render/.cache/ms-playwright").strip()
STATE_PATH = os.getenv("STATE_PATH", "/opt/render/.cache/abcex_state.json").strip()

# Install behavior
SKIP_BROWSER_INSTALL = os.getenv("SKIP_BROWSER_INSTALL", "0") == "1"
INSTALL_COOLDOWN_SECONDS = int(os.getenv("INSTALL_COOLDOWN_SECONDS", "600"))

# Timeouts
NAV_TIMEOUT_MS = int(os.getenv("NAV_TIMEOUT_MS", "120000"))
DOM_WAIT_TIMEOUT_MS = int(os.getenv("DOM_WAIT_TIMEOUT_MS", "45000"))
LOGIN_WAIT_MS = int(os.getenv("LOGIN_WAIT_MS", "30000"))
SCRAPE_TIMEOUT_SECONDS = float(os.getenv("SCRAPE_TIMEOUT_SECONDS", "35"))
RELOAD_EVERY_SECONDS = float(os.getenv("RELOAD_EVERY_SECONDS", "600"))
HEARTBEAT_SECONDS = float(os.getenv("HEARTBEAT_SECONDS", "30"))

# Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "") or os.getenv("SUPABASE_KEY", "")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "kenig_rates")
SUPABASE_ON_CONFLICT = os.getenv("SUPABASE_ON_CONFLICT", "source,symbol")

COL_SOURCE = os.getenv("COL_SOURCE", "source")
COL_SYMBOL = os.getenv("COL_SYMBOL", "symbol")
COL_RATE = os.getenv("COL_RATE", "rate")
COL_UPDATED_AT = os.getenv("COL_UPDATED_AT", "updated_at")
COL_PAYLOAD = os.getenv("COL_PAYLOAD", "payload")  # set to "" if column doesn't exist

# Dedup protection (optional)
SEEN_MAX = int(os.getenv("SEEN_MAX", "20000"))

# ───────────────────────── LOGGING ─────────────────────────

try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    force=True,
)
log = logging.getLogger("abcex")

# ───────────────────────── MODEL ─────────────────────────

@dataclass(frozen=True)
class Trade:
    price: float
    qty: float
    time: str
    side: Optional[str]
    price_raw: str
    qty_raw: str

@dataclass(frozen=True)
class TradeKey:
    source: str
    symbol: str
    time: str
    price: float
    qty: float

# ───────────────────────── UTILS ─────────────────────────

_TIME_RE = re.compile(r"^\d{2}:\d{2}:\d{2}$")

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def _looks_like_time(s: str) -> bool:
    return bool(_TIME_RE.fullmatch((s or "").strip()))

def _normalize_num(text: str) -> float:
    """
    '191 889.47' / '191 889,47' / '79,80' / '1,005.70' -> float
    """
    t = (text or "").strip().replace("\xa0", " ").replace(" ", "")
    if "," in t and "." in t:
        # treat '.' as decimal, ',' as thousands
        t = t.replace(",", "")
    else:
        t = t.replace(",", ".")
    return float(t)

def parse_playwright_proxy(proxy_url: str) -> Optional[Dict[str, str]]:
    """
    Returns dict for Playwright:
      {"server": "http://host:port", "username": "...", "password": "..."}
    """
    if not proxy_url:
        return None
    u = urlparse(proxy_url)
    if not u.scheme:
        u = urlparse("http://" + proxy_url)
    if not u.hostname or not u.port:
        return None
    out: Dict[str, str] = {"server": f"{u.scheme}://{u.hostname}:{u.port}"}
    if u.username:
        out["username"] = unquote(u.username)
    if u.password:
        out["password"] = unquote(u.password)
    return out

async def save_debug(page: Page, html_path: str, png_path: str) -> None:
    # On Render, write into /tmp to avoid perms issues
    if not html_path.startswith("/"):
        html_path = "/tmp/" + html_path
    if not png_path.startswith("/"):
        png_path = "/tmp/" + png_path

    try:
        await page.screenshot(path=png_path, full_page=True)
        log.info("Saved debug screenshot: %s", png_path)
    except Exception as e:
        log.info("Could not save screenshot: %s", e)

    try:
        content = await page.content()
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(content)
        log.info("Saved debug html: %s", html_path)
    except Exception as e:
        log.info("Could not save html: %s", e)

def _ensure_parent_dir(path: str) -> None:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
    except Exception:
        pass

# ───────────────────────── PLAYWRIGHT INSTALL ─────────────────────────

_last_install_ts = 0.0

def ensure_playwright_browsers_runtime() -> None:
    """
    Runtime installation of chromium + chromium-headless-shell.
    Uses proxies (HTTP_PROXY/HTTPS_PROXY) if present. Cooldown prevents loops.
    """
    global _last_install_ts
    if SKIP_BROWSER_INSTALL:
        return

    now = time.time()
    if now - _last_install_ts < INSTALL_COOLDOWN_SECONDS:
        log.warning("Playwright install attempted recently; cooldown active, skipping.")
        return
    _last_install_ts = now

    env = dict(os.environ)
    env["PLAYWRIGHT_BROWSERS_PATH"] = PLAYWRIGHT_BROWSERS_PATH
    env.setdefault("PYTHONUNBUFFERED", "1")

    if PROXY_URL:
        env["HTTP_PROXY"] = PROXY_URL
        env["HTTPS_PROXY"] = PROXY_URL
        env["http_proxy"] = PROXY_URL
        env["https_proxy"] = PROXY_URL

    log.warning("Installing Playwright browsers (runtime)...")
    r = subprocess.run(
        [sys.executable, "-m", "playwright", "install", "chromium", "chromium-headless-shell"],
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        log.error("playwright install failed (%s)\nSTDOUT:\n%s\nSTDERR:\n%s", r.returncode, r.stdout[-4000:], r.stderr[-4000:])
    else:
        log.info("Playwright browsers installed.")

def _should_force_install(err: Exception) -> bool:
    s = str(err)
    return (
        "Executable doesn't exist" in s
        or "playwright install" in s
        or "chromium_headless_shell" in s
        or ("ms-playwright" in s and "doesn't exist" in s)
    )

# ───────────────────────── COOKIES / LOGIN ─────────────────────────

async def accept_cookies_if_any(page: Page) -> None:
    candidates = ["Принять", "Согласен", "Я согласен", "Accept", "I agree"]
    for txt in candidates:
        try:
            btn = page.locator(f"text={txt}")
            if await btn.count() > 0 and await btn.first.is_visible():
                await btn.first.click(timeout=5_000)
                log.info("Cookies banner handled via: text=%s", txt)
                await page.wait_for_timeout(800)
                return
        except Exception:
            pass

async def _is_login_visible(page: Page) -> bool:
    try:
        pw = page.locator("input[type='password']")
        if await pw.count() > 0 and await pw.first.is_visible():
            return True
    except Exception:
        pass
    return False

async def login_if_needed(page: Page, email: str, password: str) -> None:
    """
    Same logic as your local prototype, but without prompting.
    """
    if not await _is_login_visible(page):
        log.info("Login not required (already in session).")
        return

    if not email or not password:
        await save_debug(page, "abcex_need_credentials.html", "abcex_need_credentials.png")
        raise RuntimeError("Login required, but ABCEX_EMAIL/ABCEX_PASSWORD not set. See /tmp/abcex_need_credentials.*")

    log.info("Login detected. Performing sign-in ...")

    email_candidates = [
        "input[type='email']",
        "input[name='email']",
        "input[placeholder*='mail' i]",
        "input[placeholder*='Email' i]",
        "input[placeholder*='Почта' i]",
        "input[placeholder*='E-mail' i]",
    ]
    pw_candidates = [
        "input[type='password']",
        "input[name='password']",
        "input[placeholder*='Пароль' i]",
        "input[placeholder*='Password' i]",
    ]

    email_filled = False
    for sel in email_candidates:
        loc = page.locator(sel)
        try:
            if await loc.count() > 0 and await loc.first.is_visible():
                await loc.first.fill(email, timeout=10_000)
                email_filled = True
                break
        except Exception:
            continue

    pw_filled = False
    for sel in pw_candidates:
        loc = page.locator(sel)
        try:
            if await loc.count() > 0 and await loc.first.is_visible():
                await loc.first.fill(password, timeout=10_000)
                pw_filled = True
                break
        except Exception:
            continue

    if not email_filled or not pw_filled:
        await save_debug(page, "abcex_login_fields_not_found.html", "abcex_login_fields_not_found.png")
        raise RuntimeError("Could not find email/password fields. See /tmp/abcex_login_fields_not_found.*")

    btn_texts = ["Войти", "Вход", "Sign in", "Login", "Войти в аккаунт"]
    clicked = False
    for t in btn_texts:
        try:
            btn = page.locator(f"button:has-text('{t}')")
            if await btn.count() > 0 and await btn.first.is_visible():
                await btn.first.click(timeout=10_000)
                clicked = True
                break
        except Exception:
            continue

    if not clicked:
        try:
            await page.keyboard.press("Enter")
        except Exception:
            pass

    # Wait for transition
    try:
        await page.wait_for_timeout(1_000)
        await page.wait_for_load_state("networkidle", timeout=LOGIN_WAIT_MS)
    except Exception:
        pass

    await page.wait_for_timeout(2_500)

    if await _is_login_visible(page):
        await save_debug(page, "abcex_login_failed.html", "abcex_login_failed.png")
        raise RuntimeError("Login did not complete (form still visible). See /tmp/abcex_login_failed.*")

    log.info("Login successful (form disappeared).")

# ───────────────────────── TRADES TAB / PANEL ─────────────────────────

async def click_trades_tab_best_effort(page: Page) -> None:
    candidates = ["Сделки", "История", "Order history", "Trades"]
    for t in candidates:
        try:
            tab = page.locator(f"[role='tab']:has-text('{t}')")
            if await tab.count() > 0 and await tab.first.is_visible():
                await tab.first.click(timeout=8_000)
                await page.wait_for_timeout(800)
                log.info("Clicked tab: %s", t)
                return
        except Exception:
            continue

    for t in candidates:
        try:
            tab = page.locator(f"text={t}")
            if await tab.count() > 0 and await tab.first.is_visible():
                await tab.first.click(timeout=8_000)
                await page.wait_for_timeout(800)
                log.info("Clicked text tab: %s", t)
                return
        except Exception:
            continue

async def get_order_history_panel(page: Page) -> Locator:
    panel = page.locator("div[role='tabpanel'][id*='panel-orderHistory']")
    cnt = await panel.count()
    if cnt == 0:
        await save_debug(page, "abcex_no_panel.html", "abcex_no_panel.png")
        raise RuntimeError("panel-orderHistory not found. See /tmp/abcex_no_panel.*")

    for i in range(cnt):
        p = panel.nth(i)
        try:
            if await p.is_visible():
                return p
        except Exception:
            continue

    await save_debug(page, "abcex_no_visible_panel.html", "abcex_no_visible_panel.png")
    raise RuntimeError("panel-orderHistory found but none visible. See /tmp/abcex_no_visible_panel.*")

async def wait_trades_visible(page: Page, timeout_ms: int = 25_000) -> None:
    start = time.monotonic()
    while (time.monotonic() - start) * 1000 < timeout_ms:
        try:
            ok = await page.evaluate(
                """() => {
                    const re = /^\\d{2}:\\d{2}:\\d{2}$/;
                    const ps = Array.from(document.querySelectorAll('p'));
                    return ps.some(p => re.test((p.textContent||'').trim()));
                }"""
            )
            if ok:
                log.info("Trades look visible (time cells detected).")
                return
        except Exception:
            pass
        await page.wait_for_timeout(800)

    await save_debug(page, "abcex_trades_not_visible.html", "abcex_trades_not_visible.png")
    raise RuntimeError("Trades did not become visible (HH:MM:SS). See /tmp/abcex_trades_not_visible.*")

# ───────────────────────── PARSING ─────────────────────────

async def extract_trades_from_panel(panel: Locator, limit: int = MAX_TRADES) -> List[Trade]:
    """
    EXACTLY the same DOM logic as your local prototype (JS evaluate on panel root):
    - find divs with 3 direct p children: [price, qty, time]
    - time is HH:MM:SS
    - infer side by inline style containing 'green'/'red' (best-effort)
    """
    handle = await panel.element_handle()
    if handle is None:
        raise RuntimeError("Could not get element_handle for panel-orderHistory")

    raw_rows: List[Dict[str, Any]] = await handle.evaluate(
        """(root, limit) => {
          const isTime = (s) => /^\\d{2}:\\d{2}:\\d{2}$/.test((s||'').trim());
          const isNum = (s) => /^[0-9][0-9\\s\\u00A0.,]*$/.test((s||'').trim());

          const out = [];
          const divs = Array.from(root.querySelectorAll('div'));

          for (const g of divs) {
            const ps = Array.from(g.querySelectorAll(':scope > p'));
            if (ps.length < 3) continue;

            const t0 = (ps[0].textContent || '').trim();
            const t1 = (ps[1].textContent || '').trim();
            const t2 = (ps[2].textContent || '').trim();

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
        }""",
        limit,
    )

    trades: List[Trade] = []
    for r in raw_rows:
        try:
            price_raw = str(r.get("price_raw", "")).strip()
            qty_raw = str(r.get("qty_raw", "")).strip()
            time_txt = str(r.get("time", "")).strip()
            side = r.get("side", None)

            if not price_raw or not qty_raw or not time_txt:
                continue
            if not _looks_like_time(time_txt):
                continue

            price = _normalize_num(price_raw)
            qty = _normalize_num(qty_raw)

            trades.append(
                Trade(
                    price=price,
                    qty=qty,
                    time=time_txt,
                    side=side if side in ("buy", "sell") else None,
                    price_raw=price_raw,
                    qty_raw=qty_raw,
                )
            )
        except Exception:
            continue

    return trades

def compute_metrics(trades: List[Trade]) -> Dict[str, Any]:
    if not trades:
        return {"count": 0, "sum_qty_usdt": 0.0, "turnover_rub": 0.0, "vwap": None, "last_price": None}

    sum_qty = 0.0
    turnover = 0.0
    for t in trades:
        sum_qty += t.qty
        turnover += t.qty * t.price
    vwap = (turnover / sum_qty) if sum_qty > 0 else None

    # UI обычно newest-first; если нет — это всё равно “последняя увиденная”
    last_price = trades[0].price

    return {"count": len(trades), "sum_qty_usdt": sum_qty, "turnover_rub": turnover, "vwap": vwap, "last_price": last_price}

def trade_key(t: Trade) -> TradeKey:
    return TradeKey(source=SOURCE, symbol=SYMBOL, time=t.time, price=t.price, qty=t.qty)

# ───────────────────────── SUPABASE ─────────────────────────

def supabase_headers() -> Dict[str, str]:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY not set")
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }

def build_supabase_row(payload: Dict[str, Any]) -> Dict[str, Any]:
    metrics = payload.get("metrics") or {}
    rate = metrics.get("last_price") or metrics.get("vwap")

    row: Dict[str, Any] = {
        COL_SOURCE: payload.get("exchange", SOURCE),
        COL_SYMBOL: payload.get("symbol", SYMBOL),
        COL_RATE: rate,
        COL_UPDATED_AT: payload.get("fetched_at"),
    }
    if COL_PAYLOAD:
        row[COL_PAYLOAD] = payload
    return row

async def upsert_to_supabase(client: httpx.AsyncClient, row: Dict[str, Any]) -> None:
    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
    params = {"on_conflict": SUPABASE_ON_CONFLICT}
    r = await client.post(url, headers=supabase_headers(), params=params, json=[row])
    if r.status_code >= 300:
        raise RuntimeError(f"Supabase upsert failed: HTTP {r.status_code} | {r.text[:800]}")
    log.info("Supabase upsert OK | %s=%s | %s=%s | %s=%s",
             COL_SOURCE, row.get(COL_SOURCE),
             COL_SYMBOL, row.get(COL_SYMBOL),
             COL_RATE, row.get(COL_RATE))

# ───────────────────────── BROWSER SESSION ─────────────────────────

async def open_browser(pw) -> Tuple[Browser, BrowserContext, Page]:
    ensure_playwright_browsers_runtime()

    proxy_cfg = parse_playwright_proxy(PROXY_URL) if (USE_PROXY_FOR_BROWSER and PROXY_URL) else None

    launch_kwargs: Dict[str, Any] = {
        "headless": True,
        "args": [
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
        ],
    }
    if proxy_cfg:
        launch_kwargs["proxy"] = proxy_cfg

    browser: Browser = await pw.chromium.launch(**launch_kwargs)

    storage_state = STATE_PATH if os.path.exists(STATE_PATH) else None
    if storage_state:
        log.info("Using saved session state: %s", storage_state)

    context: BrowserContext = await browser.new_context(
        viewport={"width": 1440, "height": 810},
        locale="ru-RU",
        timezone_id="Europe/Moscow",
        user_agent=(
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        storage_state=storage_state,
    )

    page: Page = await context.new_page()
    page.set_default_timeout(15_000)

    # Block heavy resources to reduce timeouts
    async def _route(route, request):
        rtype = request.resource_type
        if rtype in ("image", "media", "font"):
            await route.abort()
        else:
            await route.continue_()

    try:
        await page.route("**/*", _route)
    except Exception:
        pass

    return browser, context, page

async def safe_close(browser: Optional[Browser], context: Optional[BrowserContext], page: Optional[Page]) -> None:
    try:
        if page:
            await page.close()
    except Exception:
        pass
    try:
        if context:
            await context.close()
    except Exception:
        pass
    try:
        if browser:
            await browser.close()
    except Exception:
        pass

async def goto_abcex(page: Page) -> None:
    """
    Render-safe navigation: first wait_until=commit (fast), then best-effort domcontentloaded.
    This avoids long hangs on SPA resources while still letting us wait for the UI ourselves.
    """
    log.info("Opening ABCEX: %s", ABCEX_URL)
    await page.goto(ABCEX_URL, wait_until="commit", timeout=NAV_TIMEOUT_MS)

    try:
        await page.wait_for_load_state("domcontentloaded", timeout=DOM_WAIT_TIMEOUT_MS)
    except Exception:
        log.warning("domcontentloaded not reached in time; continuing with UI waits...")

    await page.wait_for_timeout(2000)
    await accept_cookies_if_any(page)

    # If login form is visible, log in
    if await _is_login_visible(page):
        await login_if_needed(page, email=ABCEX_EMAIL, password=ABCEX_PASSWORD)

    # Save state (best-effort)
    try:
        _ensure_parent_dir(STATE_PATH)
        await page.context.storage_state(path=STATE_PATH)
        log.info("Saved session state to %s", STATE_PATH)
    except Exception as e:
        log.info("Could not save storage state: %s", e)

    await click_trades_tab_best_effort(page)
    await wait_trades_visible(page)

# ───────────────────────── SCRAPE WINDOW ─────────────────────────

async def scrape_once(page: Page) -> Dict[str, Any]:
    panel = await get_order_history_panel(page)
    trades = await extract_trades_from_panel(panel, limit=LIMIT)

    if not trades:
        await save_debug(page, "abcex_debug_no_trades_parsed.html", "abcex_debug_no_trades_parsed.png")
        raise RuntimeError("panel-orderHistory found but no trades parsed. See /tmp/abcex_debug_no_trades_parsed.*")

    metrics = compute_metrics(trades)

    return {
        "exchange": SOURCE,
        "symbol": SYMBOL,
        "url": ABCEX_URL,
        "fetched_at": now_utc().isoformat(),
        "metrics": metrics,
        "trades": [
            {
                "price": t.price,
                "qty": t.qty,
                "time": t.time,
                "side": t.side,
                "price_raw": t.price_raw,
                "qty_raw": t.qty_raw,
            }
            for t in trades
        ],
    }

# ───────────────────────── WORKER LOOP ─────────────────────────

async def worker() -> None:
    seen: Set[TradeKey] = set()
    seen_q: List[TradeKey] = []

    backoff = 2.0
    last_heartbeat = time.monotonic()
    last_reload = time.monotonic()

    async with httpx.AsyncClient(timeout=30, verify=certifi.where()) as sb:
        async with async_playwright() as pw:
            browser: Optional[Browser] = None
            context: Optional[BrowserContext] = None
            page: Optional[Page] = None

            while True:
                try:
                    if page is None:
                        log.info("Starting browser session...")
                        try:
                            browser, context, page = await open_browser(pw)
                        except Exception as e:
                            if _should_force_install(e):
                                ensure_playwright_browsers_runtime()
                                browser, context, page = await open_browser(pw)
                            else:
                                raise

                        await goto_abcex(page)
                        backoff = 2.0
                        last_reload = time.monotonic()
                        last_heartbeat = time.monotonic()

                    # Heartbeat
                    if time.monotonic() - last_heartbeat >= HEARTBEAT_SECONDS:
                        log.info("Heartbeat: alive. seen=%d", len(seen))
                        last_heartbeat = time.monotonic()

                    # Maintenance reload
                    if time.monotonic() - last_reload >= RELOAD_EVERY_SECONDS:
                        log.warning("Maintenance reload...")
                        await goto_abcex(page)
                        last_reload = time.monotonic()

                    # Scrape with a hard timeout to avoid silent hangs
                    payload = await asyncio.wait_for(scrape_once(page), timeout=SCRAPE_TIMEOUT_SECONDS)

                    # Optional dedup: keep only NEW trades in payload["trades"] (based on time+price+qty)
                    trades: List[Trade] = []
                    for t in payload.get("trades", []):
                        try:
                            trades.append(
                                Trade(
                                    price=float(t["price"]),
                                    qty=float(t["qty"]),
                                    time=str(t["time"]),
                                    side=t.get("side"),
                                    price_raw=str(t.get("price_raw", "")),
                                    qty_raw=str(t.get("qty_raw", "")),
                                )
                            )
                        except Exception:
                            continue

                    new_trades_out: List[Dict[str, Any]] = []
                    for t in trades:
                        k = trade_key(t)
                        if k in seen:
                            continue
                        new_trades_out.append(
                            {
                                "price": t.price,
                                "qty": t.qty,
                                "time": t.time,
                                "side": t.side,
                                "price_raw": t.price_raw,
                                "qty_raw": t.qty_raw,
                            }
                        )
                        seen.add(k)
                        seen_q.append(k)
                        if len(seen_q) > SEEN_MAX:
                            old = seen_q.pop(0)
                            seen.discard(old)

                    if new_trades_out:
                        payload["trades_new"] = new_trades_out
                        payload["metrics"]["new_count"] = len(new_trades_out)
                        payload["metrics"]["last_price"] = new_trades_out[0]["price"]

                    # Upsert always (even if no new trades) — keep rate fresh
                    row = build_supabase_row(payload)

                    log.info("Scraped metrics: %s", json.dumps(payload.get("metrics", {}), ensure_ascii=False))
                    await upsert_to_supabase(sb, row)

                    sleep_s = max(0.5, POLL_SECONDS + random.uniform(-0.2, 0.2))
                    await asyncio.sleep(sleep_s)

                except asyncio.TimeoutError:
                    log.error("Timeout: scrape exceeded %.1fs. Restarting browser session...", SCRAPE_TIMEOUT_SECONDS)
                    await safe_close(browser, context, page)
                    browser = context = page = None

                except Exception as e:
                    log.error("Worker error: %s", e)

                    if _should_force_install(e):
                        ensure_playwright_browsers_runtime()

                    log.info("Retrying after %.1fs ...", backoff)
                    await asyncio.sleep(backoff)
                    backoff = min(60.0, backoff * 2)

                    await safe_close(browser, context, page)
                    browser = context = page = None

# ───────────────────────── ENTRYPOINT ─────────────────────────

def main() -> None:
    log.info(
        "Starting ABCEX worker | url=%s | poll=%.1fs | proxy=%s | state=%s",
        ABCEX_URL,
        POLL_SECONDS,
        ("on" if (USE_PROXY_FOR_BROWSER and PROXY_URL) else "off"),
        STATE_PATH,
    )
    asyncio.run(worker())

if __name__ == "__main__":
    main()
