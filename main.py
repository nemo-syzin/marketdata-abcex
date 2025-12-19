#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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

ABCEX_URL = os.getenv("ABCEX_URL", "https://abcex.io/client/spot/USDTRUB").strip()
SOURCE = os.getenv("SOURCE", "abcex").strip()
SYMBOL = os.getenv("SYMBOL", "USDT/RUB").strip()

ABCEX_EMAIL = os.getenv("ABCEX_EMAIL", "").strip()
ABCEX_PASSWORD = os.getenv("ABCEX_PASSWORD", "").strip()

POLL_SECONDS = float(os.getenv("POLL_SECONDS", "10"))
MAX_TRADES = int(os.getenv("MAX_TRADES", "20"))

# Proxy (highly recommended on Render)
PROXY_URL = (os.getenv("PROXY_URL", "") or os.getenv("HTTPS_PROXY", "") or os.getenv("HTTP_PROXY", "")).strip()
USE_PROXY_FOR_BROWSER = os.getenv("USE_PROXY_FOR_BROWSER", "1").strip() == "1"

# Playwright cache path (writable)
PLAYWRIGHT_BROWSERS_PATH = os.getenv("PLAYWRIGHT_BROWSERS_PATH", "/opt/render/.cache/ms-playwright").strip()
STATE_PATH = os.getenv("STATE_PATH", "/opt/render/.cache/abcex_state.json").strip()

# Make sure Playwright itself uses the same cache path
os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", PLAYWRIGHT_BROWSERS_PATH)

# Install tuning
PW_MAX_ATTEMPTS = int(os.getenv("PW_MAX_ATTEMPTS", "8"))
PW_BACKOFF_BASE = float(os.getenv("PW_BACKOFF_BASE", "3"))
PW_COOLDOWN_SECONDS = int(os.getenv("PW_COOLDOWN_SECONDS", "600"))
PW_INSTALL_TIMEOUT_SEC = int(os.getenv("PW_INSTALL_TIMEOUT_SEC", str(20 * 60)))

# Navigation tuning
NAV_TIMEOUT_MS = int(os.getenv("NAV_TIMEOUT_MS", "120000"))          # goto(commit)
DOM_WAIT_TIMEOUT_MS = int(os.getenv("DOM_WAIT_TIMEOUT_MS", "45000")) # wait domcontentloaded best-effort

# Scrape safety
SCRAPE_HARD_TIMEOUT_SEC = float(os.getenv("SCRAPE_HARD_TIMEOUT_SEC", "45"))

# Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "") or os.getenv("SUPABASE_KEY", "")

SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "kenig_rates")
SUPABASE_ON_CONFLICT = os.getenv("SUPABASE_ON_CONFLICT", "source,symbol")

COL_SOURCE = os.getenv("COL_SOURCE", "source")
COL_SYMBOL = os.getenv("COL_SYMBOL", "symbol")
COL_RATE = os.getenv("COL_RATE", "rate")
COL_UPDATED_AT = os.getenv("COL_UPDATED_AT", "updated_at")
COL_PAYLOAD = os.getenv("COL_PAYLOAD", "payload")  # set "" if you don't have jsonb column

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
    time: str
    price: float
    qty: float

_TIME_RE = re.compile(r"^\d{2}:\d{2}:\d{2}$")

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _looks_like_time(s: str) -> bool:
    return bool(_TIME_RE.fullmatch((s or "").strip()))

def _normalize_num(text: str) -> float:
    t = (text or "").strip().replace("\xa0", " ").replace(" ", "")
    if "," in t and "." in t:
        t = t.replace(",", "")
    else:
        t = t.replace(",", ".")
    return float(t)

def _ensure_parent_dir(path: str) -> None:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
    except Exception:
        pass

def parse_playwright_proxy(proxy_url: str) -> Optional[Dict[str, str]]:
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

# ───────────────────────── DEBUG DUMPS ─────────────────────────

async def save_debug(page: Page, name: str) -> None:
    # render-safe: write into /tmp
    try:
        await page.screenshot(path=f"/tmp/{name}.png", full_page=True)
        log.info("Saved screenshot: /tmp/%s.png", name)
    except Exception as e:
        log.info("Could not screenshot: %s", e)
    try:
        html = await page.content()
        with open(f"/tmp/{name}.html", "w", encoding="utf-8") as f:
            f.write(html)
        log.info("Saved html: /tmp/%s.html", name)
    except Exception as e:
        log.info("Could not save html: %s", e)

# ───────────────────────── PLAYWRIGHT INSTALL ─────────────────────────

_last_install_ts = 0.0

def _headless_shell_exists() -> bool:
    """
    If the chromium-headless-shell folder exists, Playwright can run headless reliably on Linux.
    """
    base = PLAYWRIGHT_BROWSERS_PATH
    try:
        if not os.path.isdir(base):
            return False
        # example: chromium_headless_shell-1148/...
        for n in os.listdir(base):
            if n.startswith("chromium_headless_shell-"):
                return True
    except Exception:
        return False
    return False

def ensure_playwright_browsers_runtime(force: bool = False) -> None:
    """
    Critical: if install fails, we must NOT lock ourselves with cooldown when binary is missing.
    """
    global _last_install_ts

    if not force:
        # If already present — do nothing
        if _headless_shell_exists():
            return

        # Cooldown only if we recently tried AND we still have nothing to do
        now = time.time()
        if now - _last_install_ts < PW_COOLDOWN_SECONDS:
            log.warning("Playwright install attempted recently; cooldown active, skipping.")
            return

    _last_install_ts = time.time()

    env = dict(os.environ)
    env["PLAYWRIGHT_BROWSERS_PATH"] = PLAYWRIGHT_BROWSERS_PATH
    env.setdefault("PYTHONUNBUFFERED", "1")

    # IMPORTANT: Proxy for downloads
    if PROXY_URL:
        env["HTTP_PROXY"] = PROXY_URL
        env["HTTPS_PROXY"] = PROXY_URL
        env["http_proxy"] = PROXY_URL
        env["https_proxy"] = PROXY_URL

    cmd = [sys.executable, "-m", "playwright", "install", "chromium", "chromium-headless-shell"]

    for attempt in range(1, PW_MAX_ATTEMPTS + 1):
        log.warning("Installing Playwright browsers... attempt %d/%d | proxy=%s | path=%s",
                    attempt, PW_MAX_ATTEMPTS, ("on" if PROXY_URL else "off"), PLAYWRIGHT_BROWSERS_PATH)
        try:
            r = subprocess.run(
                cmd,
                env=env,
                check=False,
                capture_output=True,
                text=True,
                timeout=PW_INSTALL_TIMEOUT_SEC,
            )
            if r.returncode == 0 and _headless_shell_exists():
                log.info("Playwright browsers installed and verified.")
                return

            log.error("playwright install failed (%s)\nSTDOUT:\n%s\nSTDERR:\n%s",
                      r.returncode, (r.stdout or "")[-4000:], (r.stderr or "")[-4000:])
        except Exception as e:
            log.error("playwright install exception: %s", e)

        # Backoff
        backoff = min(120.0, PW_BACKOFF_BASE * (2 ** (attempt - 1))) + random.uniform(0, 1.5)
        log.info("Retrying install after %.1fs ...", backoff)
        time.sleep(backoff)

    raise RuntimeError("Failed to install Playwright browsers after retries")

def _should_force_install(err: Exception) -> bool:
    s = str(err)
    return (
        "Executable doesn't exist" in s
        or "playwright install" in s
        or "chromium_headless_shell" in s
        or ("ms-playwright" in s and "doesn't exist" in s)
    )

# ───────────────────────── SITE HELPERS ─────────────────────────

async def accept_cookies_if_any(page: Page) -> None:
    for txt in ("Принять", "Согласен", "Я согласен", "Accept", "I agree"):
        try:
            btn = page.locator(f"text={txt}")
            if await btn.count() > 0 and await btn.first.is_visible():
                await btn.first.click(timeout=5_000)
                await page.wait_for_timeout(800)
                log.info("Cookies accepted via: %s", txt)
                return
        except Exception:
            pass

async def _is_login_visible(page: Page) -> bool:
    try:
        pw = page.locator("input[type='password']")
        return (await pw.count() > 0) and (await pw.first.is_visible())
    except Exception:
        return False

async def login_if_needed(page: Page) -> None:
    if not await _is_login_visible(page):
        return

    if not ABCEX_EMAIL or not ABCEX_PASSWORD:
        await save_debug(page, "abcex_need_credentials")
        raise RuntimeError("Login required but ABCEX_EMAIL/ABCEX_PASSWORD not set")

    log.info("Login detected. Signing in...")

    email_selectors = [
        "input[type='email']",
        "input[name='email']",
        "input[placeholder*='mail' i]",
        "input[placeholder*='Email' i]",
        "input[placeholder*='Почта' i]",
    ]
    pw_selectors = [
        "input[type='password']",
        "input[name='password']",
        "input[placeholder*='Пароль' i]",
        "input[placeholder*='Password' i]",
    ]

    email_ok = False
    for sel in email_selectors:
        loc = page.locator(sel)
        try:
            if await loc.count() > 0 and await loc.first.is_visible():
                await loc.first.fill(ABCEX_EMAIL, timeout=10_000)
                email_ok = True
                break
        except Exception:
            continue

    pw_ok = False
    for sel in pw_selectors:
        loc = page.locator(sel)
        try:
            if await loc.count() > 0 and await loc.first.is_visible():
                await loc.first.fill(ABCEX_PASSWORD, timeout=10_000)
                pw_ok = True
                break
        except Exception:
            continue

    if not email_ok or not pw_ok:
        await save_debug(page, "abcex_login_fields_not_found")
        raise RuntimeError("Cannot find login fields")

    clicked = False
    for t in ("Войти", "Sign in", "Login", "Вход"):
        try:
            btn = page.locator(f"button:has-text('{t}')")
            if await btn.count() > 0 and await btn.first.is_visible():
                await btn.first.click(timeout=10_000)
                clicked = True
                break
        except Exception:
            pass
    if not clicked:
        try:
            await page.keyboard.press("Enter")
        except Exception:
            pass

    await page.wait_for_timeout(2500)
    if await _is_login_visible(page):
        await save_debug(page, "abcex_login_failed")
        raise RuntimeError("Login did not complete (form still visible)")

    log.info("Login OK.")

async def click_trades_tab_best_effort(page: Page) -> None:
    for t in ("Сделки", "История", "Order history", "Trades"):
        try:
            tab = page.locator(f"[role='tab']:has-text('{t}')")
            if await tab.count() > 0 and await tab.first.is_visible():
                await tab.first.click(timeout=8_000)
                await page.wait_for_timeout(800)
                return
        except Exception:
            pass

async def wait_trades_visible(page: Page, timeout_ms: int = 25_000) -> None:
    start = time.monotonic()
    while (time.monotonic() - start) * 1000 < timeout_ms:
        try:
            ok = await page.evaluate(
                """() => {
                    const re = /^\\d{2}:\\d{2}:\\d{2}$/;
                    return Array.from(document.querySelectorAll('p'))
                      .some(p => re.test((p.textContent||'').trim()));
                }"""
            )
            if ok:
                return
        except Exception:
            pass
        await page.wait_for_timeout(800)

    await save_debug(page, "abcex_trades_not_visible")
    raise RuntimeError("Trades not visible (HH:MM:SS not found)")

async def get_order_history_panel(page: Page) -> Locator:
    panel = page.locator("div[role='tabpanel'][id*='panel-orderHistory']")
    if await panel.count() == 0:
        await save_debug(page, "abcex_no_panel")
        raise RuntimeError("panel-orderHistory not found")
    # return first visible
    for i in range(await panel.count()):
        p = panel.nth(i)
        try:
            if await p.is_visible():
                return p
        except Exception:
            pass
    await save_debug(page, "abcex_no_visible_panel")
    raise RuntimeError("panel-orderHistory found but none visible")

async def extract_trades_from_panel(panel: Locator, limit: int) -> List[Trade]:
    handle = await panel.element_handle()
    if handle is None:
        raise RuntimeError("Cannot get element_handle(panel)")

    raw_rows: List[Dict[str, Any]] = await handle.evaluate(
        """(root, limit) => {
          const isTime = (s) => /^\\d{2}:\\d{2}:\\d{2}$/.test((s||'').trim());
          const isNum  = (s) => /^[0-9][0-9\\s\\u00A0.,]*$/.test((s||'').trim());

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
        }""",
        limit,
    )

    out: List[Trade] = []
    for r in raw_rows:
        try:
            pr = str(r.get("price_raw", "")).strip()
            qr = str(r.get("qty_raw", "")).strip()
            tt = str(r.get("time", "")).strip()
            if not pr or not qr or not _looks_like_time(tt):
                continue
            out.append(
                Trade(
                    price=_normalize_num(pr),
                    qty=_normalize_num(qr),
                    time=tt,
                    side=(r.get("side") if r.get("side") in ("buy", "sell") else None),
                    price_raw=pr,
                    qty_raw=qr,
                )
            )
        except Exception:
            continue
    return out

def compute_metrics(trades: List[Trade]) -> Dict[str, Any]:
    if not trades:
        return {"count": 0, "vwap": None, "last_price": None, "sum_qty_usdt": 0.0, "turnover_rub": 0.0}
    sum_qty = sum(t.qty for t in trades)
    turnover = sum(t.qty * t.price for t in trades)
    vwap = (turnover / sum_qty) if sum_qty > 0 else None
    return {
        "count": len(trades),
        "vwap": vwap,
        "last_price": trades[0].price,
        "sum_qty_usdt": sum_qty,
        "turnover_rub": turnover,
    }

# ───────────────────────── SUPABASE ─────────────────────────

def supabase_headers() -> Dict[str, str]:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("SUPABASE_URL/SUPABASE_SERVICE_ROLE_KEY not set")
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
    log.info("Supabase upsert OK | rate=%s", row.get(COL_RATE))

# ───────────────────────── BROWSER / NAVIGATION ─────────────────────────

async def open_browser(pw) -> Tuple[Browser, BrowserContext, Page]:
    # Install browsers if missing
    ensure_playwright_browsers_runtime(force=not _headless_shell_exists())

    proxy_cfg = parse_playwright_proxy(PROXY_URL) if (USE_PROXY_FOR_BROWSER and PROXY_URL) else None

    launch_kwargs: Dict[str, Any] = {
        "headless": True,
        "args": ["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu", "--disable-blink-features=AutomationControlled"],
    }
    if proxy_cfg:
        launch_kwargs["proxy"] = proxy_cfg

    browser = await pw.chromium.launch(**launch_kwargs)

    storage_state = STATE_PATH if os.path.exists(STATE_PATH) else None
    context = await browser.new_context(
        viewport={"width": 1440, "height": 810},
        locale="ru-RU",
        timezone_id="Europe/Moscow",
        user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        storage_state=storage_state,
    )
    page = await context.new_page()
    page.set_default_timeout(15_000)

    # block heavy resources
    async def _route(route, request):
        if request.resource_type in ("image", "media", "font"):
            await route.abort()
        else:
            await route.continue_()
    try:
        await page.route("**/*", _route)
    except Exception:
        pass

    return browser, context, page

async def safe_close(browser: Optional[Browser], context: Optional[BrowserContext], page: Optional[Page]) -> None:
    for obj, fn in ((page, "close"), (context, "close"), (browser, "close")):
        try:
            if obj:
                await getattr(obj, fn)()
        except Exception:
            pass

async def goto_abcex(page: Page) -> None:
    log.info("NAVIGATE | url=%s | proxy(browser)=%s", ABCEX_URL, ("on" if (USE_PROXY_FOR_BROWSER and PROXY_URL) else "off"))

    # ключевое: НЕ ждём domcontentloaded в goto (это у тебя и падало)
    await page.goto(ABCEX_URL, wait_until="commit", timeout=NAV_TIMEOUT_MS)

    # best-effort domcontentloaded
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=DOM_WAIT_TIMEOUT_MS)
    except Exception:
        log.warning("domcontentloaded not reached in time; continuing...")

    await page.wait_for_timeout(2000)
    await accept_cookies_if_any(page)
    await login_if_needed(page)

    # save session
    try:
        _ensure_parent_dir(STATE_PATH)
        await page.context.storage_state(path=STATE_PATH)
        log.info("Saved session state: %s", STATE_PATH)
    except Exception as e:
        log.info("Could not save storage state: %s", e)

    await click_trades_tab_best_effort(page)
    await wait_trades_visible(page)

async def scrape_once(page: Page) -> Dict[str, Any]:
    panel = await get_order_history_panel(page)
    trades = await extract_trades_from_panel(panel, limit=MAX_TRADES)
    if not trades:
        await save_debug(page, "abcex_no_trades_parsed")
        raise RuntimeError("Trades not parsed from panel-orderHistory")

    metrics = compute_metrics(trades)
    return {
        "exchange": SOURCE,
        "symbol": SYMBOL,
        "url": ABCEX_URL,
        "fetched_at": now_utc().isoformat(),
        "metrics": metrics,
        "trades": [t.__dict__ for t in trades],
    }

# ───────────────────────── WORKER ─────────────────────────

async def worker() -> None:
    seen: Set[TradeKey] = set()
    backoff = 2.0

    async with httpx.AsyncClient(timeout=30, verify=certifi.where()) as sb:
        async with async_playwright() as pw:
            browser = context = page = None

            while True:
                try:
                    if page is None:
                        log.info("Starting browser session...")
                        try:
                            browser, context, page = await open_browser(pw)
                        except Exception as e:
                            if _should_force_install(e):
                                # force install immediately (NO cooldown lock)
                                ensure_playwright_browsers_runtime(force=True)
                                browser, context, page = await open_browser(pw)
                            else:
                                raise

                        await goto_abcex(page)
                        backoff = 2.0

                    payload = await asyncio.wait_for(scrape_once(page), timeout=SCRAPE_HARD_TIMEOUT_SEC)

                    # optional dedup for trades_new
                    new_trades = []
                    for t in payload.get("trades", []):
                        k = TradeKey(time=t["time"], price=float(t["price"]), qty=float(t["qty"]))
                        if k in seen:
                            continue
                        seen.add(k)
                        new_trades.append(t)
                    if new_trades:
                        payload["trades_new"] = new_trades
                        payload["metrics"]["new_count"] = len(new_trades)

                    row = build_supabase_row(payload)
                    log.info("Metrics: %s", json.dumps(payload.get("metrics", {}), ensure_ascii=False))
                    await upsert_to_supabase(sb, row)

                    await asyncio.sleep(max(0.5, POLL_SECONDS + random.uniform(-0.2, 0.2)))

                except Exception as e:
                    log.error("Worker error: %s", e)

                    # If browser binary missing => force install NOW (no cooldown)
                    if _should_force_install(e):
                        try:
                            ensure_playwright_browsers_runtime(force=True)
                        except Exception as ie:
                            log.error("Forced install failed: %s", ie)

                    await safe_close(browser, context, page)
                    browser = context = page = None

                    log.info("Retrying after %.1fs ...", backoff)
                    await asyncio.sleep(backoff)
                    backoff = min(60.0, backoff * 2)

def main() -> None:
    log.info(
        "Starting ABCEX worker | url=%s | poll=%.1fs | proxy=%s | pw_path=%s",
        ABCEX_URL, POLL_SECONDS,
        ("on" if (USE_PROXY_FOR_BROWSER and PROXY_URL) else "off"),
        PLAYWRIGHT_BROWSERS_PATH,
    )
    asyncio.run(worker())

if __name__ == "__main__":
    main()
