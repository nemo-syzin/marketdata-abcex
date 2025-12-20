#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import json
import logging
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from urllib.parse import urlsplit
import glob

import httpx
from playwright.async_api import async_playwright, Page, Locator, TimeoutError as PWTimeoutError

# ───────────────────────── CONFIG ─────────────────────────

ABCEX_URL = os.getenv("ABCEX_URL", "https://abcex.io/client/spot/USDTRUB")
STATE_PATH = os.getenv("STATE_PATH", "abcex_state.json")

SOURCE = os.getenv("SOURCE", "abcex")
SYMBOL = os.getenv("SYMBOL", "USDT/RUB")

LIMIT = int(os.getenv("LIMIT", "200"))
POLL_SEC = float(os.getenv("POLL_SEC", "1"))

ABCEX_EMAIL = os.getenv("ABCEX_EMAIL", "")
ABCEX_PASSWORD = os.getenv("ABCEX_PASSWORD", "")

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "exchange_trades")

GOTO_TIMEOUT_MS = int(os.getenv("GOTO_TIMEOUT_MS", "60000"))
WAIT_TRADES_TIMEOUT_MS = int(os.getenv("WAIT_TRADES_TIMEOUT_MS", "25000"))

TIME_RE = re.compile(r"\b(\d{1,2}:\d{2}:\d{2})\b")

# Render cache path
BROWSERS_ROOT = os.getenv("PLAYWRIGHT_BROWSERS_PATH", "/opt/render/.cache/ms-playwright")
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = BROWSERS_ROOT

# ───────────────────────── LOGGING ─────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
logger = logging.getLogger("abcex-worker")

# ───────────────────────── INSTALL HELPERS ─────────────────────────

def _env_without_proxies() -> Dict[str, str]:
    """
    На playwright install убираем прокси, иначе CDN скачивание часто рвётся.
    """
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


def _playwright_install() -> None:
    """
    Установка браузеров ровно как в твоём примере:
      python -m playwright install chromium chromium-headless-shell
    """
    logger.warning("Installing Playwright browsers (runtime) to %s ...", BROWSERS_ROOT)

    r = subprocess.run(
        [sys.executable, "-m", "playwright", "install", "chromium", "chromium-headless-shell"],
        check=False,
        capture_output=True,
        text=True,
        env=_env_without_proxies(),
        timeout=15 * 60,
    )

    logger.warning("playwright install returncode=%s", r.returncode)
    if r.stdout:
        logger.warning("playwright install STDOUT:\n%s", r.stdout[-4000:])
    if r.stderr:
        logger.warning("playwright install STDERR:\n%s", r.stderr[-4000:])

    if r.returncode != 0:
        raise RuntimeError("playwright install failed")

    logger.warning(
        "Install verification: chromium=%s headless_shell=%s",
        _chromium_exists(),
        _headless_shell_exists(),
    )


def _should_install(err: Exception) -> bool:
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

# ───────────────────────── PAGE ACTIONS ─────────────────────────

async def accept_cookies_if_any(page: Page) -> None:
    for label in ["Я согласен", "Принять", "Accept", "I agree", "Согласен"]:
        try:
            btn = page.locator(f"text={label}")
            if await btn.count() > 0 and await btn.first.is_visible():
                logger.info("Cookies banner: clicking '%s'...", label)
                await btn.first.click(timeout=5_000)
                await page.wait_for_timeout(300)
                return
        except Exception:
            pass


async def _find_visible_in_frames(page: Page, selectors: List[str]) -> Optional[Locator]:
    for fr in page.frames:
        for sel in selectors:
            try:
                loc = fr.locator(sel)
                cnt = await loc.count()
                if cnt <= 0:
                    continue
                for i in range(min(cnt, 8)):
                    cand = loc.nth(i)
                    if await cand.is_visible():
                        return cand
            except Exception:
                continue
    return None


async def _is_login_visible(page: Page) -> bool:
    pw = await _find_visible_in_frames(page, [
        "input[type='password']",
        "input[autocomplete='current-password']",
        "input[name*='pass' i]",
    ])
    return pw is not None


async def _open_login_modal_if_needed(page: Page) -> None:
    if await _is_login_visible(page):
        return

    candidates = [
        "text=Войти", "text=Login", "text=Sign in",
        "a:has-text('Войти')", "button:has-text('Войти')",
        "button:has-text('Login')", "button:has-text('Sign in')",
    ]
    for sel in candidates:
        try:
            btn = page.locator(sel)
            if await btn.count() > 0 and await btn.first.is_visible():
                await btn.first.click(timeout=7_000)
                await page.wait_for_timeout(600)
                break
        except Exception:
            pass


async def login_if_needed(page: Page, email: str, password: str) -> None:
    await _open_login_modal_if_needed(page)

    if not await _is_login_visible(page):
        logger.info("Login not required (no password field detected).")
        return

    logger.info("Login detected. Performing sign-in ...")

    email_selectors = [
        "input[type='email']",
        "input[name='email']",
        "input[name*='mail' i]",
        "input[placeholder*='mail' i]",
        "input[placeholder*='email' i]",
        "input[placeholder*='почта' i]",
        "input[autocomplete='username']",
        "input[type='text'][placeholder*='mail' i]",
        "input[type='text'][placeholder*='email' i]",
        "input[type='text'][name*='login' i]",
    ]
    pw_selectors = [
        "input[type='password']",
        "input[autocomplete='current-password']",
        "input[name*='pass' i]",
    ]

    email_loc = await _find_visible_in_frames(page, email_selectors)
    pw_loc = await _find_visible_in_frames(page, pw_selectors)

    if email_loc is None or pw_loc is None:
        raise RuntimeError("Не смог найти поля email/password.")

    await email_loc.fill(email, timeout=10_000)
    await pw_loc.fill(password, timeout=10_000)

    submit_selectors = [
        "button:has-text('Войти')",
        "button:has-text('Login')",
        "button:has-text('Sign in')",
        "button[type='submit']",
    ]
    clicked = False
    for sel in submit_selectors:
        btn = await _find_visible_in_frames(page, [sel])
        if btn is not None:
            try:
                await btn.click(timeout=10_000)
                clicked = True
                break
            except Exception:
                pass

    if not clicked:
        try:
            await page.keyboard.press("Enter")
        except Exception:
            pass

    await page.wait_for_timeout(800)
    for _ in range(30):
        if not await _is_login_visible(page):
            logger.info("Login successful (password field disappeared).")
            return
        await page.wait_for_timeout(400)

    raise RuntimeError("Логин не прошёл (password поле всё ещё видно). Возможны 2FA/капча/антибот.")

# ───────────────────────── TRADES / PARSING ─────────────────────────

async def ensure_last_trades_tab(page: Page) -> None:
    candidates = ["Последние сделки", "Сделки", "История", "Order history", "Trades"]
    for t in candidates:
        try:
            tab = page.locator(f"[role='tab']:has-text('{t}')")
            if await tab.count() > 0 and await tab.first.is_visible():
                await tab.first.click(timeout=6_000)
                await page.wait_for_timeout(300)
                return
        except Exception:
            pass

    for t in candidates:
        try:
            tab = page.locator(f"text={t}")
            if await tab.count() > 0 and await tab.first.is_visible():
                await tab.first.click(timeout=6_000)
                await page.wait_for_timeout(300)
                return
        except Exception:
            pass


async def wait_trades_visible(page: Page, timeout_ms: int) -> None:
    start = time.time()
    while (time.time() - start) * 1000 < timeout_ms:
        try:
            ok = await page.evaluate(
                """() => {
                    const re = /^\\d{2}:\\d{2}:\\d{2}$/;
                    const ps = Array.from(document.querySelectorAll('p'));
                    return ps.some(p => re.test((p.textContent||'').trim()));
                }"""
            )
            if ok:
                return
        except Exception:
            pass
        await page.wait_for_timeout(600)
    raise RuntimeError("Не дождался появления сделок (HH:MM:SS).")


def _normalize_num(text: str) -> float:
    t = (text or "").strip().replace("\xa0", " ").replace(" ", "")
    if "," in t and "." in t:
        t = t.replace(",", "")
    else:
        t = t.replace(",", ".")
    return float(t)


def _extract_time(text: str) -> Optional[str]:
    m = TIME_RE.search((text or "").replace("\xa0", " "))
    if not m:
        return None
    hh, mm, ss = m.group(1).split(":")
    if len(hh) == 1:
        hh = "0" + hh
    return f"{hh}:{mm}:{ss}"


def _trade_time_to_iso_moscow(hhmmss: str) -> str:
    now = datetime.utcnow() + timedelta(hours=3)
    h, m, s = [int(x) for x in hhmmss.split(":")]
    dt = now.replace(hour=h, minute=m, second=s, microsecond=0)
    if dt > now + timedelta(seconds=2):
        dt -= timedelta(days=1)
    return dt.isoformat()


async def get_order_history_panel(page: Page) -> Optional[Locator]:
    """
    В некоторых UI этого элемента нет. Тогда вернём None и уйдём в fallback.
    """
    try:
        panel = page.locator("div[role='tabpanel'][id*='panel-orderHistory']")
        if await panel.count() <= 0:
            return None
        # видимая
        for i in range(await panel.count()):
            p = panel.nth(i)
            try:
                if await p.is_visible():
                    return p
            except Exception:
                continue
        return None
    except Exception:
        return None


async def extract_trades_from_panel(panel: Locator, limit: int) -> List[Dict[str, Any]]:
    handle = await panel.element_handle()
    if handle is None:
        return []

    raw_rows: List[Dict[str, Any]] = await handle.evaluate(
        """(root, limit) => {
          const isTime = (s) => /^\\d{2}:\\d{2}:\\d{2}$/.test((s||'').trim());
          const isNum  = (s) => /^[0-9][0-9\\s\\u00A0.,]*$/.test((s||'').trim());

          const out = [];
          const divs = Array.from(root.querySelectorAll('div'));

          for (const g of divs) {
            const ps = Array.from(g.querySelectorAll(':scope > p'));
            if (ps.length < 3) continue;

            const p0 = (ps[0].textContent || '').trim();
            const p1 = (ps[1].textContent || '').trim();
            const p2 = (ps[2].textContent || '').trim();

            if (!isTime(p2)) continue;
            if (!isNum(p0) || !isNum(p1)) continue;

            const style0 = (ps[0].getAttribute('style') || '').toLowerCase();
            let side = null;
            if (style0.includes('green')) side = 'buy';
            if (style0.includes('red')) side = 'sell';

            out.push({ price_raw: p0, qty_raw: p1, time: p2, side });
            if (out.length >= limit) break;
          }
          return out;
        }""",
        limit,
    )

    return _rows_to_trades(raw_rows)


async def extract_trades_fallback_from_document(page: Page, limit: int) -> List[Dict[str, Any]]:
    """
    Фоллбек: ищем сделки по ВСЕМУ документу (без panel-orderHistory).
    Это спасает, когда UI другой.
    """
    raw_rows: List[Dict[str, Any]] = await page.evaluate(
        """(limit) => {
          const isTime = (s) => /^\\d{2}:\\d{2}:\\d{2}$/.test((s||'').trim());
          const isNum  = (s) => /^[0-9][0-9\\s\\u00A0.,]*$/.test((s||'').trim());

          const out = [];
          const divs = Array.from(document.querySelectorAll('div'));

          for (const g of divs) {
            const ps = Array.from(g.querySelectorAll(':scope > p'));
            if (ps.length < 3) continue;

            const p0 = (ps[0].textContent || '').trim();
            const p1 = (ps[1].textContent || '').trim();
            const p2 = (ps[2].textContent || '').trim();

            if (!isTime(p2)) continue;
            if (!isNum(p0) || !isNum(p1)) continue;

            const style0 = (ps[0].getAttribute('style') || '').toLowerCase();
            let side = null;
            if (style0.includes('green')) side = 'buy';
            if (style0.includes('red')) side = 'sell';

            out.push({ price_raw: p0, qty_raw: p1, time: p2, side });
            if (out.length >= limit) break;
          }
          return out;
        }""",
        limit,
    )

    return _rows_to_trades(raw_rows)


def _rows_to_trades(raw_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    trades: List[Dict[str, Any]] = []
    for r in raw_rows:
        try:
            price_raw = str(r.get("price_raw", "")).strip()
            qty_raw = str(r.get("qty_raw", "")).strip()
            time_raw = str(r.get("time", "")).strip()
            side = r.get("side") if r.get("side") in ("buy", "sell") else None

            tt = _extract_time(time_raw)
            if not tt:
                continue

            price = _normalize_num(price_raw)
            qty = _normalize_num(qty_raw)

            trades.append(
                {
                    "source": SOURCE,
                    "symbol": SYMBOL,
                    "trade_time": _trade_time_to_iso_moscow(tt),
                    "price": str(price),
                    "volume_usdt": str(qty),
                    "turnover_rub": str(price * qty),
                    "side": side,
                    "raw_time": tt,
                    "price_raw": price_raw,
                    "qty_raw": qty_raw,
                    "parsed_at": datetime.utcnow().isoformat(),
                }
            )
        except Exception:
            continue
    return trades

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


async def supabase_upsert_trades(trades: List[Dict[str, Any]]) -> None:
    if not trades:
        return
    if not SUPABASE_URL or not SUPABASE_KEY or not SUPABASE_TABLE:
        logger.warning("Supabase env is not fully set; skipping DB write.")
        return

    url = SUPABASE_URL.rstrip("/") + f"/rest/v1/{SUPABASE_TABLE}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }

    async with httpx.AsyncClient(timeout=30.0, headers=headers, trust_env=True) as client:
        resp = await client.post(url, content=json.dumps(trades, ensure_ascii=False).encode("utf-8"))
        if resp.status_code in (200, 201, 204):
            logger.info("Supabase upsert OK: %s trades", len(trades))
            return
        logger.error("Supabase write failed: %s %s", resp.status_code, resp.text[:1500])

# ───────────────────────── RUN ONCE ─────────────────────────

async def run_once() -> List[Dict[str, Any]]:
    proxy_url = _get_proxy_url()
    pw_proxy = _parse_proxy_for_playwright(proxy_url) if proxy_url else None

    async with async_playwright() as p:
        browser = None
        for attempt in (1, 2):
            try:
                launch_kwargs: Dict[str, Any] = {
                    "headless": True,
                    "args": [
                        "--disable-blink-features=AutomationControlled",
                        "--no-sandbox",
                        "--disable-dev-shm-usage",
                    ],
                }
                if pw_proxy:
                    launch_kwargs["proxy"] = pw_proxy

                browser = await p.chromium.launch(**launch_kwargs)
                break
            except Exception as e:
                if _should_install(e):
                    logger.warning("Chromium launch failed (missing executable). Installing and retry... attempt=%s", attempt)
                    _playwright_install()
                    await asyncio.sleep(2.0)
                    continue
                raise

        if browser is None:
            raise RuntimeError("Cannot launch browser after retries.")

        storage_state = STATE_PATH if os.path.exists(STATE_PATH) else None
        if storage_state:
            logger.info("Using saved session state: %s", storage_state)

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

        try:
            logger.info("Opening ABCEX: %s", ABCEX_URL)
            await page.goto(ABCEX_URL, wait_until="networkidle", timeout=GOTO_TIMEOUT_MS)
            await page.wait_for_timeout(1200)

            await accept_cookies_if_any(page)

            if not ABCEX_EMAIL or not ABCEX_PASSWORD:
                raise RuntimeError("ABCEX_EMAIL/ABCEX_PASSWORD must be set in env.")
            await login_if_needed(page, ABCEX_EMAIL, ABCEX_PASSWORD)

            try:
                await context.storage_state(path=STATE_PATH)
                logger.info("Saved session state to %s", STATE_PATH)
            except Exception as e:
                logger.warning("Could not save storage state: %s", e)

            await ensure_last_trades_tab(page)
            await wait_trades_visible(page, timeout_ms=WAIT_TRADES_TIMEOUT_MS)

            panel = await get_order_history_panel(page)

            trades: List[Dict[str, Any]] = []
            if panel is not None:
                trades = await extract_trades_from_panel(panel, limit=LIMIT)

            if not trades:
                logger.warning("panel-orderHistory not found or empty. Using fallback DOM scan...")
                trades = await extract_trades_fallback_from_document(page, limit=LIMIT)

            logger.info("Parsed trades: %d", len(trades))
            return trades

        except PWTimeoutError:
            raise
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
    if not _chromium_exists() or not _headless_shell_exists():
        logger.warning("Browsers not found. Installing now...")
        _playwright_install()

    seen: Dict[TradeKey, float] = {}
    seen_ttl_sec = 60 * 30

    while True:
        try:
            trades = await run_once()

            now = time.time()
            for k, ts in list(seen.items()):
                if now - ts > seen_ttl_sec:
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
                logger.info("No new trades after in-memory dedup.")

        except Exception as e:
            logger.error("Cycle error: %s", e)
            if _should_install(e):
                logger.warning("Detected missing browser executable. Re-installing...")
                _playwright_install()
            await asyncio.sleep(3.0)

        await asyncio.sleep(POLL_SEC)


if __name__ == "__main__":
    asyncio.run(main())
