import asyncio
import json
import logging
import os
import random
import re
import subprocess
import sys
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

import httpx
from playwright.async_api import async_playwright, Page, Browser, BrowserContext, Locator

# ───────────────────────── CONFIG ─────────────────────────

ABCEX_URL = os.getenv("ABCEX_URL", "https://abcex.io/spot/USDT-RUB")  # проверьте ваш точный URL
SOURCE = os.getenv("SOURCE", "abcex")
SYMBOL = os.getenv("SYMBOL", "USDT/RUB")

ABCEX_EMAIL = os.getenv("ABCEX_EMAIL", "")
ABCEX_PASSWORD = os.getenv("ABCEX_PASSWORD", "")

LIMIT = int(os.getenv("LIMIT", "200"))                 # сколько строк брать с UI за проход
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "1.5")) # частота опроса

SCRAPE_TIMEOUT_SECONDS = float(os.getenv("SCRAPE_TIMEOUT_SECONDS", "25"))
UPSERT_TIMEOUT_SECONDS = float(os.getenv("UPSERT_TIMEOUT_SECONDS", "25"))
HEARTBEAT_SECONDS = float(os.getenv("HEARTBEAT_SECONDS", "30"))
RELOAD_EVERY_SECONDS = float(os.getenv("RELOAD_EVERY_SECONDS", "600"))

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "exchange_trades")

# Должно совпадать с вашим unique index в Supabase
ON_CONFLICT = os.getenv("ON_CONFLICT", "source,symbol,trade_time,price,volume_usdt")

SEEN_MAX = int(os.getenv("SEEN_MAX", "20000"))
UPSERT_BATCH = int(os.getenv("UPSERT_BATCH", "200"))

# Если хотите временно выключить runtime-install
SKIP_BROWSER_INSTALL = os.getenv("SKIP_BROWSER_INSTALL", "0") == "1"

# Логи без буферизации (Render)
try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    force=True,
)
log = logging.getLogger("abcex-worker")

# numeric(18,8)
Q8 = Decimal("0.00000001")

TIME_RE = re.compile(r"^\d{2}:\d{2}:\d{2}$")


# ───────────────────────── NUM HELPERS ─────────────────────────

def normalize_decimal(text: str) -> Optional[Decimal]:
    t = (text or "").strip()
    if not t:
        return None
    t = t.replace("\xa0", " ").replace(" ", "").replace(",", ".")
    try:
        return Decimal(t)
    except (InvalidOperation, ValueError):
        return None


def q8_str(x: Decimal) -> str:
    return str(x.quantize(Q8, rounding=ROUND_HALF_UP))


# ───────────────────────── PLAYWRIGHT INSTALL (как в rapira) ─────────────────────────

def _should_force_install(err: Exception) -> bool:
    s = str(err)
    return (
        "Executable doesn't exist" in s
        or "playwright install" in s
        or "chromium_headless_shell" in s
        or ("ms-playwright" in s and "doesn't exist" in s)
    )


def ensure_playwright_browsers(
    browsers: tuple[str, ...] = ("chromium", "chromium-headless-shell"),
) -> None:
    """
    Гарантируем, что браузеры Playwright скачаны.

    ВАЖНО:
    - только download (без системных deps)
    - никаких su / --with-deps
    - использует Playwright CLI: `playwright install ...`

    browsers: какие браузеры/каналы ставить (по умолчанию chromium + headless-shell)
    """
    try:
        logging.info("Ensuring Playwright browsers are installed: %s ...", ", ".join(browsers))

        result = subprocess.run(
            ["playwright", "install", *browsers],
            check=False,
            capture_output=True,
            text=True,
        )

        if result.returncode == 0:
            logging.info("Playwright browsers installed (or already present).")
            return

        logging.error(
            "playwright install returned code %s\nSTDOUT:\n%s\nSTDERR:\n%s",
            result.returncode,
            result.stdout,
            result.stderr,
        )

    except FileNotFoundError:
        logging.error(
            "playwright CLI not found in PATH. "
            "Убедись, что Playwright установлен и доступен как команда 'playwright'."
        )
    except Exception as e:
        logging.error("Unexpected error while installing Playwright browsers: %s", e)


# ───────────────────────── SUPABASE ─────────────────────────

def _sb_headers() -> Dict[str, str]:
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=ignore-duplicates,return=minimal",
    }


async def supabase_upsert(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    if not SUPABASE_URL or not SUPABASE_KEY:
        log.warning("SUPABASE_URL or SUPABASE_KEY not set; skipping insert.")
        return

    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
    params = {"on_conflict": ON_CONFLICT}

    async with httpx.AsyncClient(timeout=UPSERT_TIMEOUT_SECONDS) as client:
        for i in range(0, len(rows), UPSERT_BATCH):
            chunk = rows[i:i + UPSERT_BATCH]
            try:
                r = await client.post(url, headers=_sb_headers(), params=params, json=chunk)
            except Exception as e:
                log.error("Supabase POST error: %s", e)
                return

            if r.status_code >= 300:
                log.error("Supabase upsert failed (%s): %s", r.status_code, r.text)
                return

    log.info("Inserted (or ignored duplicates) %d rows into '%s'.", len(rows), SUPABASE_TABLE)


# ───────────────────────── ABCEX LOGIN / NAV ─────────────────────────

async def _is_login_visible(page: Page) -> bool:
    try:
        pw = page.locator("input[type='password']")
        if await pw.count() > 0 and await pw.first.is_visible():
            return True
    except Exception:
        pass
    return False


async def login_if_needed(page: Page, email: str, password: str) -> None:
    if not await _is_login_visible(page):
        log.info("Login not required (already in session).")
        return

    if not email or not password:
        raise RuntimeError("Login required but ABCEX_EMAIL/ABCEX_PASSWORD are not set in env.")

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
        raise RuntimeError("Could not find email/password fields on ABCEX login page.")

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

    try:
        await page.wait_for_timeout(1_000)
        await page.wait_for_load_state("networkidle", timeout=30_000)
    except Exception:
        pass

    await page.wait_for_timeout(2_000)

    if await _is_login_visible(page):
        raise RuntimeError("Login failed (login form still visible). Possible 2FA/captcha.")

    log.info("Login successful (form disappeared).")


async def click_trades_tab_best_effort(page: Page) -> None:
    candidates = ["Сделки", "История", "Order history", "Trades"]
    for t in candidates:
        try:
            tab = page.locator(f"[role='tab']:has-text('{t}')")
            if await tab.count() > 0 and await tab.first.is_visible():
                await tab.first.click(timeout=8_000)
                await page.wait_for_timeout(400)
                return
        except Exception:
            continue

    for t in candidates:
        try:
            tab = page.locator(f"text={t}")
            if await tab.count() > 0 and await tab.first.is_visible():
                await tab.first.click(timeout=8_000)
                await page.wait_for_timeout(400)
                return
        except Exception:
            continue


async def get_order_history_panel(page: Page) -> Locator:
    panel = page.locator("div[role='tabpanel'][id*='panel-orderHistory']")
    cnt = await panel.count()
    if cnt == 0:
        raise RuntimeError("panel-orderHistory not found")
    for i in range(cnt):
        p = panel.nth(i)
        try:
            if await p.is_visible():
                return p
        except Exception:
            continue
    raise RuntimeError("panel-orderHistory found but no visible panel")


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
                return
        except Exception:
            pass
        await page.wait_for_timeout(500)
    raise RuntimeError("Trades not visible (no HH:MM:SS detected)")


# ───────────────────────── PARSING ─────────────────────────

@dataclass(frozen=True)
class TradeKey:
    source: str
    symbol: str
    trade_time: str
    price: str
    volume_usdt: str


def trade_key(row: Dict[str, Any]) -> TradeKey:
    return TradeKey(
        source=row["source"],
        symbol=row["symbol"],
        trade_time=row["trade_time"],
        price=row["price"],
        volume_usdt=row["volume_usdt"],
    )


async def extract_trades_from_panel(panel: Locator, limit: int) -> List[Dict[str, Any]]:
    handle = await panel.element_handle()
    if handle is None:
        raise RuntimeError("panel element handle is None")

    raw_rows: List[Dict[str, Any]] = await handle.evaluate(
        """(root, limit) => {
          const isTime = (s) => /^\\d{2}:\\d{2}:\\d{2}$/.test((s||'').trim());
          const isNum = (s) => /^[0-9][0-9\\s\\u00A0.,]*$/.test((s||'').trim());

          const out = [];
          const ps = Array.from(root.querySelectorAll('p'));

          // Ищем тройки p: price, qty, time
          for (let i = 0; i < ps.length - 2; i++) {
            const a = (ps[i].textContent || '').trim();
            const b = (ps[i+1].textContent || '').trim();
            const c = (ps[i+2].textContent || '').trim();

            if (!isNum(a) || !isNum(b) || !isTime(c)) continue;

            // side по цвету price-ячейки (если есть)
            let side = null;
            const st = window.getComputedStyle(ps[i]);
            const color = (st && st.color) ? st.color : '';
            if (color) {
              // упрощённо: green/red
              if (color.includes('0, 128') || color.includes('0,128') || color.toLowerCase().includes('green')) side = 'buy';
              if (color.includes('255, 0') || color.includes('255,0') || color.toLowerCase().includes('red')) side = 'sell';
            }

            out.push({ price_raw: a, qty_raw: b, time: c, side });
            if (out.length >= limit) break;
          }

          return out;
        }""",
        limit,
    )

    rows: List[Dict[str, Any]] = []
    for r in raw_rows:
        price_d = normalize_decimal(r.get("price_raw", ""))
        qty_d = normalize_decimal(r.get("qty_raw", ""))
        t = (r.get("time") or "").strip()

        if price_d is None or qty_d is None:
            continue
        if not TIME_RE.match(t):
            continue
        if price_d <= 0 or qty_d <= 0:
            continue

        volume_rub = price_d * qty_d

        rows.append(
            {
                "source": SOURCE,
                "symbol": SYMBOL,
                "price": q8_str(price_d),
                "volume_usdt": q8_str(qty_d),
                "volume_rub": q8_str(volume_rub),
                "trade_time": t,
            }
        )

    return rows


async def scrape_window(page: Page) -> List[Dict[str, Any]]:
    panel = await get_order_history_panel(page)
    return await extract_trades_from_panel(panel, limit=LIMIT)


# ───────────────────────── BROWSER SESSION ─────────────────────────

async def open_browser(pw) -> Tuple[Browser, BrowserContext, Page]:
    browser = await pw.chromium.launch(
        headless=True,
        args=["--no-sandbox", "--disable-dev-shm-usage"],
    )
    context = await browser.new_context(
        viewport={"width": 1440, "height": 810},
        locale="ru-RU",
        timezone_id="Europe/Moscow",
        user_agent=(
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
    )
    page = await context.new_page()
    page.set_default_timeout(10_000)

    await page.goto(ABCEX_URL, wait_until="domcontentloaded", timeout=60_000)
    await page.wait_for_timeout(800)

    await login_if_needed(page, ABCEX_EMAIL, ABCEX_PASSWORD)
    await click_trades_tab_best_effort(page)
    await wait_trades_visible(page, timeout_ms=25_000)

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


# ───────────────────────── WORKER LOOP ─────────────────────────

async def worker() -> None:
    seen: Set[TradeKey] = set()
    seen_q: Deque[TradeKey] = deque()

    backoff = 2.0
    last_heartbeat = time.monotonic()
    last_reload = time.monotonic()

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
                        if (not SKIP_BROWSER_INSTALL) and _should_force_install(e):
                            ensure_playwright_browsers(("chromium", "chromium-headless-shell"))
                            browser, context, page = await open_browser(pw)
                        else:
                            raise

                    backoff = 2.0
                    last_reload = time.monotonic()
                    last_heartbeat = time.monotonic()

                if time.monotonic() - last_heartbeat >= HEARTBEAT_SECONDS:
                    log.info("Heartbeat: alive. seen=%d", len(seen))
                    last_heartbeat = time.monotonic()

                if time.monotonic() - last_reload >= RELOAD_EVERY_SECONDS:
                    log.warning("Maintenance reload...")
                    await page.goto(ABCEX_URL, wait_until="domcontentloaded", timeout=60_000)
                    await page.wait_for_timeout(800)
                    await login_if_needed(page, ABCEX_EMAIL, ABCEX_PASSWORD)
                    await click_trades_tab_best_effort(page)
                    await wait_trades_visible(page, timeout_ms=25_000)
                    last_reload = time.monotonic()

                window = await asyncio.wait_for(scrape_window(page), timeout=SCRAPE_TIMEOUT_SECONDS)

                if not window:
                    log.warning("No rows parsed. Reloading...")
                    await page.goto(ABCEX_URL, wait_until="domcontentloaded", timeout=60_000)
                    await page.wait_for_timeout(800)
                    await login_if_needed(page, ABCEX_EMAIL, ABCEX_PASSWORD)
                    await click_trades_tab_best_effort(page)
                    await wait_trades_visible(page, timeout_ms=25_000)
                    await asyncio.sleep(max(0.5, POLL_SECONDS))
                    continue

                new_rows: List[Dict[str, Any]] = []
                for row in reversed(window):
                    k = trade_key(row)
                    if k in seen:
                        continue
                    new_rows.append(row)
                    seen.add(k)
                    seen_q.append(k)
                    if len(seen_q) > SEEN_MAX:
                        old = seen_q.popleft()
                        seen.discard(old)

                if new_rows:
                    log.info("Parsed %d new trades. Newest: %s", len(new_rows), json.dumps(new_rows[-1], ensure_ascii=False))
                    await supabase_upsert(new_rows)

                sleep_s = max(0.35, POLL_SECONDS + random.uniform(-0.15, 0.15))
                await asyncio.sleep(sleep_s)

            except asyncio.TimeoutError:
                log.error("Timeout: scrape_window exceeded %.1fs. Restarting browser session...", SCRAPE_TIMEOUT_SECONDS)
                await safe_close(browser, context, page)
                browser = context = page = None

            except Exception as e:
                log.error("Worker error: %s", e)

                if (not SKIP_BROWSER_INSTALL) and _should_force_install(e):
                    ensure_playwright_browsers(("chromium", "chromium-headless-shell"))

                log.info("Retrying after %.1fs ...", backoff)
                await asyncio.sleep(backoff)
                backoff = min(60.0, backoff * 2)

                await safe_close(browser, context, page)
                browser = context = page = None


def main() -> None:
    asyncio.run(worker())


if __name__ == "__main__":
    main()
