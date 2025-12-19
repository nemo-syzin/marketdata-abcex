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
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

import httpx
from playwright.async_api import async_playwright, Page, Browser, BrowserContext


# ───────────────────────── CONFIG ─────────────────────────

ABCEX_URL = os.getenv("ABCEX_URL", "https://abcex.io/spot/USDT-RUB")
SOURCE = os.getenv("SOURCE", "abcex")
SYMBOL = os.getenv("SYMBOL", "USDT/RUB")

ABCEX_EMAIL = os.getenv("ABCEX_EMAIL", "")
ABCEX_PASSWORD = os.getenv("ABCEX_PASSWORD", "")

LIMIT = int(os.getenv("LIMIT", "200"))
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "1.5"))

SCRAPE_TIMEOUT_SECONDS = float(os.getenv("SCRAPE_TIMEOUT_SECONDS", "25"))
UPSERT_TIMEOUT_SECONDS = float(os.getenv("UPSERT_TIMEOUT_SECONDS", "25"))
HEARTBEAT_SECONDS = float(os.getenv("HEARTBEAT_SECONDS", "30"))
RELOAD_EVERY_SECONDS = float(os.getenv("RELOAD_EVERY_SECONDS", "600"))

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "exchange_trades")

# Должно совпадать с unique index в БД:
ON_CONFLICT = os.getenv("ON_CONFLICT", "source,symbol,trade_time,price,volume_usdt")

SKIP_BROWSER_INSTALL = os.getenv("SKIP_BROWSER_INSTALL", "0") == "1"

SEEN_MAX = int(os.getenv("SEEN_MAX", "20000"))
UPSERT_BATCH = int(os.getenv("UPSERT_BATCH", "200"))

try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    force=True,
)
logger = logging.getLogger("abcex-worker")

TIME_RE = re.compile(r"\b\d{2}:\d{2}:\d{2}\b")
Q8 = Decimal("0.00000001")


# ───────────────────────── ВАШ “РАБОЧИЙ КУСОК” УСТАНОВКИ ─────────────────────────

def ensure_playwright_browsers(
    browsers: tuple[str, ...] = ("chromium", "chromium-headless-shell"),
) -> bool:
    """
    Гарантируем, что браузеры Playwright скачаны.

    ВАЖНО:
    - только download (без системных deps)
    - никаких su / --with-deps
    - использует Playwright CLI: `playwright install ...`
    """
    try:
        logger.warning("Ensuring Playwright browsers are installed: %s ...", ", ".join(browsers))

        result = subprocess.run(
            ["playwright", "install", *browsers],
            check=False,
            capture_output=True,
            text=True,
        )

        if result.returncode == 0:
            logger.info("Playwright browsers installed (or already present).")
            return True

        logger.error(
            "playwright install returned code %s\nSTDOUT:\n%s\nSTDERR:\n%s",
            result.returncode,
            result.stdout,
            result.stderr,
        )
        return False

    except FileNotFoundError:
        logger.error(
            "playwright CLI not found in PATH. "
            "Убедись, что Playwright установлен и доступен как команда 'playwright'."
        )
        return False
    except Exception as e:
        logger.error("Unexpected error while installing Playwright browsers: %s", e)
        return False


def ensure_playwright_browsers_with_retry(
    attempts: int = 3,
    base_sleep: float = 5.0,
) -> bool:
    """
    Ретраим install, потому что на Render часто бывает:
    'server closed connection' на CDN.
    """
    for i in range(1, attempts + 1):
        ok = ensure_playwright_browsers(("chromium", "chromium-headless-shell"))
        if ok:
            return True
        sleep_s = base_sleep * i
        logger.warning("Retrying playwright install after %.1fs (attempt %d/%d)...", sleep_s, i, attempts)
        time.sleep(sleep_s)
    return False


def _should_force_install(err: Exception) -> bool:
    s = str(err)
    return (
        "Executable doesn't exist" in s
        or "playwright install" in s
        or "chromium_headless_shell" in s
        or ("ms-playwright" in s and "doesn't exist" in s)
    )


# ───────────────────────── HELPERS ─────────────────────────

def normalize_decimal(text: str) -> Optional[Decimal]:
    t = (text or "").strip()
    if not t:
        return None
    t = t.replace("\xa0", " ").replace(" ", "")
    t = t.replace(",", ".")
    try:
        return Decimal(t)
    except (InvalidOperation, ValueError):
        return None


def extract_time(text: str) -> Optional[str]:
    m = TIME_RE.search((text or "").replace("\xa0", " "))
    if not m:
        return None
    return m.group(0)


def q8_str(x: Decimal) -> str:
    return str(x.quantize(Q8, rounding=ROUND_HALF_UP))


@dataclass(frozen=True)
class TradeKey:
    source: str
    symbol: str
    trade_time: str
    price: str
    volume_usdt: str


def trade_key(t: Dict[str, Any]) -> TradeKey:
    return TradeKey(
        source=t["source"],
        symbol=t["symbol"],
        trade_time=t["trade_time"],
        price=t["price"],
        volume_usdt=t["volume_usdt"],
    )


# ───────────────────────── PAGE ACTIONS ─────────────────────────

async def accept_cookies_if_any(page: Page) -> None:
    for label in ["Принять", "Я согласен", "Accept", "Agree"]:
        try:
            btn = page.locator(f"text={label}")
            if await btn.count() > 0:
                logger.info("Found cookies banner, clicking '%s'...", label)
                await btn.first.click(timeout=5_000)
                await page.wait_for_timeout(300)
                return
        except Exception:
            pass


async def login_if_needed(page: Page) -> None:
    """
    Логин опционален: если вы уже залогинены/логин не нужен — пропустим.
    """
    if not ABCEX_EMAIL or not ABCEX_PASSWORD:
        logger.info("ABCEX_EMAIL / ABCEX_PASSWORD not set. Skipping login.")
        return

    try:
        # Если на странице есть кнопка "Войти" — пробуем логиниться
        login_btn = page.locator("text=Войти")
        if await login_btn.count() == 0:
            logger.info("Login button not found; assuming already logged in or login not required.")
            return

        logger.info("Logging in...")
        await login_btn.first.click(timeout=10_000)
        await page.wait_for_timeout(700)

        # Поля
        email_input = page.locator("input[type='email'], input[placeholder*='Email'], input[name*='email']")
        pass_input = page.locator("input[type='password'], input[placeholder*='Пароль'], input[name*='pass']")

        await email_input.first.fill(ABCEX_EMAIL, timeout=15_000)
        await pass_input.first.fill(ABCEX_PASSWORD, timeout=15_000)

        submit_btn = page.locator("button:has-text('Войти'), button:has-text('Login')")
        if await submit_btn.count() > 0:
            await submit_btn.first.click(timeout=15_000)

        # Ждём исчезновения формы/кнопки
        await page.wait_for_timeout(1500)
        logger.info("Login attempt done.")
    except Exception as e:
        logger.warning("Login skipped/failed (non-fatal): %s", e)


# ───────────────────────── ABCEX PARSING ─────────────────────────

@dataclass
class Trade:
    time: str
    price: Decimal
    qty: Decimal
    price_raw: str
    qty_raw: str


async def wait_for_trades_loaded(page: Page, timeout_ms: int = 20_000) -> None:
    """
    Ждём появления панели истории (по исходному парсеру abcex.py).
    """
    panel = page.locator("div.panel-orderHistory")
    await panel.wait_for(state="visible", timeout=timeout_ms)


async def extract_trades_from_panel(page: Page, limit: int = 200) -> List[Trade]:
    """
    Достаём строки истории из orderHistory, как в вашем abcex.py:
    - берём div.panel-orderHistory
    - пробуем найти “гриды” с 3 значениями (time, price, qty)
    """
    panel = page.locator("div.panel-orderHistory")
    await panel.wait_for(state="visible", timeout=20_000)

    handle = await panel.element_handle()
    if handle is None:
        raise RuntimeError("Не смог получить element_handle панели.")

    raw_rows: List[Dict[str, Any]] = await handle.evaluate(
        """(root, limit) => {
          const isTime = (s) => /^\\d{2}:\\d{2}:\\d{2}$/.test((s||'').trim());
          const isNum = (s) => /^[0-9][0-9\\s\\u00A0.,]*$/.test((s||'').trim());

          const out = [];
          const grids = Array.from(root.querySelectorAll('div'));

          // Ищем блоки, похожие на строки сделок: 3 значения, одно время + 2 числа
          for (const g of grids) {
            const txt = (g.innerText || '').replace(/\\r/g, '');
            if (!txt) continue;

            const parts = txt.split('\\n').map(s => s.trim()).filter(Boolean);
            if (parts.length < 3) continue;

            // Возьмём первые 3 “смысленных” элемента
            const a = parts[0], b = parts[1], c = parts[2];

            // Определяем комбинацию: где время?
            const isATime = isTime(a), isBTime = isTime(b), isCTime = isTime(c);

            let time = null, price_raw = null, qty_raw = null;

            if (isATime && isNum(b) && isNum(c)) {
              time = a; price_raw = b; qty_raw = c;
            } else if (isBTime && isNum(a) && isNum(c)) {
              time = b; price_raw = a; qty_raw = c;
            } else if (isCTime && isNum(a) && isNum(b)) {
              time = c; price_raw = a; qty_raw = b;
            } else {
              continue;
            }

            out.push({ time, price_raw, qty_raw });

            if (out.length >= limit) break;
          }

          return out;
        }""",
        limit,
    )

    out: List[Trade] = []
    for r in raw_rows:
        try:
            price_raw = str(r.get("price_raw", "")).strip()
            qty_raw = str(r.get("qty_raw", "")).strip()
            time_txt = str(r.get("time", "")).strip()

            t = extract_time(time_txt)
            if not t:
                continue

            price = normalize_decimal(price_raw)
            qty = normalize_decimal(qty_raw)
            if price is None or qty is None:
                continue
            if price <= 0 or qty <= 0:
                continue

            out.append(
                Trade(
                    time=t,
                    price=price,
                    qty=qty,
                    price_raw=price_raw,
                    qty_raw=qty_raw,
                )
            )
        except Exception:
            continue

    return out


async def scrape_window(page: Page) -> List[Dict[str, Any]]:
    await wait_for_trades_loaded(page, timeout_ms=20_000)
    trades = await extract_trades_from_panel(page, limit=LIMIT)

    # В БД: volume_usdt = qty, volume_rub = price*qty
    rows: List[Dict[str, Any]] = []
    for tr in trades:
        volume_rub = tr.price * tr.qty
        rows.append(
            {
                "source": SOURCE,
                "symbol": SYMBOL,
                "trade_time": tr.time,
                "price": q8_str(tr.price),
                "volume_usdt": q8_str(tr.qty),
                "volume_rub": q8_str(volume_rub),
            }
        )
    return rows


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
        logger.warning("SUPABASE_URL or SUPABASE_KEY not set; skipping insert.")
        return

    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
    params = {"on_conflict": ON_CONFLICT}

    async with httpx.AsyncClient(timeout=UPSERT_TIMEOUT_SECONDS) as client:
        for i in range(0, len(rows), UPSERT_BATCH):
            chunk = rows[i:i + UPSERT_BATCH]
            try:
                r = await client.post(url, headers=_sb_headers(), params=params, json=chunk)
            except Exception as e:
                logger.error("Supabase POST error: %s", e)
                return

            if r.status_code >= 300:
                logger.error("Supabase upsert failed (%s): %s", r.status_code, r.text)
                return

        logger.info("Inserted (or ignored duplicates) %d rows into '%s'.", len(rows), SUPABASE_TABLE)


# ───────────────────────── BROWSER SESSION ─────────────────────────

async def open_browser(pw) -> Tuple[Browser, BrowserContext, Page]:
    browser = await pw.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-blink-features=AutomationControlled",
        ],
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

    logger.info("Opening %s ...", ABCEX_URL)
    await page.goto(ABCEX_URL, wait_until="domcontentloaded", timeout=60_000)
    await page.wait_for_timeout(800)

    await accept_cookies_if_any(page)
    await login_if_needed(page)

    # даём UI подгрузить историю
    await page.wait_for_timeout(1200)
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
                    logger.info("Starting browser session...")

                    try:
                        browser, context, page = await open_browser(pw)
                    except Exception as e:
                        # Если браузера нет — ставим и пробуем ещё раз
                        if (not SKIP_BROWSER_INSTALL) or _should_force_install(e):
                            logger.warning("Browser launch failed; trying to install Playwright browsers...")
                            ensure_playwright_browsers_with_retry(attempts=3, base_sleep=5.0)
                            browser, context, page = await open_browser(pw)
                        else:
                            raise

                    backoff = 2.0
                    last_reload = time.monotonic()
                    last_heartbeat = time.monotonic()

                # Heartbeat
                if time.monotonic() - last_heartbeat >= HEARTBEAT_SECONDS:
                    logger.info("Heartbeat: alive. seen=%d", len(seen))
                    last_heartbeat = time.monotonic()

                # Maintenance reload
                if time.monotonic() - last_reload >= RELOAD_EVERY_SECONDS:
                    logger.warning("Maintenance reload...")
                    await page.goto(ABCEX_URL, wait_until="domcontentloaded", timeout=60_000)
                    await page.wait_for_timeout(800)
                    await accept_cookies_if_any(page)
                    await login_if_needed(page)
                    last_reload = time.monotonic()

                window = await asyncio.wait_for(scrape_window(page), timeout=SCRAPE_TIMEOUT_SECONDS)

                if not window:
                    logger.warning("No rows parsed. Reloading page...")
                    await page.goto(ABCEX_URL, wait_until="domcontentloaded", timeout=60_000)
                    await page.wait_for_timeout(800)
                    await accept_cookies_if_any(page)
                    await login_if_needed(page)
                    await asyncio.sleep(max(0.5, POLL_SECONDS))
                    continue

                new_rows: List[Dict[str, Any]] = []
                for t in reversed(window):
                    k = trade_key(t)
                    if k in seen:
                        continue
                    new_rows.append(t)

                    seen.add(k)
                    seen_q.append(k)
                    if len(seen_q) > SEEN_MAX:
                        old = seen_q.popleft()
                        seen.discard(old)

                if new_rows:
                    logger.info(
                        "Parsed %d new trades. Newest: %s",
                        len(new_rows),
                        json.dumps(new_rows[-1], ensure_ascii=False),
                    )
                    await supabase_upsert(new_rows)

                sleep_s = max(0.35, POLL_SECONDS + random.uniform(-0.15, 0.15))
                await asyncio.sleep(sleep_s)

            except asyncio.TimeoutError:
                logger.error(
                    "Timeout: scrape_window exceeded %.1fs. Restarting browser session...",
                    SCRAPE_TIMEOUT_SECONDS,
                )
                await safe_close(browser, context, page)
                browser = context = page = None

            except Exception as e:
                logger.error("Worker error: %s", e)

                if (not SKIP_BROWSER_INSTALL) or _should_force_install(e):
                    logger.warning("Trying to (re)install browsers after error...")
                    ensure_playwright_browsers_with_retry(attempts=3, base_sleep=5.0)

                logger.info("Retrying after %.1fs ...", backoff)
                await asyncio.sleep(backoff)
                backoff = min(60.0, backoff * 2)

                await safe_close(browser, context, page)
                browser = context = page = None


def main() -> None:
    asyncio.run(worker())


if __name__ == "__main__":
    main()
