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

ABCEX_URL = os.getenv("ABCEX_URL", "https://abcex.io/")
# Если у тебя есть конкретная страница (например trading), лучше задать env:
# ABCEX_TRADES_URL=https://abcex.io/trading/usdt_rub
ABCEX_TRADES_URL = os.getenv("ABCEX_TRADES_URL", ABCEX_URL)

ABCEX_EMAIL = os.getenv("ABCEX_EMAIL", "")
ABCEX_PASSWORD = os.getenv("ABCEX_PASSWORD", "")

SOURCE = os.getenv("SOURCE", "abcex")
SYMBOL = os.getenv("SYMBOL", "USDT/RUB")

LIMIT = int(os.getenv("LIMIT", "400"))
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "1.5"))

SCRAPE_TIMEOUT_SECONDS = float(os.getenv("SCRAPE_TIMEOUT_SECONDS", "25"))
UPSERT_TIMEOUT_SECONDS = float(os.getenv("UPSERT_TIMEOUT_SECONDS", "25"))
HEARTBEAT_SECONDS = float(os.getenv("HEARTBEAT_SECONDS", "30"))
RELOAD_EVERY_SECONDS = float(os.getenv("RELOAD_EVERY_SECONDS", "600"))

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "exchange_trades")

# Должно совпадать с unique index в таблице
ON_CONFLICT = os.getenv("ON_CONFLICT", "source,symbol,trade_time,price,volume_usdt")

SKIP_BROWSER_INSTALL = os.getenv("SKIP_BROWSER_INSTALL", "0") == "1"
SEEN_MAX = int(os.getenv("SEEN_MAX", "20000"))
UPSERT_BATCH = int(os.getenv("UPSERT_BATCH", "200"))

# ── Proxy for Playwright browser (launch proxy) ──
# Вариант 1: одной строкой:
# PLAYWRIGHT_PROXY_SERVER=http://IP:PORT
# PLAYWRIGHT_PROXY_USERNAME=...
# PLAYWRIGHT_PROXY_PASSWORD=...
PW_PROXY_SERVER = os.getenv("PLAYWRIGHT_PROXY_SERVER", "").strip()
PW_PROXY_USERNAME = os.getenv("PLAYWRIGHT_PROXY_USERNAME", "").strip()
PW_PROXY_PASSWORD = os.getenv("PLAYWRIGHT_PROXY_PASSWORD", "").strip()

# ── Proxy for downloads (playwright install) ──
# Ничего специально не делаем: если ты задашь HTTP_PROXY/HTTPS_PROXY в Render ENV,
# subprocess "python -m playwright install ..." их подхватит автоматически.

# stdout line-buffered (Render)
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

# ───────────────────────── NUM/TIME HELPERS ─────────────────────────

Q8 = Decimal("0.00000001")

# Время может быть:
#  - HH:MM:SS
#  - HH:MM
#  - YYYY-MM-DD HH:MM:SS / DD.MM.YYYY HH:MM:SS
TIME_RE = re.compile(r"\b(\d{1,2}:\d{2}(?::\d{2})?)\b")
DATE_TIME_RE = re.compile(
    r"\b(\d{4}-\d{2}-\d{2}\s+\d{1,2}:\d{2}(?::\d{2})?|\d{1,2}\.\d{1,2}\.\d{4}\s+\d{1,2}:\d{2}(?::\d{2})?)\b"
)

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

def q8_str(x: Decimal) -> str:
    return str(x.quantize(Q8, rounding=ROUND_HALF_UP))

def extract_trade_time(text: str) -> Optional[str]:
    s = (text or "").replace("\xa0", " ").strip()
    m = DATE_TIME_RE.search(s)
    if m:
        return m.group(1)
    m = TIME_RE.search(s)
    if m:
        hhmmss = m.group(1)
        # нормализуем HH:MM -> HH:MM:00, если хочешь
        if len(hhmmss.split(":")) == 2:
            hh, mm = hhmmss.split(":")
            if len(hh) == 1:
                hh = "0" + hh
            return f"{hh}:{mm}:00"
        hh, mm, ss = hhmmss.split(":")
        if len(hh) == 1:
            hh = "0" + hh
        return f"{hh}:{mm}:{ss}"
    return None

# ───────────────────────── PLAYWRIGHT INSTALL (runtime) ─────────────────────────

_last_install_ts = 0.0

def _playwright_install() -> None:
    """
    Runtime-установка браузеров Playwright (как в твоём рабочем воркере).
    cooldown 10 минут + несколько попыток.
    """
    global _last_install_ts
    now = time.time()
    if now - _last_install_ts < 600:
        logger.warning("Playwright install was attempted recently; skipping (cooldown).")
        return
    _last_install_ts = now

    # пробуем сначала headless-shell, потом chromium (или наоборот)
    attempts = [
        ["chromium-headless-shell"],
        ["chromium"],
        ["chromium", "chromium-headless-shell"],
    ]

    for i, args in enumerate(attempts, start=1):
        logger.warning(
            "Ensuring Playwright browser: python -m playwright install %s (attempt %d/%d)",
            " ".join(args), i, len(attempts)
        )
        try:
            r = subprocess.run(
                [sys.executable, "-m", "playwright", "install", *args],
                check=False,
                capture_output=True,
                text=True,
                env=os.environ.copy(),
            )
            if r.returncode == 0:
                logger.info("Playwright browsers installed: %s", " ".join(args))
                return
            logger.warning(
                "Playwright install failed: %s\ncode=%s\nSTDOUT:\n%s\nSTDERR:\n%s",
                "playwright install " + " ".join(args),
                r.returncode,
                r.stdout[-3000:],
                r.stderr[-3000:],
            )
        except Exception as e:
            logger.error("Cannot run playwright install: %s", e)

        # небольшой backoff между попытками
        time.sleep(5.0 if i == 1 else 10.0)

def _should_force_install(err: Exception) -> bool:
    s = str(err)
    return (
        "Executable doesn't exist" in s
        or "playwright install" in s
        or "chromium_headless_shell" in s
        or ("ms-playwright" in s and "doesn't exist" in s)
    )

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
                logger.error("Supabase upsert failed (%s): %s", r.status_code, r.text[:1500])
                return

        logger.info("Inserted (or ignored duplicates) %d rows into '%s'.", len(rows), SUPABASE_TABLE)

# ───────────────────────── DEDUPE KEYS ─────────────────────────

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

# ───────────────────────── PAGE HELPERS ─────────────────────────

async def safe_click(locator, timeout=5000) -> bool:
    try:
        await locator.first.click(timeout=timeout)
        return True
    except Exception:
        return False

async def is_logged_in(page: Page) -> bool:
    """
    Эвристика: если видим inputs для login — не залогинены.
    Если видим панель истории — скорее залогинены.
    """
    try:
        panel = page.locator("#panel-orderHistory")
        if await panel.count() > 0:
            return True
    except Exception:
        pass

    # признаки формы входа
    try:
        pw = page.locator('input[type="password"]')
        if await pw.count() > 0:
            return False
    except Exception:
        pass

    # fallback: не уверены — считаем, что не залогинены
    return False

async def perform_login(page: Page) -> None:
    """
    Универсальный login: ищем кнопку "Login/Sign in/Войти", затем поля email/password.
    """
    if not ABCEX_EMAIL or not ABCEX_PASSWORD:
        raise RuntimeError("ABCEX_EMAIL / ABCEX_PASSWORD are not set in environment variables.")

    # Попытка открыть/перейти на login (если кнопка есть)
    for txt in ["Login", "Sign in", "Войти", "Вход"]:
        try:
            btn = page.locator(f"text={txt}")
            if await btn.count() > 0:
                logger.info("Opening login form via '%s'...", txt)
                await safe_click(btn, timeout=8000)
                await page.wait_for_timeout(800)
                break
        except Exception:
            pass

    # Поля email / password
    email_locators = [
        'input[type="email"]',
        'input[name*="email" i]',
        'input[placeholder*="Email" i]',
        'input[placeholder*="Почта" i]',
        'input[autocomplete="email"]',
    ]
    pass_locators = [
        'input[type="password"]',
        'input[name*="pass" i]',
        'input[placeholder*="Password" i]',
        'input[placeholder*="Пароль" i]',
        'input[autocomplete="current-password"]',
    ]

    email_input = None
    for sel in email_locators:
        loc = page.locator(sel)
        if await loc.count() > 0:
            email_input = loc.first
            break

    pass_input = None
    for sel in pass_locators:
        loc = page.locator(sel)
        if await loc.count() > 0:
            pass_input = loc.first
            break

    if not email_input or not pass_input:
        raise RuntimeError("Login inputs not found (email/password). Check selectors or page flow.")

    logger.info("Filling credentials...")
    await email_input.fill(ABCEX_EMAIL)
    await pass_input.fill(ABCEX_PASSWORD)

    # Submit
    submit_candidates = [
        'button[type="submit"]',
        'input[type="submit"]',
        "text=Login",
        "text=Sign in",
        "text=Войти",
    ]
    submitted = False
    for sel in submit_candidates:
        try:
            loc = page.locator(sel)
            if await loc.count() > 0:
                submitted = await safe_click(loc, timeout=8000)
                if submitted:
                    break
        except Exception:
            pass

    if not submitted:
        # Enter как fallback
        try:
            await pass_input.press("Enter")
            submitted = True
        except Exception:
            pass

    if not submitted:
        raise RuntimeError("Could not submit login form.")

    # Ждём появления панели истории / исчезновения password input
    logger.info("Waiting for login to complete...")
    await page.wait_for_timeout(1500)

async def click_trades_tab_best_effort(page: Page) -> None:
    """
    По аналогии с твоим локальным скриптом: пытаемся открыть таб с историей/сделками.
    """
    candidates = [
        "История", "Сделки", "Trades", "Trade history", "Order history", "Orders",
        "Последние сделки",
    ]
    for t in candidates:
        try:
            loc = page.locator(f"text={t}")
            if await loc.count() > 0:
                if await safe_click(loc, timeout=4000):
                    await page.wait_for_timeout(400)
                    return
        except Exception:
            pass

async def get_history_text_items(page: Page) -> List[Dict[str, str]]:
    """
    Забираем p-элементы из #panel-orderHistory.
    Возвращаем список dict {text, className}.
    """
    panel = page.locator("#panel-orderHistory")
    if await panel.count() == 0:
        return []

    try:
        handle = await panel.element_handle()
        if not handle:
            return []
        items = await handle.evaluate(
            """
            (el) => {
              const ps = Array.from(el.querySelectorAll('p'));
              return ps
                .map(p => ({
                  text: (p.textContent || '').replace(/\\u00A0/g,' ').trim(),
                  className: (p.getAttribute('class') || '').trim(),
                }))
                .filter(x => x.text && x.text.length > 0);
            }
            """
        )
        if isinstance(items, list):
            return items
    except Exception:
        pass
    return []

def parse_items_to_trades(items: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    """
    Превращаем плоский список text элементов в строки trades.

    Поддерживаем популярные раскладки:
      - [side, price, amount, time] * N
      - [price, amount, time] * N
      - потоковый парсинг по встрече time

    Возвращаем trades как dict для Supabase.
    """
    texts = [x.get("text", "").strip() for x in items if x.get("text")]
    if not texts:
        return []

    # 1) Попробуем размер группы 4
    out: List[Dict[str, Any]] = []

    def make_trade(price_s: str, vol_s: str, time_s: str) -> Optional[Dict[str, Any]]:
        tt = extract_trade_time(time_s)
        if not tt:
            return None
        price = normalize_decimal(price_s)
        vol = normalize_decimal(vol_s)
        if price is None or vol is None:
            return None
        if price <= 0 or vol <= 0:
            return None
        volume_rub = price * vol
        return {
            "source": SOURCE,
            "symbol": SYMBOL,
            "price": q8_str(price),
            "volume_usdt": q8_str(vol),
            "volume_rub": q8_str(volume_rub),
            "trade_time": tt,
        }

    if len(texts) >= 8 and len(texts) % 4 == 0:
        groups = [texts[i:i+4] for i in range(0, len(texts), 4)]
        ok = sum(1 for g in groups if extract_trade_time(g[3]))
        if ok >= max(1, len(groups) // 2):
            for g in groups:
                # g = [side, price, amount, time] (side может быть мусором — игнорируем)
                t = make_trade(g[1], g[2], g[3])
                if t:
                    out.append(t)
            return out

    # 2) Попробуем размер группы 3
    if len(texts) >= 6 and len(texts) % 3 == 0:
        groups = [texts[i:i+3] for i in range(0, len(texts), 3)]
        ok = sum(1 for g in groups if extract_trade_time(g[2]))
        if ok >= max(1, len(groups) // 2):
            for g in groups:
                t = make_trade(g[0], g[1], g[2])
                if t:
                    out.append(t)
            return out

    # 3) Потоковый fallback: встречаем time -> берём два предыдущих числовых значения
    # (с конца панели обычно идут последние сделки)
    numeric_buf: List[str] = []
    for txt in texts:
        if extract_trade_time(txt):
            if len(numeric_buf) >= 2:
                price_s = numeric_buf[-2]
                vol_s = numeric_buf[-1]
                t = make_trade(price_s, vol_s, txt)
                if t:
                    out.append(t)
            numeric_buf = []
            continue

        n = normalize_decimal(txt)
        if n is not None:
            numeric_buf.append(txt)

    return out

async def scrape_window(page: Page) -> List[Dict[str, Any]]:
    """
    Одна попытка чтения окна (без бесконечных ожиданий).
    """
    await click_trades_tab_best_effort(page)

    items = await get_history_text_items(page)
    if not items:
        return []

    trades = parse_items_to_trades(items)
    # Подрезаем LIMIT (обычно UI отдаёт “новые сверху” — но это зависит от сайта)
    return trades[:LIMIT]

# ───────────────────────── BROWSER SESSION ─────────────────────────

def _pw_proxy_obj() -> Optional[Dict[str, str]]:
    if not PW_PROXY_SERVER:
        return None
    obj = {"server": PW_PROXY_SERVER}
    if PW_PROXY_USERNAME:
        obj["username"] = PW_PROXY_USERNAME
    if PW_PROXY_PASSWORD:
        obj["password"] = PW_PROXY_PASSWORD
    return obj

async def open_browser(pw) -> Tuple[Browser, BrowserContext, Page]:
    proxy_obj = _pw_proxy_obj()
    if proxy_obj:
        logger.info("Launching browser with proxy server=%s", proxy_obj.get("server"))

    browser = await pw.chromium.launch(
        headless=True,
        proxy=proxy_obj,
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

    await page.goto(ABCEX_TRADES_URL, wait_until="domcontentloaded", timeout=60_000)
    await page.wait_for_timeout(1000)

    if not await is_logged_in(page):
        logger.info("Not logged in — performing login...")
        await perform_login(page)
        # после логина остаёмся/переходим на страницу торгов
        await page.goto(ABCEX_TRADES_URL, wait_until="domcontentloaded", timeout=60_000)
        await page.wait_for_timeout(1000)

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
                        if (not SKIP_BROWSER_INSTALL) or _should_force_install(e):
                            _playwright_install()
                            browser, context, page = await open_browser(pw)
                        else:
                            raise
                    backoff = 2.0
                    last_reload = time.monotonic()
                    last_heartbeat = time.monotonic()

                if time.monotonic() - last_heartbeat >= HEARTBEAT_SECONDS:
                    logger.info("Heartbeat: alive. seen=%d", len(seen))
                    last_heartbeat = time.monotonic()

                if time.monotonic() - last_reload >= RELOAD_EVERY_SECONDS:
                    logger.warning("Maintenance reload...")
                    await page.goto(ABCEX_TRADES_URL, wait_until="domcontentloaded", timeout=60_000)
                    await page.wait_for_timeout(1000)
                    if not await is_logged_in(page):
                        logger.warning("Session looks logged out — re-login...")
                        await perform_login(page)
                        await page.goto(ABCEX_TRADES_URL, wait_until="domcontentloaded", timeout=60_000)
                        await page.wait_for_timeout(1000)
                    last_reload = time.monotonic()

                window = await asyncio.wait_for(
                    scrape_window(page),
                    timeout=SCRAPE_TIMEOUT_SECONDS
                )

                if not window:
                    logger.warning("No trades parsed. Reloading page...")
                    await page.goto(ABCEX_TRADES_URL, wait_until="domcontentloaded", timeout=60_000)
                    await page.wait_for_timeout(1200)
                    await asyncio.sleep(max(0.5, POLL_SECONDS))
                    continue

                # Как и в твоём Rapira: вставляем “старые -> новые”
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
                    SCRAPE_TIMEOUT_SECONDS
                )
                await safe_close(browser, context, page)
                browser = context = page = None

            except Exception as e:
                logger.error("Worker error: %s", e)

                if (not SKIP_BROWSER_INSTALL) or _should_force_install(e):
                    _playwright_install()

                logger.info("Retrying after %.1fs ...", backoff)
                await asyncio.sleep(backoff)
                backoff = min(60.0, backoff * 2)

                await safe_close(browser, context, page)
                browser = context = page = None

def main() -> None:
    asyncio.run(worker())

if __name__ == "__main__":
    main()
