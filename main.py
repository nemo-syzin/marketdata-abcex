import asyncio
import json
import logging
import os
import random
import re
import subprocess
import time
from collections import deque
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Deque, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

import httpx
from playwright.async_api import async_playwright, Browser, BrowserContext, Page

# ───────────────────────── CONFIG ─────────────────────────

ABCEX_URL = os.getenv("ABCEX_URL", os.getenv("ABCEX_BASE_URL", "https://abcex.io/spot/USDT-RUB"))
SOURCE = os.getenv("SOURCE", "abcex")
SYMBOL = os.getenv("SYMBOL", "USDT/RUB")

ABCEX_EMAIL = os.getenv("ABCEX_EMAIL", "")
ABCEX_PASSWORD = os.getenv("ABCEX_PASSWORD", "")

LIMIT = int(os.getenv("LIMIT", "200"))                 # сколько строк с UI за проход
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "1.5")) # частота опроса

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "exchange_trades")

ON_CONFLICT = os.getenv("ON_CONFLICT", "source,symbol,trade_time,price,volume_usdt")

SEEN_MAX = int(os.getenv("SEEN_MAX", "20000"))
UPSERT_BATCH = int(os.getenv("UPSERT_BATCH", "500"))
HEARTBEAT_SEC = int(os.getenv("HEARTBEAT_SEC", "30"))

# Playwright install behavior
SKIP_BROWSER_INSTALL = os.getenv("SKIP_BROWSER_INSTALL", "0") == "1"
INSTALL_RETRIES = int(os.getenv("PW_INSTALL_RETRIES", "6"))   # ретраи скачивания браузера
INSTALL_TIMEOUT = int(os.getenv("PW_INSTALL_TIMEOUT", "900")) # сек на одну попытку
INSTALL_SLEEP_BASE = float(os.getenv("PW_INSTALL_SLEEP_BASE", "2.0"))

# Proxy (поддерживаем и готовую строку, и раздельные поля)
# Вариант А: PROXY_SERVER="http://user:pass@host:port"
# Вариант Б: PROXY_HOST/PROXY_PORT/PROXY_USERNAME/PROXY_PASSWORD
PROXY_SERVER = os.getenv("PROXY_SERVER", "").strip()
PROXY_HOST = os.getenv("PROXY_HOST", "").strip()
PROXY_PORT = os.getenv("PROXY_PORT", "").strip()
PROXY_USERNAME = os.getenv("PROXY_USERNAME", "").strip()
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD", "").strip()

# ───────────────────────── LOGGING ─────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("abcex-worker")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

TIME_RE = re.compile(r"\b\d{1,2}:\d{2}:\d{2}\b")
Q8 = Decimal("0.00000001")


# ───────────────────────── PROXY HELPERS ─────────────────────────

def build_proxy_url() -> Optional[str]:
    """
    Возвращает proxy URL вида http://user:pass@host:port
    Поддерживает:
      - PROXY_SERVER (готовая строка)
      - PROXY_HOST/PROXY_PORT + (опц) PROXY_USERNAME/PROXY_PASSWORD
    """
    if PROXY_SERVER:
        # если забыли схему — добавим http://
        if "://" not in PROXY_SERVER:
            return f"http://{PROXY_SERVER}"
        return PROXY_SERVER

    if not (PROXY_HOST and PROXY_PORT):
        return None

    if PROXY_USERNAME and PROXY_PASSWORD:
        return f"http://{PROXY_USERNAME}:{PROXY_PASSWORD}@{PROXY_HOST}:{PROXY_PORT}"
    return f"http://{PROXY_HOST}:{PROXY_PORT}"


def playwright_proxy_dict(proxy_url: str) -> Dict[str, str]:
    """
    Превращает proxy_url в dict для playwright launch(proxy=...).
    """
    u = urlparse(proxy_url)
    server = f"{u.scheme}://{u.hostname}:{u.port}"
    out: Dict[str, str] = {"server": server}
    if u.username:
        out["username"] = u.username
    if u.password:
        out["password"] = u.password
    return out


def env_with_proxy(base: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """
    Проставляет HTTP(S)_PROXY для subprocess/install (и вообще для всего окружения).
    """
    env = dict(base or os.environ)
    px = build_proxy_url()
    if px:
        env.setdefault("HTTP_PROXY", px)
        env.setdefault("HTTPS_PROXY", px)
        env.setdefault("ALL_PROXY", px)
    return env


# ───────────────────────── DECIMAL HELPERS ─────────────────────────

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


def extract_time(text: str) -> Optional[str]:
    m = TIME_RE.search((text or "").replace("\xa0", " "))
    if not m:
        return None
    hh, mm, ss = m.group(0).split(":")
    if len(hh) == 1:
        hh = "0" + hh
    return f"{hh}:{mm}:{ss}"


# ───────────────────────── PLAYWRIGHT INSTALL (ROBUST) ─────────────────────────

def _playwright_install_once() -> Tuple[int, str, str]:
    cmd = ["python", "-m", "playwright", "install", "chromium", "chromium-headless-shell"]
    env = env_with_proxy()
    r = subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
        env=env,
        timeout=INSTALL_TIMEOUT,
    )
    return r.returncode, r.stdout, r.stderr


def _is_browser_installed_fast() -> bool:
    """
    Быстрая проверка: пробуем получить executable_path и проверить наличие файла.
    """
    try:
        from playwright.sync_api import sync_playwright  # type: ignore
        with sync_playwright() as pw:
            path = pw.chromium.executable_path
        return bool(path and os.path.exists(path))
    except Exception:
        return False


def ensure_playwright_browsers() -> None:
    if SKIP_BROWSER_INSTALL:
        log.warning("SKIP_BROWSER_INSTALL=1, skipping browser install.")
        return

    if _is_browser_installed_fast():
        log.info("Playwright browser already installed, skipping download.")
        return

    for attempt in range(1, INSTALL_RETRIES + 1):
        log.warning("Installing Playwright browsers (runtime)... attempt %d/%d", attempt, INSTALL_RETRIES)
        try:
            code, out, err = _playwright_install_once()
            if code == 0:
                log.info("Playwright browsers installed.")
                return

            log.error("playwright install failed (code=%s).", code)
            if out.strip():
                log.error("STDOUT:\n%s", out[-2000:])
            if err.strip():
                log.error("STDERR:\n%s", err[-2000:])

        except subprocess.TimeoutExpired:
            log.error("playwright install timed out after %ss.", INSTALL_TIMEOUT)
        except Exception as e:
            log.error("Cannot run playwright install: %s", e)

        sleep_s = min(60.0, INSTALL_SLEEP_BASE * (2 ** (attempt - 1)))
        log.info("Retrying browser install after %.1fs ...", sleep_s)
        time.sleep(sleep_s)

    raise RuntimeError("Failed to install Playwright browsers after multiple retries.")


# ───────────────────────── SUPABASE ─────────────────────────

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


def chunked(xs: List[Dict[str, Any]], n: int) -> List[List[Dict[str, Any]]]:
    if n <= 0:
        return [xs]
    return [xs[i:i + n] for i in range(0, len(xs), n)]


async def supabase_upsert(client: httpx.AsyncClient, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    if not SUPABASE_URL or not SUPABASE_KEY:
        log.warning("SUPABASE_URL or SUPABASE_KEY not set; skipping insert.")
        return

    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
    params = {"on_conflict": ON_CONFLICT}
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=ignore-duplicates,return=minimal",
    }

    r = await client.post(url, headers=headers, params=params, json=rows)
    if r.status_code >= 300:
        log.error("Supabase upsert failed (%s): %s", r.status_code, r.text[:1500])
    else:
        log.info("Inserted (or ignored duplicates) %d rows into '%s'.", len(rows), SUPABASE_TABLE)


# ───────────────────────── ABCEX PAGE PARSING ─────────────────────────

async def accept_cookies_if_any(page: Page) -> None:
    for label in ["Я согласен", "Принять", "Accept", "I agree", "Agree"]:
        try:
            btn = page.locator(f"text={label}")
            if await btn.count() > 0 and await btn.first.is_visible():
                log.info("Found cookies banner, clicking '%s'...", label)
                await btn.first.click(timeout=5_000)
                await page.wait_for_timeout(300)
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
    if not email or not password:
        return
    if not await _is_login_visible(page):
        return

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

    for sel in email_candidates:
        loc = page.locator(sel)
        try:
            if await loc.count() > 0 and await loc.first.is_visible():
                await loc.first.fill(email, timeout=10_000)
                break
        except Exception:
            continue

    for sel in pw_candidates:
        loc = page.locator(sel)
        try:
            if await loc.count() > 0 and await loc.first.is_visible():
                await loc.first.fill(password, timeout=10_000)
                break
        except Exception:
            continue

    # кнопка входа
    btn_candidates = [
        "button:has-text('Войти')",
        "button:has-text('Login')",
        "button:has-text('Sign in')",
        "button[type='submit']",
    ]
    clicked = False
    for sel in btn_candidates:
        btn = page.locator(sel)
        try:
            if await btn.count() > 0 and await btn.first.is_visible():
                await btn.first.click(timeout=10_000)
                clicked = True
                break
        except Exception:
            continue

    if clicked:
        await page.wait_for_timeout(1500)
        log.info("Login submitted.")


async def open_market_trades_panel(page: Page) -> None:
    """
    Открывает вкладку/панель Market Trades (названия могут отличаться).
    """
    candidates = [
        "text=Market Trades",
        "text=Сделки",
        "text=Последние сделки",
        "text=Trades",
        "text=История сделок",
    ]
    for sel in candidates:
        try:
            loc = page.locator(sel)
            if await loc.count() > 0 and await loc.first.is_visible():
                await loc.first.click(timeout=7_000)
                await page.wait_for_timeout(300)
                return
        except Exception:
            continue


async def extract_trades_from_panel(page: Page, limit: int) -> List[Dict[str, Any]]:
    """
    Универсальная эвристика:
    - берём строки таблицы/листинга сделок
    - выдёргиваем время HH:MM:SS + два числа (price, qty)
    """
    # пробуем несколько контейнеров
    row_selectors = [
        "table tbody tr",
        "div:has(.trade):has-text(':')",
        "div.trade-row",
        "div[class*='trade']",
    ]

    rows: List[Any] = []
    for rs in row_selectors:
        try:
            r = await page.query_selector_all(rs)
            if r and len(r) >= 3:
                rows = r
                break
        except Exception:
            continue

    if not rows:
        return []

    out: List[Dict[str, Any]] = []

    for row in rows[: max(10, limit)]:
        try:
            txt = (await row.inner_text()).strip()
            t = extract_time(txt)
            if not t:
                continue

            # найдём все "похожие на число" токены
            # обычно в строке есть price и qty
            tokens = re.findall(r"[0-9][0-9\s\xa0.,]*", txt)
            nums: List[Decimal] = []
            for tok in tokens:
                d = normalize_decimal(tok)
                if d is not None:
                    nums.append(d)

            if len(nums) < 2:
                continue

            # эвристика: цена ~ 40..200, объём (qty) обычно другой порядок
            price = None
            for d in nums:
                if Decimal("40") <= d <= Decimal("200"):
                    price = d
                    break
            if price is None:
                price = nums[0]

            qty = None
            for d in nums:
                if d != price:
                    qty = d
                    break
            if qty is None:
                qty = nums[1]

            if price <= 0 or qty <= 0:
                continue

            volume_rub = price * qty

            out.append(
                {
                    "source": SOURCE,
                    "symbol": SYMBOL,
                    "price": q8_str(price),
                    "volume_usdt": q8_str(qty),
                    "volume_rub": q8_str(volume_rub),
                    "trade_time": t,
                }
            )
        except Exception:
            continue

        if len(out) >= limit:
            break

    return out


# ───────────────────────── BROWSER SESSION ─────────────────────────

async def open_browser(pw) -> Tuple[Browser, BrowserContext, Page]:
    px = build_proxy_url()
    proxy_dict = playwright_proxy_dict(px) if px else None

    browser = await pw.chromium.launch(
        headless=True,
        args=["--no-sandbox", "--disable-dev-shm-usage"],
        proxy=proxy_dict,
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
    await page.goto(ABCEX_URL, wait_until="domcontentloaded", timeout=60_000)
    await page.wait_for_timeout(1000)
    await accept_cookies_if_any(page)
    await login_if_needed(page, ABCEX_EMAIL, ABCEX_PASSWORD)
    await open_market_trades_panel(page)
    await page.wait_for_timeout(700)

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
    seen_q: Deque[TradeKey] = deque(maxlen=SEEN_MAX)

    backoff = 2.0
    last_heartbeat = 0.0

    # httpx client (Supabase) тоже умеет прокси через env, но мы явно доверяем env
    async with httpx.AsyncClient(timeout=30, trust_env=True) as sclient:
        # гарантируем браузеры до старта
        try:
            await asyncio.to_thread(ensure_playwright_browsers)
        except Exception as e:
            log.error("Playwright install fatal: %s", e)
            # дальше всё равно попробуем: вдруг уже есть браузер
            pass

        async with async_playwright() as pw:
            browser: Optional[Browser] = None
            context: Optional[BrowserContext] = None
            page: Optional[Page] = None

            while True:
                t0 = time.time()
                try:
                    if page is None:
                        log.info("Starting browser session...")
                        # если запуск не удался — пробуем доустановить и повторить
                        try:
                            browser, context, page = await open_browser(pw)
                        except Exception as e:
                            log.error("Browser open failed: %s", e)
                            if not SKIP_BROWSER_INSTALL:
                                await asyncio.to_thread(ensure_playwright_browsers)
                            browser, context, page = await open_browser(pw)
                        backoff = 2.0

                    # держим вкладку с сделками
                    await open_market_trades_panel(page)

                    window = await extract_trades_from_panel(page, LIMIT)
                    if not window:
                        log.warning("No rows parsed. Reloading page...")
                        await page.goto(ABCEX_URL, wait_until="domcontentloaded", timeout=60_000)
                        await page.wait_for_timeout(1000)
                        await accept_cookies_if_any(page)
                        await login_if_needed(page, ABCEX_EMAIL, ABCEX_PASSWORD)
                        await open_market_trades_panel(page)
                        await asyncio.sleep(max(0.5, POLL_SECONDS))
                        continue

                    new_rows: List[Dict[str, Any]] = []
                    # как обычно: пишем старые -> новые
                    for r in reversed(window):
                        k = trade_key(r)
                        if k in seen:
                            continue
                        new_rows.append(r)
                        seen.add(k)
                        seen_q.append(k)

                    # синхронизируем set при переполнении deque
                    if len(seen) > len(seen_q) + 200:
                        seen = set(seen_q)

                    if new_rows:
                        log.info("Parsed %d new trades. Newest: %s", len(new_rows), json.dumps(new_rows[-1], ensure_ascii=False))
                        for batch in chunked(new_rows, UPSERT_BATCH):
                            await supabase_upsert(sclient, batch)
                    else:
                        # не спамим каждую итерацию
                        pass

                    now = time.time()
                    if now - last_heartbeat >= HEARTBEAT_SEC:
                        log.info(
                            "Heartbeat: alive | poll=%.2fs | window=%d | seen=%d | url=%s",
                            POLL_SECONDS, len(window), len(seen), ABCEX_URL
                        )
                        last_heartbeat = now

                    dt = time.time() - t0
                    sleep_s = max(0.35, POLL_SECONDS - dt + random.uniform(-0.10, 0.10))
                    await asyncio.sleep(sleep_s)
                    backoff = 2.0

                except Exception as e:
                    log.error("Worker error: %s", e)
                    log.info("Retrying after %.1fs ...", backoff)
                    await asyncio.sleep(backoff)
                    backoff = min(60.0, backoff * 2)

                    await safe_close(browser, context, page)
                    browser = context = page = None


def main() -> None:
    asyncio.run(worker())


if __name__ == "__main__":
    main()
