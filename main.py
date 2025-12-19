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

import certifi
import httpx
from playwright.async_api import async_playwright, Page, Browser, BrowserContext

# ───────────────────── CONFIG (как на скрине Render) ─────────────────────
ABCEX_EMAIL = os.getenv("ABCEX_EMAIL", "")
ABCEX_PASSWORD = os.getenv("ABCEX_PASSWORD", "")

LIMIT = int(os.getenv("LIMIT", "200"))              # сколько строк брать за проход
POLL_SEC = float(os.getenv("POLL_SEC", "2.0"))      # частота опроса

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "exchange_trades")

# можно не задавать — дефолты ок
SOURCE = os.getenv("SOURCE", "abcex")
SYMBOL = os.getenv("SYMBOL", "USDT/RUB")

# должно совпадать с unique index в таблице
ON_CONFLICT = os.getenv("ON_CONFLICT", "source,symbol,trade_time,price,volume_usdt")

# поведение воркера
HEADLESS = os.getenv("HEADLESS", "1") == "1"
SEEN_MAX = int(os.getenv("SEEN_MAX", "20000"))
HEARTBEAT_SEC = int(os.getenv("HEARTBEAT_SEC", "30"))
UPSERT_BATCH = int(os.getenv("UPSERT_BATCH", "500"))

# ABCEX URLs / selectors (оставляем как в локальной версии)
LOGIN_URL = "https://abcex.io/en/login"
TRADING_URL = "https://abcex.io/en/trading/usdt_rub"
STATE_PATH = os.getenv("STATE_PATH", "abcex_state.json")

# ───────────────────── LOGGER ─────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
log = logging.getLogger("abcex-worker")

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# ───────────────────── DECIMAL HELPERS ─────────────────────
Q8 = Decimal("0.00000001")
TIME_RE = re.compile(r"\b\d{1,2}:\d{2}:\d{2}\b")


def q8_str(x: Decimal) -> str:
    return str(x.quantize(Q8, rounding=ROUND_HALF_UP))


def normalize_decimal(text: str) -> Optional[Decimal]:
    t = (text or "").strip()
    if not t:
        return None
    t = t.replace("\xa0", " ").replace(" ", "").replace(",", ".")
    try:
        return Decimal(t)
    except (InvalidOperation, ValueError):
        return None


def extract_time(text: str) -> Optional[str]:
    m = TIME_RE.search((text or "").replace("\xa0", " "))
    if not m:
        return None
    hh, mm, ss = m.group(0).split(":")
    if len(hh) == 1:
        hh = "0" + hh
    return f"{hh}:{mm}:{ss}"


def chunked(xs: List[Dict[str, Any]], n: int) -> List[List[Dict[str, Any]]]:
    if n <= 0:
        return [xs]
    return [xs[i:i + n] for i in range(0, len(xs), n)]


# ───────────────────── PLAYWRIGHT INSTALL (runtime) ─────────────────────
def ensure_playwright_browsers() -> None:
    """
    Без изменения Render-команд: ставим браузеры в рантайме, если их нет.
    chromium-headless-shell добавлен, чтобы не ловить историю с headless_shell.
    """
    log.warning("Installing Playwright browsers (runtime)...")
    try:
        r = subprocess.run(
            ["python", "-m", "playwright", "install", "chromium", "chromium-headless-shell"],
            check=False,
            capture_output=True,
            text=True,
        )
        if r.returncode != 0:
            log.error("playwright install failed (%s)\nSTDOUT:\n%s\nSTDERR:\n%s", r.returncode, r.stdout, r.stderr)
        else:
            log.info("Playwright browsers installed.")
    except Exception as e:
        log.error("Cannot run playwright install: %s", e)


def should_force_install(e: Exception) -> bool:
    s = str(e)
    return (
        "Executable doesn't exist" in s
        or "playwright install" in s
        or "chromium_headless_shell" in s
        or ("ms-playwright" in s and "doesn't exist" in s)
    )


def playwright_proxy_from_env() -> Optional[Dict[str, str]]:
    """
    Поддержка HTTP_PROXY/HTTPS_PROXY в формате:
      http://USER:PASS@IP:PORT
    """
    proxy_url = os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY")
    if not proxy_url:
        return None

    u = urlparse(proxy_url)
    if not u.scheme or not u.hostname or not u.port:
        log.warning("Proxy URL looks invalid: %s", proxy_url)
        return None

    proxy: Dict[str, str] = {"server": f"{u.scheme}://{u.hostname}:{u.port}"}
    if u.username:
        proxy["username"] = u.username
    if u.password:
        proxy["password"] = u.password
    return proxy


# ───────────────────── SUPABASE UPSERT ─────────────────────
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
        log.error("Supabase upsert failed (%s): %s", r.status_code, r.text)
    else:
        log.info("Inserted (or ignored duplicates) %d rows into '%s'.", len(rows), SUPABASE_TABLE)


# ───────────────────── ABCEX: helpers (из локалки, слегка усилено) ─────────────────────
async def save_debug(page: Page, html_path: str, png_path: str) -> None:
    try:
        content = await page.content()
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(content)
    except Exception:
        pass
    try:
        await page.screenshot(path=png_path, full_page=True)
    except Exception:
        pass


async def _is_login_visible(page: Page) -> bool:
    try:
        # стараемся по полю email
        loc = page.locator("input[name='email'], input[type='email']")
        return await loc.count() > 0
    except Exception:
        return False


async def login_if_needed(page: Page) -> None:
    await page.goto(TRADING_URL, wait_until="domcontentloaded", timeout=60_000)
    await page.wait_for_timeout(800)

    if not await _is_login_visible(page):
        # уже залогинен (или редиректнуло сразу в трейдинг)
        return

    if not ABCEX_EMAIL or not ABCEX_PASSWORD:
        raise RuntimeError("ABCEX_EMAIL/ABCEX_PASSWORD not set")

    log.info("Login required. Going to login page...")
    await page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=60_000)
    await page.wait_for_timeout(800)

    email_input = page.locator("input[name='email'], input[type='email']")
    pass_input = page.locator("input[name='password'], input[type='password']")

    await email_input.first.fill(ABCEX_EMAIL)
    await pass_input.first.fill(ABCEX_PASSWORD)

    # кнопка submit (best effort)
    candidates = [
        "button[type='submit']",
        "button:has-text('Login')",
        "button:has-text('Войти')",
        "button:has-text('Sign in')",
    ]
    clicked = False
    for sel in candidates:
        try:
            btn = page.locator(sel)
            if await btn.count() > 0:
                await btn.first.click(timeout=5_000)
                clicked = True
                break
        except Exception:
            pass

    if not clicked:
        await save_debug(page, "abcex_login_no_button.html", "abcex_login_no_button.png")
        raise RuntimeError("Login button not found. See debug files.")

    await page.wait_for_timeout(1500)

    # ждём, что форма исчезнет
    for _ in range(20):
        if not await _is_login_visible(page):
            log.info("Login successful (form disappeared).")
            return
        await page.wait_for_timeout(500)

    await save_debug(page, "abcex_login_failed.html", "abcex_login_failed.png")
    raise RuntimeError("Login failed (login form still visible). Possible 2FA/captcha/flow change.")


async def click_trades_tab_best_effort(page: Page) -> None:
    candidates = ["Сделки", "История", "Order history", "Trades"]
    for t in candidates:
        try:
            tab = page.locator(f"text={t}")
            if await tab.count() > 0:
                await tab.first.click(timeout=5_000)
                await page.wait_for_timeout(400)
                return
        except Exception:
            pass


ORDERHISTORY_ROWS_SELECTOR = "div#panel-orderHistory table tbody tr"


async def parse_order_history_rows(page: Page) -> List[Dict[str, Any]]:
    rows = await page.query_selector_all(ORDERHISTORY_ROWS_SELECTOR)
    out: List[Dict[str, Any]] = []

    for row in rows[:LIMIT]:
        try:
            tds = await row.query_selector_all("td")
            if len(tds) < 3:
                continue

            texts = [(await td.inner_text()).strip() for td in tds]
            # эвристика: обычно [time, price, qty] или [time, side, price, qty] и т.п.
            # найдём время
            trade_time = None
            time_idx = None
            for i, txt in enumerate(texts):
                tt = extract_time(txt)
                if tt:
                    trade_time = tt
                    time_idx = i
                    break
            if not trade_time:
                continue

            # вытащим числа
            nums: List[Decimal] = []
            for i, txt in enumerate(texts):
                if i == time_idx:
                    continue
                d = normalize_decimal(txt)
                if d is not None:
                    nums.append(d)

            if len(nums) < 2:
                continue

            # цена (обычно ~ 40..200)
            price = None
            for n in nums:
                if Decimal("40") <= n <= Decimal("200"):
                    price = n
                    break
            if price is None:
                price = nums[0]

            # qty (USDT) — другое число
            qty = None
            for n in nums:
                if n != price:
                    qty = n
                    break
            if qty is None:
                qty = nums[1]

            if price <= 0 or qty <= 0:
                continue

            # сторона
            side = ""
            for txt in texts:
                s = txt.strip().lower()
                if s in ("buy", "sell"):
                    side = s
                    break

            out.append({"time": trade_time, "price": price, "qty": qty, "side": side})
        except Exception:
            continue

    return out


def to_exchange_trade(row: Dict[str, Any]) -> Dict[str, Any]:
    price: Decimal = row["price"]
    qty: Decimal = row["qty"]
    volume_rub = price * qty

    return {
        "source": SOURCE,
        "symbol": SYMBOL,
        "price": q8_str(price),
        "volume_usdt": q8_str(qty),
        "volume_rub": q8_str(volume_rub),
        "trade_time": row["time"],  # HH:MM:SS
    }


# ───────────────────── DEDUP KEYS ─────────────────────
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


# ───────────────────── BROWSER SESSION ─────────────────────
async def open_session(pw) -> Tuple[Browser, BrowserContext, Page]:
    proxy = playwright_proxy_from_env()

    browser = await pw.chromium.launch(
        headless=HEADLESS,
        proxy=proxy,
        args=["--no-sandbox", "--disable-dev-shm-usage"],
    )

    storage_state = STATE_PATH if os.path.exists(STATE_PATH) else None
    if storage_state:
        log.info("Using saved session state: %s", storage_state)

    context = await browser.new_context(
        storage_state=storage_state,
        viewport={"width": 1440, "height": 810},
        locale="ru-RU",
        timezone_id="Europe/Moscow",
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
    )
    page = await context.new_page()

    await login_if_needed(page)
    await page.goto(TRADING_URL, wait_until="domcontentloaded", timeout=60_000)
    await page.wait_for_timeout(800)
    await click_trades_tab_best_effort(page)
    await page.wait_for_timeout(500)

    # сохраняем state
    try:
        await context.storage_state(path=STATE_PATH)
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


# ───────────────────── WORKER LOOP ─────────────────────
async def worker() -> None:
    seen: Set[TradeKey] = set()
    seen_q: Deque[TradeKey] = deque(maxlen=SEEN_MAX)

    backoff = 2.0
    last_heartbeat = 0.0

    async with httpx.AsyncClient(
        timeout=30,
        verify=certifi.where(),
        follow_redirects=True,
        trust_env=True,  # подхватит HTTP(S)_PROXY для Supabase, если нужно
    ) as http:

        async with async_playwright() as pw:
            browser: Optional[Browser] = None
            context: Optional[BrowserContext] = None
            page: Optional[Page] = None

            while True:
                t0 = time.time()
                try:
                    if page is None:
                        log.info("Starting browser session...")
                        try:
                            browser, context, page = await open_session(pw)
                        except Exception as e:
                            if should_force_install(e):
                                ensure_playwright_browsers()
                                browser, context, page = await open_session(pw)
                            else:
                                raise

                        backoff = 2.0

                    # парсим окно
                    raw_rows = await parse_order_history_rows(page)
                    if not raw_rows:
                        log.warning("No rows parsed. Reloading trading page...")
                        await page.goto(TRADING_URL, wait_until="domcontentloaded", timeout=60_000)
                        await page.wait_for_timeout(800)
                        await click_trades_tab_best_effort(page)
                        await asyncio.sleep(max(0.5, POLL_SEC))
                        continue

                    window = [to_exchange_trade(r) for r in raw_rows]

                    # новые: старые -> новые (для последовательной записи)
                    new_rows: List[Dict[str, Any]] = []
                    for t in reversed(window):
                        k = trade_key(t)
                        if k in seen:
                            continue
                        new_rows.append(t)
                        seen.add(k)
                        seen_q.append(k)

                    # синхронизация set/deque
                    if len(seen) > len(seen_q) + 200:
                        seen = set(seen_q)

                    if new_rows:
                        log.info("Parsed %d new trades. Newest: %s", len(new_rows), json.dumps(new_rows[-1], ensure_ascii=False))
                        for batch in chunked(new_rows, UPSERT_BATCH):
                            await supabase_upsert(http, batch)
                    else:
                        # не спамим слишком часто, но логируем что живы
                        log.info("No new trades.")

                    # heartbeat
                    now = time.time()
                    if now - last_heartbeat >= HEARTBEAT_SEC:
                        log.info("Heartbeat: alive | poll=%.2fs | window=%d | seen=%d", POLL_SEC, len(window), len(seen))
                        last_heartbeat = now

                    # небольшой джиттер
                    dt = time.time() - t0
                    sleep_s = max(0.35, POLL_SEC - dt) + random.uniform(0, 0.2)
                    await asyncio.sleep(sleep_s)

                except Exception as e:
                    log.error("Worker error: %s", e)

                    if should_force_install(e):
                        ensure_playwright_browsers()

                    log.info("Retrying after %.1fs ...", backoff)
                    await asyncio.sleep(backoff)
                    backoff = min(60.0, backoff * 2)

                    await safe_close(browser, context, page)
                    browser = context = page = None


def main() -> None:
    asyncio.run(worker())


if __name__ == "__main__":
    main()
