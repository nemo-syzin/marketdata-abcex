import asyncio
import json
import logging
import os
import random
import re
import subprocess
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

import httpx
from playwright.async_api import async_playwright, Page, Browser, BrowserContext

# ───────────────────────── CONFIG (ENV как на скрине) ─────────────────────────

ABCEX_URL = os.getenv("ABCEX_URL", "https://abcex.io/spot/USDT-RUB")
ABCEX_EMAIL = os.getenv("ABCEX_EMAIL", "")
ABCEX_PASSWORD = os.getenv("ABCEX_PASSWORD", "")

LIMIT = int(os.getenv("LIMIT", "200"))         # сколько строк/элементов смотреть за проход
POLL_SEC = float(os.getenv("POLL_SEC", "1.5")) # частота опроса

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "exchange_trades")

# логические дефолты
SOURCE = os.getenv("SOURCE", "abcex")
SYMBOL = os.getenv("SYMBOL", "USDT/RUB")

# должно совпадать с уникальным индексом в Supabase:
# create unique index ... (source, symbol, trade_time, price, volume_usdt)
ON_CONFLICT = os.getenv("ON_CONFLICT", "source,symbol,trade_time,price,volume_usdt")

# локальная защита от дублей/переупорядочивания
SEEN_MAX = int(os.getenv("SEEN_MAX", "20000"))

# чтобы воркер не “молчал”
HEARTBEAT_SEC = int(os.getenv("HEARTBEAT_SEC", "30"))

# не обязательно, но удобно для отладки
DEBUG_SNAPSHOTS = os.getenv("DEBUG_SNAPSHOTS", "0") == "1"

# ───────────────────────── LOGGER ─────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("abcex-worker")

# ───────────────────────── DECIMALS ─────────────────────────

Q8 = Decimal("0.00000001")


def q8_str(x: Decimal) -> str:
    return str(x.quantize(Q8, rounding=ROUND_HALF_UP))


def parse_decimal(text: str) -> Optional[Decimal]:
    s = (text or "").strip()
    if not s:
        return None
    s = s.replace("\xa0", " ").replace(" ", "")
    s = s.replace(",", ".")
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


# ───────────────────────── PLAYWRIGHT INSTALL (как в твоём main.py) ─────────────────────────

def ensure_playwright_browsers() -> None:
    """
    Ровно как в приложенном main.py: ставим chromium и chromium-headless-shell в рантайме.
    Не требует менять build/start команды на Render.
    """
    log.warning("Installing Playwright browsers (runtime)...")
    r = subprocess.run(
        ["playwright", "install", "chromium", "chromium-headless-shell"],
        check=False,
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        log.error(
            "playwright install failed (%s)\nSTDOUT:\n%s\nSTDERR:\n%s",
            r.returncode, r.stdout, r.stderr
        )
    else:
        log.info("Playwright browsers installed.")


def should_install_playwright(err: Exception) -> bool:
    s = str(err)
    return (
        "Executable doesn't exist" in s
        or "playwright install" in s
        or "chromium_headless_shell" in s
        or ("ms-playwright" in s and "doesn't exist" in s)
    )


# ───────────────────────── PARSING (ABCeX UI) ─────────────────────────

TIME_RE = re.compile(r"^\d{2}:\d{2}:\d{2}$")


def looks_like_time(s: str) -> bool:
    return bool(TIME_RE.match((s or "").strip()))


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


async def accept_cookies(page: Page) -> None:
    for label in ["Я согласен", "Принять", "Accept"]:
        try:
            btn = page.locator(f"text={label}")
            if await btn.count() > 0:
                log.info("Cookies banner: clicking '%s'...", label)
                await btn.first.click(timeout=5_000)
                await page.wait_for_timeout(800)
                return
        except Exception:
            pass


async def save_debug_snapshot(page: Page, prefix: str) -> None:
    if not DEBUG_SNAPSHOTS:
        return
    try:
        await page.screenshot(path=f"{prefix}.png", full_page=True)
        html = await page.content()
        with open(f"{prefix}.html", "w", encoding="utf-8") as f:
            f.write(html)
        log.info("Saved debug snapshot: %s.(png/html)", prefix)
    except Exception as e:
        log.warning("Snapshot failed: %s", e)


async def try_login(page: Page, email: str, password: str) -> None:
    if not email or not password:
        raise RuntimeError("ABCEX_EMAIL / ABCEX_PASSWORD пустые.")

    # На разных сборках UI могут отличаться селекторы — держим несколько вариантов.
    email_selectors = [
        "input[type='email']",
        "input[name='email']",
        "input[placeholder*='Email' i]",
        "input[placeholder*='Почт' i]",
    ]
    pass_selectors = [
        "input[type='password']",
        "input[name='password']",
        "input[placeholder*='Password' i]",
        "input[placeholder*='Пароль' i]",
    ]

    email_ok = False
    for sel in email_selectors:
        try:
            loc = page.locator(sel)
            if await loc.count() > 0:
                await loc.first.fill(email, timeout=10_000)
                email_ok = True
                break
        except Exception:
            pass

    pass_ok = False
    for sel in pass_selectors:
        try:
            loc = page.locator(sel)
            if await loc.count() > 0:
                await loc.first.fill(password, timeout=10_000)
                pass_ok = True
                break
        except Exception:
            pass

    if not email_ok or not pass_ok:
        await save_debug_snapshot(page, "abcex_login_fields_not_found")
        raise RuntimeError("Не смог найти поля email/password в UI ABCEX.")

    # Кнопка "Войти"
    login_selectors = [
        "button:has-text('Войти')",
        "button:has-text('Login')",
        "button[type='submit']",
    ]
    clicked = False
    for sel in login_selectors:
        try:
            btn = page.locator(sel)
            if await btn.count() > 0:
                await btn.first.click(timeout=10_000)
                clicked = True
                break
        except Exception:
            pass

    if not clicked:
        await save_debug_snapshot(page, "abcex_login_button_not_found")
        raise RuntimeError("Не смог найти кнопку Login/Войти.")

    await page.wait_for_timeout(1000)
    try:
        await page.wait_for_load_state("networkidle", timeout=30_000)
    except Exception:
        pass

    # если форма всё ещё видна — логин не прошёл
    try:
        if await page.locator("input[type='password']").count() > 0:
            await save_debug_snapshot(page, "abcex_login_failed")
            raise RuntimeError("Логин не прошёл (форма логина всё ещё видна). Возможна капча/2FA/иной флоу.")
    except Exception:
        # если селектор не нашёлся — ок
        pass


async def open_order_history_panel(page: Page) -> None:
    """
    ABCEX: пытаемся переключиться на таб "История" / "История ордеров" / "Последние сделки".
    Селекторы оставляем максимально терпимыми.
    """
    candidates = [
        "text=История",
        "text=История ордеров",
        "text=История сделок",
        "text=Последние сделки",
        "text=Order History",
        "text=Trade History",
        "text=History",
    ]
    for sel in candidates:
        try:
            tab = page.locator(sel)
            if await tab.count() > 0:
                await tab.first.click(timeout=8_000)
                await page.wait_for_timeout(800)
                return
        except Exception:
            pass


async def wait_trades_visible(page: Page, timeout_ms: int = 25_000) -> None:
    start = datetime.utcnow().timestamp()
    while (datetime.utcnow().timestamp() - start) * 1000 < timeout_ms:
        try:
            # грубо: если видим HH:MM:SS в p — значит блок сделок подгрузился
            ps = page.locator("p:has-text(':')")
            cnt = await ps.count()
            if cnt > 0:
                for i in range(min(cnt, 30)):
                    txt = (await ps.nth(i).inner_text()).strip()
                    if looks_like_time(txt):
                        log.info("Trades visible (time cells detected).")
                        return
        except Exception:
            pass
        await page.wait_for_timeout(800)

    await save_debug_snapshot(page, "abcex_trades_not_visible")
    raise RuntimeError("Не дождался появления сделок (HH:MM:SS) на ABCEX.")


async def extract_trades_from_panel(page: Page, max_items: int) -> List[Dict[str, Any]]:
    """
    Эвристика как в твоём abcex.py:
    ищем в панели элементы, где подряд встречаются 3 p: [price, qty, time].
    """
    panel = page.locator("div.panel-orderHistory")
    if await panel.count() == 0:
        await save_debug_snapshot(page, "abcex_no_panel_orderhistory")
        raise RuntimeError("Не нашёл panel-orderHistory.")

    # берём первую видимую панель
    panel_handle = None
    for i in range(await panel.count()):
        if await panel.nth(i).is_visible():
            panel_handle = panel.nth(i)
            break
    if panel_handle is None:
        await save_debug_snapshot(page, "abcex_no_visible_panel")
        raise RuntimeError("panel-orderHistory есть, но ни одна не видима.")

    ps = panel_handle.locator("p")
    cnt = await ps.count()
    if cnt < 3:
        return []

    # собираем тексты
    texts: List[str] = []
    for i in range(min(cnt, max_items * 10)):  # запас, потому что p много
        t = (await ps.nth(i).inner_text()).strip()
        if t:
            texts.append(t)

    rows: List[Dict[str, Any]] = []
    # ищем шаблон price/qty/time
    # price: число 40..200, qty: любое число >0, time: HH:MM:SS
    for i in range(len(texts) - 2):
        t0, t1, t2 = texts[i], texts[i + 1], texts[i + 2]
        if not looks_like_time(t2):
            continue

        price = parse_decimal(t0)
        qty = parse_decimal(t1)
        if price is None or qty is None:
            continue
        if not (Decimal("40") <= price <= Decimal("200")):
            continue
        if qty <= 0 or price <= 0:
            continue

        volume_rub = price * qty
        rows.append(
            {
                "source": SOURCE,
                "symbol": SYMBOL,
                "price": q8_str(price),
                "volume_usdt": q8_str(qty),
                "volume_rub": q8_str(volume_rub),
                "trade_time": t2,
            }
        )

        if len(rows) >= max_items:
            break

    return rows


# ───────────────────────── SUPABASE UPSERT ─────────────────────────

async def supabase_upsert(rows: List[Dict[str, Any]]) -> None:
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

    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(url, headers=headers, params=params, json=rows)
        if r.status_code >= 300:
            log.error("Supabase upsert failed (%s): %s", r.status_code, r.text)
        else:
            log.info("Inserted (or ignored duplicates) %d rows into '%s'.", len(rows), SUPABASE_TABLE)


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
    await page.goto(ABCEX_URL, wait_until="networkidle", timeout=60_000)
    await page.wait_for_timeout(1500)
    await accept_cookies(page)
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
                        if should_install_playwright(e):
                            ensure_playwright_browsers()
                            browser, context, page = await open_browser(pw)
                        else:
                            raise

                    # логин
                    await try_login(page, ABCEX_EMAIL, ABCEX_PASSWORD)

                    # открыть историю и дождаться сделок
                    await open_order_history_panel(page)
                    await wait_trades_visible(page)

                    backoff = 2.0

                # опрос
                await open_order_history_panel(page)

                window = await extract_trades_from_panel(page, max_items=LIMIT)
                if not window:
                    log.warning("No trades parsed. Reloading page...")
                    await page.goto(ABCEX_URL, wait_until="networkidle", timeout=60_000)
                    await page.wait_for_timeout(1500)
                    await accept_cookies(page)
                    await try_login(page, ABCEX_EMAIL, ABCEX_PASSWORD)
                    await open_order_history_panel(page)
                    await wait_trades_visible(page)
                    await asyncio.sleep(max(0.5, POLL_SEC))
                    continue

                # вставляем старые -> новые
                new_rows: List[Dict[str, Any]] = []
                for t in reversed(window):
                    k = trade_key(t)
                    if k in seen:
                        continue
                    new_rows.append(t)
                    seen.add(k)
                    seen_q.append(k)

                # если deque вытеснил — пересобираем set
                if len(seen) > len(seen_q) + 200:
                    seen = set(seen_q)

                if new_rows:
                    log.info("Parsed %d new trades. Newest: %s", len(new_rows), json.dumps(new_rows[-1], ensure_ascii=False))
                    await supabase_upsert(new_rows)
                else:
                    log.info("No new trades.")

                # heartbeat
                now = asyncio.get_event_loop().time()
                if now - last_heartbeat >= HEARTBEAT_SEC:
                    log.info("Heartbeat: alive | url=%s | poll=%.2fs | seen=%d", ABCEX_URL, POLL_SEC, len(seen))
                    last_heartbeat = now

                # sleep + jitter
                sleep_s = max(0.35, POLL_SEC + random.uniform(-0.15, 0.15))
                await asyncio.sleep(sleep_s)

            except Exception as e:
                log.error("Worker error: %s", e)

                # если отвалилась установка/исполняемый файл браузера — поставим и перезапустим сессию
                if should_install_playwright(e):
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
