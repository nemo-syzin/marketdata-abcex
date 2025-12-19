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
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, unquote

import certifi
import httpx
from playwright.async_api import async_playwright, Page, Browser, BrowserContext

# ───────────────────────── EARLY ENV DEFAULTS ─────────────────────────
# Важно: путь к браузерам должен быть writable на Render
os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "/opt/render/.cache/ms-playwright")
# Чтобы Render не буферизовал логи
os.environ.setdefault("PYTHONUNBUFFERED", "1")

# ───────────────────────── CONFIG ─────────────────────────
ABCEX_URL = os.getenv("ABCEX_URL", "https://abcex.io/spot/USDT-RUB")
SOURCE = os.getenv("SOURCE", "abcex")
SYMBOL = os.getenv("SYMBOL", "USDT/RUB")

ABCEX_EMAIL = os.getenv("ABCEX_EMAIL", "").strip()
ABCEX_PASSWORD = os.getenv("ABCEX_PASSWORD", "").strip()

LIMIT = int(os.getenv("LIMIT", "200"))
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "10"))

# Прокси используем только для браузера (доступ к сайту), НЕ для установки браузера.
PROXY_URL = os.getenv("PROXY_URL", "").strip()

# Если все-таки хочешь, чтобы playwright install шел через прокси (обычно НЕ нужно):
PLAYWRIGHT_INSTALL_USE_PROXY = os.getenv("PLAYWRIGHT_INSTALL_USE_PROXY", "0") == "1"

# Стабильность
SCRAPE_TIMEOUT_SECONDS = float(os.getenv("SCRAPE_TIMEOUT_SECONDS", "25"))
HEARTBEAT_SECONDS = float(os.getenv("HEARTBEAT_SECONDS", "30"))
RELOAD_EVERY_SECONDS = float(os.getenv("RELOAD_EVERY_SECONDS", "900"))  # 15 минут

# Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "") or os.getenv("SUPABASE_KEY", "")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "kenig_rates")
SUPABASE_ON_CONFLICT = os.getenv("SUPABASE_ON_CONFLICT", "source,symbol")

# Колонки (подстрой под свою таблицу)
COL_SOURCE = os.getenv("COL_SOURCE", "source")
COL_SYMBOL = os.getenv("COL_SYMBOL", "symbol")
COL_RATE = os.getenv("COL_RATE", "rate")
COL_UPDATED_AT = os.getenv("COL_UPDATED_AT", "updated_at")
COL_PAYLOAD = os.getenv("COL_PAYLOAD", "payload")  # если нет jsonb-колонки — поставь пустым

# Логи
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

# ───────────────────────── HELPERS ─────────────────────────
def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").replace("\xa0", " ")).strip()

def _as_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        s = str(x)
        s = s.replace("\u00a0", " ").replace(" ", "").replace(",", ".")
        return float(s)
    except Exception:
        return None

def _parse_side(text: str) -> Optional[str]:
    t = (text or "").lower()
    if "buy" in t or "покуп" in t:
        return "buy"
    if "sell" in t or "прод" in t:
        return "sell"
    return None

def playwright_proxy_from_url(url: str) -> Optional[Dict[str, str]]:
    if not url:
        return None
    try:
        p = urlparse(url)
        if not p.scheme or not p.hostname or not p.port:
            return None
        proxy: Dict[str, str] = {"server": f"{p.scheme}://{p.hostname}:{p.port}"}
        if p.username:
            proxy["username"] = unquote(p.username)
        if p.password:
            proxy["password"] = unquote(p.password)
        return proxy
    except Exception:
        return None

def _sb_headers() -> Dict[str, str]:
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }

def _should_force_install(err: Exception) -> bool:
    s = str(err)
    return (
        "Executable doesn't exist" in s
        or "playwright install" in s
        or "chromium_headless_shell" in s
        or ("ms-playwright" in s and "doesn't exist" in s)
    )

_last_install_ts = 0.0

def _playwright_install() -> None:
    """
    Рантайм-установка как в рабочем rapira-worker.
    Ключевой момент: ставим и chromium, и chromium-headless-shell.
    И по умолчанию НЕ даем прокси-переменным окружения ломать скачивание.
    """
    global _last_install_ts
    now = time.time()
    if now - _last_install_ts < 600:
        log.warning("Playwright install was attempted recently; skipping (cooldown).")
        return
    _last_install_ts = now

    log.warning("Installing Playwright browsers (runtime)...")
    env = dict(os.environ)

    # Стабильный кейс на Render: не пропускать install через HTTP_PROXY/HTTPS_PROXY,
    # т.к. часто рвется на 80–90% (server closed connection).
    if not PLAYWRIGHT_INSTALL_USE_PROXY:
        for k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
            env.pop(k, None)

    # Но при этом сохраняем путь к браузерам
    env["PLAYWRIGHT_BROWSERS_PATH"] = os.environ.get("PLAYWRIGHT_BROWSERS_PATH", "/opt/render/.cache/ms-playwright")

    try:
        r = subprocess.run(
            [sys.executable, "-m", "playwright", "install", "chromium", "chromium-headless-shell"],
            check=False,
            capture_output=True,
            text=True,
            env=env,
            timeout=20 * 60,
        )
        if r.returncode != 0:
            log.error("playwright install failed (%s)\nSTDOUT:\n%s\nSTDERR:\n%s",
                      r.returncode, (r.stdout or "")[-4000:], (r.stderr or "")[-4000:])
        else:
            log.info("Playwright browsers installed.")
    except Exception as e:
        log.error("Cannot run playwright install: %s", e)

# ───────────────────────── MODEL ─────────────────────────
@dataclass(frozen=True)
class Trade:
    ts: datetime
    price: float
    amount: float
    side: Optional[str] = None

# ───────────────────────── PAGE / SCRAPE ─────────────────────────
async def accept_cookies_if_any(page: Page) -> None:
    for label in ["Accept", "Принять", "Я согласен", "Согласен", "OK"]:
        try:
            btn = page.locator(f"text={label}")
            if await btn.count() > 0:
                await btn.first.click(timeout=5_000)
                await page.wait_for_timeout(250)
                return
        except Exception:
            pass

async def _is_login_visible(page: Page) -> bool:
    try:
        login_btn = page.locator("button:has-text('Log in'), button:has-text('Login'), button:has-text('Войти')")
        return (await login_btn.count()) > 0
    except Exception:
        return False

async def login_if_needed(page: Page) -> None:
    if not ABCEX_EMAIL or not ABCEX_PASSWORD:
        return

    try:
        if not await _is_login_visible(page):
            return

        log.info("Login appears required; trying to login...")
        btn = page.locator("button:has-text('Log in'), button:has-text('Login'), button:has-text('Войти')").first
        await btn.click(timeout=7_000)
        await page.wait_for_timeout(600)

        email = page.locator("input[type='email'], input[placeholder*='mail' i], input[name*='email' i]").first
        pwd = page.locator("input[type='password'], input[name*='pass' i]").first

        await email.fill(ABCEX_EMAIL)
        await pwd.fill(ABCEX_PASSWORD)

        submit = page.locator("button[type='submit'], button:has-text('Sign in'), button:has-text('Войти')").first
        await submit.click(timeout=7_000)

        await page.wait_for_timeout(1500)
        log.info("Login flow finished (best-effort).")
    except Exception as e:
        log.warning("Login flow failed/skipped (non-fatal): %s", e)

async def click_trades_tab_best_effort(page: Page) -> None:
    candidates = [
        "button:has-text('Trades')",
        "button:has-text('Сделки')",
        "button:has-text('Последние сделки')",
        "[role='tab']:has-text('Trades')",
        "[role='tab']:has-text('Сделки')",
    ]
    for sel in candidates:
        try:
            loc = page.locator(sel)
            if await loc.count() > 0:
                await loc.first.click(timeout=5_000)
                await page.wait_for_timeout(250)
                return
        except Exception:
            continue

async def get_order_history_panel(page: Page):
    selectors = [
        "#panel-orderHistory",
        "[data-testid='orderHistory']",
        "section:has-text('Trades')",
        "section:has-text('Сделки')",
    ]
    for sel in selectors:
        try:
            loc = page.locator(sel)
            if await loc.count() > 0:
                return loc.first
        except Exception:
            continue
    return None

async def wait_trades_visible(page: Page, timeout_ms: int = 25_000) -> bool:
    t0 = time.monotonic()
    while (time.monotonic() - t0) * 1000 < timeout_ms:
        panel = await get_order_history_panel(page)
        if panel is not None:
            try:
                txt = _clean(await panel.inner_text())
                if re.search(r"\d", txt):
                    return True
            except Exception:
                pass
        await page.wait_for_timeout(250)
    return False

async def extract_trades_from_panel(panel_locator, limit: int) -> List[Trade]:
    """
    Пытаемся извлечь сделки из панели истории.
    Логика максимально “грязеустойчивая”: берём текст строк, ищем два числа (price, amount) и side.
    """
    trades: List[Trade] = []
    try:
        text = await panel_locator.inner_text()
        lines = [l.strip() for l in (text or "").splitlines() if l.strip()]
        # Берём последние строки (обычно внизу свежие)
        lines = lines[-max(limit * 2, 200):]

        for line in reversed(lines):
            if len(trades) >= limit:
                break
            ln = _clean(line)
            if not re.search(r"\d", ln):
                continue

            nums = re.findall(r"[\d\s]+[.,]?\d*", ln)
            nums = [x for x in nums if re.search(r"\d", x)]
            floats = [_as_float(x) for x in nums]
            floats = [x for x in floats if x is not None]

            if len(floats) < 2:
                continue

            price = floats[0]
            amount = floats[1]
            if price <= 0 or amount <= 0:
                continue

            side = _parse_side(ln)
            trades.append(Trade(ts=now_utc(), price=price, amount=amount, side=side))
    except Exception:
        pass

    return trades

def compute_metrics(trades: List[Trade]) -> Dict[str, Any]:
    if not trades:
        return {"count": 0}

    last_price = trades[0].price  # мы собирали в порядке “свежее → старее”
    sum_qty = sum(t.amount for t in trades)
    vwap = (sum(t.price * t.amount for t in trades) / sum_qty) if sum_qty > 0 else 0.0
    return {"count": len(trades), "last_price": last_price, "sum_qty": sum_qty, "vwap": vwap}

# ───────────────────────── BROWSER SESSION ─────────────────────────
async def open_browser(pw) -> Tuple[Browser, BrowserContext, Page]:
    proxy_cfg = playwright_proxy_from_url(PROXY_URL)

    launch_kwargs: Dict[str, Any] = {
        "headless": True,
        "args": ["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
    }
    if proxy_cfg:
        launch_kwargs["proxy"] = proxy_cfg

    browser = await pw.chromium.launch(**launch_kwargs)

    context = await browser.new_context(
        viewport={"width": 1440, "height": 900},
        locale="en-US",
        timezone_id="Europe/Amsterdam",
        user_agent=(
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
    )
    page = await context.new_page()
    page.set_default_timeout(12_000)

    await page.goto(ABCEX_URL, wait_until="domcontentloaded", timeout=60_000)
    await page.wait_for_timeout(900)
    await accept_cookies_if_any(page)
    await login_if_needed(page)
    await click_trades_tab_best_effort(page)
    await page.wait_for_timeout(600)

    return browser, context, page

async def safe_close(browser: Optional[Browser], context: Optional[BrowserContext], page: Optional[Page]) -> None:
    for obj, fn in [(page, "close"), (context, "close"), (browser, "close")]:
        try:
            if obj:
                await getattr(obj, fn)()
        except Exception:
            pass

# ───────────────────────── SCRAPE ONCE ─────────────────────────
async def scrape_abcex_once(page: Page) -> Dict[str, Any]:
    ok = await wait_trades_visible(page, timeout_ms=25_000)
    if not ok:
        return {"source": SOURCE, "symbol": SYMBOL, "url": ABCEX_URL, "fetched_at": now_utc().isoformat(),
                "metrics": {"count": 0}, "trades": []}

    panel = await get_order_history_panel(page)
    if panel is None:
        return {"source": SOURCE, "symbol": SYMBOL, "url": ABCEX_URL, "fetched_at": now_utc().isoformat(),
                "metrics": {"count": 0}, "trades": []}

    trades = await extract_trades_from_panel(panel, LIMIT)
    metrics = compute_metrics(trades)

    return {
        "source": SOURCE,
        "symbol": SYMBOL,
        "url": ABCEX_URL,
        "fetched_at": now_utc().isoformat(),
        "metrics": metrics,
        "trades": [{"ts": t.ts.isoformat(), "price": t.price, "amount": t.amount, "side": t.side} for t in trades],
    }

# ───────────────────────── SUPABASE ─────────────────────────
def build_supabase_row(scrape: Dict[str, Any]) -> Dict[str, Any]:
    metrics = scrape.get("metrics") or {}
    rate = metrics.get("last_price") or metrics.get("vwap")

    row: Dict[str, Any] = {
        COL_SOURCE: scrape.get("source", SOURCE),
        COL_SYMBOL: scrape.get("symbol", SYMBOL),
        COL_RATE: rate,
        COL_UPDATED_AT: scrape.get("fetched_at"),
    }
    if COL_PAYLOAD:
        row[COL_PAYLOAD] = scrape
    return row

async def supabase_upsert_one(client: httpx.AsyncClient, row: Dict[str, Any]) -> None:
    if not SUPABASE_URL or not SUPABASE_KEY:
        log.warning("SUPABASE_URL / SUPABASE_KEY not set; skipping upsert.")
        return

    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
    params = {"on_conflict": SUPABASE_ON_CONFLICT}

    r = await client.post(url, headers=_sb_headers(), params=params, json=[row])
    if r.status_code >= 300:
        raise RuntimeError(f"Supabase upsert failed: HTTP {r.status_code} | {r.text[:800]}")

    log.info("Supabase upsert OK | %s=%s | %s=%s | %s=%s",
             COL_SOURCE, row.get(COL_SOURCE),
             COL_SYMBOL, row.get(COL_SYMBOL),
             COL_RATE, row.get(COL_RATE))

# ───────────────────────── WORKER LOOP ─────────────────────────
async def worker() -> None:
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
                                _playwright_install()
                                browser, context, page = await open_browser(pw)
                            else:
                                raise
                        backoff = 2.0
                        last_reload = time.monotonic()
                        last_heartbeat = time.monotonic()

                    # heartbeat
                    if time.monotonic() - last_heartbeat >= HEARTBEAT_SECONDS:
                        log.info("Heartbeat: alive.")
                        last_heartbeat = time.monotonic()

                    # профилактический reload
                    if time.monotonic() - last_reload >= RELOAD_EVERY_SECONDS:
                        log.warning("Maintenance reload...")
                        await page.goto(ABCEX_URL, wait_until="domcontentloaded", timeout=60_000)
                        await page.wait_for_timeout(800)
                        await accept_cookies_if_any(page)
                        await login_if_needed(page)
                        await click_trades_tab_best_effort(page)
                        last_reload = time.monotonic()

                    scrape = await asyncio.wait_for(scrape_abcex_once(page), timeout=SCRAPE_TIMEOUT_SECONDS)
                    row = build_supabase_row(scrape)

                    metrics = scrape.get("metrics", {})
                    if metrics.get("count", 0) > 0:
                        log.info("Parsed trades=%s | last_price=%s | vwap=%s",
                                 metrics.get("count"), metrics.get("last_price"), metrics.get("vwap"))
                    else:
                        log.warning("No trades parsed (UI not ready / changed).")

                    await supabase_upsert_one(sb, row)

                    # jitter
                    sleep_s = max(1.0, POLL_SECONDS + random.uniform(-0.35, 0.35))
                    await asyncio.sleep(sleep_s)

                except asyncio.TimeoutError:
                    log.error("Timeout: scrape exceeded %.1fs. Restarting browser session...", SCRAPE_TIMEOUT_SECONDS)
                    await safe_close(browser, context, page)
                    browser = context = page = None

                except Exception as e:
                    log.error("Worker error: %s", e)

                    # если снова “Executable doesn't exist” — попробуем поставить и перезапустить
                    if _should_force_install(e):
                        _playwright_install()

                    log.info("Retrying after %.1fs ...", backoff)
                    await asyncio.sleep(backoff)
                    backoff = min(60.0, backoff * 2)

                    await safe_close(browser, context, page)
                    browser = context = page = None

def main() -> None:
    log.info("Starting ABCEX worker | url=%s | symbol=%s | poll=%.1fs | proxy(browser)=%s | install_use_proxy=%s",
             ABCEX_URL, SYMBOL, POLL_SECONDS, "yes" if PROXY_URL else "no", PLAYWRIGHT_INSTALL_USE_PROXY)
    asyncio.run(worker())

if __name__ == "__main__":
    main()
