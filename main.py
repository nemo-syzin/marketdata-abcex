import asyncio
import json
import logging
import os
import random
import re
import subprocess
import sys
import time
import traceback
from collections import deque
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

import httpx
from playwright.async_api import async_playwright, Browser, BrowserContext, Locator, Page

# ───────────────────────── CONFIG ─────────────────────────

ABCEX_URL = os.getenv("ABCEX_URL", "https://abcex.io/client/spot/USDTRUB")
SOURCE = os.getenv("SOURCE", "abcex")
SYMBOL = os.getenv("SYMBOL", "USDT/RUB")

ABCEX_EMAIL = os.getenv("ABCEX_EMAIL", "")
ABCEX_PASSWORD = os.getenv("ABCEX_PASSWORD", "")

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "exchange_trades")

# Должно совпадать с unique index:
# create unique index ... (source, symbol, trade_time, price, volume_usdt)
ON_CONFLICT = os.getenv("ON_CONFLICT", "source,symbol,trade_time,price,volume_usdt")

LIMIT = int(os.getenv("LIMIT", "200"))
POLL_SECONDS = float(os.getenv("POLL_SECONDS", os.getenv("POLL_SEC", "1.2")))

SCRAPE_TIMEOUT_SECONDS = float(os.getenv("SCRAPE_TIMEOUT_SECONDS", "25"))
UPSERT_TIMEOUT_SECONDS = float(os.getenv("UPSERT_TIMEOUT_SECONDS", "25"))
HEARTBEAT_SECONDS = float(os.getenv("HEARTBEAT_SECONDS", "30"))
RELOAD_EVERY_SECONDS = float(os.getenv("RELOAD_EVERY_SECONDS", "600"))

SEEN_MAX = int(os.getenv("SEEN_MAX", "20000"))
UPSERT_BATCH = int(os.getenv("UPSERT_BATCH", "200"))

STATE_PATH = os.getenv("STATE_PATH", "abcex_state.json")

# Если нужно быстро отключить runtime install:
SKIP_BROWSER_INSTALL = os.getenv("SKIP_BROWSER_INSTALL", "0") == "1"

# Артефакты (html/screenshot) — опционально
DEBUG_ARTIFACTS = os.getenv("DEBUG_ARTIFACTS", "0") == "1"
ARTIFACTS_DIR = os.getenv("ARTIFACTS_DIR", "./artifacts")

# numeric(18,8)
Q8 = Decimal("0.00000001")

TIME_RE = re.compile(r"^\d{2}:\d{2}:\d{2}$")

# Render: не буферизуем stdout
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

# ───────────────────────── INSTALL (РОВНО КАК В КОДЕ 1) ─────────────────────────

_last_install_ts = 0.0


def _now_ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def _ensure_artifacts_dir() -> None:
    if not DEBUG_ARTIFACTS:
        return
    try:
        os.makedirs(ARTIFACTS_DIR, exist_ok=True)
    except Exception:
        pass


def _playwright_install_runtime() -> None:
    """
    РОВНО КАК В КОДЕ 1:
      python -m playwright install chromium chromium-headless-shell

    + cooldown 10 минут (чтобы не дергать install бесконечно)
    """
    global _last_install_ts

    if SKIP_BROWSER_INSTALL:
        log.warning("SKIP_BROWSER_INSTALL=1, skipping browser install.")
        return

    now = time.time()
    if now - _last_install_ts < 600:
        log.warning("Playwright install attempted recently; skipping (cooldown).")
        return
    _last_install_ts = now

    log.warning("Installing Playwright browsers (runtime) via python -m playwright ...")
    log.info("Env PLAYWRIGHT_BROWSERS_PATH=%s", os.getenv("PLAYWRIGHT_BROWSERS_PATH", ""))

    try:
        # ВАЖНО: используем sys.executable -m playwright (как в твоем рабочем коде)
        r = subprocess.run(
            [sys.executable, "-m", "playwright", "install", "chromium", "chromium-headless-shell"],
            check=False,
            capture_output=True,
            text=True,
        )
        log.info("playwright install returncode=%s", r.returncode)
        if r.stdout:
            log.info("playwright install STDOUT:\n%s", r.stdout[-6000:])
        if r.stderr:
            log.info("playwright install STDERR:\n%s", r.stderr[-6000:])

        if r.returncode == 0:
            log.warning("Playwright browsers installed successfully.")
        else:
            log.error("Playwright browsers install FAILED (code=%s).", r.returncode)

    except Exception as e:
        log.error("Cannot run playwright install: %s", e)


def _should_force_install(err: Exception) -> bool:
    s = str(err)
    return (
        "Executable doesn't exist" in s
        or "Looks like Playwright was just installed or updated" in s
        or "playwright install" in s
        or "chromium_headless_shell" in s
        or ("ms-playwright" in s and "doesn't exist" in s)
    )


def _install_with_retries(max_attempts: int = 4) -> None:
    """
    Ретраи установки (не меняют команду).
    Полезно при "server closed connection".
    """
    base = 3.0
    for attempt in range(1, max_attempts + 1):
        log.warning("Install attempt %d/%d ...", attempt, max_attempts)
        _playwright_install_runtime()

        # Если install прошел — выходим (по логу returncode мы это увидим, но здесь
        # явной проверки нет. Дадим шанс следующему launch определить).
        if attempt < max_attempts:
            sleep_s = min(45.0, base * (2 ** (attempt - 1)))
            log.warning("Waiting %.1fs before next install retry ...", sleep_s)
            time.sleep(sleep_s)


# ───────────────────────── NUM HELPERS ─────────────────────────

def normalize_decimal(text: str) -> Optional[Decimal]:
    t = (text or "").strip().replace("\xa0", " ")
    if not t:
        return None

    t = t.replace(" ", "")
    if "," in t and "." in t:
        t = t.replace(",", "")
    else:
        t = t.replace(",", ".")

    try:
        return Decimal(t)
    except (InvalidOperation, ValueError):
        return None


def q8_str(x: Decimal) -> str:
    return str(x.quantize(Q8, rounding=ROUND_HALF_UP))


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

    t0 = time.monotonic()
    async with httpx.AsyncClient(timeout=UPSERT_TIMEOUT_SECONDS) as client:
        for i in range(0, len(rows), UPSERT_BATCH):
            chunk = rows[i:i + UPSERT_BATCH]
            try:
                r = await client.post(url, headers=_sb_headers(), params=params, json=chunk)
            except Exception as e:
                log.error("Supabase POST error: %s", e)
                return

            if r.status_code >= 300:
                log.error("Supabase upsert failed (%s): %s", r.status_code, r.text[:2000])
                return

    log.info("Supabase upsert ok: %d rows in %.2fs", len(rows), time.monotonic() - t0)


# ───────────────────────── DEBUG HOOKS ─────────────────────────

async def dump_debug(page: Page, label: str) -> None:
    if not DEBUG_ARTIFACTS:
        return
    _ensure_artifacts_dir()
    ts = int(time.time())
    safe_label = re.sub(r"[^a-zA-Z0-9_-]+", "_", label)[:80]
    try:
        path_png = os.path.join(ARTIFACTS_DIR, f"{ts}_{safe_label}.png")
        await page.screenshot(path=path_png, full_page=True)
        log.info("Saved screenshot: %s", path_png)
    except Exception as e:
        log.info("Screenshot failed: %s", e)

    try:
        path_html = os.path.join(ARTIFACTS_DIR, f"{ts}_{safe_label}.html")
        content = await page.content()
        with open(path_html, "w", encoding="utf-8") as f:
            f.write(content)
        log.info("Saved HTML: %s", path_html)
    except Exception as e:
        log.info("HTML dump failed: %s", e)


def attach_listeners(page: Page, context: BrowserContext) -> None:
    # console logs
    page.on("console", lambda msg: log.info("PAGE_CONSOLE[%s]: %s", msg.type, msg.text))
    page.on("pageerror", lambda err: log.error("PAGE_ERROR: %s", err))
    page.on("crash", lambda: log.error("PAGE_CRASHED"))

    def _req_failed(req):
        try:
            log.warning("REQ_FAILED: %s %s (%s)", req.method, req.url, req.failure)
        except Exception:
            pass

    def _resp(resp):
        try:
            st = resp.status
            if st >= 400:
                log.warning("RESP_%s: %s", st, resp.url)
        except Exception:
            pass

    context.on("requestfailed", _req_failed)
    page.on("response", _resp)


# ───────────────────────── PAGE HELPERS ─────────────────────────

async def accept_cookies_if_any(page: Page) -> None:
    candidates = ["Принять", "Согласен", "Я согласен", "Accept", "I agree"]
    for txt in candidates:
        try:
            btn = page.locator(f"text={txt}")
            if await btn.count() > 0 and await btn.first.is_visible():
                await btn.first.click(timeout=5_000)
                await page.wait_for_timeout(400)
                log.info("Cookies banner handled via: %s", txt)
                return
        except Exception:
            pass


async def _is_login_visible(page: Page) -> bool:
    """
    Детект логина по видимому password input.
    """
    try:
        pw = page.locator("input[type='password']")
        return (await pw.count() > 0) and (await pw.first.is_visible())
    except Exception:
        return False


async def login_if_needed(page: Page) -> None:
    if not await _is_login_visible(page):
        log.info("Login not required (password input not visible).")
        return

    if not ABCEX_EMAIL or not ABCEX_PASSWORD:
        raise RuntimeError("Login required, but ABCEX_EMAIL/ABCEX_PASSWORD are not set in env.")

    log.warning("Login detected. Performing sign-in ...")
    await dump_debug(page, "login_screen")

    email_candidates = [
        "input[type='email']",
        "input[name='email']",
        "input[placeholder*='mail' i]",
        "input[placeholder*='email' i]",
        "input[placeholder*='почта' i]",
        "input[placeholder*='e-mail' i]",
    ]
    pw_candidates = [
        "input[type='password']",
        "input[name='password']",
        "input[placeholder*='пароль' i]",
        "input[placeholder*='password' i]",
    ]

    email_filled = False
    for sel in email_candidates:
        loc = page.locator(sel)
        try:
            if await loc.count() > 0 and await loc.first.is_visible():
                await loc.first.fill(ABCEX_EMAIL, timeout=15_000)
                email_filled = True
                log.info("Filled email via selector: %s", sel)
                break
        except Exception as e:
            log.info("Email selector failed %s: %s", sel, e)

    pw_filled = False
    for sel in pw_candidates:
        loc = page.locator(sel)
        try:
            if await loc.count() > 0 and await loc.first.is_visible():
                await loc.first.fill(ABCEX_PASSWORD, timeout=15_000)
                pw_filled = True
                log.info("Filled password via selector: %s", sel)
                break
        except Exception as e:
            log.info("Password selector failed %s: %s", sel, e)

    if not email_filled or not pw_filled:
        await dump_debug(page, "login_fields_not_found")
        raise RuntimeError("Could not find visible email/password fields (UI changed or blocked).")

    # Click sign-in
    btn_texts = ["Войти", "Вход", "Sign in", "Login", "Войти в аккаунт"]
    clicked = False
    for t in btn_texts:
        try:
            btn = page.locator(f"button:has-text('{t}')")
            if await btn.count() > 0 and await btn.first.is_visible():
                await btn.first.click(timeout=15_000)
                clicked = True
                log.info("Clicked login button: %s", t)
                break
        except Exception as e:
            log.info("Login button '%s' click failed: %s", t, e)

    if not clicked:
        try:
            await page.keyboard.press("Enter")
            log.info("Pressed Enter to submit login form.")
        except Exception:
            pass

    # Wait a bit for SPA auth
    try:
        await page.wait_for_timeout(1200)
        await page.wait_for_load_state("domcontentloaded", timeout=30_000)
    except Exception:
        pass

    await page.wait_for_timeout(1500)
    await accept_cookies_if_any(page)

    if await _is_login_visible(page):
        await dump_debug(page, "login_failed_still_password")
        raise RuntimeError("Login failed (password field still visible). Possible 2FA/captcha/flow change.")

    log.warning("Login successful (password field disappeared).")


async def click_trades_tab_best_effort(page: Page) -> None:
    candidates = ["Сделки", "История", "Order history", "Trades", "Trade history"]
    for t in candidates:
        try:
            tab = page.locator(f"[role='tab']:has-text('{t}')")
            if await tab.count() > 0 and await tab.first.is_visible():
                await tab.first.click(timeout=10_000)
                await page.wait_for_timeout(500)
                log.info("Clicked tab via role=tab: %s", t)
                return
        except Exception:
            continue

    for t in candidates:
        try:
            tab = page.locator(f"text={t}")
            if await tab.count() > 0 and await tab.first.is_visible():
                await tab.first.click(timeout=10_000)
                await page.wait_for_timeout(500)
                log.info("Clicked tab via text: %s", t)
                return
        except Exception:
            continue

    log.info("Trades tab not found by heuristics (maybe already visible).")


async def get_order_history_panel(page: Page) -> Locator:
    panel = page.locator("div[role='tabpanel'][id*='panel-orderHistory']")
    cnt = await panel.count()
    log.info("panel-orderHistory count=%d", cnt)
    if cnt == 0:
        await dump_debug(page, "panel_orderHistory_not_found")
        raise RuntimeError("panel-orderHistory not found (UI changed or not logged in).")

    for i in range(cnt):
        p = panel.nth(i)
        try:
            if await p.is_visible():
                log.info("panel-orderHistory visible index=%d", i)
                return p
        except Exception:
            continue

    await dump_debug(page, "panel_orderHistory_all_hidden")
    raise RuntimeError("panel-orderHistory found but none is visible.")


async def wait_trades_visible(panel: Locator, timeout_ms: int = 25_000) -> None:
    handle = await panel.element_handle()
    if handle is None:
        raise RuntimeError("Cannot get element_handle(panel).")

    deadline = time.monotonic() + (timeout_ms / 1000)
    while time.monotonic() < deadline:
        try:
            ok = await handle.evaluate(
                """(root) => {
                    const re = /^\\d{2}:\\d{2}:\\d{2}$/;
                    const ps = Array.from(root.querySelectorAll('p'));
                    return ps.some(p => re.test((p.textContent || '').trim()));
                }"""
            )
            if ok:
                return
        except Exception:
            pass
        await asyncio.sleep(0.35)

    raise RuntimeError("Trades not visible (no HH:MM:SS detected in panel).")


async def extract_trades_from_panel(panel: Locator, limit: int) -> List[Dict[str, Any]]:
    """
    Возвращает список dict, готовых для вставки в БД:
      {source, symbol, price, volume_usdt, volume_rub, trade_time}
    """
    handle = await panel.element_handle()
    if handle is None:
        raise RuntimeError("Cannot get element_handle(panel).")

    raw_rows: List[Dict[str, Any]] = await handle.evaluate(
        """(root, limit) => {
          const isTime = (s) => /^\\d{2}:\\d{2}:\\d{2}$/.test((s||'').trim());
          const isNum = (s) => /^[0-9][0-9\\s\\u00A0.,]*$/.test((s||'').trim());

          const out = [];
          const divs = Array.from(root.querySelectorAll('div'));

          for (const d of divs) {
            const ps = Array.from(d.querySelectorAll(':scope > p'));
            if (ps.length < 3) continue;

            const price_raw = (ps[0].textContent || '').trim();
            const qty_raw   = (ps[1].textContent || '').trim();
            const time_txt  = (ps[2].textContent || '').trim();

            if (!isTime(time_txt)) continue;
            if (!isNum(price_raw) || !isNum(qty_raw)) continue;

            out.push({ price_raw, qty_raw, time: time_txt });
            if (out.length >= limit) break;
          }

          return out;
        }""",
        limit,
    )

    out: List[Dict[str, Any]] = []
    for r in raw_rows:
        price_d = normalize_decimal(str(r.get("price_raw", "")))
        qty_d = normalize_decimal(str(r.get("qty_raw", "")))
        t = str(r.get("time", "")).strip()

        if not price_d or not qty_d or not TIME_RE.fullmatch(t):
            continue
        if price_d <= 0 or qty_d <= 0:
            continue

        volume_rub = price_d * qty_d

        out.append({
            "source": SOURCE,
            "symbol": SYMBOL,
            "price": q8_str(price_d),
            "volume_usdt": q8_str(qty_d),
            "volume_rub": q8_str(volume_rub),
            "trade_time": t,
        })

    return out


# ───────────────────────── DEDUPE KEY ─────────────────────────

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


# ───────────────────────── BROWSER SESSION ─────────────────────────

async def open_browser(pw) -> Tuple[Browser, BrowserContext, Page]:
    """
    Открывает браузер и подготавливает страницу:
      goto -> cookies -> login(if needed) -> open trades tab
    """
    log.info("Launching chromium ...")
    browser = await pw.chromium.launch(
        headless=True,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
        ],
    )

    storage_state = STATE_PATH if os.path.exists(STATE_PATH) else None
    log.info("storage_state=%s", storage_state or "None")

    context = await browser.new_context(
        viewport={"width": 1440, "height": 810},
        locale="ru-RU",
        timezone_id="Europe/Moscow",
        user_agent=(
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        storage_state=storage_state,
    )

    page = await context.new_page()
    page.set_default_timeout(12_000)

    attach_listeners(page, context)

    log.info("Goto: %s", ABCEX_URL)
    await page.goto(ABCEX_URL, wait_until="domcontentloaded", timeout=60_000)
    await page.wait_for_timeout(1200)

    await accept_cookies_if_any(page)
    await login_if_needed(page)

    # save state best-effort
    try:
        await context.storage_state(path=STATE_PATH)
        log.info("Saved session state to %s", STATE_PATH)
    except Exception as e:
        log.info("Could not save storage state: %s", e)

    await click_trades_tab_best_effort(page)
    await page.wait_for_timeout(600)

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


async def scrape_window(page: Page) -> List[Dict[str, Any]]:
    panel = await get_order_history_panel(page)
    await wait_trades_visible(panel, timeout_ms=25_000)
    return await extract_trades_from_panel(panel, limit=LIMIT)


# ───────────────────────── WORKER LOOP ─────────────────────────

async def worker() -> None:
    log.info("Boot @ %s", _now_ts())
    log.info("Python: %s", sys.version.replace("\n", " "))
    log.info("ABCEX_URL=%s", ABCEX_URL)
    log.info("LIMIT=%s POLL_SECONDS=%s", LIMIT, POLL_SECONDS)
    log.info("SCRAPE_TIMEOUT_SECONDS=%s RELOAD_EVERY_SECONDS=%s", SCRAPE_TIMEOUT_SECONDS, RELOAD_EVERY_SECONDS)
    log.info("SUPABASE_URL set=%s, table=%s", bool(SUPABASE_URL), SUPABASE_TABLE)
    log.info("STATE_PATH=%s exists=%s", STATE_PATH, os.path.exists(STATE_PATH))

    seen: Set[TradeKey] = set()
    seen_q: Deque[TradeKey] = deque()

    backoff = 2.0
    last_heartbeat = time.monotonic()
    last_reload = time.monotonic()
    last_tab_click = 0.0

    # 1) СНАЧАЛА — установка браузеров (в стиле кода 1)
    #    делаем ретраи, т.к. у тебя бывает "server closed connection"
    _install_with_retries(max_attempts=4)

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
                        log.error("open_browser failed: %s", e)
                        log.info("Traceback:\n%s", traceback.format_exc())

                        # Если похоже на отсутствие браузера — ставим как в коде 1 и пробуем снова
                        if _should_force_install(e):
                            log.warning("Detected missing browser; will run install (style=code1) and retry open_browser.")
                            _install_with_retries(max_attempts=4)
                            browser, context, page = await open_browser(pw)
                        else:
                            raise

                    backoff = 2.0
                    last_reload = time.monotonic()
                    last_heartbeat = time.monotonic()

                # heartbeat
                if time.monotonic() - last_heartbeat >= HEARTBEAT_SECONDS:
                    log.info("Heartbeat: alive. seen=%d", len(seen))
                    last_heartbeat = time.monotonic()

                # профилактический reload
                if time.monotonic() - last_reload >= RELOAD_EVERY_SECONDS:
                    log.warning("Maintenance reload...")
                    await page.goto(ABCEX_URL, wait_until="domcontentloaded", timeout=60_000)
                    await page.wait_for_timeout(1200)
                    await accept_cookies_if_any(page)
                    await login_if_needed(page)
                    await click_trades_tab_best_effort(page)
                    last_reload = time.monotonic()

                # не кликаем вкладку каждый цикл
                if time.monotonic() - last_tab_click >= 20:
                    await click_trades_tab_best_effort(page)
                    last_tab_click = time.monotonic()

                # таймаут на скрейп
                t0 = time.monotonic()
                window = await asyncio.wait_for(scrape_window(page), timeout=SCRAPE_TIMEOUT_SECONDS)
                log.info("scrape_window ok: parsed=%d rows in %.2fs", len(window), time.monotonic() - t0)

                if not window:
                    log.warning("No trades parsed. Reloading page...")
                    await dump_debug(page, "no_trades_reload")
                    await page.goto(ABCEX_URL, wait_until="domcontentloaded", timeout=60_000)
                    await page.wait_for_timeout(1200)
                    await accept_cookies_if_any(page)
                    await login_if_needed(page)
                    await click_trades_tab_best_effort(page)
                    await asyncio.sleep(max(0.6, POLL_SECONDS))
                    continue

                # фильтрация seen (старые -> новые)
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
                    log.info(
                        "Parsed %d NEW trades. Newest: %s",
                        len(new_rows),
                        json.dumps(new_rows[-1], ensure_ascii=False),
                    )
                    await supabase_upsert(new_rows)

                sleep_s = max(0.35, POLL_SECONDS + random.uniform(-0.15, 0.15))
                await asyncio.sleep(sleep_s)

            except asyncio.TimeoutError:
                log.error("Timeout: scrape exceeded %.1fs. Restarting browser session...", SCRAPE_TIMEOUT_SECONDS)
                await safe_close(browser, context, page)
                browser = context = page = None

            except Exception as e:
                log.error("Worker error: %s", e)
                log.info("Traceback:\n%s", traceback.format_exc())

                # если похоже на missing browser — install как в коде 1
                if _should_force_install(e):
                    log.warning("Missing browser signature detected; reinstalling (style=code1).")
                    _install_with_retries(max_attempts=4)

                log.info("Retrying after %.1fs ...", backoff)
                await asyncio.sleep(backoff)
                backoff = min(60.0, backoff * 2)

                await safe_close(browser, context, page)
                browser = context = page = None


def main() -> None:
    asyncio.run(worker())


if __name__ == "__main__":
    main()
