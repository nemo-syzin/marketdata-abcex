import asyncio
import json
import logging
import os
import subprocess
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

import certifi
import httpx
from playwright.async_api import async_playwright, Page, Locator

# ───────────────────────── ENV / CONFIG ─────────────────────────

ABCEX_EMAIL = os.getenv("ABCEX_EMAIL", "").strip()
ABCEX_PASSWORD = os.getenv("ABCEX_PASSWORD", "").strip()

ABCEX_URL = os.getenv("ABCEX_URL", "https://abcex.io/client/spot/USDTRUB").strip()
SOURCE = os.getenv("SOURCE", "abcex").strip()
SYMBOL = os.getenv("SYMBOL", "USDT/RUB").strip()

LIMIT = int(os.getenv("LIMIT", "50"))          # сколько сделок читать из панели за один проход
POLL_SEC = float(os.getenv("POLL_SEC", "5"))  # как часто обновляться

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "exchange_trades")

ON_CONFLICT = os.getenv("ON_CONFLICT", "source,symbol,trade_time,price,volume_usdt")

SEEN_MAX = int(os.getenv("SEEN_MAX", "20000"))
UPSERT_BATCH = int(os.getenv("UPSERT_BATCH", "500"))
HEARTBEAT_SEC = int(os.getenv("HEARTBEAT_SEC", "30"))

# Playwright install behavior
PW_INSTALL_RETRIES = int(os.getenv("PW_INSTALL_RETRIES", "3"))
PW_INSTALL_TIMEOUT_SEC = int(os.getenv("PW_INSTALL_TIMEOUT_SEC", "600"))  # 10 минут на скачивание
SKIP_PW_INSTALL = os.getenv("SKIP_PW_INSTALL", "0") == "1"

# ───────────────────────── LOGGING ─────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
log = logging.getLogger("abcex-worker")

# глушим шум
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# ───────────────────────── DECIMALS ─────────────────────────

Q8 = Decimal("0.00000001")


def _as_decimal(x: Any) -> Optional[Decimal]:
    try:
        if x is None:
            return None
        s = str(x).strip().replace("\xa0", " ").replace(" ", "").replace(",", ".")
        if not s:
            return None
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


def q8_str(x: Decimal) -> str:
    return str(x.quantize(Q8, rounding=ROUND_HALF_UP))


# ───────────────────────── MODEL ─────────────────────────

@dataclass(frozen=True)
class Trade:
    price: Decimal
    qty: Decimal
    time: str  # HH:MM:SS
    side: Optional[str] = None  # buy/sell
    price_raw: str = ""
    qty_raw: str = ""


def _looks_like_time(s: str) -> bool:
    s = s.strip()
    # HH:MM:SS
    if len(s) != 8 or s[2] != ":" or s[5] != ":":
        return False
    hh, mm, ss = s[:2], s[3:5], s[6:8]
    return hh.isdigit() and mm.isdigit() and ss.isdigit()


def _trade_key(t: Trade) -> Tuple[str, str, str]:
    # соответствует уникальности в БД: trade_time + price + volume_usdt (+ source/symbol на уровне таблицы)
    return (t.time, q8_str(t.price), q8_str(t.qty))


# ───────────────────────── PLAYWRIGHT INSTALL ─────────────────────────

def ensure_playwright_browsers() -> None:
    """
    Как в примере Rapira, но:
    - ретраи
    - сначала пробуем chromium-headless-shell (часто меньше/стабильнее), затем chromium.
    - уважает HTTP_PROXY/HTTPS_PROXY из env (Render variables).
    """
    if SKIP_PW_INSTALL:
        log.info("SKIP_PW_INSTALL=1 → skipping playwright install.")
        return

    cmds = [
        ["playwright", "install", "chromium-headless-shell"],
        ["playwright", "install", "chromium"],
    ]

    env = os.environ.copy()

    # Важно: чтобы кэш не улетал в home, можно закрепить в проект (опционально).
    # Если не хочешь — убери эту строку.
    env.setdefault("PLAYWRIGHT_BROWSERS_PATH", "/opt/render/project/src/.cache/ms-playwright")

    last_err = None
    for attempt in range(1, PW_INSTALL_RETRIES + 1):
        for cmd in cmds:
            try:
                log.info("Ensuring Playwright browser: %s (attempt %d/%d)", " ".join(cmd), attempt, PW_INSTALL_RETRIES)
                res = subprocess.run(
                    cmd,
                    check=False,
                    capture_output=True,
                    text=True,
                    env=env,
                    timeout=PW_INSTALL_TIMEOUT_SEC,
                )
                if res.returncode == 0:
                    log.info("Playwright install OK: %s", " ".join(cmd))
                    return
                else:
                    last_err = f"code={res.returncode}\nSTDOUT:\n{res.stdout}\nSTDERR:\n{res.stderr}"
                    log.warning("Playwright install failed: %s\n%s", " ".join(cmd), last_err)
            except Exception as e:
                last_err = str(e)
                log.warning("Playwright install exception: %s", e)

        sleep_s = min(60, 5 * attempt)
        log.info("Retrying Playwright install after %ds ...", sleep_s)
        time.sleep(sleep_s)

    raise RuntimeError(f"Playwright browsers install failed after retries. Last error:\n{last_err}")


# ───────────────────────── ABCEX FLOW ─────────────────────────

async def close_silently(page: Page) -> None:
    try:
        await page.close()
    except Exception:
        pass


async def save_debug(page: Page, html_name: str, png_name: str) -> None:
    try:
        html = await page.content()
        with open(html_name, "w", encoding="utf-8") as f:
            f.write(html)
        await page.screenshot(path=png_name, full_page=True)
        log.info("Saved debug: %s, %s", html_name, png_name)
    except Exception as e:
        log.warning("Failed to save debug artifacts: %s", e)


async def login_abcex_if_needed(page: Page) -> None:
    """
    Пытаемся:
    - открыть ABCEX_URL
    - если видим форму логина → вводим email/pass и submit
    """
    if not ABCEX_EMAIL or not ABCEX_PASSWORD:
        raise RuntimeError("ABCEX_EMAIL / ABCEX_PASSWORD are not set in env.")

    log.info("Opening %s ...", ABCEX_URL)
    await page.goto(ABCEX_URL, wait_until="domcontentloaded", timeout=60_000)
    await page.wait_for_timeout(1500)

    # эвристика: если есть input email/password — логин
    email_sel = "input[type='email'], input[name='email'], input[placeholder*='mail' i]"
    pass_sel = "input[type='password'], input[name='password']"
    btn_sel = "button[type='submit'], button:has-text('Войти'), button:has-text('Login'), button:has-text('Sign')"

    email_count = await page.locator(email_sel).count()
    pass_count = await page.locator(pass_sel).count()

    if email_count > 0 and pass_count > 0:
        log.info("Login form detected. Logging in ...")
        await page.locator(email_sel).first.fill(ABCEX_EMAIL)
        await page.locator(pass_sel).first.fill(ABCEX_PASSWORD)

        btn = page.locator(btn_sel)
        if await btn.count() > 0:
            await btn.first.click(timeout=10_000)
        else:
            # fallback: Enter
            await page.keyboard.press("Enter")

        # ждём переход/прогрузку
        await page.wait_for_load_state("domcontentloaded", timeout=60_000)
        await page.wait_for_timeout(2500)
        log.info("Login flow done (or attempted).")
    else:
        log.info("Login form not detected. Assuming already authenticated.")


async def find_order_history_panel(page: Page) -> Locator:
    """
    Ищем panel-orderHistory (как в твоём локальном скрипте).
    """
    candidates = [
        "#panel-orderHistory",
        "[id='panel-orderHistory']",
        "div#panel-orderHistory",
    ]
    for sel in candidates:
        loc = page.locator(sel)
        if await loc.count() > 0:
            # иногда их несколько
            for i in range(await loc.count()):
                item = loc.nth(i)
                try:
                    if await item.is_visible():
                        log.info("Found visible orderHistory panel: %s (idx=%d)", sel, i)
                        return item
                except Exception:
                    continue

    await save_debug(page, "abcex_no_panel.html", "abcex_no_panel.png")
    raise RuntimeError("panel-orderHistory not found (see abcex_no_panel.*)")


async def wait_trades_visible(page: Page, timeout_ms: int = 25_000) -> None:
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
                log.info("Trades look visible (HH:MM:SS detected).")
                return
        except Exception:
            pass
        await page.wait_for_timeout(800)

    await save_debug(page, "abcex_trades_not_visible.html", "abcex_trades_not_visible.png")
    raise RuntimeError("Trades not visible (see abcex_trades_not_visible.*)")


async def extract_trades_from_panel(panel: Locator, limit: int) -> List[Trade]:
    """
    JS-эвристика как в твоём локальном файле:
    - ищем блоки где подряд 3 p: [price, qty, time]
    - time строго HH:MM:SS
    - price/qty как числа
    """
    handle = await panel.element_handle()
    if handle is None:
        raise RuntimeError("Cannot get element_handle for panel-orderHistory.")

    raw_rows: List[Dict[str, Any]] = await handle.evaluate(
        """(root, limit) => {
          const isTime = (s) => /^\\d{2}:\\d{2}:\\d{2}$/.test((s||'').trim());
          const isNum = (s) => /^[0-9][0-9\\s\\u00A0.,]*$/.test((s||'').trim());

          const out = [];
          const divs = Array.from(root.querySelectorAll('div'));

          for (const d of divs) {
            const ps = Array.from(d.querySelectorAll(':scope > p'));
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

    trades: List[Trade] = []
    for r in raw_rows:
        price_raw = str(r.get("price_raw", "")).strip()
        qty_raw = str(r.get("qty_raw", "")).strip()
        tm = str(r.get("time", "")).strip()
        side = r.get("side", None)

        if not price_raw or not qty_raw or not _looks_like_time(tm):
            continue

        price = _as_decimal(price_raw)
        qty = _as_decimal(qty_raw)
        if price is None or qty is None or price <= 0 or qty <= 0:
            continue

        trades.append(
            Trade(
                price=price,
                qty=qty,
                time=tm,
                side=side if side in ("buy", "sell") else None,
                price_raw=price_raw,
                qty_raw=qty_raw,
            )
        )

    return trades


# ───────────────────────── SUPABASE ─────────────────────────

def trade_to_row(t: Trade) -> Dict[str, Any]:
    volume_rub = (t.price * t.qty)
    return {
        "source": SOURCE,
        "symbol": SYMBOL,
        "price": q8_str(t.price),
        "volume_usdt": q8_str(t.qty),
        "volume_rub": q8_str(volume_rub),
        "trade_time": t.time,  # time without tz in твоей схеме
    }


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
        log.error("Supabase upsert failed (%s): %s", r.status_code, r.text)
    else:
        log.info("Inserted (or ignored duplicates) %d rows into '%s'.", len(rows), SUPABASE_TABLE)


# ───────────────────────── WORKER ─────────────────────────

async def worker() -> None:
    ensure_playwright_browsers()

    seen: Set[Tuple[str, str, str]] = set()
    seen_q: Deque[Tuple[str, str, str]] = deque(maxlen=SEEN_MAX)

    backoff = 2.0
    last_heartbeat = 0.0

    # Supabase client (httpx)
    async with httpx.AsyncClient(
        timeout=20,
        follow_redirects=True,
        verify=certifi.where(),
        trust_env=True,  # HTTP_PROXY/HTTPS_PROXY подхватятся
    ) as sb:

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ],
            )
            context = await browser.new_context(
                viewport={"width": 1440, "height": 900},
                locale="ru-RU",
                timezone_id="Europe/Moscow",
            )
            page = await context.new_page()

            try:
                await login_abcex_if_needed(page)
                await wait_trades_visible(page, timeout_ms=35_000)
                panel = await find_order_history_panel(page)

                log.info("ABCEX worker started | url=%s | poll=%.2fs | limit=%d", ABCEX_URL, POLL_SEC, LIMIT)

                while True:
                    t0 = time.time()
                    try:
                        trades = await extract_trades_from_panel(panel, limit=LIMIT)

                        # ABCEX обычно отдаёт сверху новые → перевернём в старые→новые для записи
                        trades = list(reversed(trades))

                        new_rows: List[Dict[str, Any]] = []
                        for t in trades:
                            k = _trade_key(t)
                            if k in seen:
                                continue
                            seen.add(k)
                            seen_q.append(k)
                            new_rows.append(trade_to_row(t))

                        # пересборка set если разъехался
                        if len(seen) > len(seen_q) + 200:
                            seen = set(seen_q)

                        if new_rows:
                            log.info("Parsed %d new trades. Newest=%s", len(new_rows), json.dumps(new_rows[-1], ensure_ascii=False))
                            for batch in chunked(new_rows, UPSERT_BATCH):
                                await supabase_upsert(sb, batch)
                        else:
                            log.info("No new trades.")

                        now = time.time()
                        if now - last_heartbeat >= HEARTBEAT_SEC:
                            log.info("Heartbeat: alive | seen=%d", len(seen))
                            last_heartbeat = now

                        backoff = 2.0
                        dt = time.time() - t0
                        await asyncio.sleep(max(0.5, POLL_SEC - dt))

                    except Exception as e:
                        log.error("Loop error: %s", e)
                        log.info("Retrying after %.1fs ...", backoff)
                        await asyncio.sleep(backoff)
                        backoff = min(60.0, backoff * 2)

                        # если страница “сломалась” — пробуем перезайти
                        try:
                            await login_abcex_if_needed(page)
                            await wait_trades_visible(page, timeout_ms=35_000)
                            panel = await find_order_history_panel(page)
                        except Exception as ee:
                            log.warning("Re-login attempt failed: %s", ee)

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


def main() -> None:
    asyncio.run(worker())


if __name__ == "__main__":
    main()
