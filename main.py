import asyncio
import json
import logging
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Set, Tuple

import certifi
import httpx

# ───────────────────────── CONFIG ─────────────────────────

ABCEX_URL = os.getenv("ABCEX_URL", "https://abcex.io/spot/USDT-RUB")
SOURCE = os.getenv("SOURCE", "abcex")
SYMBOL = os.getenv("SYMBOL", "USDT/RUB")

ABCEX_EMAIL = os.getenv("ABCEX_EMAIL", "")
ABCEX_PASSWORD = os.getenv("ABCEX_PASSWORD", "")

LIMIT = int(os.getenv("LIMIT", "200"))
POLL_SECONDS = float(os.getenv("POLL_SECONDS", os.getenv("POLL_SEC", "2.0")))

# Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "exchange_trades")
SUPABASE_ON_CONFLICT = os.getenv(
    "SUPABASE_ON_CONFLICT",
    "source,symbol,trade_time,price,volume_usdt"
)

# Proxy (optional) — можно использовать и для Playwright и для httpx
PROXY_SERVER = os.getenv("PROXY_SERVER", "").strip()          # пример: http://5.22.207.65:50100
PROXY_USERNAME = os.getenv("PROXY_USERNAME", "").strip()
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD", "").strip()

# Playwright browsers path INSIDE project => попадает в artifact
PW_BROWSERS_PATH = os.getenv("PLAYWRIGHT_BROWSERS_PATH", os.path.join(os.getcwd(), ".pw-browsers"))
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = PW_BROWSERS_PATH

# Можно закрепить хост скачивания (иногда помогает)
os.environ.setdefault("PLAYWRIGHT_DOWNLOAD_HOST", "https://playwright.azureedge.net")

# Если задан proxy — пробрасываем его и в subprocess (install), и в http-клиенты (опционально)
def _proxy_url() -> Optional[str]:
    if not PROXY_SERVER:
        return None
    if PROXY_USERNAME and PROXY_PASSWORD:
        # вставляем креды в URL, если они не вставлены
        if "://" in PROXY_SERVER and "@" not in PROXY_SERVER:
            scheme, rest = PROXY_SERVER.split("://", 1)
            return f"{scheme}://{PROXY_USERNAME}:{PROXY_PASSWORD}@{rest}"
    return PROXY_SERVER

PROXY_URL = _proxy_url()
if PROXY_URL:
    os.environ.setdefault("HTTP_PROXY", PROXY_URL)
    os.environ.setdefault("HTTPS_PROXY", PROXY_URL)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
}

# ───────────────────── LOGGER ───────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("abcex-worker")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# ───────────────────── MODEL ────────────────────────

@dataclass(frozen=True)
class Trade:
    price: Decimal
    qty: Decimal         # USDT
    time_str: str        # HH:MM:SS
    side: Optional[str]  # buy/sell/None

def _d(x: Any) -> Optional[Decimal]:
    if x is None:
        return None
    s = str(x).strip().replace(" ", "").replace(",", ".")
    if not s:
        return None
    try:
        return Decimal(s)
    except InvalidOperation:
        return None

def _round4(x: Decimal) -> Decimal:
    return x.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

def _round2(x: Decimal) -> Decimal:
    return x.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

def trade_key(t: Trade) -> Tuple[str, str, str, str]:
    # дедуп по ключевым полям
    return (t.time_str, str(t.price), str(t.qty), (t.side or ""))

# ───────────────────── SUPABASE ─────────────────────

def _sb_headers() -> Dict[str, str]:
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }

async def supabase_upsert(rows: List[Dict[str, Any]]) -> None:
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.warning("Supabase is not configured (SUPABASE_URL / SUPABASE_KEY empty). Skip upsert.")
        return
    if not rows:
        return

    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
    params = {"on_conflict": SUPABASE_ON_CONFLICT}

    async with httpx.AsyncClient(
        headers=_sb_headers(),
        timeout=30,
        verify=certifi.where(),
    ) as client:
        r = await client.post(url, params=params, json=rows)
        if r.status_code >= 300:
            logger.error("Supabase upsert failed | HTTP %s | %s", r.status_code, r.text[:500])
            r.raise_for_status()

# ───────────────────── PLAYWRIGHT INSTALL ─────────────────────

def _has_headless_shell() -> bool:
    # Ищем headless_shell в PLAYWRIGHT_BROWSERS_PATH
    root = PW_BROWSERS_PATH
    if not os.path.isdir(root):
        return False

    # возможные варианты папок у PW:
    # chromium_headless_shell-1148/chrome-linux/headless_shell
    # chromium_headless_shell-*/chrome-linux/headless_shell
    for name in os.listdir(root):
        if not name.startswith("chromium_headless_shell-"):
            continue
        candidate = os.path.join(root, name, "chrome-linux", "headless_shell")
        if os.path.isfile(candidate):
            return True
    return False

def ensure_playwright_browsers(max_attempts: int = 6) -> None:
    """
    Ставит ТОЛЬКО chromium-headless-shell (меньше скачивание, выше шанс успеха).
    Ставит в ./.pw-browsers (artifact), чтобы не качать на каждом рестарте.
    """
    os.makedirs(PW_BROWSERS_PATH, exist_ok=True)

    if _has_headless_shell():
        logger.info("Playwright browser already present in %s", PW_BROWSERS_PATH)
        return

    delay = 2.0
    for attempt in range(1, max_attempts + 1):
        logger.warning("Installing Playwright browsers (runtime)... attempt %d/%d", attempt, max_attempts)
        cmd = [sys.executable, "-m", "playwright", "install", "chromium-headless-shell"]

        env = os.environ.copy()
        # На всякий случай пробросим прокси и сюда
        if PROXY_URL:
            env["HTTP_PROXY"] = PROXY_URL
            env["HTTPS_PROXY"] = PROXY_URL

        p = subprocess.run(cmd, capture_output=True, text=True, env=env)
        if p.returncode == 0 and _has_headless_shell():
            logger.info("Playwright browsers installed successfully.")
            return

        logger.error("playwright install failed (%s)", p.returncode)
        if p.stdout:
            logger.error("STDOUT:\n%s", p.stdout[-1200:])
        if p.stderr:
            logger.error("STDERR:\n%s", p.stderr[-1200:])

        if attempt < max_attempts:
            logger.info("Retrying after %.1fs ...", delay)
            time.sleep(delay)
            delay = min(delay * 2, 60.0)

    raise RuntimeError("Failed to install Playwright browsers after retries.")

# ───────────────────── SCRAPER ─────────────────────

async def _login_if_needed(page) -> None:
    # Логика максимально мягкая: если видим email/password — логинимся
    try:
        email = page.locator("input[type='email'], input[name='email']")
        pwd = page.locator("input[type='password'], input[name='password']")
        if await email.count() == 0 or await pwd.count() == 0:
            return

        if not ABCEX_EMAIL or not ABCEX_PASSWORD:
            logger.warning("Login form is visible but ABCEX_EMAIL/ABCEX_PASSWORD are empty. Skip login.")
            return

        await email.first.fill(ABCEX_EMAIL)
        await pwd.first.fill(ABCEX_PASSWORD)

        # кнопка входа (подстраховка по текстам)
        btn = page.locator("button:has-text('Login'), button:has-text('Войти'), button[type='submit']")
        if await btn.count() > 0:
            await btn.first.click()
            await page.wait_for_timeout(1500)
            logger.info("Login submitted.")
    except Exception as e:
        logger.warning("Login step skipped/failed: %s", e)

async def extract_trades_from_panel(page) -> List[Trade]:
    """
    Достаём последние сделки из панели сделок.
    Подход как у тебя локально: берём наборы <p> в блоке и режем в строки.
    """
    # ждём, что страница прогрузится
    await page.wait_for_timeout(1500)

    # Пытаемся найти хоть одну "временную" ячейку
    try:
        await page.locator("p:has-text(':')").first.wait_for(timeout=7000)
    except Exception:
        pass

    # Берём body как root и вытаскиваем до LIMIT строк
    handle = page.locator("body").first

    raw_rows: List[Dict[str, Any]] = await handle.evaluate(
        """(root, limit) => {
          const out = [];

          // Берём все <p>, которые похожи на строки панели сделок.
          // Типичная строка: Price / Amount / Time (+ color/side)
          const ps = Array.from(root.querySelectorAll('p')).map(p => ({
            text: (p.textContent || '').trim(),
            color: (getComputedStyle(p).color || ''),
          }));

          function looksLikeTime(s){ return /^\\d{2}:\\d{2}:\\d{2}$/.test(s); }
          function looksLikeNumber(s){ return /^\\d+(?:[\\s,\\.]\\d+)?$/.test(s.replace(/\\s/g,'')); }

          // Скользящим окном собираем тройки (price, qty, time)
          for (let i = 0; i < ps.length - 2; i++) {
            const a = ps[i], b = ps[i+1], c = ps[i+2];
            if (!looksLikeTime(c.text)) continue;
            if (!looksLikeNumber(a.text) || !looksLikeNumber(b.text)) continue;

            // side по цвету (очень грубо, но лучше чем ничего)
            let side = null;
            const color = a.color || '';
            if (color.includes('rgb')) {
              // часто buy = зелёный, sell = красный
              if (color.includes('0, 128') || color.includes('0,128') || color.includes('46, 204')) side = 'buy';
              if (color.includes('231, 76') || color.includes('255, 0') || color.includes('220, 53')) side = 'sell';
            }

            out.push({ price_raw: a.text, qty_raw: b.text, time: c.text, side });
            if (out.length >= limit) break;
          }

          return out;
        }""",
        LIMIT,
    )

    trades: List[Trade] = []
    for r in raw_rows:
        price = _d(r.get("price_raw"))
        qty = _d(r.get("qty_raw"))
        t = str(r.get("time", "")).strip()
        side = r.get("side")
        if isinstance(side, str):
            side = side.strip().lower()
            if side not in ("buy", "sell"):
                side = None

        if price is None or qty is None:
            continue
        if not re.match(r"^\d{2}:\d{2}:\d{2}$", t):
            continue

        trades.append(
            Trade(
                price=_round2(price),
                qty=_round4(qty),
                time_str=t,
                side=side,
            )
        )

    return trades

async def scrape_abcex_trades() -> List[Trade]:
    # импортируем здесь, чтобы переменные окружения PLAYWRIGHT_BROWSERS_PATH применились корректно
    from playwright.async_api import async_playwright

    ensure_playwright_browsers()

    proxy_cfg = None
    if PROXY_URL:
        # playwright хочет server без логина в URL (лучше разнести)
        server = PROXY_SERVER
        if server and "@" in server:
            # если пользователь всё-таки положил креды в PROXY_SERVER — оставим как есть
            proxy_cfg = {"server": server}
        else:
            proxy_cfg = {"server": server}
            if PROXY_USERNAME and PROXY_PASSWORD:
                proxy_cfg["username"] = PROXY_USERNAME
                proxy_cfg["password"] = PROXY_PASSWORD

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
            proxy=proxy_cfg,
        )
        context = await browser.new_context(user_agent=HEADERS["User-Agent"])
        page = await context.new_page()

        await page.goto(ABCEX_URL, wait_until="domcontentloaded", timeout=45_000)
        await _login_if_needed(page)

        trades = await extract_trades_from_panel(page)

        await context.close()
        await browser.close()

    return trades

# ───────────────────── WORKER LOOP ─────────────────────

def _rows_for_supabase(trades: List[Trade]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    now_utc = datetime.now(timezone.utc).isoformat()

    for t in trades:
        volume_rub = _round2(t.price * t.qty)
        out.append(
            {
                "source": SOURCE,
                "symbol": SYMBOL,
                "price": float(t.price),
                "volume_usdt": float(t.qty),
                "volume_rub": float(volume_rub),
                "trade_time": t.time_str,     # как в твоей таблице/скриптах
                "side": t.side,
                "created_at": now_utc,
            }
        )
    return out

async def main() -> None:
    logger.info("Starting ABCEX worker | url=%s | limit=%s | poll=%.2fs", ABCEX_URL, LIMIT, POLL_SECONDS)
    logger.info("Playwright browsers path: %s", PW_BROWSERS_PATH)

    seen: Set[Tuple[str, str, str, str]] = set()

    while True:
        try:
            trades = await scrape_abcex_trades()

            # На первом прогоне просто инициализируем seen
            if not seen:
                for t in trades:
                    seen.add(trade_key(t))
                logger.info("INIT: fetched=%d", len(trades))
            else:
                new_trades = [t for t in trades if trade_key(t) not in seen]
                if new_trades:
                    for t in trades:
                        seen.add(trade_key(t))

                    rows = _rows_for_supabase(new_trades)
                    await supabase_upsert(rows)

                    logger.info("NEW: inserted=%d (fetched=%d)", len(new_trades), len(trades))
                else:
                    logger.info("No new trades (fetched=%d).", len(trades))

            # подчищаем set, чтобы не рос бесконечно
            if len(seen) > 50_000:
                seen = {trade_key(t) for t in trades}

        except Exception as e:
            logger.exception("Worker error: %s", e)

        await asyncio.sleep(POLL_SECONDS)

if __name__ == "__main__":
    asyncio.run(main())
