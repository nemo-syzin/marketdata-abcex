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
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

import certifi
import httpx
from playwright.async_api import async_playwright, Browser, BrowserContext, Page

# ───────────────────────── CONFIG ─────────────────────────

ABCEX_URL = os.getenv("ABCEX_URL", "https://abcex.io/spot/USDT-RUB")
SOURCE = os.getenv("SOURCE", "abcex")
SYMBOL = os.getenv("SYMBOL", "USDT/RUB")

ABCEX_EMAIL = os.getenv("ABCEX_EMAIL", "").strip()
ABCEX_PASSWORD = os.getenv("ABCEX_PASSWORD", "").strip()

POLL_SECONDS = float(os.getenv("POLL_SECONDS", "10"))
RELOAD_EVERY_SECONDS = float(os.getenv("RELOAD_EVERY_SECONDS", "900"))
HEARTBEAT_SECONDS = float(os.getenv("HEARTBEAT_SECONDS", "60"))

# Прокси (важно: используем ТОЛЬКО для браузера/запросов в браузере)
# Формат: http://user:pass@ip:port   или http://ip:port
PROXY_URL = os.getenv("PROXY_URL", "").strip()
USE_PROXY_FOR_BROWSER = os.getenv("USE_PROXY_FOR_BROWSER", "1") == "1"

# Playwright cache path (должен быть writable на Render)
PLAYWRIGHT_BROWSERS_PATH = os.getenv("PLAYWRIGHT_BROWSERS_PATH", "/opt/render/.cache/ms-playwright").strip()

# State file for login session (writable)
ABCEX_STATE_PATH = os.getenv("ABCEX_STATE_PATH", "/opt/render/.cache/abcex_state.json").strip()
STATE_TTL_HOURS = int(os.getenv("STATE_TTL_HOURS", "72"))

# Playwright install runtime
SKIP_BROWSER_INSTALL = os.getenv("SKIP_BROWSER_INSTALL", "0") == "1"
INSTALL_COOLDOWN_SECONDS = int(os.getenv("INSTALL_COOLDOWN_SECONDS", "600"))
INSTALL_MAX_ATTEMPTS = int(os.getenv("INSTALL_MAX_ATTEMPTS", "6"))

# Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "") or os.getenv("SUPABASE_KEY", "")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "kenig_rates")
SUPABASE_ON_CONFLICT = os.getenv("SUPABASE_ON_CONFLICT", "source,symbol")

COL_SOURCE = os.getenv("COL_SOURCE", "source")
COL_SYMBOL = os.getenv("COL_SYMBOL", "symbol")
COL_RATE = os.getenv("COL_RATE", "rate")
COL_UPDATED_AT = os.getenv("COL_UPDATED_AT", "updated_at")
COL_PAYLOAD = os.getenv("COL_PAYLOAD", "payload")  # можно "" чтобы отключить

# Logs: Render любит line-buffering
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

ORDERBOOK_API = "https://api.abcex.io/spot-market/public/orderBook?symbol=USDT-RUB"


# ───────────────────────── UTIL ─────────────────────────

def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def ensure_dir_for(path: str) -> None:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)


def _strip_proxy_env(env: Dict[str, str]) -> Dict[str, str]:
    # ВАЖНО: установка браузеров должна идти без прокси (у тебя именно на этом падало).
    bad_keys = [
        "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY",
        "http_proxy", "https_proxy", "no_proxy",
    ]
    out = dict(env)
    for k in bad_keys:
        out.pop(k, None)
    return out


def _should_force_install(err: Exception) -> bool:
    s = str(err)
    return (
        "Executable doesn't exist" in s
        or "playwright install" in s
        or "chromium_headless_shell" in s
        or ("ms-playwright" in s and "doesn't exist" in s)
    )


_last_install_ts = 0.0


def _playwright_install_runtime() -> None:
    """
    Ставит chromium + chromium-headless-shell (как в твоём рабочем рапира-скрипте),
    с кулдауном и ретраями. В env для subprocess мы вычищаем прокси.
    """
    global _last_install_ts
    now = time.time()
    if now - _last_install_ts < INSTALL_COOLDOWN_SECONDS:
        logger.warning("Playwright install attempted recently; cooldown active, skipping.")
        return
    _last_install_ts = now

    ensure_dir_for(os.path.join(PLAYWRIGHT_BROWSERS_PATH, ".keep"))
    base_env = dict(os.environ)
    base_env["PLAYWRIGHT_BROWSERS_PATH"] = PLAYWRIGHT_BROWSERS_PATH
    base_env["PYTHONUNBUFFERED"] = "1"
    env = _strip_proxy_env(base_env)

    cmd = [sys.executable, "-m", "playwright", "install", "chromium", "chromium-headless-shell"]

    for attempt in range(1, INSTALL_MAX_ATTEMPTS + 1):
        logger.warning("Installing Playwright browsers (runtime)... attempt %d/%d", attempt, INSTALL_MAX_ATTEMPTS)
        try:
            r = subprocess.run(
                cmd,
                env=env,
                check=False,
                capture_output=True,
                text=True,
                timeout=20 * 60,
            )
            if r.returncode == 0:
                logger.info("Playwright browsers installed.")
                return
            logger.error("playwright install failed (%s)\nSTDOUT:\n%s\nSTDERR:\n%s",
                         r.returncode, (r.stdout or "")[-4000:], (r.stderr or "")[-4000:])
        except Exception as e:
            logger.error("Cannot run playwright install: %s", e)

        backoff = min(120.0, (2 ** (attempt - 1)) * 4.0) + random.uniform(0, 1.5)
        logger.info("Retrying after %.1fs ...", backoff)
        time.sleep(backoff)

    raise RuntimeError("Failed to install Playwright browsers after retries")


def _parse_proxy(proxy_url: str) -> Optional[Dict[str, str]]:
    if not proxy_url:
        return None
    u = urlparse(proxy_url)
    if not u.scheme or not u.hostname or not u.port:
        return None

    server = f"{u.scheme}://{u.hostname}:{u.port}"
    proxy: Dict[str, str] = {"server": server}
    if u.username:
        proxy["username"] = u.username
    if u.password:
        proxy["password"] = u.password
    return proxy


def _d(x: Any) -> Optional[Decimal]:
    if x is None:
        return None
    try:
        s = str(x).replace("\u00a0", " ").replace(" ", "").replace(",", ".")
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


# ───────────────────────── AUTH STATE ─────────────────────────

def load_state_if_valid(path: str) -> Optional[Dict[str, Any]]:
    try:
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            state = json.load(f)

        meta = state.get("_meta") or {}
        exp = meta.get("expires_at")
        if not exp:
            return None
        expires_at = datetime.fromisoformat(exp.replace("Z", "+00:00"))
        if now_utc() >= expires_at:
            return None
        return state
    except Exception:
        return None


def attach_meta(state: Dict[str, Any], ttl_hours: int) -> Dict[str, Any]:
    st = dict(state)
    st["_meta"] = {
        "created_at": now_utc().isoformat().replace("+00:00", "Z"),
        "expires_at": (now_utc() + timedelta(hours=ttl_hours)).isoformat().replace("+00:00", "Z"),
        "source": "abcex",
    }
    return st


async def ensure_logged_in(page: Page) -> None:
    """
    Логин делаем только если заданы креды и реально нужно.
    Логика максимально терпимая к изменениям UI.
    """
    if not ABCEX_EMAIL or not ABCEX_PASSWORD:
        return

    # Если уже есть признаки залогиненности — выходим
    try:
        if await page.locator("text=Log out").count() > 0:
            return
        if await page.locator("text=Logout").count() > 0:
            return
        if await page.locator("text=Выйти").count() > 0:
            return
    except Exception:
        pass

    # Кнопка логина
    login_candidates = ["Sign in", "Login", "Log in", "Войти", "Вход"]
    clicked = False
    for t in login_candidates:
        try:
            btn = page.locator(f"text={t}")
            if await btn.count() > 0:
                await btn.first.click(timeout=8000)
                clicked = True
                break
        except Exception:
            pass

    if not clicked:
        # возможно логин не нужен / нет кнопки
        return

    await page.wait_for_timeout(800)

    # Поля
    email_sel = "input[type='email'], input[name='email'], input[placeholder*='mail'], input[placeholder*='Email']"
    pass_sel = "input[type='password'], input[name='password'], input[placeholder*='Password'], input[placeholder*='пароль']"

    try:
        await page.locator(email_sel).first.fill(ABCEX_EMAIL)
        await page.locator(pass_sel).first.fill(ABCEX_PASSWORD)
    except Exception:
        logger.warning("Login inputs not found; skipping login (non-fatal).")
        return

    # Submit
    submit_candidates = ["Sign in", "Login", "Log in", "Войти", "Подтвердить"]
    for t in submit_candidates:
        try:
            b = page.locator(f"button:has-text('{t}')")
            if await b.count() > 0:
                await b.first.click(timeout=8000)
                break
        except Exception:
            pass

    await page.wait_for_timeout(1500)


# ───────────────────────── NETWORK SCRAPE ─────────────────────────

async def fetch_json_via_page(page: Page, url: str) -> Any:
    """
    Запрос делаем ВНУТРИ браузера (как у тебя локально), чтобы:
    - были нужные cookies/localStorage
    - работал прокси браузера
    """
    js = """
    async (url) => {
      const res = await fetch(url, {
        method: "GET",
        credentials: "include",
        headers: {
          "accept": "application/json, text/plain, */*",
          "cache-control": "no-cache",
          "pragma": "no-cache"
        }
      });
      const ct = res.headers.get("content-type") || "";
      const text = await res.text();
      return { ok: res.ok, status: res.status, ct, text };
    }
    """
    out = await page.evaluate(js, url)
    ct = (out.get("ct") or "").lower()
    if "application/json" not in ct and "text/json" not in ct:
        # часто капча/защита возвращает html
        raise RuntimeError(f"Non-JSON from ABCEX API | status={out.get('status')} | ct={out.get('ct')} | body={out.get('text','')[:300]}")
    try:
        return json.loads(out.get("text") or "{}")
    except Exception as e:
        raise RuntimeError(f"JSON parse error: {e} | body={out.get('text','')[:300]}") from e


def extract_best_bid_ask(orderbook: Any) -> Tuple[Optional[Decimal], Optional[Decimal]]:
    """
    Поддерживает разные форматы (list of dict / list of lists).
    """
    bids = (orderbook or {}).get("bids") or []
    asks = (orderbook or {}).get("asks") or []

    def _get_price(x: Any) -> Optional[Decimal]:
        if isinstance(x, dict):
            return _d(x.get("price"))
        if isinstance(x, (list, tuple)) and len(x) >= 1:
            return _d(x[0])
        return None

    best_bid = _get_price(bids[0]) if bids else None
    best_ask = _get_price(asks[0]) if asks else None
    return best_bid, best_ask


def build_payload(best_bid: Optional[Decimal], best_ask: Optional[Decimal], raw: Any) -> Dict[str, Any]:
    mid = None
    spread = None
    if best_bid is not None and best_ask is not None and best_bid > 0 and best_ask > 0:
        mid = (best_bid + best_ask) / Decimal("2")
        spread = best_ask - best_bid

    return {
        "source": SOURCE,
        "symbol": SYMBOL,
        "url": ABCEX_URL,
        "fetched_at": now_utc().isoformat().replace("+00:00", "Z"),
        "best_bid": str(best_bid) if best_bid is not None else None,
        "best_ask": str(best_ask) if best_ask is not None else None,
        "mid": str(mid) if mid is not None else None,
        "spread": str(spread) if spread is not None else None,
        "raw": raw,
    }


# ───────────────────────── SUPABASE ─────────────────────────

def sb_headers() -> Dict[str, str]:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("SUPABASE_URL / SUPABASE_KEY is not set")
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }


def build_supabase_row(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    По умолчанию пишем rate = mid (если есть) иначе best_ask/best_bid.
    """
    rate = payload.get("mid") or payload.get("best_ask") or payload.get("best_bid")

    row: Dict[str, Any] = {
        COL_SOURCE: payload.get("source", SOURCE),
        COL_SYMBOL: payload.get("symbol", SYMBOL),
        COL_RATE: float(rate) if rate is not None else None,
        COL_UPDATED_AT: payload.get("fetched_at"),
    }

    if COL_PAYLOAD:
        row[COL_PAYLOAD] = payload

    return row


async def upsert_supabase(client: httpx.AsyncClient, row: Dict[str, Any]) -> None:
    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
    params = {"on_conflict": SUPABASE_ON_CONFLICT}

    r = await client.post(url, headers=sb_headers(), params=params, json=[row])
    if r.status_code >= 300:
        raise RuntimeError(f"Supabase upsert failed: HTTP {r.status_code} | {r.text[:800]}")
    logger.info("Supabase upsert OK | %s=%s | %s=%s | %s=%s",
                COL_SOURCE, row.get(COL_SOURCE),
                COL_SYMBOL, row.get(COL_SYMBOL),
                COL_RATE, row.get(COL_RATE))


# ───────────────────────── BROWSER SESSION ─────────────────────────

async def open_browser(pw) -> Tuple[Browser, BrowserContext, Page]:
    proxy_cfg = _parse_proxy(PROXY_URL) if (USE_PROXY_FOR_BROWSER and PROXY_URL) else None

    launch_kwargs: Dict[str, Any] = {
        "headless": True,
        "args": ["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
    }
    if proxy_cfg:
        launch_kwargs["proxy"] = proxy_cfg

    browser = await pw.chromium.launch(**launch_kwargs)

    state = load_state_if_valid(ABCEX_STATE_PATH)
    ctx_kwargs: Dict[str, Any] = {
        "viewport": {"width": 1440, "height": 900},
        "locale": "en-US",
        "timezone_id": "Europe/Amsterdam",
    }
    if state:
        ctx_kwargs["storage_state"] = state

    context = await browser.new_context(**ctx_kwargs)
    page = await context.new_page()
    page.set_default_timeout(15_000)

    await page.goto(ABCEX_URL, wait_until="domcontentloaded", timeout=60_000)
    await page.wait_for_timeout(1200)

    # login if needed + persist state
    await ensure_logged_in(page)

    try:
        st = await context.storage_state()
        st = attach_meta(st, STATE_TTL_HOURS)
        ensure_dir_for(ABCEX_STATE_PATH)
        with open(ABCEX_STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(st, f)
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


# ───────────────────────── WORKER LOOP ─────────────────────────

async def worker() -> None:
    backoff = 2.0
    last_heartbeat = time.monotonic()
    last_reload = time.monotonic()

    ensure_dir_for(ABCEX_STATE_PATH)
    ensure_dir_for(os.path.join(PLAYWRIGHT_BROWSERS_PATH, ".keep"))

    async with httpx.AsyncClient(timeout=30, verify=certifi.where()) as sb:
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
                                _playwright_install_runtime()
                                browser, context, page = await open_browser(pw)
                            else:
                                raise
                        backoff = 2.0
                        last_reload = time.monotonic()
                        last_heartbeat = time.monotonic()

                    if time.monotonic() - last_heartbeat >= HEARTBEAT_SECONDS:
                        logger.info("Heartbeat: alive.")
                        last_heartbeat = time.monotonic()

                    if time.monotonic() - last_reload >= RELOAD_EVERY_SECONDS:
                        logger.warning("Maintenance reload...")
                        await page.goto(ABCEX_URL, wait_until="domcontentloaded", timeout=60_000)
                        await page.wait_for_timeout(1200)
                        last_reload = time.monotonic()

                    # 1) OrderBook
                    raw = await fetch_json_via_page(page, ORDERBOOK_API)
                    best_bid, best_ask = extract_best_bid_ask(raw)
                    payload = build_payload(best_bid, best_ask, raw)

                    # 2) Supabase upsert
                    row = build_supabase_row(payload)
                    await upsert_supabase(sb, row)

                    await asyncio.sleep(max(1.0, POLL_SECONDS + random.uniform(-0.3, 0.3)))

                except Exception as e:
                    logger.error("Worker error: %s", e)

                    if (not SKIP_BROWSER_INSTALL) or _should_force_install(e):
                        try:
                            _playwright_install_runtime()
                        except Exception as ie:
                            logger.error("Install failed: %s", ie)

                    logger.info("Retrying after %.1fs ...", backoff)
                    await asyncio.sleep(backoff)
                    backoff = min(60.0, backoff * 2)

                    await safe_close(browser, context, page)
                    browser = context = page = None


def main() -> None:
    asyncio.run(worker())


if __name__ == "__main__":
    main()
