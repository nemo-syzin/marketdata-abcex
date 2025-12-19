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
from datetime import datetime, timezone, timedelta
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

import certifi
import httpx
from playwright.async_api import async_playwright, Page, Browser, BrowserContext, Locator

# ───────────────────────── CONFIG ─────────────────────────

ABCEX_URL = os.getenv("ABCEX_URL", "https://abcex.io/spot/USDT-RUB")
SOURCE = os.getenv("SOURCE", "abcex")
SYMBOL = os.getenv("SYMBOL", "USDT/RUB")

ABCEX_EMAIL = os.getenv("ABCEX_EMAIL", "")
ABCEX_PASSWORD = os.getenv("ABCEX_PASSWORD", "")

LIMIT = int(os.getenv("LIMIT", "200"))
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "10"))

# Proxy for outbound traffic (including playwright install downloads)
# Example: http://user:pass@ip:port
PROXY_URL = os.getenv("PROXY_URL", "").strip()

# If your proxy is HTTP, it's still fine to set for HTTPS_PROXY;
# it will be used as a CONNECT proxy.
USE_PROXY_FOR_BROWSER = os.getenv("USE_PROXY_FOR_BROWSER", "1") == "1"

# Playwright install tuning
PW_INSTALL_MAX_ATTEMPTS = int(os.getenv("PW_INSTALL_MAX_ATTEMPTS", "8"))
PW_INSTALL_BASE_BACKOFF_SEC = float(os.getenv("PW_INSTALL_BASE_BACKOFF_SEC", "4"))

# Use headless shell to reduce download size (recommended for Render)
PW_INSTALL_ONLY_SHELL = os.getenv("PW_INSTALL_ONLY_SHELL", "1") == "1"

# Keep browsers in a writable cache path
# Render typically allows /opt/render/.cache
PLAYWRIGHT_BROWSERS_PATH = os.getenv("PLAYWRIGHT_BROWSERS_PATH", "/opt/render/.cache/ms-playwright").strip()

# Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "") or os.getenv("SUPABASE_KEY", "")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "kenig_rates")
SUPABASE_ON_CONFLICT = os.getenv("SUPABASE_ON_CONFLICT", "source,symbol")  # adjust if needed

# Column mapping (change if your table differs)
COL_SOURCE = os.getenv("COL_SOURCE", "source")
COL_SYMBOL = os.getenv("COL_SYMBOL", "symbol")
COL_RATE = os.getenv("COL_RATE", "rate")
COL_UPDATED_AT = os.getenv("COL_UPDATED_AT", "updated_at")
COL_PAYLOAD = os.getenv("COL_PAYLOAD", "payload")  # optional jsonb column

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("abcex-worker")

# ───────────────────────── MODEL ─────────────────────────

@dataclass(frozen=True)
class Trade:
    ts: datetime
    price: float
    amount: float
    side: Optional[str] = None  # buy/sell

# ─────────────────────── HELPERS ────────────────────────

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def _as_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        # normalize "12 345,67" -> "12345.67"
        s = str(x)
        s = s.replace("\u00a0", " ").replace(" ", "").replace(",", ".")
        return float(s)
    except Exception:
        return None

def _parse_side(text: str) -> Optional[str]:
    t = text.lower()
    if "buy" in t or "покуп" in t:
        return "buy"
    if "sell" in t or "прод" in t:
        return "sell"
    return None

def env_with_proxy(base_env: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    env = dict(base_env or os.environ)
    env["PLAYWRIGHT_BROWSERS_PATH"] = PLAYWRIGHT_BROWSERS_PATH

    if PROXY_URL:
        env["HTTP_PROXY"] = PROXY_URL
        env["HTTPS_PROXY"] = PROXY_URL
        env["http_proxy"] = PROXY_URL
        env["https_proxy"] = PROXY_URL

    # Helps some environments keep downloads stable / avoid buffering
    env.setdefault("PYTHONUNBUFFERED", "1")
    return env

def ensure_playwright_browsers() -> None:
    """
    Ensures Playwright browser binaries exist.
    Uses --only-shell (smaller download) and respects PROXY_URL.
    """
    # quick heuristic: if cache dir contains chromium* folders - likely installed
    try:
        if os.path.isdir(PLAYWRIGHT_BROWSERS_PATH):
            for name in os.listdir(PLAYWRIGHT_BROWSERS_PATH):
                if name.startswith("chromium") or "chromium" in name:
                    # not a guarantee, but avoids re-install loops
                    return
    except Exception:
        pass

    args = [sys.executable, "-m", "playwright", "install"]
    if PW_INSTALL_ONLY_SHELL:
        # reduces downloads significantly for headless workloads
        args += ["--only-shell"]
    args += ["chromium"]

    env = env_with_proxy()

    for attempt in range(1, PW_INSTALL_MAX_ATTEMPTS + 1):
        logger.warning("Installing Playwright browsers (runtime)... attempt %d/%d", attempt, PW_INSTALL_MAX_ATTEMPTS)
        try:
            p = subprocess.run(
                args,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=15 * 60,
            )
            if p.returncode == 0:
                logger.info("Playwright browsers installed successfully.")
                return

            logger.error("playwright install failed (%s)\nSTDOUT:\n%s\nSTDERR:\n%s", p.returncode, p.stdout[-4000:], p.stderr[-4000:])
        except Exception as e:
            logger.exception("playwright install exception: %s", e)

        backoff = PW_INSTALL_BASE_BACKOFF_SEC * (2 ** (attempt - 1))
        backoff = min(backoff, 120.0) + random.uniform(0, 1.5)
        logger.info("Retrying after %.1fs ...", backoff)
        time.sleep(backoff)

    raise RuntimeError("Failed to install Playwright browsers after retries")

# ─────────────────────── SCRAPER ────────────────────────

async def login_if_needed(page: Page) -> None:
    """
    Minimal login flow.
    If you don't need auth for the table, you can keep creds empty.
    """
    if not ABCEX_EMAIL or not ABCEX_PASSWORD:
        return

    # Heuristic: if login button exists
    try:
        login_btn = page.locator("text=Login").first
        if await login_btn.count() == 0:
            login_btn = page.locator("text=Войти").first
        if await login_btn.count() == 0:
            return
        await login_btn.click(timeout=5000)

        email_inp = page.locator("input[type='email'], input[name='email']").first
        pass_inp = page.locator("input[type='password'], input[name='password']").first
        await email_inp.fill(ABCEX_EMAIL)
        await pass_inp.fill(ABCEX_PASSWORD)

        submit = page.locator("button[type='submit']").first
        if await submit.count() == 0:
            submit = page.locator("text=Sign in").first
        if await submit.count() == 0:
            submit = page.locator("text=Войти").nth(1)
        await submit.click(timeout=5000)

        await page.wait_for_timeout(1500)
    except Exception:
        # do not hard-fail login; some pages show data without auth
        logger.info("Login flow skipped/failed (non-fatal).")

async def parse_trades_from_table(page: Page, limit: int) -> List[Trade]:
    """
    Tries to parse last trades table.
    Selectors may require adjustment if ABCEX changes UI.
    """
    # This selector strategy is intentionally defensive:
    # - find a table-like block containing recent trades
    # - parse rows with (time, price, amount, side)
    trades: List[Trade] = []

    # wait for the page to load some content
    await page.wait_for_timeout(1200)

    # candidate rows
    row_candidates = [
        "table tbody tr",
        "[role='row']",
        "div:has(> div):has-text(':')",  # fallback
    ]

    rows: Optional[Locator] = None
    for sel in row_candidates:
        loc = page.locator(sel)
        if await loc.count() > 5:
            rows = loc
            break

    if rows is None:
        return trades

    n = min(await rows.count(), limit)
    for i in range(n):
        row = rows.nth(i)
        try:
            txt = _clean(await row.inner_text())
            # basic filter: must contain digits (price/amount)
            if not re.search(r"\d", txt):
                continue

            # try to extract floats (price/amount) from the row text
            nums = re.findall(r"[\d\s]+[.,]?\d*", txt)
            nums = [x for x in nums if re.search(r"\d", x)]
            floats = [_as_float(x) for x in nums]
            floats = [x for x in floats if x is not None]

            if len(floats) < 2:
                continue

            price = floats[0]
            amount = floats[1]

            side = _parse_side(txt)
            trades.append(Trade(ts=now_utc(), price=price, amount=amount, side=side))
        except Exception:
            continue

    return trades

def compute_metrics(trades: List[Trade]) -> Dict[str, Any]:
    if not trades:
        return {"count": 0}

    # assume newest first from UI; if not, still fine
    last_price = trades[0].price
    sum_qty = sum(t.amount for t in trades)
    vwap = (sum(t.price * t.amount for t in trades) / sum_qty) if sum_qty > 0 else 0.0

    return {
        "count": len(trades),
        "last_price": last_price,
        "sum_qty": sum_qty,
        "vwap": vwap,
    }

async def scrape_abcex_once() -> Dict[str, Any]:
    ensure_playwright_browsers()

    proxy_cfg = None
    if USE_PROXY_FOR_BROWSER and PROXY_URL:
        # Playwright expects server without credentials separately if you want auth fields,
        # but it also accepts full URL for simple cases.
        proxy_cfg = {"server": PROXY_URL}

    async with async_playwright() as p:
        browser: Browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
            ],
        )
        context_kwargs = {}
        if proxy_cfg:
            context_kwargs["proxy"] = proxy_cfg

        context: BrowserContext = await browser.new_context(**context_kwargs)
        page: Page = await context.new_page()

        await page.goto(ABCEX_URL, wait_until="domcontentloaded", timeout=60_000)
        await login_if_needed(page)

        # give UI time to render table
        await page.wait_for_timeout(1500)

        trades = await parse_trades_from_table(page, LIMIT)
        metrics = compute_metrics(trades)

        await context.close()
        await browser.close()

    return {
        "source": SOURCE,
        "symbol": SYMBOL,
        "url": ABCEX_URL,
        "fetched_at": now_utc().isoformat(),
        "metrics": metrics,
        "trades": [
            {"ts": t.ts.isoformat(), "price": t.price, "amount": t.amount, "side": t.side}
            for t in trades
        ],
    }

# ─────────────────────── SUPABASE ────────────────────────

def supabase_headers() -> Dict[str, str]:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY not set")
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }

def build_supabase_row(scrape: Dict[str, Any]) -> Dict[str, Any]:
    """
    Adjust mapping via env COL_* if your table columns differ.
    """
    metrics = scrape.get("metrics") or {}
    rate = metrics.get("last_price") or metrics.get("vwap")

    row: Dict[str, Any] = {
        COL_SOURCE: scrape.get("source", SOURCE),
        COL_SYMBOL: scrape.get("symbol", SYMBOL),
        COL_RATE: rate,
        COL_UPDATED_AT: scrape.get("fetched_at"),
    }

    # Optional payload column (jsonb). If your table doesn't have it,
    # set COL_PAYLOAD="" or remove in env.
    if COL_PAYLOAD:
        row[COL_PAYLOAD] = scrape

    return row

async def upsert_to_supabase(client: httpx.AsyncClient, row: Dict[str, Any]) -> None:
    # POST with upsert behavior
    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?on_conflict={SUPABASE_ON_CONFLICT}"
    r = await client.post(url, headers=supabase_headers(), json=[row])
    if r.status_code >= 300:
        raise RuntimeError(f"Supabase upsert failed: HTTP {r.status_code} | {r.text[:800]}")
    logger.info("Supabase upsert OK | %s=%s | %s=%s | %s=%s",
                COL_SOURCE, row.get(COL_SOURCE),
                COL_SYMBOL, row.get(COL_SYMBOL),
                COL_RATE, row.get(COL_RATE))

# ───────────────────────── MAIN ─────────────────────────

async def main() -> None:
    logger.info("Starting ABCEX worker | url=%s | symbol=%s | poll=%.1fs", ABCEX_URL, SYMBOL, POLL_SECONDS)

    async with httpx.AsyncClient(timeout=30, verify=certifi.where()) as sb:
        while True:
            try:
                scrape = await scrape_abcex_once()
                row = build_supabase_row(scrape)
                await upsert_to_supabase(sb, row)
            except Exception as e:
                logger.error("Worker error: %s", e)

            await asyncio.sleep(POLL_SECONDS)

if __name__ == "__main__":
    asyncio.run(main())
