#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import httpx
from playwright.async_api import async_playwright, Page

# ==========================
# CONFIG (ENV)
# ==========================
ABCEX_URL = os.getenv("ABCEX_URL", "https://abcex.io/client/spot/USDTRUB")
STATE_PATH = os.getenv("ABCEX_STATE_PATH", "abcex_state.json")

SOURCE = os.getenv("SOURCE", "abcex")
SYMBOL = os.getenv("SYMBOL", "USDT/RUB")

ABCEX_EMAIL = os.getenv("ABCEX_EMAIL", "")
ABCEX_PASSWORD = os.getenv("ABCEX_PASSWORD", "")

MAX_TRADES = int(os.getenv("MAX_TRADES", "30"))
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "7"))
HEADLESS = os.getenv("ABCEX_HEADLESS", "1").strip().lower() not in ("0", "false", "no")

# Render-friendly browsers cache
PW_CACHE = os.getenv("PLAYWRIGHT_BROWSERS_PATH", "/opt/render/.cache/ms-playwright")
os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", PW_CACHE)

# Optional: if you have proxy for downloads / site access
# os.environ.setdefault("HTTPS_PROXY", "http://user:pass@host:port")

# Optional: Supabase sink (if configured)
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "") or os.getenv("SUPABASE_KEY", "")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "abcex_trades")  # default separate table

# ==========================
# LOGGING
# ==========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("abcex-render")


# ==========================
# HELPERS
# ==========================
TIME_RE = re.compile(r"^\d{2}:\d{2}:\d{2}$")


def mask_email(email: str) -> str:
    if not email or "@" not in email:
        return ""
    name, dom = email.split("@", 1)
    if len(name) <= 2:
        return "***@" + dom
    return f"{name[:2]}***@{dom}"


def normalize_num(text: str) -> float:
    t = (text or "").strip().replace("\xa0", " ")
    t = t.replace(" ", "")
    if "," in t and "." in t:
        t = t.replace(",", "")
    else:
        t = t.replace(",", ".")
    return float(t)


async def save_debug(page: Page, tag: str) -> None:
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    png = f"abcex_{tag}_{ts}.png"
    html = f"abcex_{tag}_{ts}.html"
    try:
        await page.screenshot(path=png, full_page=True)
        log.warning("Saved debug screenshot: %s", png)
    except Exception as e:
        log.warning("Could not save screenshot: %s", e)

    try:
        content = await page.content()
        with open(html, "w", encoding="utf-8") as f:
            f.write(content)
        log.warning("Saved debug html: %s", html)
    except Exception as e:
        log.warning("Could not save html: %s", e)


async def accept_cookies_if_any(page: Page) -> None:
    candidates = ["Принять", "Согласен", "Я согласен", "Accept", "I agree"]
    for txt in candidates:
        try:
            btn = page.locator(f"text={txt}").first
            if await btn.count() > 0 and await btn.is_visible():
                await btn.click(timeout=5_000)
                log.info("Cookies banner: clicked '%s'", txt)
                await page.wait_for_timeout(700)
                return
        except Exception:
            pass


def browsers_installed() -> Tuple[bool, bool]:
    # Playwright stores browsers like:
    #   chromium-XXXX/
    #   chromium_headless_shell-XXXX/
    try:
        if not os.path.isdir(PW_CACHE):
            return False, False
        has_chromium = any(name.startswith("chromium-") for name in os.listdir(PW_CACHE))
        has_shell = any(name.startswith("chromium_headless_shell-") for name in os.listdir(PW_CACHE))
        return has_chromium, has_shell
    except Exception:
        return False, False


def ensure_playwright_browsers(force: bool = False, retries: int = 5) -> None:
    """
    Robust installation for Render:
    - no cooldown logic
    - uses `python -m playwright install ...`
    - retries for flaky CDN connections
    """
    has_chromium, has_shell = browsers_installed()
    if (has_chromium and has_shell) and not force:
        log.info("Playwright browsers already present in %s", PW_CACHE)
        return

    cmd = [sys.executable, "-m", "playwright", "install", "chromium", "chromium-headless-shell"]

    for attempt in range(1, retries + 1):
        log.warning("Installing Playwright browsers... attempt=%d/%d path=%s", attempt, retries, PW_CACHE)
        try:
            p = subprocess.run(
                cmd,
                check=False,
                capture_output=True,
                text=True,
                env=os.environ.copy(),
            )
            if p.returncode == 0:
                has_chromium, has_shell = browsers_installed()
                log.warning("Install verification: chromium=%s headless_shell=%s", has_chromium, has_shell)
                if has_shell:  # headless_shell is the key one for headless runs
                    return
            else:
                log.warning("playwright install returncode=%s", p.returncode)
                if p.stdout:
                    log.warning("playwright install STDOUT:\n%s", p.stdout[-4000:])
                if p.stderr:
                    log.warning("playwright install STDERR:\n%s", p.stderr[-4000:])
        except Exception as e:
            log.warning("Install exception: %s", e)

        # backoff
        sleep_s = min(60, 3 * attempt + random.random() * 2)
        time.sleep(sleep_s)

    # Final check
    has_chromium, has_shell = browsers_installed()
    if not has_shell:
        raise RuntimeError(
            "Playwright browsers install failed. "
            "CDN downloads are unstable on your Render instance. "
            "Best fix: deploy via Docker using Playwright base image (browsers preinstalled)."
        )


# ==========================
# LOGIN (more robust)
# ==========================
async def password_field_visible(page: Page) -> bool:
    try:
        pw = page.locator("input[type='password']").first
        return await pw.count() > 0 and await pw.is_visible()
    except Exception:
        return False


async def looks_logged_in(page: Page) -> bool:
    """
    Heuristic: if we see an account/email-like button or "Выход"/"Logout".
    """
    try:
        if ABCEX_EMAIL and await page.locator(f"text={ABCEX_EMAIL}").count() > 0:
            return True
        # any '@' snippet in visible buttons (often user menu)
        ok = await page.evaluate(
            """() => {
                const els = Array.from(document.querySelectorAll('button,a,div'));
                return els.some(el => {
                  const t = (el.textContent || '').trim();
                  if (!t) return false;
                  if (t.includes('@') && t.length < 60) return true;
                  if (/logout|выход/i.test(t)) return true;
                  return false;
                });
            }"""
        )
        return bool(ok)
    except Exception:
        return False


async def open_login_form(page: Page) -> None:
    candidates = [
        "button:has-text('Войти')",
        "a:has-text('Войти')",
        "button:has-text('Login')",
        "a:has-text('Login')",
        "button:has-text('Sign in')",
        "a:has-text('Sign in')",
    ]
    for sel in candidates:
        try:
            loc = page.locator(sel).first
            if await loc.count() > 0 and await loc.is_visible():
                await loc.click(timeout=10_000)
                await page.wait_for_timeout(800)
                return
        except Exception:
            pass


async def login_if_needed(page: Page, email: str, password: str) -> None:
    if await looks_logged_in(page):
        log.info("Login not required (already in session).")
        return

    # Sometimes login modal is not open; try to open it
    if not await password_field_visible(page):
        await open_login_form(page)

    # Wait a bit for fields to appear
    for _ in range(25):
        if await password_field_visible(page):
            break
        await page.wait_for_timeout(300)

    if not await password_field_visible(page):
        await save_debug(page, "login_form_not_found")
        raise RuntimeError("Login required, but password input not visible (modal not opened / DOM changed).")

    # Fill email/password with multiple selectors
    email_selectors = [
        "input[type='email']",
        "input[name='email']",
        "input[placeholder*='mail' i]",
        "input[placeholder*='почт' i]",
        "input[autocomplete='username']",
    ]
    pw_selectors = [
        "input[type='password']",
        "input[name='password']",
        "input[placeholder*='парол' i]",
        "input[autocomplete='current-password']",
    ]

    email_filled = False
    for sel in email_selectors:
        try:
            loc = page.locator(sel).first
            if await loc.count() > 0 and await loc.is_visible():
                await loc.fill(email, timeout=10_000)
                email_filled = True
                break
        except Exception:
            pass

    pw_filled = False
    for sel in pw_selectors:
        try:
            loc = page.locator(sel).first
            if await loc.count() > 0 and await loc.is_visible():
                await loc.fill(password, timeout=10_000)
                pw_filled = True
                break
        except Exception:
            pass

    if not email_filled or not pw_filled:
        await save_debug(page, "login_fields_not_found")
        raise RuntimeError("Не смог найти/заполнить поля email/password (DOM отличается).")

    # Click submit
    btn_texts = ["Войти", "Login", "Sign in", "Вход"]
    clicked = False
    for t in btn_texts:
        try:
            btn = page.locator(f"button:has-text('{t}')").first
            if await btn.count() > 0 and await btn.is_visible():
                await btn.click(timeout=10_000)
                clicked = True
                break
        except Exception:
            pass

    if not clicked:
        try:
            await page.keyboard.press("Enter")
        except Exception:
            pass

    # Wait for password input to disappear OR logged-in heuristic
    deadline = time.time() + 35
    while time.time() < deadline:
        if not await password_field_visible(page) and await looks_logged_in(page):
            log.info("Login successful.")
            return
        await page.wait_for_timeout(500)

    await save_debug(page, "login_failed")
    raise RuntimeError("Логин не подтвердился (возможно 2FA/капча/иной флоу).")


# ==========================
# TRADES PARSING (no panel dependency)
# ==========================
async def wait_trades_presence(page: Page, timeout_ms: int = 35_000) -> None:
    start = time.time()
    while (time.time() - start) * 1000 < timeout_ms:
        try:
            ok = await page.evaluate(
                """() => {
                    const re = /^\\d{2}:\\d{2}:\\d{2}$/;
                    const ps = Array.from(document.querySelectorAll('p,span,div'));
                    return ps.some(p => re.test((p.textContent||'').trim()));
                }"""
            )
            if ok:
                return
        except Exception:
            pass
        await page.wait_for_timeout(700)

    await save_debug(page, "trades_not_visible")
    raise RuntimeError("Не дождался появления времени сделок (HH:MM:SS) в DOM.")


@dataclass
class Trade:
    price: float
    qty: float
    time: str
    side: Optional[str]
    price_raw: str
    qty_raw: str


async def extract_trades_best_effort(page: Page, limit: int) -> List[Trade]:
    """
    универсальный парсер:
    - ищем элементы с текстом времени HH:MM:SS
    - берём родителя, ищем рядом цену/кол-во
    - фильтруем по numeric паттернам
    """
    raw: List[Dict[str, Any]] = await page.evaluate(
        """(limit) => {
          const isTime = (s) => /^\\d{2}:\\d{2}:\\d{2}$/.test((s||'').trim());
          const isNum = (s) => /^[0-9][0-9\\s\\u00A0.,]*$/.test((s||'').trim());

          // try to detect side via computed color (green/red-ish)
          const detectSide = (el) => {
            try {
              const cs = window.getComputedStyle(el);
              const c = (cs.color || '').toLowerCase();
              // crude heuristic: green channel dominant => buy; red channel dominant => sell
              // parse rgb(a)
              const m = c.match(/rgba?\\((\\d+),\\s*(\\d+),\\s*(\\d+)/);
              if (!m) return null;
              const r = parseInt(m[1],10), g = parseInt(m[2],10), b = parseInt(m[3],10);
              if (g > r + 20 && g > b + 20) return 'buy';
              if (r > g + 20 && r > b + 20) return 'sell';
              return null;
            } catch { return null; }
          };

          const times = [];
          const nodes = Array.from(document.querySelectorAll('p,span,div'));
          for (const n of nodes) {
            const t = (n.textContent || '').trim();
            if (isTime(t)) times.push(n);
          }

          const out = [];
          for (const tNode of times) {
            // pick a reasonable container
            let root = tNode.parentElement;
            for (let i = 0; i < 4 && root; i++) {
              // we want containers that have multiple children
              if (root.children && root.children.length >= 3) break;
              root = root.parentElement;
            }
            if (!root) continue;

            // scan within root: try to find triples [price, qty, time]
            const kids = Array.from(root.querySelectorAll(':scope > p, :scope > span, :scope > div'));
            if (kids.length < 3) continue;

            // we need the exact node in this kids list if possible
            const idx = kids.findIndex(x => x === tNode);
            // If not direct child, fallback to text match and local window
            const windowNodes = (idx >= 0)
              ? kids.slice(Math.max(0, idx-4), Math.min(kids.length, idx+2))
              : kids;

            // build candidate list by sequential scan
            for (let i = 0; i < windowNodes.length - 2; i++) {
              const a = (windowNodes[i].textContent || '').trim();
              const b = (windowNodes[i+1].textContent || '').trim();
              const c = (windowNodes[i+2].textContent || '').trim();

              if (!isTime(c)) continue;
              if (!isNum(a) || !isNum(b)) continue;

              const side = detectSide(windowNodes[i]) || detectSide(windowNodes[i+1]) || null;

              out.push({ price_raw: a, qty_raw: b, time: c, side });
              if (out.length >= limit) return out;
            }
          }

          // As fallback: global triples in DOM order (no container assumptions)
          if (out.length === 0) {
            const texts = [];
            const ps = Array.from(document.querySelectorAll('p'));
            for (const p of ps) {
              const t = (p.textContent || '').trim();
              texts.push({t, el: p});
            }
            for (let i = 0; i < texts.length - 2; i++) {
              const a = texts[i].t, b = texts[i+1].t, c = texts[i+2].t;
              if (isTime(c) && isNum(a) && isNum(b)) {
                out.push({ price_raw: a, qty_raw: b, time: c, side: null });
                if (out.length >= limit) break;
              }
            }
          }

          return out;
        }""",
        limit,
    )

    trades: List[Trade] = []
    for r in raw:
        try:
            pr = str(r.get("price_raw", "")).strip()
            qr = str(r.get("qty_raw", "")).strip()
            tt = str(r.get("time", "")).strip()
            side = r.get("side", None)
            if not pr or not qr or not tt or not TIME_RE.match(tt):
                continue
            price = normalize_num(pr)
            qty = normalize_num(qr)
            trades.append(Trade(price=price, qty=qty, time=tt, side=side if side in ("buy", "sell") else None, price_raw=pr, qty_raw=qr))
        except Exception:
            continue

    return trades[:limit]


def compute_metrics(trades: List[Trade]) -> Dict[str, Any]:
    if not trades:
        return {"count": 0, "sum_qty_usdt": 0.0, "turnover_rub": 0.0, "vwap": None}
    sum_qty = 0.0
    turnover = 0.0
    for t in trades:
        sum_qty += t.qty
        turnover += t.qty * t.price
    vwap = (turnover / sum_qty) if sum_qty > 0 else None
    return {"count": len(trades), "sum_qty_usdt": sum_qty, "turnover_rub": turnover, "vwap": vwap}


# ==========================
# SUPABASE (optional)
# ==========================
async def push_to_supabase(rows: List[Dict[str, Any]]) -> None:
    if not (SUPABASE_URL and SUPABASE_SERVICE_KEY and SUPABASE_TABLE):
        return
    url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/{SUPABASE_TABLE}"
    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(url, headers=headers, json=rows)
        if r.status_code >= 300:
            raise RuntimeError(f"Supabase insert failed: {r.status_code} {r.text}")


# ==========================
# SCRAPE CYCLE
# ==========================
async def scrape_once() -> Dict[str, Any]:
    ensure_playwright_browsers(force=False, retries=6)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=HEADLESS,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-blink-features=AutomationControlled"],
        )

        storage_state = STATE_PATH if os.path.exists(STATE_PATH) else None
        if storage_state:
            log.info("Using saved session state: %s", storage_state)

        context = await browser.new_context(
            viewport={"width": 1440, "height": 900},
            locale="ru-RU",
            timezone_id="Europe/Moscow",
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            storage_state=storage_state,
        )
        page = await context.new_page()

        try:
            log.info("Opening ABCEX: %s", ABCEX_URL)

            # IMPORTANT: do NOT use networkidle for SPA with websockets
            await page.goto(ABCEX_URL, wait_until="domcontentloaded", timeout=120_000)
            await page.wait_for_timeout(1500)

            await accept_cookies_if_any(page)

            # Login if needed
            if not ABCEX_EMAIL or not ABCEX_PASSWORD:
                # if creds empty and session not valid, login will fail with explicit error
                pass

            if not await looks_logged_in(page):
                if not (ABCEX_EMAIL and ABCEX_PASSWORD):
                    await save_debug(page, "need_creds")
                    raise RuntimeError("Нужен логин, но ABCEX_EMAIL/ABCEX_PASSWORD не заданы в env.")
                log.info("Login detected. Performing sign-in as %s", mask_email(ABCEX_EMAIL))
                await login_if_needed(page, ABCEX_EMAIL, ABCEX_PASSWORD)

            # Save session
            try:
                await context.storage_state(path=STATE_PATH)
                log.info("Saved session state to %s", STATE_PATH)
            except Exception as e:
                log.warning("Could not save storage state: %s", e)

            # Ensure we are on the pair page (sometimes redirects to /client/spot)
            if "USDTRUB" not in (page.url or ""):
                log.warning("URL after login is %s (pair missing). Re-opening pair URL...", page.url)
                await page.goto(ABCEX_URL, wait_until="domcontentloaded", timeout=120_000)
                await page.wait_for_timeout(1200)

            await wait_trades_presence(page, timeout_ms=45_000)
            trades = await extract_trades_best_effort(page, limit=MAX_TRADES)

            if not trades:
                await save_debug(page, "no_trades_parsed")
                log.warning("Parsed trades: 0 (DOM changed or trades panel not rendered).")
            else:
                log.info("Parsed trades: %d", len(trades))

            metrics = compute_metrics(trades)
            result = {
                "exchange": SOURCE,
                "symbol": SYMBOL,
                "url": ABCEX_URL,
                "ts": datetime.utcnow().isoformat(),
                **metrics,
                "trades": [
                    {
                        "price": t.price,
                        "qty": t.qty,
                        "time": t.time,
                        "side": t.side,
                        "price_raw": t.price_raw,
                        "qty_raw": t.qty_raw,
                    }
                    for t in trades
                ],
            }
            return result

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


# ==========================
# LOOP (Render worker style)
# ==========================
def make_supabase_rows(result: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Row format for SUPABASE_TABLE (default abcex_trades).
    You can adapt columns on DB side; we send a simple, explicit schema.
    """
    rows = []
    for t in result.get("trades", []):
        rows.append(
            {
                "source": SOURCE,
                "symbol": SYMBOL,
                "price": t["price"],
                "qty": t["qty"],
                "side": t.get("side"),
                "trade_time": t["time"],     # HH:MM:SS (as text)
                "observed_at": result["ts"],  # ISO timestamp
                "url": ABCEX_URL,
                "raw": t,                    # JSONB column recommended
            }
        )
    return rows


async def main() -> None:
    log.info("Starting ABCEX scraper loop. headless=%s", HEADLESS)

    seen = set()  # in-memory dedup
    while True:
        try:
            result = await scrape_once()

            # Dedup by (time, price_raw, qty_raw)
            new_trades = []
            for t in result.get("trades", []):
                key = (t.get("time"), t.get("price_raw"), t.get("qty_raw"))
                if key in seen:
                    continue
                seen.add(key)
                new_trades.append(t)

            if not new_trades:
                log.info("No new trades after in-memory dedup.")
            else:
                log.info("New trades: %d", len(new_trades))

            # Push to Supabase (optional)
            if SUPABASE_URL and SUPABASE_SERVICE_KEY:
                payload = {
                    **result,
                    "trades": new_trades,
                    "count": len(new_trades),
                }
                rows = make_supabase_rows(payload)
                if rows:
                    await push_to_supabase(rows)
                    log.info("Pushed to Supabase: %d rows into %s", len(rows), SUPABASE_TABLE)

            # Also print JSON for logs
            print(json.dumps(result, ensure_ascii=False))

            await asyncio.sleep(POLL_SECONDS)

        except Exception as e:
            log.error("Cycle error: %s", e)
            # backoff on errors
            await asyncio.sleep(min(60, max(5, POLL_SECONDS)))


if __name__ == "__main__":
    asyncio.run(main())
