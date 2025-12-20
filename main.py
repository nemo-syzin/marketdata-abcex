#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import json
import logging
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from urllib.parse import urlsplit

import httpx
from playwright.async_api import async_playwright, Page, Locator, TimeoutError as PWTimeoutError

# ───────────────────────── CONFIG ─────────────────────────

ABCEX_URL = os.getenv("ABCEX_URL", "https://abcex.io/client/spot/USDTRUB")
STATE_PATH = os.getenv("STATE_PATH", "abcex_state.json")

SOURCE = os.getenv("SOURCE", "abcex")
SYMBOL = os.getenv("SYMBOL", "USDT/RUB")

LIMIT = int(os.getenv("LIMIT", "200"))
POLL_SEC = float(os.getenv("POLL_SEC", "1"))

ABCEX_EMAIL = os.getenv("ABCEX_EMAIL", "")
ABCEX_PASSWORD = os.getenv("ABCEX_PASSWORD", "")

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "exchange_trades")

GOTO_TIMEOUT_MS = int(os.getenv("GOTO_TIMEOUT_MS", "60000"))
WAIT_TRADES_TIMEOUT_MS = int(os.getenv("WAIT_TRADES_TIMEOUT_MS", "25000"))

TIME_RE = re.compile(r"\b(\d{1,2}:\d{2}:\d{2})\b")

# ───────────────────────── LOGGING ─────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
logger = logging.getLogger("abcex-worker")

# ───────────────────────── PLAYWRIGHT INSTALL (как в твоём фрагменте) ─────────────────────────

_last_install_ts = 0.0

def _playwright_install(force: bool = False) -> None:
    """
    Runtime-установка браузеров. Команда как в примере:
      python -m playwright install chromium chromium-headless-shell

    ВАЖНО:
    - cooldown 10 минут действует только если force=False
    - при force=True (например, "Executable doesn't exist") — ставим всегда
    """
    global _last_install_ts
    now = time.time()

    if not force and now - _last_install_ts < 600:
        logger.warning("Playwright install was attempted recently; skipping (cooldown).")
        return

    _last_install_ts = now
    logger.warning("Installing Playwright browsers (runtime)... force=%s", force)

    try:
        r = subprocess.run(
            [sys.executable, "-m", "playwright", "install", "chromium", "chromium-headless-shell"],
            check=False,
            capture_output=True,
            text=True,
        )
        if r.returncode != 0:
            logger.error(
                "playwright install failed (%s)\nSTDOUT:\n%s\nSTDERR:\n%s",
                r.returncode, r.stdout, r.stderr
            )
        else:
            logger.info("Playwright browsers installed.")
    except Exception as e:
        logger.error("Cannot run playwright install: %s", e)


def _should_force_install(err: Exception) -> bool:
    s = str(err)
    return (
        "Executable doesn't exist" in s
        or "playwright install" in s
        or "chromium_headless_shell" in s
        or ("ms-playwright" in s and "doesn't exist" in s)
    )

# ───────────────────────── PROXY HELPERS ─────────────────────────

def _get_proxy_url() -> Optional[str]:
    return os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY") or None


def _parse_proxy_for_playwright(proxy_url: str) -> Dict[str, Any]:
    """
    Playwright proxy:
      {"server": "http://host:port", "username": "...", "password": "..."}
    """
    u = urlsplit(proxy_url)
    if not u.scheme or not u.hostname or not u.port:
        # если формат нестандартный — пробуем как есть
        return {"server": proxy_url}

    out: Dict[str, Any] = {"server": f"{u.scheme}://{u.hostname}:{u.port}"}
    if u.username:
        out["username"] = u.username
    if u.password:
        out["password"] = u.password
    return out

# ───────────────────────── DEBUG ─────────────────────────

async def save_debug(page: Page, tag: str) -> None:
    try:
        png_path = f"abcex_{tag}.png"
        html_path = f"abcex_{tag}.html"
        await page.screenshot(path=png_path, full_page=True)
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(await page.content())
        logger.info("Saved debug: %s, %s", png_path, html_path)
    except Exception as e:
        logger.warning("Could not save debug artifacts: %s", e)

# ───────────────────────── PAGE ACTIONS ─────────────────────────

async def accept_cookies_if_any(page: Page) -> None:
    for label in ["Я согласен", "Принять", "Accept", "I agree", "Согласен"]:
        try:
            btn = page.locator(f"text={label}")
            if await btn.count() > 0 and await btn.first.is_visible():
                logger.info("Cookies banner: clicking '%s'...", label)
                await btn.first.click(timeout=5_000)
                await page.wait_for_timeout(300)
                return
        except Exception:
            pass


async def _is_login_visible(page: Page) -> bool:
    try:
        pw = page.locator("input[type='password']")
        return (await pw.count() > 0) and (await pw.first.is_visible())
    except Exception:
        return False


async def login_if_needed(page: Page, email: str, password: str) -> None:
    if not await _is_login_visible(page):
        logger.info("Login not required (already in session).")
        return

    logger.info("Login detected. Performing sign-in ...")

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
                await loc.first.fill(email, timeout=10_000)
                email_filled = True
                break
        except Exception:
            continue

    pw_filled = False
    for sel in pw_candidates:
        loc = page.locator(sel)
        try:
            if await loc.count() > 0 and await loc.first.is_visible():
                await loc.first.fill(password, timeout=10_000)
                pw_filled = True
                break
        except Exception:
            continue

    if not email_filled or not pw_filled:
        await save_debug(page, "login_fields_not_found")
        raise RuntimeError("Не смог найти поля email/password.")

    # кнопка входа
    btn_texts = ["Войти", "Вход", "Sign in", "Login", "Войти в аккаунт"]
    clicked = False
    for t in btn_texts:
        try:
            btn = page.locator(f"button:has-text('{t}')")
            if await btn.count() > 0 and await btn.first.is_visible():
                await btn.first.click(timeout=10_000)
                clicked = True
                break
        except Exception:
            continue

    if not clicked:
        try:
            await page.keyboard.press("Enter")
        except Exception:
            pass

    # ждём прогрузку
    try:
        await page.wait_for_timeout(1000)
        await page.wait_for_load_state("networkidle", timeout=30_000)
    except Exception:
        pass

    await page.wait_for_timeout(2500)

    if await _is_login_visible(page):
        await save_debug(page, "login_failed")
        raise RuntimeError("Логин не прошёл (форма логина всё ещё видна). Возможны 2FA/капча/иной флоу.")

    logger.info("Login successful.")


async def ensure_last_trades_tab(page: Page) -> None:
    """
    Best-effort: стараемся держаться вкладки со сделками/историей.
    """
    candidates = ["Последние сделки", "Сделки", "История", "Order history", "Trades"]
    for t in candidates:
        try:
            tab = page.locator(f"[role='tab']:has-text('{t}')")
            if await tab.count() > 0 and await tab.first.is_visible():
                await tab.first.click(timeout=6_000)
                await page.wait_for_timeout(300)
                return
        except Exception:
            pass

    for t in candidates:
        try:
            tab = page.locator(f"text={t}")
            if await tab.count() > 0 and await tab.first.is_visible():
                await tab.first.click(timeout=6_000)
                await page.wait_for_timeout(300)
                return
        except Exception:
            pass


async def get_order_history_panel(page: Page) -> Locator:
    panel = page.locator("div[role='tabpanel'][id*='panel-orderHistory']")
    cnt = await panel.count()
    if cnt == 0:
        await save_debug(page, "no_panel_orderHistory")
        raise RuntimeError("Не нашёл панель panel-orderHistory.")

    for i in range(cnt):
        p = panel.nth(i)
        try:
            if await p.is_visible():
                return p
        except Exception:
            continue

    await save_debug(page, "no_visible_panel_orderHistory")
    raise RuntimeError("panel-orderHistory найдена, но ни одна не видима.")


async def wait_trades_visible(page: Page, timeout_ms: int) -> None:
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
                return
        except Exception:
            pass
        await page.wait_for_timeout(600)

    await save_debug(page, "trades_not_visible")
    raise RuntimeError("Не дождался появления сделок (HH:MM:SS).")


def _normalize_num(text: str) -> float:
    t = (text or "").strip().replace("\xa0", " ").replace(" ", "")
    if "," in t and "." in t:
        t = t.replace(",", "")   # 1,234.56
    else:
        t = t.replace(",", ".")  # 1234,56
    return float(t)


def _extract_time(text: str) -> Optional[str]:
    m = TIME_RE.search((text or "").replace("\xa0", " "))
    if not m:
        return None
    hh, mm, ss = m.group(1).split(":")
    if len(hh) == 1:
        hh = "0" + hh
    return f"{hh}:{mm}:{ss}"


def _trade_time_to_iso_moscow(hhmmss: str) -> str:
    """
    На UI только HH:MM:SS. Привязываем к "сегодня" по Москве.
    Если получившееся время > текущего времени — считаем, что это было "вчера".
    """
    now = datetime.utcnow() + timedelta(hours=3)  # MSK (приближенно)
    h, m, s = [int(x) for x in hhmmss.split(":")]
    dt = now.replace(hour=h, minute=m, second=s, microsecond=0)
    if dt > now + timedelta(seconds=2):
        dt -= timedelta(days=1)
    return dt.isoformat()

# ───────────────────────── PARSE ─────────────────────────

async def extract_trades_from_panel(panel: Locator, limit: int) -> List[Dict[str, Any]]:
    handle = await panel.element_handle()
    if handle is None:
        raise RuntimeError("Не смог получить element_handle панели.")

    raw_rows: List[Dict[str, Any]] = await handle.evaluate(
        """(root, limit) => {
          const isTime = (s) => /^\\d{2}:\\d{2}:\\d{2}$/.test((s||'').trim());
          const isNum  = (s) => /^[0-9][0-9\\s\\u00A0.,]*$/.test((s||'').trim());

          const out = [];
          const divs = Array.from(root.querySelectorAll('div'));

          for (const g of divs) {
            const ps = Array.from(g.querySelectorAll(':scope > p'));
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

    trades: List[Dict[str, Any]] = []
    for r in raw_rows:
        try:
            price_raw = str(r.get("price_raw", "")).strip()
            qty_raw = str(r.get("qty_raw", "")).strip()
            time_raw = str(r.get("time", "")).strip()
            side = r.get("side") if r.get("side") in ("buy", "sell") else None

            tt = _extract_time(time_raw)
            if not tt:
                continue

            price = _normalize_num(price_raw)
            qty = _normalize_num(qty_raw)

            trades.append(
                {
                    "source": SOURCE,
                    "symbol": SYMBOL,
                    "trade_time": _trade_time_to_iso_moscow(tt),
                    "price": str(price),
                    "volume_usdt": str(qty),
                    "turnover_rub": str(price * qty),
                    "side": side,
                    "raw_time": tt,
                    "price_raw": price_raw,
                    "qty_raw": qty_raw,
                    "parsed_at": datetime.utcnow().isoformat(),
                }
            )
        except Exception:
            continue

    return trades

# ───────────────────────── SUPABASE ─────────────────────────

@dataclass(frozen=True)
class TradeKey:
    source: str
    symbol: str
    trade_time: str
    price: str
    volume_usdt: str


def trade_key(t: Dict[str, Any]) -> TradeKey:
    return TradeKey(
        source=str(t.get("source", "")),
        symbol=str(t.get("symbol", "")),
        trade_time=str(t.get("trade_time", "")),
        price=str(t.get("price", "")),
        volume_usdt=str(t.get("volume_usdt", "")),
    )


async def supabase_upsert_trades(trades: List[Dict[str, Any]]) -> None:
    if not trades:
        return

    if not SUPABASE_URL or not SUPABASE_KEY or not SUPABASE_TABLE:
        logger.warning("Supabase env is not fully set; skipping DB write.")
        return

    url = SUPABASE_URL.rstrip("/") + f"/rest/v1/{SUPABASE_TABLE}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }

    async with httpx.AsyncClient(timeout=30.0, headers=headers, trust_env=True) as client:
        resp = await client.post(url, content=json.dumps(trades, ensure_ascii=False).encode("utf-8"))
        if resp.status_code in (200, 201, 204):
            logger.info("Supabase upsert OK: %s trades", len(trades))
            return
        logger.error("Supabase write failed: %s %s", resp.status_code, resp.text[:1500])

# ───────────────────────── SCRAPE ONCE ─────────────────────────

async def run_once() -> List[Dict[str, Any]]:
    proxy_url = _get_proxy_url()
    pw_proxy = _parse_proxy_for_playwright(proxy_url) if proxy_url else None

    async with async_playwright() as p:
        # launch with гарантированным force-install при missing executable
        browser = None
        for attempt in (1, 2, 3):
            try:
                launch_kwargs: Dict[str, Any] = {
                    "headless": True,
                    "args": [
                        "--disable-blink-features=AutomationControlled",
                        "--no-sandbox",
                        "--disable-dev-shm-usage",
                    ],
                }
                if pw_proxy:
                    launch_kwargs["proxy"] = pw_proxy

                browser = await p.chromium.launch(**launch_kwargs)
                break
            except Exception as e:
                if _should_force_install(e):
                    logger.warning("Chromium launch failed (missing executable). Forcing install and retry... attempt=%s", attempt)
                    _playwright_install(force=True)
                    await asyncio.sleep(2.0)
                    continue
                raise

        if browser is None:
            raise RuntimeError("Cannot launch browser after retries.")

        storage_state = STATE_PATH if os.path.exists(STATE_PATH) else None
        if storage_state:
            logger.info("Using saved session state: %s", storage_state)

        context = await browser.new_context(
            viewport={"width": 1440, "height": 810},
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
            logger.info("Opening ABCEX: %s", ABCEX_URL)
            await page.goto(ABCEX_URL, wait_until="networkidle", timeout=GOTO_TIMEOUT_MS)
            await page.wait_for_timeout(1200)

            await accept_cookies_if_any(page)

            if await _is_login_visible(page):
                if not ABCEX_EMAIL or not ABCEX_PASSWORD:
                    await save_debug(page, "need_credentials")
                    raise RuntimeError("Нужен логин, но ABCEX_EMAIL/ABCEX_PASSWORD не заданы в env.")
                await login_if_needed(page, ABCEX_EMAIL, ABCEX_PASSWORD)

            # сохраняем session state
            try:
                await context.storage_state(path=STATE_PATH)
                logger.info("Saved session state to %s", STATE_PATH)
            except Exception as e:
                logger.warning("Could not save storage state: %s", e)

            await ensure_last_trades_tab(page)
            await wait_trades_visible(page, timeout_ms=WAIT_TRADES_TIMEOUT_MS)

            panel = await get_order_history_panel(page)
            trades = await extract_trades_from_panel(panel, limit=LIMIT)

            if not trades:
                await save_debug(page, "no_trades_parsed")
                logger.warning("No trades parsed this cycle.")
                return []

            logger.info("Parsed trades: %d", len(trades))
            return trades

        except PWTimeoutError:
            await save_debug(page, "timeout")
            raise
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

# ───────────────────────── MAIN LOOP ─────────────────────────

async def main() -> None:
    # СТРОГО как нужно для Render: ставим браузер runtime (FORCE)
    _playwright_install(force=True)

    # in-memory дедуп (чтобы не спамить одинаковыми сделками)
    seen: Dict[TradeKey, float] = {}
    seen_ttl_sec = 60 * 30

    while True:
        try:
            trades = await run_once()

            now = time.time()
            for k, ts in list(seen.items()):
                if now - ts > seen_ttl_sec:
                    del seen[k]

            fresh: List[Dict[str, Any]] = []
            for t in trades:
                k = trade_key(t)
                if k in seen:
                    continue
                seen[k] = now
                fresh.append(t)

            if fresh:
                await supabase_upsert_trades(fresh)
            else:
                logger.info("No new trades after in-memory dedup.")

        except Exception as e:
            logger.error("Cycle error: %s", e)
            if _should_force_install(e):
                _playwright_install(force=True)

            # чтобы не уйти в быстрый спин при постоянной проблеме
            await asyncio.sleep(3.0)

        await asyncio.sleep(POLL_SEC)


if __name__ == "__main__":
    asyncio.run(main())
