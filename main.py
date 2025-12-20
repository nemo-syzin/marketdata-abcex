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
from typing import Any, Dict, List, Optional, Tuple
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

# Таймауты
GOTO_TIMEOUT_MS = int(os.getenv("GOTO_TIMEOUT_MS", "60000"))
WAIT_TRADES_TIMEOUT_MS = int(os.getenv("WAIT_TRADES_TIMEOUT_MS", "25000"))

# ───────────────────────── LOGGING ─────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
logger = logging.getLogger("abcex-worker")

TIME_RE = re.compile(r"\b(\d{1,2}:\d{2}:\d{2})\b")

# ───────────────────────── PLAYWRIGHT INSTALL (как во 2 фрагменте) ─────────────────────────

_last_install_ts = 0.0

def _playwright_install() -> None:

    global _last_install_ts
    now = time.time()
    if now - _last_install_ts < 600:
        logger.warning("Playwright install was attempted recently; skipping (cooldown).")
        return
    _last_install_ts = now

    logger.warning("Installing Playwright browsers (runtime)...")
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


# ───────────────────────── PROXY HELPERS ─────────────────────────

def _get_proxy_url() -> Optional[str]:
    return os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY") or None


def _parse_proxy_for_playwright(proxy_url: str) -> Dict[str, Any]:
    """
    Playwright proxy expects:
      {"server": "http://host:port", "username": "...", "password": "..."}
    """
    u = urlsplit(proxy_url)
    server = f"{u.scheme}://{u.hostname}:{u.port}" if u.hostname and u.port else proxy_url
    out: Dict[str, Any] = {"server": server}
    if u.username:
        out["username"] = u.username
    if u.password:
        out["password"] = u.password
    return out


# ───────────────────────── PAGE UTILS ─────────────────────────

async def save_debug(page: Page, tag: str) -> None:
    try:
        png_path = f"abcex_{tag}.png"
        html_path = f"abcex_{tag}.html"
        await page.screenshot(path=png_path, full_page=True)
        content = await page.content()
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(content)
        logger.info("Saved debug: %s, %s", png_path, html_path)
    except Exception as e:
        logger.warning("Could not save debug artifacts: %s", e)


async def accept_cookies_if_any(page: Page) -> None:
    for label in ["Я согласен", "Принять", "Accept", "I agree", "Согласен"]:
        try:
            btn = page.locator(f"text={label}")
            if await btn.count() > 0 and await btn.first.is_visible():
                logger.info("Found cookies banner, clicking '%s'...", label)
                await btn.first.click(timeout=5_000)
                await page.wait_for_timeout(300)
                return
        except Exception:
            pass


async def _is_login_visible(page: Page) -> bool:
    try:
        pw = page.locator("input[type='password']")
        if await pw.count() > 0 and await pw.first.is_visible():
            return True
    except Exception:
        pass
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
        "input[placeholder*='Email' i]",
        "input[placeholder*='Почта' i]",
        "input[placeholder*='E-mail' i]",
    ]
    pw_candidates = [
        "input[type='password']",
        "input[name='password']",
        "input[placeholder*='Пароль' i]",
        "input[placeholder*='Password' i]",
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

    # Мягкое ожидание прогруза
    try:
        await page.wait_for_timeout(1_000)
        await page.wait_for_load_state("networkidle", timeout=30_000)
    except Exception:
        pass

    await page.wait_for_timeout(2_500)

    if await _is_login_visible(page):
        await save_debug(page, "login_failed")
        raise RuntimeError("Логин не прошёл (форма логина всё ещё видна). Возможны 2FA/капча/иной флоу.")

    logger.info("Login successful.")


async def ensure_last_trades_tab(page: Page) -> None:
    """
    Best-effort: держимся ближе к 'Последние сделки' / 'Сделки' / 'История'.
    """
    candidates = ["Последние сделки", "Сделки", "История", "Order history", "Trades"]
    for t in candidates:
        try:
            tab = page.locator(f"[role='tab']:has-text('{t}')")
            if await tab.count() > 0 and await tab.first.is_visible():
                await tab.first.click(timeout=5_000)
                await page.wait_for_timeout(250)
                return
        except Exception:
            pass

    for t in candidates:
        try:
            tab = page.locator(f"text={t}")
            if await tab.count() > 0 and await tab.first.is_visible():
                await tab.first.click(timeout=5_000)
                await page.wait_for_timeout(250)
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
                return
        except Exception:
            pass
        await page.wait_for_timeout(600)

    await save_debug(page, "trades_not_visible")
    raise RuntimeError("Не дождался появления сделок (HH:MM:SS).")


def _normalize_num(text: str) -> float:
    t = (text or "").strip().replace("\xa0", " ").replace(" ", "")
    if "," in t and "." in t:
        # "1,234.56"
        t = t.replace(",", "")
    else:
        t = t.replace(",", ".")
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
    На UI только HH:MM:SS.
    Привязываем к "сегодня" по Москве, с защитой от перехода через полночь:
    если время сделки "больше" текущего времени — считаем, что это было "вчера".
    """
    try:
        now = datetime.utcnow() + timedelta(hours=3)  # приближенно MSK без zoneinfo
        h, m, s = [int(x) for x in hhmmss.split(":")]
        dt = now.replace(hour=h, minute=m, second=s, microsecond=0)
        if dt > now + timedelta(seconds=2):
            dt = dt - timedelta(days=1)
        # Храним ISO без tz, либо можно добавить +03:00 — зависит от вашей схемы
        return dt.isoformat()
    except Exception:
        # fallback: хотя бы строка времени
        return hhmmss


async def extract_trades_from_panel(panel: Locator, limit: int) -> List[Dict[str, Any]]:
    """
    JS-логика как у твоего локального парсера:
      - ищем элементы, где 3 p подряд: [price, qty, time]
      - time строго HH:MM:SS
      - side по style price-ячейки (green/red) если есть
    """
    handle = await panel.element_handle()
    if handle is None:
        raise RuntimeError("Не смог получить element_handle панели.")

    raw_rows: List[Dict[str, Any]] = await handle.evaluate(
        """(root, limit) => {
          const isTime = (s) => /^\\d{2}:\\d{2}:\\d{2}$/.test((s||'').trim());
          const isNum = (s) => /^[0-9][0-9\\s\\u00A0.,]*$/.test((s||'').trim());

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
            t = str(r.get("time", "")).strip()
            side = r.get("side") if r.get("side") in ("buy", "sell") else None

            tt = _extract_time(t)
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

    # httpx будет использовать HTTP(S)_PROXY из env автоматически
    async with httpx.AsyncClient(timeout=30.0, headers=headers, trust_env=True) as client:
        resp = await client.post(url, content=json.dumps(trades, ensure_ascii=False).encode("utf-8"))
        if resp.status_code in (200, 201, 204):
            logger.info("Supabase upsert OK: %s trades", len(trades))
            return

        # 409 может быть при конфликте без корректного merge (если нет unique constraint)
        logger.error("Supabase write failed: %s %s", resp.status_code, resp.text[:1500])


# ───────────────────────── SCRAPE LOOP ─────────────────────────

async def run_once() -> List[Dict[str, Any]]:
    proxy_url = _get_proxy_url()
    pw_proxy = _parse_proxy_for_playwright(proxy_url) if proxy_url else None

    async with async_playwright() as p:
        # launch с retry на установку браузеров
        for attempt in (1, 2):
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
                if attempt == 1 and _should_force_install(e):
                    logger.warning("Chromium launch failed due to missing executable; forcing install and retry...")
                    _playwright_install()
                    await asyncio.sleep(2.0)
                    continue
                raise

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
                    raise RuntimeError("Нужен логин, но ABCEX_EMAIL/ABCEX_PASSWORD не заданы.")
                await login_if_needed(page, ABCEX_EMAIL, ABCEX_PASSWORD)

            # сохраняем state (даже если уже был)
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

        except PWTimeoutError as e:
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


async def main() -> None:
    # первичная runtime-установка (без фанатизма: дальше есть авто-retry)
    _playwright_install()

    # локальный анти-дубликат, чтобы не долбить Supabase одинаковыми батчами
    seen: Dict[TradeKey, float] = {}
    seen_ttl_sec = 60 * 30  # 30 минут

    while True:
        try:
            trades = await run_once()

            # дедуп в памяти
            now = time.time()
            # чистим старые ключи
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
            # если похоже на отсутствие браузера — поставим и продолжим
            logger.error("Cycle error: %s", e)
            if _should_force_install(e):
                _playwright_install()

        await asyncio.sleep(POLL_SEC)


if __name__ == "__main__":
    asyncio.run(main())
