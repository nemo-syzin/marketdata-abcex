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
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import httpx
from playwright.async_api import async_playwright, Page, Locator, TimeoutError as PWTimeoutError

# ───────────────────────── CONFIG ─────────────────────────

ABCEX_URL = os.getenv("ABCEX_URL", "https://abcex.io/client/spot/USDTRUB")
SOURCE = os.getenv("SOURCE", "abcex")
SYMBOL = os.getenv("SYMBOL", "USDT/RUB")

STATE_PATH = os.getenv("STATE_PATH", "abcex_state.json")
DEBUG_DIR = os.getenv("DEBUG_DIR", ".")  # на Render можно оставить "."

LIMIT = int(os.getenv("LIMIT", "200"))
POLL_SEC = float(os.getenv("POLL_SEC", os.getenv("POLL_SECONDS", "1")))

ABCEX_EMAIL = os.getenv("ABCEX_EMAIL", "")
ABCEX_PASSWORD = os.getenv("ABCEX_PASSWORD", "")

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "exchange_trades")

# Если в таблице есть уникальный индекс/constraint под эти поля — включи on_conflict.
# Подставь реальные имена колонок, если у тебя отличаются.
SUPABASE_ON_CONFLICT = os.getenv(
    "SUPABASE_ON_CONFLICT",
    "source,symbol,trade_time,price,volume_usdt",
)

# Playwright cache path (Render-friendly)
PW_BROWSERS_PATH = os.getenv("PLAYWRIGHT_BROWSERS_PATH", "/opt/render/.cache/ms-playwright")

# Таймауты
GOTO_TIMEOUT_MS = int(os.getenv("GOTO_TIMEOUT_MS", "120000"))
UI_TIMEOUT_MS = int(os.getenv("UI_TIMEOUT_MS", "30000"))

# Headless
HEADLESS = os.getenv("ABCEX_HEADLESS", "1").strip() not in ("0", "false", "False")

# ───────────────────────── LOGGING ─────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
logger = logging.getLogger("abcex-render")

TIME_RE = re.compile(r"\b\d{2}:\d{2}:\d{2}\b")

Q8 = Decimal("0.00000001")


# ───────────────────────── HELPERS ─────────────────────────

def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def normalize_decimal(text: str) -> Optional[Decimal]:
    t = (text or "").strip()
    if not t:
        return None
    t = t.replace("\xa0", " ").replace(" ", "")
    # 1,234.56 => 1234.56 (если оба есть)
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

def extract_time(text: str) -> Optional[str]:
    m = TIME_RE.search((text or "").replace("\xa0", " "))
    if not m:
        return None
    return m.group(0)

def parse_proxy_from_env() -> Optional[Dict[str, str]]:
    """
    Playwright proxy dict:
      {"server": "http://host:port", "username": "...", "password": "..."}
    Берём HTTPS_PROXY, если нет — HTTP_PROXY.
    """
    raw = os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY")
    if not raw:
        return None

    u = urlparse(raw)
    if not u.scheme or not u.hostname or not u.port:
        logger.warning("Proxy URL looks invalid: %s", raw)
        return None

    proxy: Dict[str, str] = {
        "server": f"{u.scheme}://{u.hostname}:{u.port}",
    }
    if u.username:
        proxy["username"] = u.username
    if u.password:
        proxy["password"] = u.password

    return proxy


# ───────────────────────── PLAYWRIGHT INSTALL (как в твоём фрагменте 2) ─────────────────────────

_last_install_ts = 0.0

def _browsers_exist() -> Tuple[bool, bool]:
    """
    Проверяем наличие chromium и chromium-headless-shell в кеше.
    Точная структура зависит от версии, но папки обычно:
      chromium-XXXX
      chromium_headless_shell-XXXX
    """
    if not os.path.isdir(PW_BROWSERS_PATH):
        return (False, False)

    has_chromium = any(name.startswith("chromium-") for name in os.listdir(PW_BROWSERS_PATH))
    has_headless = any(name.startswith("chromium_headless_shell-") for name in os.listdir(PW_BROWSERS_PATH))
    return (has_chromium, has_headless)

def _playwright_install(force: bool = False) -> None:
    """
    Runtime-установка браузеров.
    Ставим и chromium, и chromium-headless-shell.
    ВАЖНО: делаем это sys.executable -m playwright ... (ровно как твой фрагмент 2).
    """
    global _last_install_ts

    # Если уже есть — не дёргаем install без нужды
    chromium_ok, headless_ok = _browsers_exist()
    if chromium_ok and headless_ok and not force:
        logger.info("Playwright browsers already present. Skipping install.")
        return

    now = time.time()
    # мягкий cooldown только если НЕ force
    if not force and (now - _last_install_ts) < 600:
        logger.warning("Playwright install was attempted recently; skipping (cooldown).")
        return
    _last_install_ts = now

    logger.warning("Installing Playwright browsers (runtime)... force=%s", force)
    env = os.environ.copy()
    env["PLAYWRIGHT_BROWSERS_PATH"] = PW_BROWSERS_PATH

    try:
        r = subprocess.run(
            [sys.executable, "-m", "playwright", "install", "chromium", "chromium-headless-shell"],
            check=False,
            capture_output=True,
            text=True,
            env=env,
        )
        logger.warning("playwright install returncode=%s", r.returncode)
        if r.stdout:
            logger.warning("playwright install STDOUT:\n%s", r.stdout[-4000:])
        if r.stderr:
            logger.warning("playwright install STDERR:\n%s", r.stderr[-4000:])

    except Exception as e:
        logger.error("Cannot run playwright install: %s", e)

    chromium_ok, headless_ok = _browsers_exist()
    logger.warning("Install verification: chromium=%s headless_shell=%s", chromium_ok, headless_ok)

def _should_force_install(err: Exception) -> bool:
    s = str(err)
    return (
        "Executable doesn't exist" in s
        or "playwright install" in s
        or "chromium_headless_shell" in s
        or ("ms-playwright" in s and "doesn't exist" in s)
    )


# ───────────────────────── DEBUG ─────────────────────────

async def save_debug(page: Page, tag: str) -> None:
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    png_path = os.path.join(DEBUG_DIR, f"{tag}_{ts}.png")
    html_path = os.path.join(DEBUG_DIR, f"{tag}_{ts}.html")
    try:
        await page.screenshot(path=png_path, full_page=True)
        logger.info("Saved debug screenshot: %s", png_path)
    except Exception as e:
        logger.warning("Could not save screenshot: %s", e)

    try:
        content = await page.content()
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(content)
        logger.info("Saved debug html: %s", html_path)
    except Exception as e:
        logger.warning("Could not save html: %s", e)


# ───────────────────────── NAVIGATION ─────────────────────────

async def goto_stable(page: Page, url: str) -> None:
    """
    Для SPA: грузим до domcontentloaded и даём UI догрузиться.
    НЕ используем networkidle.
    """
    await page.goto(url, wait_until="domcontentloaded", timeout=GOTO_TIMEOUT_MS)
    await page.wait_for_timeout(1200)

async def accept_cookies_if_any(page: Page) -> None:
    for label in ["Принять", "Согласен", "Я согласен", "Accept", "I agree"]:
        try:
            btn = page.locator(f"text={label}")
            if await btn.count() > 0 and await btn.first.is_visible():
                logger.info("Cookies banner: clicking '%s'...", label)
                await btn.first.click(timeout=5_000)
                await page.wait_for_timeout(400)
                return
        except Exception:
            pass

async def click_trades_tab_best_effort(page: Page) -> None:
    candidates = [
        "Сделки", "История", "История сделок", "История ордеров",
        "Order history", "Trades", "Trade history", "My trades",
    ]
    # role=tab
    for t in candidates:
        try:
            tab = page.locator(f"[role='tab']:has-text('{t}')")
            if await tab.count() > 0 and await tab.first.is_visible():
                await tab.first.click(timeout=8_000)
                await page.wait_for_timeout(600)
                logger.info("Clicked tab: %s", t)
                return
        except Exception:
            continue

    # fallback text
    for t in candidates:
        try:
            tab = page.locator(f"text={t}")
            if await tab.count() > 0 and await tab.first.is_visible():
                await tab.first.click(timeout=8_000)
                await page.wait_for_timeout(600)
                logger.info("Clicked text tab: %s", t)
                return
        except Exception:
            continue

    logger.info("Trades tab click skipped (not found explicitly).")

async def wait_trades_visible(page: Page, timeout_ms: int = 30_000) -> None:
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
                logger.info("Trades look visible (HH:MM:SS detected in DOM).")
                return
        except Exception:
            pass
        await page.wait_for_timeout(600)

    await save_debug(page, "trades_not_visible")
    raise RuntimeError("Не дождался появления сделок (HH:MM:SS).")


# ───────────────────────── LOGIN ─────────────────────────

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
        logger.info("Login not required (no password field detected).")
        return

    if not email or not password:
        raise RuntimeError("Login required but ABCEX_EMAIL / ABCEX_PASSWORD are empty.")

    logger.info("Login detected. Performing sign-in ...")

    # Дождёмся появления password (на SPA иногда рендерится с задержкой)
    try:
        await page.locator("input[type='password']").first.wait_for(timeout=15_000)
    except Exception:
        pass

    email_candidates = [
        "input[type='email']",
        "input[name='email']",
        "input[autocomplete='username']",
        "input[name='login']",
        "input[placeholder*='mail' i]",
        "input[placeholder*='Email' i]",
        "input[placeholder*='Почта' i]",
        "input[placeholder*='E-mail' i]",
    ]
    pw_candidates = [
        "input[type='password']",
        "input[name='password']",
        "input[autocomplete='current-password']",
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

    # Подождём исчезновения password
    try:
        await page.wait_for_timeout(800)
        await page.locator("input[type='password']").first.wait_for(state="detached", timeout=30_000)
    except Exception:
        pass

    await page.wait_for_timeout(1200)

    if await _is_login_visible(page):
        await save_debug(page, "login_failed")
        raise RuntimeError("Логин не прошёл (форма логина всё ещё видна).")

    logger.info("Login successful (password field disappeared).")


# ───────────────────────── PARSING ─────────────────────────

@dataclass
class Trade:
    price: Decimal
    volume_usdt: Decimal
    trade_time: str  # HH:MM:SS (как в UI)
    side: Optional[str] = None
    price_raw: str = ""
    volume_raw: str = ""

def _is_time(s: str) -> bool:
    return bool(re.fullmatch(r"\d{2}:\d{2}:\d{2}", (s or "").strip()))

async def get_order_history_panel(page: Page) -> Optional[Locator]:
    panel = page.locator("div[role='tabpanel'][id*='panel-orderHistory']")
    try:
        cnt = await panel.count()
    except Exception:
        return None
    if cnt == 0:
        return None
    for i in range(cnt):
        p = panel.nth(i)
        try:
            if await p.is_visible():
                return p
        except Exception:
            continue
    return None

async def extract_trades_triplets(root: Any, limit: int) -> List[Dict[str, Any]]:
    """
    Универсальный JS-экстрактор:
    ищет блоки, где есть 3 прямых <p>: [price, qty, time]
    """
    return await root.evaluate(
        """(node, limit) => {
          const isTime = (s) => /^\\d{2}:\\d{2}:\\d{2}$/.test((s||'').trim());
          const isNum  = (s) => /^[0-9][0-9\\s\\u00A0.,]*$/.test((s||'').trim());

          const out = [];

          const root = node || document;
          const divs = Array.from(root.querySelectorAll('div'));

          for (const d of divs) {
            const ps = Array.from(d.querySelectorAll(':scope > p'));
            if (ps.length < 3) continue;

            const t0 = (ps[0].textContent || '').trim();
            const t1 = (ps[1].textContent || '').trim();
            const t2 = (ps[2].textContent || '').trim();

            if (!isTime(t2)) continue;
            if (!isNum(t0) || !isNum(t1)) continue;

            const style0 = (ps[0].getAttribute('style') || '').toLowerCase();
            let side = null;
            if (style0.includes('green')) side = 'buy';
            if (style0.includes('red')) side = 'sell';

            out.push({ price_raw: t0, volume_raw: t1, trade_time: t2, side });
            if (out.length >= limit) break;
          }

          return out;
        }""",
        limit,
    )

async def extract_trades(page: Page, limit: int) -> List[Trade]:
    # 1) Пытаемся через panel-orderHistory (как у тебя локально)
    panel = await get_order_history_panel(page)
    if panel is not None:
        handle = await panel.element_handle()
        if handle is not None:
            raw = await extract_trades_triplets(handle, limit)
            trades = _normalize_trades(raw)
            if trades:
                return trades

    # 2) Fallback: скан по всему document
    logger.warning("panel-orderHistory not found or empty. Using fallback DOM scan...")
    raw2 = await page.evaluate(
        """(limit) => {
          const isTime = (s) => /^\\d{2}:\\d{2}:\\d{2}$/.test((s||'').trim());
          const isNum  = (s) => /^[0-9][0-9\\s\\u00A0.,]*$/.test((s||'').trim());

          const out = [];
          const divs = Array.from(document.querySelectorAll('div'));
          for (const d of divs) {
            const ps = Array.from(d.querySelectorAll(':scope > p'));
            if (ps.length < 3) continue;

            const t0 = (ps[0].textContent || '').trim();
            const t1 = (ps[1].textContent || '').trim();
            const t2 = (ps[2].textContent || '').trim();

            if (!isTime(t2)) continue;
            if (!isNum(t0) || !isNum(t1)) continue;

            const style0 = (ps[0].getAttribute('style') || '').toLowerCase();
            let side = null;
            if (style0.includes('green')) side = 'buy';
            if (style0.includes('red')) side = 'sell';

            out.push({ price_raw: t0, volume_raw: t1, trade_time: t2, side });
            if (out.length >= limit) break;
          }
          return out;
        }""",
        limit,
    )
    return _normalize_trades(raw2)

def _normalize_trades(raw_rows: List[Dict[str, Any]]) -> List[Trade]:
    trades: List[Trade] = []
    for r in raw_rows or []:
        pr = str(r.get("price_raw", "")).strip()
        vr = str(r.get("volume_raw", "")).strip()
        tt = str(r.get("trade_time", "")).strip()
        side = r.get("side")

        if not pr or not vr or not tt or not _is_time(tt):
            continue

        price = normalize_decimal(pr)
        vol = normalize_decimal(vr)
        if price is None or vol is None:
            continue

        trades.append(
            Trade(
                price=price,
                volume_usdt=vol,
                trade_time=tt,
                side=side if side in ("buy", "sell") else None,
                price_raw=pr,
                volume_raw=vr,
            )
        )
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
        source=t["source"],
        symbol=t["symbol"],
        trade_time=t["trade_time"],
        price=t["price"],
        volume_usdt=t["volume_usdt"],
    )

async def supabase_insert_rows(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    if not SUPABASE_URL or not SUPABASE_KEY or not SUPABASE_TABLE:
        logger.warning("Supabase env not set (SUPABASE_URL/KEY/TABLE). Skipping insert.")
        return

    url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/{SUPABASE_TABLE}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        # merge-duplicates требует подходящего уникального ключа/constraint;
        # если он есть — это лучший вариант.
        "Prefer": "return=minimal,resolution=ignore-duplicates",
    }

    params = {}
    if SUPABASE_ON_CONFLICT:
        params["on_conflict"] = SUPABASE_ON_CONFLICT

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(url, headers=headers, params=params, json=rows)

    if resp.status_code in (200, 201, 204):
        logger.info("Inserted rows into Supabase: %d", len(rows))
        return

    # 409/409-like — конфликт уникальности (зависит от конфигурации)
    if resp.status_code in (409,):
        logger.info("Supabase conflict (duplicates). Rows=%d", len(rows))
        return

    logger.error("Supabase insert failed: status=%s body=%s", resp.status_code, resp.text[:2000])


# ───────────────────────── MAIN LOOP ─────────────────────────

def build_rows(trades: List[Trade]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for t in trades:
        turnover_rub = (t.price * t.volume_usdt) if (t.price and t.volume_usdt) else None
        out.append(
            {
                "source": SOURCE,
                "symbol": SYMBOL,
                "trade_time": t.trade_time,
                "price": q8_str(t.price),
                "volume_usdt": q8_str(t.volume_usdt),
                "turnover_rub": q8_str(turnover_rub) if turnover_rub is not None else None,
                "side": t.side,
                "fetched_at": _utc_iso(),
                "meta": json.dumps({"price_raw": t.price_raw, "volume_raw": t.volume_raw}, ensure_ascii=False),
            }
        )
    return out

async def run_once(page: Page) -> List[Dict[str, Any]]:
    logger.info("Opening ABCEX: %s", ABCEX_URL)
    await goto_stable(page, ABCEX_URL)

    await accept_cookies_if_any(page)

    # Логин при необходимости
    if await _is_login_visible(page):
        await login_if_needed(page, ABCEX_EMAIL, ABCEX_PASSWORD)
        # После логина часто редиректит на /client/spot — вернёмся на конкретную пару
        await goto_stable(page, ABCEX_URL)

    # Клик таба "Сделки" (best effort)
    await click_trades_tab_best_effort(page)

    # Ждём появления HH:MM:SS
    await wait_trades_visible(page, timeout_ms=30_000)

    trades = await extract_trades(page, limit=LIMIT)
    logger.info("Parsed trades: %d", len(trades))
    return build_rows(trades)

async def main() -> None:
    # 0) Установка браузеров (runtime)
    os.environ["PLAYWRIGHT_BROWSERS_PATH"] = PW_BROWSERS_PATH
    _playwright_install(force=False)

    proxy = parse_proxy_from_env()

    # in-memory dedup, чтобы не спамить supabase
    seen: set[TradeKey] = set()

    async with async_playwright() as p:
        # 1) Запуск браузера с обработкой missing-executable
        launch_args = [
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-blink-features=AutomationControlled",
        ]

        for attempt in range(1, 4):
            try:
                browser = await p.chromium.launch(
                    headless=HEADLESS,
                    args=launch_args,
                    proxy=proxy,
                )
                break
            except Exception as e:
                if _should_force_install(e) and attempt < 3:
                    logger.warning("Chromium launch failed (missing executable). Forcing install and retry... attempt=%s", attempt)
                    _playwright_install(force=True)
                    await asyncio.sleep(1)
                    continue
                raise

        storage_state = STATE_PATH if os.path.exists(STATE_PATH) else None
        if storage_state:
            logger.info("Using saved session state: %s", storage_state)

        context = await browser.new_context(
            viewport={"width": 1440, "height": 900},
            locale="ru-RU",
            timezone_id="Europe/Moscow",
            storage_state=storage_state,
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
        )
        context.set_default_navigation_timeout(GOTO_TIMEOUT_MS)
        context.set_default_timeout(UI_TIMEOUT_MS)

        page = await context.new_page()

        try:
            while True:
                try:
                    rows = await run_once(page)

                    # in-memory dedup
                    new_rows = []
                    for r in rows:
                        k = trade_key(r)
                        if k in seen:
                            continue
                        seen.add(k)
                        new_rows.append(r)

                    if not new_rows:
                        logger.info("No new trades after in-memory dedup.")
                    else:
                        await supabase_insert_rows(new_rows)

                except PWTimeoutError as e:
                    logger.error("Cycle error: %s", e)
                    try:
                        await save_debug(page, "timeout")
                    except Exception:
                        pass
                    # перезагрузим вкладку, но без networkidle
                    try:
                        await goto_stable(page, ABCEX_URL)
                    except Exception:
                        pass

                except Exception as e:
                    logger.error("Cycle error: %s", e)
                    try:
                        await save_debug(page, "cycle_error")
                    except Exception:
                        pass
                    # мягкий reset: перезагрузим страницу
                    try:
                        await goto_stable(page, ABCEX_URL)
                    except Exception:
                        pass

                await asyncio.sleep(POLL_SEC)

        finally:
            try:
                await context.storage_state(path=STATE_PATH)
                logger.info("Saved session state to %s", STATE_PATH)
            except Exception:
                pass

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


if __name__ == "__main__":
    asyncio.run(main())
