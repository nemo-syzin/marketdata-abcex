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
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, List, Optional
from urllib.parse import urlsplit
import glob

import httpx
from playwright.async_api import async_playwright, Page, Locator

# ───────────────────────── CONFIG ─────────────────────────

ABCEX_URL = os.getenv("ABCEX_URL", "https://abcex.io/client/spot/USDTRUB")
STATE_PATH = os.getenv("STATE_PATH", "abcex_state.json")

SOURCE = os.getenv("SOURCE", "abcex")
SYMBOL = os.getenv("SYMBOL", "USDT/RUB")

LIMIT = int(os.getenv("LIMIT", "200"))
POLL_SEC = float(os.getenv("POLL_SEC", "2.0"))

ABCEX_EMAIL = os.getenv("ABCEX_EMAIL", "")
ABCEX_PASSWORD = os.getenv("ABCEX_PASSWORD", "")

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "exchange_trades")

GOTO_TIMEOUT_MS = int(os.getenv("GOTO_TIMEOUT_MS", "60000"))
WAIT_TRADES_TIMEOUT_MS = int(os.getenv("WAIT_TRADES_TIMEOUT_MS", "35000"))

# Render cache path
BROWSERS_ROOT = os.getenv("PLAYWRIGHT_BROWSERS_PATH", "/opt/render/.cache/ms-playwright")
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = BROWSERS_ROOT

# ───────────────────────── LOGGING ─────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
log = logging.getLogger("abcex")

# ───────────────────────── INSTALL (как у тебя) ─────────────────────────

def _env_without_proxies() -> Dict[str, str]:
    """
    На playwright install убираем прокси — иначе скачивание CDN может рваться.
    """
    env = dict(os.environ)
    for k in [
        "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY",
        "http_proxy", "https_proxy", "all_proxy", "no_proxy",
    ]:
        env.pop(k, None)
    env["PLAYWRIGHT_BROWSERS_PATH"] = BROWSERS_ROOT
    return env

def _headless_shell_exists() -> bool:
    pattern = os.path.join(BROWSERS_ROOT, "chromium_headless_shell-*", "chrome-linux", "headless_shell")
    return len(glob.glob(pattern)) > 0

def _chromium_exists() -> bool:
    pattern = os.path.join(BROWSERS_ROOT, "chromium-*", "chrome-linux", "chrome")
    return len(glob.glob(pattern)) > 0

def ensure_playwright_browsers(force: bool = False) -> None:
    """
    Runtime-установка браузеров.
    """
    if not force and _chromium_exists() and _headless_shell_exists():
        log.info("Playwright browsers already present.")
        return

    log.warning("Installing Playwright browsers (runtime) to %s ... force=%s", BROWSERS_ROOT, force)
    r = subprocess.run(
        [sys.executable, "-m", "playwright", "install", "chromium", "chromium-headless-shell"],
        check=False,
        capture_output=True,
        text=True,
        env=_env_without_proxies(),
        timeout=20 * 60,
    )

    log.warning("playwright install returncode=%s", r.returncode)
    if r.stdout:
        log.warning("playwright install STDOUT (tail):\n%s", r.stdout[-4000:])
    if r.stderr:
        log.warning("playwright install STDERR (tail):\n%s", r.stderr[-4000:])

    if r.returncode != 0:
        raise RuntimeError("playwright install failed")

    log.warning("Install verification: chromium=%s headless_shell=%s", _chromium_exists(), _headless_shell_exists())

def _should_force_install(err: Exception) -> bool:
    s = str(err)
    return (
        "Executable doesn't exist" in s
        or "playwright install" in s
        or "chromium_headless_shell" in s
        or ("ms-playwright" in s and "doesn't exist" in s)
    )

# ───────────────────────── PROXY ─────────────────────────

def _get_proxy_url() -> Optional[str]:
    return os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY") or None

def _parse_proxy_for_playwright(proxy_url: str) -> Dict[str, Any]:
    u = urlsplit(proxy_url)
    if not u.scheme or not u.hostname or not u.port:
        return {"server": proxy_url}
    out: Dict[str, Any] = {"server": f"{u.scheme}://{u.hostname}:{u.port}"}
    if u.username:
        out["username"] = u.username
    if u.password:
        out["password"] = u.password
    return out

# ───────────────────────── NUMBERS / TIME ─────────────────────────

TIME_RE = re.compile(r"\b(\d{1,2}:\d{2}(?::\d{2})?)\b")

def _q8(x: Decimal) -> Decimal:
    return x.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)

def _to_decimal_num(text: str) -> Decimal:
    """
    Грязестойкий парсер чисел:
    - убирает валюты/буквы/символы типа ₽, USDT
    - поддерживает '191 889,47' / '191889.47'
    """
    t = (text or "").strip().replace("\xa0", " ")
    t = re.sub(r"[^0-9\s\.,]", "", t)  # оставляем цифры/пробел/./,
    t = t.replace(" ", "")

    if not t:
        raise InvalidOperation("empty numeric")

    if "," in t and "." in t:
        t = t.replace(",", "")
    else:
        t = t.replace(",", ".")

    return Decimal(t)

def _normalize_time_to_hhmmss(text: str) -> Optional[str]:
    m = TIME_RE.search((text or "").replace("\xa0", " "))
    if not m:
        return None
    raw = m.group(1)
    parts = raw.split(":")
    if len(parts) == 2:
        hh, mm = parts
        if len(hh) == 1:
            hh = "0" + hh
        return f"{hh}:{mm}:00"
    hh, mm, ss = parts
    if len(hh) == 1:
        hh = "0" + hh
    if len(ss) == 1:
        ss = "0" + ss
    return f"{hh}:{mm}:{ss}"

# ───────────────────────── DEBUG ARTIFACTS ─────────────────────────

_last_zero_debug_ts = 0.0

async def save_debug(page: Page, tag: str) -> None:
    """
    Сохраняем скрин и html. На Render можно скачать через Shell.
    """
    from datetime import datetime
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    png = f"debug_{tag}_{ts}.png"
    html = f"debug_{tag}_{ts}.html"
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

# ───────────────────────── PAGE ACTIONS ─────────────────────────

async def accept_cookies_if_any(page: Page) -> None:
    for txt in ["Принять", "Согласен", "Я согласен", "Accept", "I agree"]:
        try:
            btn = page.locator(f"text={txt}")
            if await btn.count() > 0 and await btn.first.is_visible():
                log.info("Cookies banner: clicking '%s'...", txt)
                await btn.first.click(timeout=5_000)
                await page.wait_for_timeout(500)
                return
        except Exception:
            pass

async def _find_visible_in_frames(page: Page, selectors: List[str]) -> Optional[Locator]:
    for fr in page.frames:
        for sel in selectors:
            try:
                loc = fr.locator(sel)
                cnt = await loc.count()
                if cnt <= 0:
                    continue
                for i in range(min(cnt, 10)):
                    cand = loc.nth(i)
                    if await cand.is_visible():
                        return cand
            except Exception:
                continue
    return None

async def _is_login_visible(page: Page) -> bool:
    pw = await _find_visible_in_frames(page, [
        "input[type='password']",
        "input[autocomplete='current-password']",
        "input[name*='pass' i]",
    ])
    return pw is not None

async def _open_login_modal_if_needed(page: Page) -> None:
    if await _is_login_visible(page):
        return
    for sel in [
        "button:has-text('Войти')",
        "a:has-text('Войти')",
        "text=Войти",
        "button:has-text('Login')",
        "a:has-text('Login')",
        "text=Login",
        "button:has-text('Sign in')",
        "a:has-text('Sign in')",
        "text=Sign in",
    ]:
        try:
            btn = page.locator(sel)
            if await btn.count() > 0 and await btn.first.is_visible():
                await btn.first.click(timeout=7_000)
                await page.wait_for_timeout(800)
                return
        except Exception:
            pass

async def login_if_needed(page: Page, email: str, password: str) -> None:
    await _open_login_modal_if_needed(page)

    if not await _is_login_visible(page):
        log.info("Login not required (no password field detected).")
        return

    log.info("Login detected. Performing sign-in ...")

    email_loc = await _find_visible_in_frames(page, [
        "input[type='email']",
        "input[name='email']",
        "input[name*='mail' i]",
        "input[placeholder*='mail' i]",
        "input[placeholder*='email' i]",
        "input[placeholder*='почта' i]",
        "input[autocomplete='username']",
        "input[type='text'][name*='login' i]",
    ])
    pw_loc = await _find_visible_in_frames(page, [
        "input[type='password']",
        "input[autocomplete='current-password']",
        "input[name*='pass' i]",
    ])

    if email_loc is None or pw_loc is None:
        await save_debug(page, "login_fields_not_found")
        raise RuntimeError("Не смог найти поля email/password.")

    await email_loc.fill(email, timeout=10_000)
    await pw_loc.fill(password, timeout=10_000)

    btn = await _find_visible_in_frames(page, [
        "button:has-text('Войти')",
        "button:has-text('Вход')",
        "button:has-text('Sign in')",
        "button:has-text('Login')",
        "button[type='submit']",
    ])
    if btn is not None:
        try:
            await btn.click(timeout=10_000)
        except Exception:
            try:
                await page.keyboard.press("Enter")
            except Exception:
                pass
    else:
        try:
            await page.keyboard.press("Enter")
        except Exception:
            pass

    await page.wait_for_timeout(900)

    # ждём исчезновение password поля
    for _ in range(50):
        if not await _is_login_visible(page):
            log.info("Login successful (password field disappeared).")
            return
        await page.wait_for_timeout(400)

    await save_debug(page, "login_failed")
    raise RuntimeError("Логин не прошёл (форма логина всё ещё видна).")

async def click_trades_tab_best_effort(page: Page) -> None:
    for t in ["Сделки", "История", "Order history", "Trades", "Последние сделки"]:
        try:
            tab = page.locator(f"[role='tab']:has-text('{t}')")
            if await tab.count() > 0 and await tab.first.is_visible():
                await tab.first.click(timeout=8_000)
                await page.wait_for_timeout(700)
                return
        except Exception:
            continue
    for t in ["Сделки", "История", "Order history", "Trades", "Последние сделки"]:
        try:
            tab = page.locator(f"text={t}")
            if await tab.count() > 0 and await tab.first.is_visible():
                await tab.first.click(timeout=8_000)
                await page.wait_for_timeout(700)
                return
        except Exception:
            continue

# ───────────────────────── PARSING ─────────────────────────

async def extract_trades_anywhere(page: Page, limit: int) -> List[Dict[str, Any]]:
    """
    Универсальный DOM-парсер:
    - ищем элементы с временем HH:MM(:SS)
    - поднимаемся вверх и пытаемся найти “ряд”, где до времени есть 2 числовых значения (price/qty)
    - числовые значения допускают хвосты типа ₽/USDT и т.п. (чистим)
    """
    raw: List[Dict[str, Any]] = await page.evaluate(
        """(limit) => {
          const timeRe = /\\b(\\d{1,2}:\\d{2}(?::\\d{2})?)\\b/;

          const normTime = (s) => {
            const m = ((s||'').match(timeRe)||[])[1];
            if (!m) return null;
            const parts = m.split(':');
            if (parts.length === 2) {
              let [hh, mm] = parts;
              if (hh.length === 1) hh = '0'+hh;
              return `${hh}:${mm}:00`;
            }
            let [hh, mm, ss] = parts;
            if (hh.length === 1) hh = '0'+hh;
            if (ss.length === 1) ss = '0'+ss;
            return `${hh}:${mm}:${ss}`;
          };

          const cleanNum = (s) => (s||'')
            .replace(/\\u00A0/g,' ')
            .replace(/[^0-9\\s.,]/g,'')   // убираем ₽, USDT, буквы и т.п.
            .trim();

          const isNum = (s) => /^[0-9][0-9\\s.,]*$/.test(cleanNum(s));

          const timeEls = Array.from(document.querySelectorAll('p,span,div'))
            .filter(e => {
              const t = (e.textContent||'').trim();
              return t && t.length <= 22 && timeRe.test(t);
            });

          const out = [];
          const seen = new Set();

          for (const te of timeEls) {
            const tnorm = normTime(te.textContent||'');
            if (!tnorm) continue;

            let node = te;

            for (let up=0; up<10 && node; up++) {
              const row = node.closest ? node.closest('div') : null;
              if (!row) break;

              // собираем короткие “ячейки” в DOM-порядке
              const cells = [];
              const walker = document.createTreeWalker(row, NodeFilter.SHOW_ELEMENT, null);
              let cur = walker.currentNode;
              while (cur) {
                const el = cur;
                const tx = (el.textContent||'').trim();
                if (tx && tx.length <= 48) cells.push(tx);
                cur = walker.nextNode();
              }

              // индекс времени
              let timeIdx = -1;
              for (let i=0;i<cells.length;i++){
                if (normTime(cells[i]) === tnorm) { timeIdx = i; break; }
              }
              if (timeIdx < 0) { node = row.parentElement; continue; }

              // два последних числа ДО времени
              const nums = [];
              for (let i=0;i<timeIdx;i++){
                if (isNum(cells[i])) nums.push(cleanNum(cells[i]));
              }
              if (nums.length < 2) { node = row.parentElement; continue; }

              const price_raw = nums[nums.length-2];
              const qty_raw   = nums[nums.length-1];

              const key = `${tnorm}|${price_raw}|${qty_raw}`;
              if (!seen.has(key)) {
                seen.add(key);
                out.push({ trade_time: tnorm, price_raw, qty_raw });
              }
              break;
            }

            if (out.length >= limit) break;
          }

          return out.slice(0, limit);
        }""",
        limit,
    )

    rows: List[Dict[str, Any]] = []
    for r in raw:
        try:
            tt = str(r.get("trade_time") or "").strip()
            pr = str(r.get("price_raw") or "").strip()
            qr = str(r.get("qty_raw") or "").strip()

            tt = _normalize_time_to_hhmmss(tt) or tt
            if not tt or not pr or not qr:
                continue

            price = _q8(_to_decimal_num(pr))
            vol_usdt = _q8(_to_decimal_num(qr))
            vol_rub = _q8(price * vol_usdt)

            if price <= 0 or vol_usdt <= 0 or vol_rub <= 0:
                continue

            # строго под схему Supabase
            rows.append({
                "source": SOURCE,
                "symbol": SYMBOL,
                "price": str(price),
                "volume_usdt": str(vol_usdt),
                "volume_rub": str(vol_rub),
                "trade_time": tt,  # time without time zone => 'HH:MM:SS'
            })
        except (InvalidOperation, ValueError):
            continue
        except Exception:
            continue

    return rows

async def wait_trades_parsable(page: Page, timeout_ms: int, min_rows: int = 5) -> None:
    """
    Ждём не просто “время где-то”, а реальные парсабельные сделки (time + 2 числа).
    """
    start = time.time()
    last_n = -1
    while (time.time() - start) * 1000 < timeout_ms:
        try:
            probe = await extract_trades_anywhere(page, limit=50)
            n = len(probe)
            if n != last_n:
                log.info("Trades parsable probe: %d", n)
                last_n = n
            if n >= min_rows:
                return
        except Exception:
            pass
        await page.wait_for_timeout(900)

    await save_debug(page, "trades_not_parsable")
    raise RuntimeError("Не дождался парсабельных сделок (time + 2 nums).")

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

async def supabase_upsert_trades(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    if not SUPABASE_URL or not SUPABASE_KEY or not SUPABASE_TABLE:
        log.warning("Supabase env is not fully set; skipping DB write.")
        return

    # upsert по твоему unique index:
    # (source, symbol, trade_time, price, volume_usdt)
    url = (
        SUPABASE_URL.rstrip("/")
        + f"/rest/v1/{SUPABASE_TABLE}"
        + "?on_conflict=source,symbol,trade_time,price,volume_usdt"
    )
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }

    async with httpx.AsyncClient(timeout=35.0, headers=headers, trust_env=True) as client:
        resp = await client.post(url, content=json.dumps(rows, ensure_ascii=False).encode("utf-8"))
        if resp.status_code in (200, 201, 204):
            log.info("Supabase upsert OK: %s trades", len(rows))
            return

        # иногда Supabase/PostgREST может ругаться на schema cache сразу после миграций
        body = resp.text[:2000]
        log.error("Supabase write failed: %s %s", resp.status_code, body)

# ───────────────────────── CORE ─────────────────────────

async def _goto_stable(page: Page, url: str) -> None:
    """
    SPA-устойчивый goto:
    - domcontentloaded как базовый
    - networkidle пробуем дополнительно, но не считаем критичным
    """
    await page.goto(url, wait_until="domcontentloaded", timeout=GOTO_TIMEOUT_MS)
    try:
        await page.wait_for_load_state("networkidle", timeout=20_000)
    except Exception:
        pass
    await page.wait_for_timeout(1200)

async def run_once() -> List[Dict[str, Any]]:
    proxy_url = _get_proxy_url()
    pw_proxy = _parse_proxy_for_playwright(proxy_url) if proxy_url else None

    async with async_playwright() as p:
        browser = None
        for attempt in (1, 2, 3):
            try:
                kwargs: Dict[str, Any] = {
                    "headless": True,
                    "args": [
                        "--disable-blink-features=AutomationControlled",
                        "--no-sandbox",
                        "--disable-dev-shm-usage",
                    ],
                }
                if pw_proxy:
                    kwargs["proxy"] = pw_proxy

                browser = await p.chromium.launch(**kwargs)
                break
            except Exception as e:
                if _should_force_install(e):
                    log.warning("Chromium launch failed; forcing install and retry... attempt=%s", attempt)
                    ensure_playwright_browsers(force=True)
                    await asyncio.sleep(2.0 * attempt)
                    continue
                raise

        if browser is None:
            raise RuntimeError("Cannot launch browser after retries.")

        storage_state = STATE_PATH if os.path.exists(STATE_PATH) else None
        if storage_state:
            log.info("Using saved session state: %s", storage_state)

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
            log.info("Opening ABCEX: %s", ABCEX_URL)
            await _goto_stable(page, ABCEX_URL)

            await accept_cookies_if_any(page)

            if not ABCEX_EMAIL or not ABCEX_PASSWORD:
                raise RuntimeError("ABCEX_EMAIL/ABCEX_PASSWORD must be set in env.")
            await login_if_needed(page, ABCEX_EMAIL, ABCEX_PASSWORD)

            # после логина сайт иногда уводит на /client/spot без пары — возвращаемся
            await _goto_stable(page, ABCEX_URL)

            try:
                await context.storage_state(path=STATE_PATH)
                log.info("Saved session state to %s", STATE_PATH)
            except Exception as e:
                log.warning("Could not save storage state: %s", e)

            await click_trades_tab_best_effort(page)

            # главное ожидание
            await wait_trades_parsable(page, timeout_ms=WAIT_TRADES_TIMEOUT_MS, min_rows=5)

            trades = await extract_trades_anywhere(page, limit=LIMIT)
            log.info("Parsed trades: %d", len(trades))

            if len(trades) == 0:
                global _last_zero_debug_ts
                now = time.time()
                if now - _last_zero_debug_ts > 600:
                    _last_zero_debug_ts = now
                    await save_debug(page, "parsed_zero_trades")

            return trades

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
    # установка браузеров (если CDN иногда рвёт — ретраи будут дальше)
    ensure_playwright_browsers(force=False)

    seen: Dict[TradeKey, float] = {}
    seen_ttl = 60 * 30  # 30 минут

    while True:
        try:
            trades = await run_once()

            now = time.time()
            for k, ts in list(seen.items()):
                if now - ts > seen_ttl:
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
                log.info("No new trades after in-memory dedup.")

        except Exception as e:
            log.error("Cycle error: %s", e)
            if _should_force_install(e):
                try:
                    ensure_playwright_browsers(force=True)
                except Exception as ie:
                    log.error("Forced install failed: %s", ie)
            await asyncio.sleep(4.0)

        await asyncio.sleep(POLL_SEC)

if __name__ == "__main__":
    asyncio.run(main())
