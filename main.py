#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import glob
import json
import logging
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, List, Optional
from urllib.parse import urlsplit

import httpx
from playwright.async_api import async_playwright, Browser, BrowserContext, Locator, Page

# ───────────────────────── CONFIG ─────────────────────────

ABCEX_URL = os.getenv("ABCEX_URL", "https://abcex.io/client/spot/USDTRUB")
STATE_PATH = os.getenv("STATE_PATH", "abcex_state.json")

SOURCE = os.getenv("SOURCE", "abcex")
SYMBOL = os.getenv("SYMBOL", "USDT/RUB")

LIMIT = int(os.getenv("LIMIT", "120"))              # сколько строк брать из “Сделки”
POLL_SEC = float(os.getenv("POLL_SEC", "5.0"))      # на Render стабильнее >= 4-5 сек

ABCEX_EMAIL = os.getenv("ABCEX_EMAIL", "")
ABCEX_PASSWORD = os.getenv("ABCEX_PASSWORD", "")

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "exchange_trades")

GOTO_TIMEOUT_MS = int(os.getenv("GOTO_TIMEOUT_MS", "120000"))
WAIT_TRADES_TIMEOUT_MS = int(os.getenv("WAIT_TRADES_TIMEOUT_MS", "60000"))

# Render cache path
BROWSERS_ROOT = os.getenv("PLAYWRIGHT_BROWSERS_PATH", "/opt/render/.cache/ms-playwright")
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = BROWSERS_ROOT

# ───────────────────────── LOGGING ─────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
log = logging.getLogger("abcex")

# ───────────────────────── INSTALL ─────────────────────────

def _env_without_proxies() -> Dict[str, str]:
    """
    На playwright install убираем прокси — иначе скачивание CDN часто рвётся.
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
    Runtime-установка браузеров с ретраями.
    """
    if not force and _chromium_exists() and _headless_shell_exists():
        log.info("Playwright browsers already present.")
        return

    max_attempts = 4
    for attempt in range(1, max_attempts + 1):
        log.warning(
            "Installing Playwright browsers (runtime) to %s ... force=%s attempt=%s/%s",
            BROWSERS_ROOT, force, attempt, max_attempts
        )
        r = subprocess.run(
            [sys.executable, "-m", "playwright", "install", "chromium", "chromium-headless-shell"],
            check=False,
            capture_output=True,
            text=True,
            env=_env_without_proxies(),
            timeout=25 * 60,
        )
        log.warning("playwright install returncode=%s", r.returncode)
        if r.stdout:
            log.warning("playwright install STDOUT (tail):\n%s", r.stdout[-3000:])
        if r.stderr:
            log.warning("playwright install STDERR (tail):\n%s", r.stderr[-3000:])

        if r.returncode == 0 and _chromium_exists() and _headless_shell_exists():
            log.warning("Install verification: chromium=True headless_shell=True")
            return

        if attempt < max_attempts:
            sleep_s = 6 * attempt
            log.warning("Install not complete yet. Sleeping %ss and retrying...", sleep_s)
            time.sleep(sleep_s)

    raise RuntimeError("playwright install failed after retries")

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
STRICT_TIME_RE = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d$")  # 00:00:00 .. 23:59:59

def _q8(x: Decimal) -> Decimal:
    return x.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)

def _to_decimal_num(text: str) -> Decimal:
    """
    Грязестойкий парсер чисел:
    - убирает валюты/буквы/символы типа ₽, USDT
    - поддерживает '191 889,47' / '191889.47' / '14,964.9824'
    """
    t = (text or "").strip().replace("\xa0", " ")
    t = re.sub(r"[^0-9\s\.,]", "", t)  # оставляем цифры/пробел/./,
    t = t.replace(" ", "")
    if not t:
        raise InvalidOperation("empty numeric")
    if "," in t and "." in t:
        t = t.replace(",", "")  # comma as thousand-sep
    else:
        t = t.replace(",", ".")  # comma as decimal
    return Decimal(t)

def _normalize_time_to_hhmmss(text: str) -> Optional[str]:
    m = TIME_RE.search((text or "").replace("\xa0", " "))
    if not m:
        return None
    raw = m.group(1)
    parts = raw.split(":")
    if len(parts) == 2:
        hh, mm = parts
        return f"{hh.zfill(2)}:{mm}:00"
    hh, mm, ss = parts
    return f"{hh.zfill(2)}:{mm}:{ss.zfill(2)}"

def _is_valid_time_hhmmss(tt: str) -> bool:
    return bool(tt and STRICT_TIME_RE.match(tt))

# ───────────────────────── DEBUG ARTIFACTS ─────────────────────────

_last_zero_debug_ts = 0.0

async def save_debug(page: Page, tag: str) -> None:
    """
    Сохраняем скрин и html. На Render можно скачать через Shell.
    Скрин делаем быстрым (без full_page) и с таймаутом, чтобы не зависать на fonts.
    """
    ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    png = f"debug_{tag}_{ts}.png"
    html = f"debug_{tag}_{ts}.html"

    try:
        await page.screenshot(path=png, full_page=False, timeout=5_000)
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

    if not email or not password:
        await save_debug(page, "login_required_no_creds")
        raise RuntimeError("Login required but ABCEX_EMAIL/ABCEX_PASSWORD are not set.")

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

async def extract_trades_order_history(page: Page, limit: int) -> List[Dict[str, Any]]:
    """
    Достаём сделки СТРОГО из панели orderHistory:
    div[id$='-panel-orderHistory'] и внутри строки с 3 <p data-size="xs">: price, qty, time
    """
    raw: List[Dict[str, Any]] = await page.evaluate(
        """(limit) => {
          const panel = document.querySelector("[id$='-panel-orderHistory']");
          if (!panel) return [];

          const timeRe = /\\b(\\d{1,2}:\\d{2}(?::\\d{2})?)\\b/;

          const candidates = Array.from(panel.querySelectorAll("div"))
            .map(div => {
              const ps = div.querySelectorAll("p[data-size='xs']");
              if (ps.length !== 3) return null;
              const p0 = (ps[0].textContent || "").trim();
              const p1 = (ps[1].textContent || "").trim();
              const p2 = (ps[2].textContent || "").trim();
              if (!timeRe.test(p2)) return null;
              if (!/^[0-9]/.test(p0) || !/^[0-9]/.test(p1)) return null;
              return { price_raw: p0, qty_raw: p1, time_raw: p2 };
            })
            .filter(Boolean);

          const out = [];
          const seen = new Set();
          for (const r of candidates) {
            const m = (r.time_raw.match(timeRe) || [])[1];
            const key = `${m}|${r.price_raw}|${r.qty_raw}`;
            if (seen.has(key)) continue;
            seen.add(key);
            out.push(r);
            if (out.length >= limit) break;
          }
          return out;
        }""",
        limit,
    )

    rows: List[Dict[str, Any]] = []
    bad_times: List[Dict[str, str]] = []

    for r in raw:
        try:
            pr = str(r.get("price_raw") or "").strip()
            qr = str(r.get("qty_raw") or "").strip()
            tr = str(r.get("time_raw") or "").strip()

            tt = _normalize_time_to_hhmmss(tr)
            if not tt:
                continue

            if not _is_valid_time_hhmmss(tt):
                bad_times.append({"trade_time": tt, "price_raw": pr, "qty_raw": qr, "time_raw": tr})
                continue

            price = _q8(_to_decimal_num(pr))
            vol_usdt = _q8(_to_decimal_num(qr))
            vol_rub = _q8(price * vol_usdt)

            if price <= 0 or vol_usdt <= 0 or vol_rub <= 0:
                continue

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

    if bad_times:
        log.error("Found invalid trade_time values (will be skipped). examples=%s", bad_times[:5])

    return rows

async def wait_trades_parsable(page: Page, timeout_ms: int, min_rows: int = 5) -> None:
    start = time.time()
    last_n = -1
    while (time.time() - start) * 1000 < timeout_ms:
        try:
            probe = await extract_trades_order_history(page, limit=50)
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
    raise RuntimeError("Не дождался парсабельных сделок в orderHistory (time + 2 nums).")

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

async def supabase_upsert_trades(rows: List[Dict[str, Any]], client: httpx.AsyncClient) -> None:
    if not rows:
        return
    if not SUPABASE_URL or not SUPABASE_KEY or not SUPABASE_TABLE:
        log.warning("Supabase env is not fully set; skipping DB write.")
        return

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

    resp = await client.post(url, headers=headers, content=json.dumps(rows, ensure_ascii=False).encode("utf-8"))
    if resp.status_code in (200, 201, 204):
        log.info("Supabase upsert OK: %s trades", len(rows))
        return

    body = resp.text[:2000]
    log.error("Supabase write failed: %s %s", resp.status_code, body)

# ───────────────────────── NAVIGATION ─────────────────────────

async def _goto_stable(page: Page, url: str) -> None:
    """
    Без networkidle (на динамических сайтах часто не наступает),
    + ретраи на случай плавающих таймаутов Render/Cloudflare/CDN.
    """
    last_err: Optional[Exception] = None
    for attempt in range(1, 4):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=GOTO_TIMEOUT_MS)
            await page.wait_for_timeout(1200)
            return
        except Exception as e:
            last_err = e
            log.warning("goto attempt %s/3 failed: %s", attempt, e)
            try:
                await page.wait_for_timeout(1500 * attempt)
            except Exception:
                pass
    raise last_err or RuntimeError("goto failed")

# ───────────────────────── SESSION (REUSE) ─────────────────────────

class AbcexSession:
    def __init__(self) -> None:
        self.pw = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self._cycles_ok = 0

        proxy_url = _get_proxy_url()
        self.pw_proxy = _parse_proxy_for_playwright(proxy_url) if proxy_url else None

    async def start(self) -> None:
        self.pw = await async_playwright().start()

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
                if self.pw_proxy:
                    kwargs["proxy"] = self.pw_proxy

                browser = await self.pw.chromium.launch(**kwargs)
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

        self.browser = browser

        storage_state = STATE_PATH if os.path.exists(STATE_PATH) else None
        if storage_state:
            log.info("Using saved session state: %s", storage_state)

        self.context = await self.browser.new_context(
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

        self.page = await self.context.new_page()
        await self._install_routes(self.page)

        await self._bootstrap()

    async def _install_routes(self, page: Page) -> None:
        """
        Ускорение и стабильность: режем font/image/media.
        Это часто убирает Page.goto timeout на Render.
        """
        async def _route(route, request):
            rt = request.resource_type
            url = request.url.lower()

            if rt in ("image", "media", "font"):
                return await route.abort()

            if any(x in url for x in ["google-analytics", "googletagmanager", "metrika", "doubleclick"]):
                return await route.abort()

            return await route.continue_()

        await page.route("**/*", _route)

    async def _bootstrap(self) -> None:
        assert self.page is not None
        assert self.context is not None

        log.info("Opening ABCEX: %s", ABCEX_URL)
        await _goto_stable(self.page, ABCEX_URL)

        await accept_cookies_if_any(self.page)
        await login_if_needed(self.page, ABCEX_EMAIL, ABCEX_PASSWORD)

        # после логина сайт иногда уводит — возвращаемся
        await _goto_stable(self.page, ABCEX_URL)

        try:
            await self.context.storage_state(path=STATE_PATH)
            log.info("Saved session state to %s", STATE_PATH)
        except Exception as e:
            log.warning("Could not save storage state: %s", e)

        await click_trades_tab_best_effort(self.page)

        try:
            await self.page.wait_for_selector("[id$='-panel-orderHistory']", timeout=20_000)
        except Exception:
            await save_debug(self.page, "orderHistory_panel_missing")
            raise RuntimeError("Не нашёл панель orderHistory: [id$='-panel-orderHistory']")

        await wait_trades_parsable(self.page, timeout_ms=WAIT_TRADES_TIMEOUT_MS, min_rows=5)

    async def fetch_trades(self) -> List[Dict[str, Any]]:
        assert self.page is not None

        # мягкий рефреш раз в N успешных циклов (долгоживущие SPA иногда деградируют)
        if self._cycles_ok > 0 and self._cycles_ok % 25 == 0:
            log.info("Soft refresh page after %s cycles...", self._cycles_ok)
            await _goto_stable(self.page, ABCEX_URL)
            await click_trades_tab_best_effort(self.page)

        trades = await extract_trades_order_history(self.page, limit=LIMIT)
        log.info("Parsed trades: %d", len(trades))

        if len(trades) == 0:
            global _last_zero_debug_ts
            now = time.time()
            if now - _last_zero_debug_ts > 600:
                _last_zero_debug_ts = now
                await save_debug(self.page, "parsed_zero_trades")

            # попробуем быстро восстановиться (парсер/вёрстка могла мигнуть)
            try:
                await wait_trades_parsable(self.page, timeout_ms=12_000, min_rows=3)
                trades = await extract_trades_order_history(self.page, limit=LIMIT)
            except Exception:
                log.warning("Trades not parsable quickly; reloading page...")
                await _goto_stable(self.page, ABCEX_URL)
                await click_trades_tab_best_effort(self.page)
                await wait_trades_parsable(self.page, timeout_ms=WAIT_TRADES_TIMEOUT_MS, min_rows=5)
                trades = await extract_trades_order_history(self.page, limit=LIMIT)

        self._cycles_ok += 1
        return trades

    async def close(self) -> None:
        try:
            if self.page:
                await self.page.close()
        except Exception:
            pass
        try:
            if self.context:
                await self.context.close()
        except Exception:
            pass
        try:
            if self.browser:
                await self.browser.close()
        except Exception:
            pass
        try:
            if self.pw:
                await self.pw.stop()
        except Exception:
            pass

# ───────────────────────── MAIN ─────────────────────────

async def main() -> None:
    ensure_playwright_browsers(force=False)

    seen: Dict[TradeKey, float] = {}
    seen_ttl = 60 * 30  # 30 минут

    session: Optional[AbcexSession] = None

    async with httpx.AsyncClient(timeout=35.0, trust_env=True) as supa_client:
        while True:
            try:
                if session is None:
                    session = AbcexSession()
                    await session.start()

                trades = await session.fetch_trades()

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
                    await supabase_upsert_trades(fresh, supa_client)
                else:
                    log.info("No new trades after in-memory dedup.")

            except Exception as e:
                log.error("Cycle error: %s", e)

                if _should_force_install(e):
                    try:
                        ensure_playwright_browsers(force=True)
                    except Exception as ie:
                        log.error("Forced install failed: %s", ie)

                # пересоздаём сессию полностью (залипшие страницы/контексты)
                try:
                    if session:
                        await session.close()
                except Exception:
                    pass
                session = None

                await asyncio.sleep(4.0)

            await asyncio.sleep(POLL_SEC)

if __name__ == "__main__":
    asyncio.run(main())
