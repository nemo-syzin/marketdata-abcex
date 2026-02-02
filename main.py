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
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlsplit

import httpx
from playwright.async_api import async_playwright, Page, Locator, Browser, BrowserContext, Frame

# ───────────────────────── CONFIG ─────────────────────────

ABCEX_URL = os.getenv("ABCEX_URL", "https://abcex.io/client/spot/USDTRUB")
STATE_PATH = os.getenv("STATE_PATH", "abcex_state.json")

SOURCE = os.getenv("SOURCE", "abcex")
SYMBOL = os.getenv("SYMBOL", "USDT/RUB")

LIMIT = int(os.getenv("LIMIT", "120"))
POLL_SEC = float(os.getenv("POLL_SEC", "2.0"))

ABCEX_EMAIL = os.getenv("ABCEX_EMAIL", "")
ABCEX_PASSWORD = os.getenv("ABCEX_PASSWORD", "")

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "exchange_trades")

GOTO_TIMEOUT_MS = int(os.getenv("GOTO_TIMEOUT_MS", "60000"))
WAIT_UI_TIMEOUT_MS = int(os.getenv("WAIT_UI_TIMEOUT_MS", "45000"))
WAIT_TRADES_TIMEOUT_MS = int(os.getenv("WAIT_TRADES_TIMEOUT_MS", "45000"))

# sanity для USDT/RUB
PRICE_MIN = Decimal(os.getenv("PRICE_MIN", "50"))
PRICE_MAX = Decimal(os.getenv("PRICE_MAX", "200"))

# Render cache path
BROWSERS_ROOT = os.getenv("PLAYWRIGHT_BROWSERS_PATH", "/opt/render/.cache/ms-playwright")
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = BROWSERS_ROOT

# watchdog: если долго нет УСПЕШНОГО upsert-а (200/201/204) — падаем, Render перезапустит
STALL_RESTART_SECONDS = float(os.getenv("STALL_RESTART_SECONDS", "3600"))

# Диагностика: печать HTML-сниппета при проблемах
DIAG_TEXT_CHARS = int(os.getenv("DIAG_TEXT_CHARS", "2000"))

# Прокси:
# 1) стандартные env: HTTPS_PROXY / HTTP_PROXY
# 2) доп.: SOCKS_PROXY (socks5://user:pass@host:port)
# 3) режим: PROXY_MODE=auto|force|off
PROXY_MODE = os.getenv("PROXY_MODE", "auto").strip().lower()
SOCKS_PROXY = os.getenv("SOCKS_PROXY", "").strip()  # optional

# ───────────────────────── LOGGING ─────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
log = logging.getLogger("abcex")

# ───────────────────────── PLAYWRIGHT INSTALL ─────────────────────────

def _env_without_proxies() -> Dict[str, str]:
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
            log.warning("playwright install STDOUT (tail):\n%s", r.stdout[-2000:])
        if r.stderr:
            log.warning("playwright install STDERR (tail):\n%s", r.stderr[-2000:])

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

# ───────────────────────── PROXY PARSE ─────────────────────────

def _get_http_proxy_url() -> Optional[str]:
    return (os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY") or "").strip() or None

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

def _log_proxy(proxy_url: Optional[str], tag: str) -> None:
    if not proxy_url:
        log.info("%s: proxy=OFF", tag)
        return
    u = urlsplit(proxy_url)
    log.info("%s: proxy=ON scheme=%s host=%s port=%s user=%s",
             tag, u.scheme, u.hostname, u.port, u.username or "")

# ───────────────────────── NUMBERS / TIME ─────────────────────────

STRICT_TIME_RE = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d(?::[0-5]\d)?$")

def _q8(x: Decimal) -> Decimal:
    return x.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)

def _to_decimal_num(text: str) -> Decimal:
    """
    Понимает:
      79.30
      5,004.5041
      10,734.6612
      1 020.2238
      1 020,2238
    """
    t = (text or "").strip().replace("\xa0", " ")
    t = re.sub(r"[^0-9\s\.,-]", "", t)
    t = t.replace(" ", "")
    if not t:
        raise InvalidOperation("empty numeric")

    if "," in t and "." in t:
        t = t.replace(",", "")
    else:
        t = t.replace(",", ".")

    return Decimal(t)

def _is_valid_time(tt: str) -> bool:
    return bool(tt and STRICT_TIME_RE.match(tt))

def _price_ok(p: Decimal) -> bool:
    return (p >= PRICE_MIN) and (p <= PRICE_MAX)

# ───────────────────────── DIAG HELPERS ─────────────────────────

async def _html_snippet(obj: Any) -> str:
    try:
        return await obj.evaluate(
            """(maxChars) => {
                const t = (document.documentElement?.outerHTML || document.body?.outerHTML || '').slice(0, maxChars);
                return t;
            }""",
            DIAG_TEXT_CHARS
        )
    except Exception:
        try:
            t = await obj.content()
            return (t or "")[:DIAG_TEXT_CHARS]
        except Exception:
            return ""

async def log_block_hints(page: Page, tag: str) -> None:
    snippet = await _html_snippet(page)
    if not snippet:
        log.warning("DIAG[%s] html_snippet: <empty>", tag)
        return
    low = snippet.lower()
    hints = []
    for h in ["cloudflare", "captcha", "attention required", "access denied", "blocked", "bot"]:
        if h in low:
            hints.append(h)
    log.warning("DIAG[%s] html_snippet(first %d chars). hints=%s", tag, DIAG_TEXT_CHARS, hints)
    log.warning("DIAG[%s] ----\n%s\n----", tag, snippet)

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

async def _is_login_visible(page: Page) -> bool:
    try:
        pw = page.locator("input[type='password']")
        return (await pw.count() > 0) and (await pw.first.is_visible())
    except Exception:
        return False

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
        raise RuntimeError("Login form detected, but ABCEX_EMAIL/ABCEX_PASSWORD are not set.")

    log.info("Login detected. Performing sign-in ...")

    email_loc = page.locator(
        "input[type='email'], input[autocomplete='username'], input[name*='mail' i], input[name*='login' i]"
    ).first
    pw_loc = page.locator("input[type='password'], input[autocomplete='current-password']").first

    if not await email_loc.is_visible() or not await pw_loc.is_visible():
        await log_block_hints(page, "login_fields_not_visible")
        raise RuntimeError("Не смог найти/увидеть поля email/password.")

    await email_loc.fill(email, timeout=10_000)
    await pw_loc.fill(password, timeout=10_000)

    submit = page.locator(
        "button[type='submit'], button:has-text('Войти'), button:has-text('Login'), button:has-text('Sign in')"
    ).first
    try:
        if await submit.is_visible():
            await submit.click(timeout=10_000)
        else:
            await page.keyboard.press("Enter")
    except Exception:
        try:
            await page.keyboard.press("Enter")
        except Exception:
            pass

    await page.wait_for_timeout(1_000)

    for _ in range(60):
        if not await _is_login_visible(page):
            log.info("Login successful (password field disappeared).")
            return
        await page.wait_for_timeout(400)

    await log_block_hints(page, "login_failed")
    raise RuntimeError("Логин не прошёл (форма логина всё ещё видна).")

async def _goto_stable(page: Page, url: str) -> None:
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=GOTO_TIMEOUT_MS)
    except Exception as e:
        log.warning("goto failed (%s). retry with reload...", e)
        await page.reload(wait_until="domcontentloaded", timeout=GOTO_TIMEOUT_MS)

    try:
        await page.wait_for_load_state("networkidle", timeout=20_000)
    except Exception:
        pass
    await page.wait_for_timeout(900)

# ───────────────────────── UI LOCATORS (PAGE OR FRAME) ─────────────────────────

PANEL_SEL = "div[role='tabpanel'][id*='panel-orderHistory']"

async def _find_panel_in_page_or_frames(page: Page) -> Optional[Tuple[Locator, Any]]:
    # 1) main page
    try:
        loc = page.locator(PANEL_SEL)
        if await loc.count() > 0:
            for i in range(await loc.count()):
                p = loc.nth(i)
                if await p.is_visible():
                    return p, page
    except Exception:
        pass

    # 2) frames
    for fr in page.frames:
        if fr == page.main_frame:
            continue
        try:
            loc = fr.locator(PANEL_SEL)
            if await loc.count() > 0:
                for i in range(await loc.count()):
                    p = loc.nth(i)
                    if await p.is_visible():
                        return p, fr
        except Exception:
            continue

    return None

async def click_trades_tab_best_effort(page: Page) -> bool:
    # максимально “широкий” набор селекторов
    candidates = ["Сделки", "Trades", "Order history", "История", "Последние сделки"]
    selectors = []
    for t in candidates:
        selectors += [
            f"[role='tab']:has-text('{t}')",
            f"button[role='tab']:has-text('{t}')",
            f"button:has-text('{t}')",
            f"a:has-text('{t}')",
            f"text={t}",
        ]

    # page
    for sel in selectors:
        try:
            el = page.locator(sel).first
            if await el.count() > 0 and await el.is_visible():
                await el.click(timeout=8_000)
                await page.wait_for_timeout(700)
                log.info("Clicked trades tab by selector: %s", sel)
                return True
        except Exception:
            continue

    # frames
    for fr in page.frames:
        if fr == page.main_frame:
            continue
        for sel in selectors:
            try:
                el = fr.locator(sel).first
                if await el.count() > 0 and await el.is_visible():
                    await el.click(timeout=8_000)
                    await page.wait_for_timeout(700)
                    log.info("Clicked trades tab in frame by selector: %s", sel)
                    return True
            except Exception:
                continue

    return False

async def wait_ui_ready(page: Page, timeout_ms: int) -> Tuple[Locator, Any]:
    """
    Ждём появления panel-orderHistory (на странице или во фрейме).
    Параллельно пытаемся нажать Trades.
    """
    start = time.time()
    last_try = 0.0

    while (time.time() - start) * 1000 < timeout_ms:
        found = await _find_panel_in_page_or_frames(page)
        if found:
            return found

        # раз в ~1.2s пытаемся кликнуть вкладку
        now = time.time()
        if now - last_try >= 1.2:
            ok = await click_trades_tab_best_effort(page)
            if not ok:
                log.warning("Could not click trades tab (no matching selector).")
            last_try = now

        await page.wait_for_timeout(600)

    await log_block_hints(page, "ui_not_ready_timeout")
    raise RuntimeError("Не дождался появления UI для orderHistory (ни на странице, ни во фреймах).")

async def wait_trades_ready(panel: Locator, scope: Any, timeout_ms: int) -> None:
    """
    Ждём, пока в panel-orderHistory появятся p-элементы со временем HH:MM:SS
    """
    start = time.time()
    last = -1

    while (time.time() - start) * 1000 < timeout_ms:
        try:
            handle = await panel.element_handle()
            if handle:
                n = await handle.evaluate(
                    """(root) => {
                      const re = /^(?:[01]\\d|2[0-3]):[0-5]\\d:[0-5]\\d$/;
                      const ps = Array.from(root.querySelectorAll('p'));
                      return ps.map(p => (p.textContent||'').trim()).filter(t => re.test(t)).length;
                    }"""
                )
                if n != last:
                    log.info("Trades time-cells detected: %s", n)
                    last = n
                if n and n >= 1:
                    return
        except Exception:
            pass

        # иногда помогает повторный клик
        try:
            await click_trades_tab_best_effort(scope if isinstance(scope, Page) else scope.page)
        except Exception:
            pass

        await (scope.wait_for_timeout(800) if hasattr(scope, "wait_for_timeout") else asyncio.sleep(0.8))

    raise RuntimeError("Не дождался появления сделок в panel-orderHistory (времени HH:MM:SS).")

async def _extract_trades_from_panel(panel: Locator, limit: int) -> List[Dict[str, Any]]:
    handle = await panel.element_handle()
    if handle is None:
        raise RuntimeError("Не смог получить element_handle панели orderHistory.")

    raw_rows: List[Dict[str, Any]] = await handle.evaluate(
        """(root, limit) => {
          const isTime = (s) => /^(?:[01]\\d|2[0-3]):[0-5]\\d:[0-5]\\d$/.test((s||'').trim());
          const isNum  = (s) => /^[0-9][0-9\\s\\u00A0.,]*$/.test((s||'').trim());

          const ps = Array.from(root.querySelectorAll('p'));
          const texts = ps.map(p => (p.textContent || '').replace(/\\u00A0/g,' ').trim());

          const out = [];

          for (let i = 0; i < texts.length; i++) {
            const t = texts[i];
            if (!isTime(t)) continue;

            let qty = null;
            let qtyIdx = -1;
            for (let j = i - 1; j >= 0; j--) {
              if (isNum(texts[j])) { qty = texts[j]; qtyIdx = j; break; }
            }
            if (!qty) continue;

            let price = null;
            let priceIdx = -1;
            for (let j = qtyIdx - 1; j >= 0; j--) {
              if (isNum(texts[j])) { price = texts[j]; priceIdx = j; break; }
            }
            if (!price) continue;

            const el = ps[priceIdx];
            const style0 = ((el && el.getAttribute && el.getAttribute('style')) || '').toLowerCase();
            const cls0 = ((el && el.className) || '').toLowerCase();

            let side = null;
            if (style0.includes('green') || cls0.includes('green')) side = 'buy';
            if (style0.includes('red')   || cls0.includes('red'))   side = 'sell';

            out.push({ price_raw: price, qty_raw: qty, time: t, side });

            if (out.length >= limit) break;
          }

          const seen = new Set();
          const uniq = [];
          for (const r of out) {
            const k = `${r.time}|${r.price_raw}|${r.qty_raw}`;
            if (seen.has(k)) continue;
            seen.add(k);
            uniq.push(r);
          }
          return uniq;
        }""",
        limit,
    )
    return raw_rows

def _normalize_trade_row(r: Dict[str, Any]) -> Optional[Tuple[Decimal, Decimal, str, Optional[str]]]:
    price_raw = str(r.get("price_raw", "")).strip()
    qty_raw = str(r.get("qty_raw", "")).strip()
    tt = str(r.get("time", "")).strip()
    side = r.get("side", None)

    if not price_raw or not qty_raw or not tt:
        return None
    if not _is_valid_time(tt):
        return None

    try:
        price = _q8(_to_decimal_num(price_raw))
        qty = _q8(_to_decimal_num(qty_raw))
    except Exception:
        return None

    if price <= 0 or qty <= 0:
        return None
    if not _price_ok(price):
        return None

    side_s: Optional[str] = None
    if side in ("buy", "sell"):
        side_s = side

    return price, qty, tt, side_s

async def extract_trades(panel: Locator, limit: int) -> List[Dict[str, Any]]:
    raw_rows = await _extract_trades_from_panel(panel, limit=limit)
    log.info("DOM orderHistory rows found: %d", len(raw_rows))

    rows: List[Dict[str, Any]] = []
    rejected = 0

    for r in raw_rows:
        parsed = _normalize_trade_row(r)
        if not parsed:
            rejected += 1
            continue
        price, vol_usdt, tt, side = parsed
        vol_rub = _q8(price * vol_usdt)

        out: Dict[str, Any] = {
            "source": SOURCE,
            "symbol": SYMBOL,
            "price": str(price),
            "volume_usdt": str(vol_usdt),
            "volume_rub": str(vol_rub),
            "trade_time": tt,
        }
        if os.getenv("SEND_SIDE", "0") == "1":
            out["side"] = side

        rows.append(out)

    if rejected:
        log.info("Rejected rows (failed parse/sanity): %s", rejected)

    return rows

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

async def supabase_upsert_trades(rows: List[Dict[str, Any]]) -> bool:
    """
    Возвращает True только если HTTP 200/201/204 (реальный успех записи).
    """
    if not rows:
        return False
    if not SUPABASE_URL or not SUPABASE_KEY or not SUPABASE_TABLE:
        log.warning("Supabase env is not fully set; skipping DB write.")
        return False

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

    # ВАЖНО: Supabase лучше НЕ гонять через твой прокси
    async with httpx.AsyncClient(timeout=35.0, headers=headers, trust_env=False) as client:
        resp = await client.post(url, content=json.dumps(rows, ensure_ascii=False).encode("utf-8"))
        if resp.status_code in (200, 201, 204):
            log.info("Supabase upsert OK: %s trades", len(rows))
            return True
        log.error("Supabase write failed: %s %s", resp.status_code, resp.text[:2000])
        return False

# ───────────────────────── WORKER (PERSISTENT BROWSER + PROXY FALLBACK) ─────────────────────────

class AbcexWorker:
    def __init__(self) -> None:
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self.proxy_url: Optional[str] = None

    def _build_proxy_chain(self) -> List[Optional[str]]:
        http_p = _get_http_proxy_url()
        socks_p = SOCKS_PROXY or None

        if PROXY_MODE == "off":
            return [None]
        if PROXY_MODE == "force":
            chain = []
            if http_p:
                chain.append(http_p)
            if socks_p:
                chain.append(socks_p)
            if not chain:
                # force, но не задано — всё равно офф, иначе бесконечные фейлы
                chain = [None]
            return chain

        # auto (по умолчанию): сперва http/https proxy, потом socks, потом без прокси
        chain = []
        if http_p:
            chain.append(http_p)
        if socks_p and socks_p not in chain:
            chain.append(socks_p)
        chain.append(None)
        return chain

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
        self.page = None
        self.context = None
        self.browser = None

    async def start(self, p, proxy_url: Optional[str]) -> None:
        await self.close()
        self.proxy_url = proxy_url
        _log_proxy(proxy_url, "START")

        kwargs: Dict[str, Any] = {
            "headless": True,
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        }
        if proxy_url:
            kwargs["proxy"] = _parse_proxy_for_playwright(proxy_url)

        self.browser = await p.chromium.launch(**kwargs)

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

        # ускорение: режем тяжёлые ресурсы (но НЕ JS/XHR)
        async def _route_handler(route, request):
            url = request.url.lower()
            if any(url.endswith(ext) for ext in [".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".woff", ".woff2", ".ttf", ".otf"]):
                return await route.abort()
            return await route.continue_()

        await self.context.route("**/*", _route_handler)

        self.page = await self.context.new_page()
        self.page.set_default_timeout(20_000)
        self.page.set_default_navigation_timeout(GOTO_TIMEOUT_MS)

    async def ensure_logged_in_and_on_spot(self) -> None:
        assert self.page and self.context
        log.info("STEP 1: open spot")
        await _goto_stable(self.page, ABCEX_URL)

        log.info("STEP 2: cookies")
        await accept_cookies_if_any(self.page)

        log.info("STEP 3: login_if_needed")
        await login_if_needed(self.page, ABCEX_EMAIL, ABCEX_PASSWORD)

        log.info("STEP 4: reopen spot after login")
        await _goto_stable(self.page, ABCEX_URL)

        log.info("STEP 5: save storage state")
        try:
            await self.context.storage_state(path=STATE_PATH)
            log.info("Saved session state to %s", STATE_PATH)
        except Exception as e:
            log.warning("Could not save storage state: %s", e)

    async def run_cycle(self) -> List[Dict[str, Any]]:
        assert self.page
        await self.ensure_logged_in_and_on_spot()

        log.info("STEP 6: wait UI ready (page or frame)")
        panel, scope = await wait_ui_ready(self.page, timeout_ms=WAIT_UI_TIMEOUT_MS)

        log.info("STEP 7: wait trades ready")
        await wait_trades_ready(panel, scope, timeout_ms=WAIT_TRADES_TIMEOUT_MS)

        trades = await extract_trades(panel, limit=LIMIT)
        log.info("Parsed trades (validated): %d", len(trades))

        for i, t in enumerate(trades[:3]):
            log.info("Sample trade[%d]: %s", i, t)

        return trades

# ───────────────────────── MAIN LOOP ─────────────────────────

async def main() -> None:
    ensure_playwright_browsers(force=False)

    seen: Dict[TradeKey, float] = {}
    seen_ttl = 60 * 30  # 30 минут

    last_success_upsert = time.monotonic()
    consecutive_ui_fails = 0

    proxy_chain = AbcexWorker()._build_proxy_chain()
    proxy_idx = 0

    async with async_playwright() as p:
        worker = AbcexWorker()

        # стартуем с первого варианта прокси
        while True:
            try:
                await worker.start(p, proxy_chain[proxy_idx])
                break
            except Exception as e:
                if _should_force_install(e):
                    ensure_playwright_browsers(force=True)
                    continue
                proxy_idx = (proxy_idx + 1) % len(proxy_chain)
                if proxy_idx == 0:
                    raise

        while True:
            try:
                now_m = time.monotonic()
                if now_m - last_success_upsert >= STALL_RESTART_SECONDS:
                    log.error(
                        "STALL: no successful DB upsert for %.0fs (threshold=%.0fs). Exiting(1) to trigger Render restart.",
                        now_m - last_success_upsert,
                        STALL_RESTART_SECONDS,
                    )
                    raise SystemExit(1)

                trades = await worker.run_cycle()

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
                    ok = await supabase_upsert_trades(fresh)
                    if ok:
                        last_success_upsert = time.monotonic()
                    consecutive_ui_fails = 0
                else:
                    log.info("No new trades after in-memory dedup.")
                    # это НЕ ошибка

            except SystemExit:
                raise

            except Exception as e:
                log.error("Cycle error: %s", e)
                consecutive_ui_fails += 1

                # если UI-ошибки идут подряд — переключаем прокси-режим (proxy -> socks -> off)
                # это как раз твой кейс: с прокси иногда нет нужной вкладки/панели
                if consecutive_ui_fails >= 2:
                    consecutive_ui_fails = 0
                    proxy_idx = (proxy_idx + 1) % len(proxy_chain)
                    log.warning("Switching proxy variant -> %s", proxy_chain[proxy_idx] or "OFF")
                    try:
                        await worker.start(p, proxy_chain[proxy_idx])
                    except Exception as se:
                        log.error("Restart with new proxy failed: %s", se)

                # если проблема в установке браузера — пробуем форс
                if _should_force_install(e):
                    try:
                        ensure_playwright_browsers(force=True)
                    except Exception as ie:
                        log.error("Forced install failed: %s", ie)

                # мягкая попытка восстановить страницу
                try:
                    if worker.page:
                        await worker.page.reload(wait_until="domcontentloaded", timeout=GOTO_TIMEOUT_MS)
                except Exception:
                    pass

                await asyncio.sleep(4.0)

            await asyncio.sleep(POLL_SEC)

if __name__ == "__main__":
    asyncio.run(main())
