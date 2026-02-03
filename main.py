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
from playwright.async_api import async_playwright, Page, Locator, Browser, BrowserContext

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
WAIT_UI_TIMEOUT_MS = int(os.getenv("WAIT_UI_TIMEOUT_MS", "65000"))
WAIT_TRADES_TIMEOUT_MS = int(os.getenv("WAIT_TRADES_TIMEOUT_MS", "45000"))

# sanity for USDT/RUB
PRICE_MIN = Decimal(os.getenv("PRICE_MIN", "50"))
PRICE_MAX = Decimal(os.getenv("PRICE_MAX", "200"))

# Render cache path
BROWSERS_ROOT = os.getenv("PLAYWRIGHT_BROWSERS_PATH", "/opt/render/.cache/ms-playwright")
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = BROWSERS_ROOT

# watchdog: if no successful upsert for too long — exit(1), Render restarts worker
STALL_RESTART_SECONDS = float(os.getenv("STALL_RESTART_SECONDS", "3600"))

# Diagnostics
DIAG_TEXT_CHARS = int(os.getenv("DIAG_TEXT_CHARS", "2500"))

# Proxy mode: auto|force|off
PROXY_MODE = os.getenv("PROXY_MODE", "auto").strip().lower()
SOCKS_PROXY = os.getenv("SOCKS_PROXY", "").strip()

# Optional: if you KNOW abc-ex sometimes blocks your proxy, allow direct fallback even in force-mode
ALLOW_DIRECT_FALLBACK = os.getenv("ALLOW_DIRECT_FALLBACK", "1") == "1"

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
        log.warning("Installing Playwright browsers to %s ... force=%s attempt=%s/%s",
                    BROWSERS_ROOT, force, attempt, max_attempts)
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
            time.sleep(6 * attempt)

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
        log.info("%s: proxy=OFF (forced direct)", tag)
        return
    u = urlsplit(proxy_url)
    log.info("%s: proxy=ON scheme=%s host=%s port=%s user=%s", tag, u.scheme, u.hostname, u.port, u.username or "")

# ───────────────────────── NUMBERS / TIME ─────────────────────────

STRICT_TIME_RE = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d(?::[0-5]\d)?$")

def _q8(x: Decimal) -> Decimal:
    return x.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)

def _to_decimal_num(text: str) -> Decimal:
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

# ───────────────────────── DIAG ─────────────────────────

async def _html_snippet(page: Page) -> str:
    try:
        return await page.evaluate(
            """(maxChars) => (document.documentElement?.outerHTML || document.body?.outerHTML || '').slice(0, maxChars)""",
            DIAG_TEXT_CHARS
        )
    except Exception:
        try:
            t = await page.content()
            return (t or "")[:DIAG_TEXT_CHARS]
        except Exception:
            return ""

async def log_diag(page: Page, tag: str, net_errors: List[str], console_errors: List[str]) -> None:
    snippet = await _html_snippet(page)
    low = (snippet or "").lower()
    hints = []
    for h in ["cloudflare", "captcha", "attention required", "access denied", "verify you are human", "blocked", "bot"]:
        if h in low:
            hints.append(h)

    log.warning("DIAG[%s] url=%s hints=%s", tag, page.url, hints)
    if console_errors:
        log.warning("DIAG[%s] console_errors_tail:\n%s", tag, "\n".join(console_errors[-20:]))
    if net_errors:
        log.warning("DIAG[%s] net_errors_tail:\n%s", tag, "\n".join(net_errors[-20:]))

    if snippet:
        log.warning("DIAG[%s] html_snippet(first %d):\n----\n%s\n----", tag, DIAG_TEXT_CHARS, snippet)

def _looks_like_proxy_error_line(s: str) -> bool:
    s = (s or "").lower()
    return (
        "407" in s
        or "proxy authentication" in s
        or "err_tunnel_connection_failed" in s
        or "tunnel connection failed" in s
        or "proxy" in s and "required" in s
    )

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
        await page.keyboard.press("Enter")

    await page.wait_for_timeout(1_000)

    for _ in range(60):
        if not await _is_login_visible(page):
            log.info("Login successful (password field disappeared).")
            return
        await page.wait_for_timeout(400)

    raise RuntimeError("Логин не прошёл (форма логина всё ещё видна).")

async def _goto_stable(page: Page, url: str) -> None:
    await page.goto(url, wait_until="domcontentloaded", timeout=GOTO_TIMEOUT_MS)
    try:
        await page.wait_for_load_state("networkidle", timeout=20_000)
    except Exception:
        pass
    await page.wait_for_timeout(900)

# ───────────────────────── UI / PANEL SEARCH (PAGE + FRAMES) ─────────────────────────

# IMPORTANT: keep it narrow-ish; wide [id*='orderHistory'] was catching wrong container
PANEL_CANDIDATES = [
    "div[id*='panel-orderHistory']",
    "div[role='tabpanel'][id*='orderHistory']",
    "div[id*='orderHistory'][role='tabpanel']",
]

TRADE_TAB_TEXTS = ["Trades", "Сделки", "Order history", "История", "Последние сделки"]

async def _try_click_trades_anywhere(page: Page) -> bool:
    sels = []
    for t in TRADE_TAB_TEXTS:
        sels += [
            f"[role='tab']:has-text('{t}')",
            f"button[role='tab']:has-text('{t}')",
            f"button:has-text('{t}')",
            f"a:has-text('{t}')",
            f"text={t}",
        ]

    # page
    for sel in sels:
        try:
            el = page.locator(sel).first
            if await el.count() > 0 and await el.is_visible():
                await el.click(timeout=6_000)
                await page.wait_for_timeout(500)
                return True
        except Exception:
            pass

    # frames
    for fr in page.frames:
        if fr == page.main_frame:
            continue
        for sel in sels:
            try:
                el = fr.locator(sel).first
                if await el.count() > 0 and await el.is_visible():
                    await el.click(timeout=6_000)
                    await page.wait_for_timeout(500)
                    return True
            except Exception:
                pass

    return False

async def _panel_score(panel: Locator) -> int:
    """
    Score a candidate panel by how many time-like cells it already contains.
    This avoids selecting a wrong wrapper with id*='orderHistory' but no trades.
    """
    try:
        handle = await panel.element_handle()
        if not handle:
            return 0
        n = await handle.evaluate(
            """(root) => {
              const re = /^(?:[01]\\d|2[0-3]):[0-5]\\d(?::[0-5]\\d)?$/;
              const nodes = Array.from(root.querySelectorAll('p,span,div'));
              const texts = nodes.map(n => (n.textContent||'').replace(/\\u00a0/g,' ').trim());
              let c = 0;
              for (const t of texts) if (re.test(t)) c++;
              return c;
            }"""
        )
        return int(n or 0)
    except Exception:
        return 0

async def _find_best_panel(page: Page) -> Optional[Tuple[Locator, Any, str, int]]:
    best: Optional[Tuple[Locator, Any, str, int]] = None

    async def consider(loc: Locator, scope: Any, sel: str) -> None:
        nonlocal best
        try:
            cnt = await loc.count()
        except Exception:
            return
        for i in range(cnt):
            p = loc.nth(i)
            try:
                if not await p.is_visible():
                    continue
            except Exception:
                continue
            score = await _panel_score(p)
            if best is None or score > best[3]:
                best = (p, scope, sel, score)

    # main page
    for sel in PANEL_CANDIDATES:
        try:
            await consider(page.locator(sel), page, sel)
        except Exception:
            pass

    # frames
    for fr in page.frames:
        if fr == page.main_frame:
            continue
        for sel in PANEL_CANDIDATES:
            try:
                await consider(fr.locator(sel), fr, sel)
            except Exception:
                pass

    return best

async def wait_ui_ready(page: Page, timeout_ms: int) -> Tuple[Locator, Any]:
    start = time.time()
    last_try = 0.0

    while (time.time() - start) * 1000 < timeout_ms:
        best = await _find_best_panel(page)
        if best:
            panel, scope, sel, score = best
            log.info("UI ready: best panel selector=%s score=%d", sel, score)
            return panel, scope

        now = time.time()
        if now - last_try >= 1.5:
            ok = await _try_click_trades_anywhere(page)
            if not ok:
                log.warning("Could not click trades tab (no matching selector).")
            last_try = now

        await page.wait_for_timeout(650)

    raise RuntimeError("UI_not_ready_timeout")

async def wait_trades_ready(panel: Locator, timeout_ms: int) -> None:
    start = time.time()
    last = -1

    while (time.time() - start) * 1000 < timeout_ms:
        try:
            handle = await panel.element_handle()
            if handle:
                n = await handle.evaluate(
                    """(root) => {
                      const re = /^(?:[01]\\d|2[0-3]):[0-5]\\d(?::[0-5]\\d)?$/;
                      const nodes = Array.from(root.querySelectorAll('p,span,div'));
                      const texts = nodes.map(n => (n.textContent||'').replace(/\\u00a0/g,' ').trim());
                      let c = 0;
                      for (const t of texts) if (re.test(t)) c++;
                      return c;
                    }"""
                )
                n = int(n or 0)
                if n != last:
                    log.info("Trades time-cells detected: %s", n)
                    last = n
                if n >= 1:
                    return
        except Exception:
            pass

        await asyncio.sleep(0.8)

    raise RuntimeError("Trades_not_ready_timeout")

# ───────────────────────── PARSING ─────────────────────────

async def _extract_trades_from_panel(panel: Locator, limit: int) -> List[Dict[str, Any]]:
    handle = await panel.element_handle()
    if handle is None:
        raise RuntimeError("Не смог получить element_handle панели orderHistory.")

    raw_rows: List[Dict[str, Any]] = await handle.evaluate(
        """(root, limit) => {
          const isTime = (s) => /^(?:[01]\\d|2[0-3]):[0-5]\\d(?::[0-5]\\d)?$/.test((s||'').trim());
          const isNum  = (s) => /^[0-9][0-9\\s\\u00A0.,]*$/.test((s||'').trim());

          const nodes = Array.from(root.querySelectorAll('p,span,div'));
          const texts = nodes.map(n => (n.textContent || '').replace(/\\u00A0/g,' ').trim());

          const out = [];
          for (let i = 0; i < texts.length; i++) {
            const t = texts[i];
            if (!isTime(t)) continue;

            let qty = null, qtyIdx = -1;
            for (let j = i - 1; j >= 0; j--) {
              if (isNum(texts[j])) { qty = texts[j]; qtyIdx = j; break; }
            }
            if (!qty) continue;

            let price = null;
            for (let j = qtyIdx - 1; j >= 0; j--) {
              if (isNum(texts[j])) { price = texts[j]; break; }
            }
            if (!price) continue;

            out.push({ price_raw: price, qty_raw: qty, time: t });
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

def _normalize_trade_row(r: Dict[str, Any]) -> Optional[Tuple[Decimal, Decimal, str]]:
    price_raw = str(r.get("price_raw", "")).strip()
    qty_raw = str(r.get("qty_raw", "")).strip()
    tt = str(r.get("time", "")).strip()

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

    return price, qty, tt

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
        price, vol_usdt, tt = parsed
        vol_rub = _q8(price * vol_usdt)

        rows.append({
            "source": SOURCE,
            "symbol": SYMBOL,
            "price": str(price),
            "volume_usdt": str(vol_usdt),
            "volume_rub": str(vol_rub),
            "trade_time": tt,
        })

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

    # IMPORTANT: do NOT trust env proxy for Supabase requests
    async with httpx.AsyncClient(timeout=35.0, headers=headers, trust_env=False) as client:
        resp = await client.post(url, content=json.dumps(rows, ensure_ascii=False).encode("utf-8"))
        if resp.status_code in (200, 201, 204):
            log.info("Supabase upsert OK: %s trades", len(rows))
            return True
        log.error("Supabase write failed: %s %s", resp.status_code, resp.text[:2000])
        return False

# ───────────────────────── WORKER ─────────────────────────

class AbcexWorker:
    def __init__(self) -> None:
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self.proxy_url: Optional[str] = None

        self.net_errors: List[str] = []
        self.console_errors: List[str] = []

    def _proxy_chain(self) -> List[Optional[str]]:
        http_p = _get_http_proxy_url()
        socks_p = SOCKS_PROXY or None

        if PROXY_MODE == "off":
            return [None]
        if PROXY_MODE == "force":
            chain: List[Optional[str]] = []
            if http_p:
                chain.append(http_p)
            if socks_p and socks_p not in chain:
                chain.append(socks_p)
            if ALLOW_DIRECT_FALLBACK:
                chain.append(None)
            return chain or [None]

        chain: List[Optional[str]] = []
        if http_p:
            chain.append(http_p)
        if socks_p and socks_p not in chain:
            chain.append(socks_p)
        chain.append(None)  # direct fallback
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
        self.net_errors.clear()
        self.console_errors.clear()

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
        else:
            # CRITICAL: force direct (ignore any env/system proxy), otherwise you'll get 407 even in "OFF"
            kwargs["args"] += [
                "--no-proxy-server",
                "--proxy-server=direct://",
                "--proxy-bypass-list=*",
            ]

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

        # Block heavy assets (but KEEP JS/CSS!)
        async def _route_handler(route, request):
            url = request.url.lower()
            if any(url.endswith(ext) for ext in [".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg",
                                                ".woff", ".woff2", ".ttf", ".otf"]):
                return await route.abort()
            return await route.continue_()
        await self.context.route("**/*", _route_handler)

        self.page = await self.context.new_page()
        self.page.set_default_timeout(20_000)
        self.page.set_default_navigation_timeout(GOTO_TIMEOUT_MS)

        def _is_asset(u: str) -> bool:
            u = (u or "").lower()
            return (".js" in u) or (".css" in u)

        # Collect console errors
        def _on_console(m):
            try:
                if m.type in ("error", "warning"):
                    line = f"{m.type}: {m.text}"
                    self.console_errors.append(line)
            except Exception:
                pass
        self.page.on("console", _on_console)
        self.page.on("pageerror", lambda e: self.console_errors.append(f"pageerror: {e}"))

        async def _on_response(resp):
            try:
                st = resp.status
                u = resp.url
                if st >= 400 and _is_asset(u):
                    self.net_errors.append(f"{st} {u}")
            except Exception:
                pass
        self.page.on("response", _on_response)

        # Initial navigation
        log.info("STEP 0: initial open spot")
        await _goto_stable(self.page, ABCEX_URL)
        await accept_cookies_if_any(self.page)
        await login_if_needed(self.page, ABCEX_EMAIL, ABCEX_PASSWORD)
        await _goto_stable(self.page, ABCEX_URL)

        try:
            await self.context.storage_state(path=STATE_PATH)
            log.info("Saved session state to %s", STATE_PATH)
        except Exception as e:
            log.warning("Could not save storage state: %s", e)

    async def ensure_ui(self) -> Tuple[Locator, Any]:
        assert self.page
        try:
            return await wait_ui_ready(self.page, timeout_ms=WAIT_UI_TIMEOUT_MS)
        except Exception as e:
            # If proxy is broken (407/tunnel), don't waste time on reload; raise to switch proxy immediately
            tail = "\n".join(self.console_errors[-20:])
            if any(_looks_like_proxy_error_line(x) for x in self.console_errors[-20:]) or "407" in tail or "ERR_TUNNEL" in tail:
                await log_diag(self.page, "ui_fail_proxy_like", self.net_errors, self.console_errors)
                raise RuntimeError("UI_failed_due_to_proxy_like_errors") from e

            await log_diag(self.page, "ui_fail_before_reload", self.net_errors, self.console_errors)
            log.warning("UI not ready. Trying reload once... err=%s", e)

            try:
                await self.page.reload(wait_until="domcontentloaded", timeout=GOTO_TIMEOUT_MS)
                try:
                    await self.page.wait_for_load_state("networkidle", timeout=20_000)
                except Exception:
                    pass
                await self.page.wait_for_timeout(1200)
            except Exception:
                pass

            return await wait_ui_ready(self.page, timeout_ms=WAIT_UI_TIMEOUT_MS)

    async def run_cycle(self) -> List[Dict[str, Any]]:
        assert self.page and self.context

        log.info("STEP 1: ensure UI")
        panel, scope = await self.ensure_ui()

        log.info("STEP 2: wait trades ready")
        await wait_trades_ready(panel, timeout_ms=WAIT_TRADES_TIMEOUT_MS)

        trades = await extract_trades(panel, limit=LIMIT)
        log.info("Parsed trades (validated): %d", len(trades))
        for i, t in enumerate(trades[:3]):
            log.info("Sample trade[%d]: %s", i, t)
        return trades

# ───────────────────────── MAIN LOOP ─────────────────────────

async def main() -> None:
    ensure_playwright_browsers(force=False)

    seen: Dict[TradeKey, float] = {}
    seen_ttl = 60 * 30

    last_success_upsert = time.monotonic()

    async with async_playwright() as p:
        worker = AbcexWorker()
        chain = worker._proxy_chain()
        idx = 0

        # Start with first proxy option
        while True:
            try:
                await worker.start(p, chain[idx])
                break
            except Exception as e:
                if _should_force_install(e):
                    ensure_playwright_browsers(force=True)
                    continue
                idx = (idx + 1) % len(chain)
                if idx == 0:
                    raise

        while True:
            try:
                if time.monotonic() - last_success_upsert >= STALL_RESTART_SECONDS:
                    log.error("STALL: no successful DB upsert too long. Exiting(1) for Render restart.")
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
                else:
                    log.info("No new trades after in-memory dedup.")

            except SystemExit:
                raise

            except Exception as e:
                log.error("Cycle error: %s", e)
                if worker.page:
                    await log_diag(worker.page, "cycle_error", worker.net_errors, worker.console_errors)

                # Switch proxy variant and restart context
                idx = (idx + 1) % len(chain)
                log.warning("Switch proxy variant -> %s", chain[idx] or "OFF (direct)")
                try:
                    await worker.start(p, chain[idx])
                except Exception as se:
                    log.error("Restart with new proxy failed: %s", se)

                await asyncio.sleep(4.0)

            await asyncio.sleep(POLL_SEC)

if __name__ == "__main__":
    asyncio.run(main())
