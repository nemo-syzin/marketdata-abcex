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
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlsplit

import httpx
from playwright.async_api import async_playwright, Page, Frame, Response

# ───────────────────────── CONFIG ─────────────────────────

ABCEX_URL = os.getenv("ABCEX_URL", "https://abcex.io/client/spot/USDTRUB")
STATE_PATH = os.getenv("STATE_PATH", "abcex_state.json")

SOURCE = os.getenv("SOURCE", "abcex")
SYMBOL = os.getenv("SYMBOL", "USDT/RUB")

LIMIT = int(os.getenv("LIMIT", "120"))
POLL_SEC = float(os.getenv("POLL_SEC", "3.0"))

ABCEX_EMAIL = os.getenv("ABCEX_EMAIL", "")
ABCEX_PASSWORD = os.getenv("ABCEX_PASSWORD", "")

GOTO_TIMEOUT_MS = int(os.getenv("GOTO_TIMEOUT_MS", "60000"))
WAIT_TRADES_TIMEOUT_MS = int(os.getenv("WAIT_TRADES_TIMEOUT_MS", "45000"))

PRICE_MIN = Decimal(os.getenv("PRICE_MIN", "50"))
PRICE_MAX = Decimal(os.getenv("PRICE_MAX", "200"))

DIAG = os.getenv("DIAG", "1") == "1"
DIAG_TEXT_CHARS = int(os.getenv("DIAG_TEXT_CHARS", "1400"))
DIAG_MAX_SAMPLES = int(os.getenv("DIAG_MAX_SAMPLES", "6"))

# Render cache path
BROWSERS_ROOT = os.getenv("PLAYWRIGHT_BROWSERS_PATH", "/opt/render/.cache/ms-playwright")
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = BROWSERS_ROOT

# ───────────────────────── LOGGING ─────────────────────────

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
log = logging.getLogger("abcex")

# ───────────────────────── INSTALL ─────────────────────────

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
    return bool(__import__("glob").glob(pattern))

def _chromium_exists() -> bool:
    pattern = os.path.join(BROWSERS_ROOT, "chromium-*", "chrome-linux", "chrome")
    return bool(__import__("glob").glob(pattern))

def ensure_playwright_browsers(force: bool = False) -> None:
    if not force and _chromium_exists() and _headless_shell_exists():
        log.info("Playwright browsers already present.")
        return

    max_attempts = 4
    for attempt in range(1, max_attempts + 1):
        log.warning("Installing Playwright browsers to %s ... force=%s attempt=%s/%s", BROWSERS_ROOT, force, attempt, max_attempts)
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
    return ("Executable doesn't exist" in s) or ("playwright install" in s) or ("chromium_headless_shell" in s) or ("ms-playwright" in s and "doesn't exist" in s)

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

# ───────────────────────── PARSING UTILS ─────────────────────────

TIME_RE = re.compile(r"\b(\d{1,2}:\d{2}(?::\d{2})?)\b")
STRICT_TIME_RE = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d(?::[0-5]\d)?$")

def _q8(x: Decimal) -> Decimal:
    return x.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)

def _to_decimal_num(text: str) -> Decimal:
    t = (text or "").strip().replace("\xa0", " ")
    t = re.sub(r"[^0-9\s\.,]", "", t)
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
        return f"{hh.zfill(2)}:{mm}:00"
    hh, mm, ss = parts
    return f"{hh.zfill(2)}:{mm}:{ss.zfill(2)}"

def _is_valid_time(tt: str) -> bool:
    return bool(tt and STRICT_TIME_RE.match(tt))

def _price_ok(p: Decimal) -> bool:
    return (p >= PRICE_MIN) and (p <= PRICE_MAX)

# ───────────────────────── DIAG (LOG ONLY) ─────────────────────────

_JS_DIAG = r"""
(args) => {
  const maxChars = args.maxChars ?? 1400;
  const maxSamples = args.maxSamples ?? 6;
  const timeRe = /\b(\d{1,2}:\d{2}(?::\d{2})?)\b/;
  const numRe = /(?:^|[^\d])(\d[\d\s.,]*\d|\d)(?:[^\d]|$)/g;

  const pickRoot = () => {
    const roots = [
      document.querySelector("[role='tabpanel'][data-state='active']"),
      document.querySelector("[role='tabpanel']:not([hidden])"),
      document.querySelector("[role='tabpanel']"),
      document.querySelector("main"),
      document.body
    ].filter(Boolean);
    return roots[0] || document.body;
  };

  const root = pickRoot();
  const rootText = (root.innerText || "").replace(/\u00a0/g, " ").trim();
  const textSnippet = rootText.slice(0, maxChars);

  const allLines = rootText.split("\n").map(s => s.trim()).filter(Boolean);

  let timeLinesCount = 0;
  for (const line of allLines) {
    if (timeRe.test(line)) timeLinesCount++;
  }

  const candidates = [];
  for (const line of allLines) {
    if (!timeRe.test(line)) continue;
    const nums = (line.match(numRe) || []).map(x => x.replace(/^[^\d]+|[^\d]+$/g, ""));
    if (nums.length >= 2) {
      candidates.push(line.length > 220 ? line.slice(0, 220) : line);
      if (candidates.length >= maxSamples) break;
    }
  }

  const tabpanels = Array.from(document.querySelectorAll("[role='tabpanel']")).slice(0, 8).map(tp => ({
    hidden: tp.hasAttribute("hidden"),
    data_state: tp.getAttribute("data-state"),
    text: (tp.innerText || "").replace(/\u00a0/g, " ").trim().slice(0, 180)
  }));

  return {
    rootTag: root.tagName,
    rootTextLen: rootText.length,
    timeLinesCount,
    candidates,
    tabpanels,
    textSnippet,
  };
}
"""

async def _diag_on_target(target: Any, tag: str, target_name: str) -> None:
    try:
        out = await target.evaluate(_JS_DIAG, {"maxChars": DIAG_TEXT_CHARS, "maxSamples": DIAG_MAX_SAMPLES})
        log.warning("DIAG[%s] target=%s root=%s rootTextLen=%s timeLines=%s",
                    tag, target_name, out.get("rootTag"), out.get("rootTextLen"), out.get("timeLinesCount"))

        for i, tp in enumerate(out.get("tabpanels") or []):
            log.warning("DIAG[%s] target=%s tabpanel[%d] hidden=%s state=%s text=%s",
                        tag, target_name, i, tp.get("hidden"), tp.get("data_state"), tp.get("text"))

        for i, s in enumerate(out.get("candidates") or []):
            log.warning("DIAG[%s] target=%s candidateLine[%d]=%s", tag, target_name, i, s)

        snippet = (out.get("textSnippet") or "").strip()
        if snippet:
            log.warning("DIAG[%s] target=%s rootTextSnippet:\n%s", tag, target_name, snippet)

    except Exception as e:
        log.warning("DIAG[%s] target=%s evaluate failed: %s", tag, target_name, e)

async def log_diag(page: Page, tag: str) -> None:
    if not DIAG:
        return

    try:
        frames = page.frames
        log.warning("DIAG[%s] url=%s frames=%d", tag, page.url, len(frames))
        for i, fr in enumerate(frames[:10]):
            try:
                log.warning("DIAG[%s] frame[%d] url=%s", tag, i, fr.url)
            except Exception:
                pass
    except Exception as e:
        log.warning("DIAG[%s] frames failed: %s", tag, e)
        frames = []

    # main page
    await _diag_on_target(page, tag, "page")

    # each non-empty frame
    for i, fr in enumerate(frames):
        try:
            if not fr.url or fr.url == "about:blank":
                continue
            await _diag_on_target(fr, tag, f"frame[{i}]")
        except Exception:
            continue

# ───────────────────────── UI ACTIONS ─────────────────────────

async def accept_cookies_if_any(page: Page) -> None:
    for txt in ["Принять", "Согласен", "Я согласен", "Accept", "I agree"]:
        try:
            btn = page.locator(f"text={txt}")
            if await btn.count() > 0 and await btn.first.is_visible():
                log.info("Cookies banner: clicking '%s'...", txt)
                await btn.first.click(timeout=7_000)
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
        "button:has-text('Войти')", "a:has-text('Войти')", "text=Войти",
        "button:has-text('Login')", "a:has-text('Login')", "text=Login",
        "button:has-text('Sign in')", "a:has-text('Sign in')", "text=Sign in",
    ]:
        try:
            el = page.locator(sel).first
            if await el.count() > 0 and await el.is_visible():
                await el.click(timeout=7_000)
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

    email_loc = page.locator("input[type='email'], input[autocomplete='username'], input[name*='mail' i], input[name*='login' i]").first
    pw_loc = page.locator("input[type='password'], input[autocomplete='current-password']").first

    if not await email_loc.is_visible() or not await pw_loc.is_visible():
        await log_diag(page, "login_fields_not_visible")
        raise RuntimeError("Не смог найти/увидеть поля email/password.")

    await email_loc.fill(email, timeout=10_000)
    await pw_loc.fill(password, timeout=10_000)

    submit = page.locator("button[type='submit'], button:has-text('Войти'), button:has-text('Login'), button:has-text('Sign in')").first
    try:
        if await submit.is_visible():
            await submit.click(timeout=10_000)
            log.info("Clicked submit (url=%s).", page.url)
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

    await log_diag(page, "login_failed")
    raise RuntimeError("Логин не прошёл (форма логина всё ещё видна).")

async def click_trades_tab_best_effort(page: Page) -> None:
    candidates = ["Сделки", "Trades", "Order history", "История", "Последние сделки", "Recent trades"]
    for t in candidates:
        for sel in [f"[role='tab']:has-text('{t}')", f"button:has-text('{t}')", f"a:has-text('{t}')", f"text={t}"]:
            try:
                el = page.locator(sel).first
                if await el.count() > 0 and await el.is_visible():
                    await el.click(timeout=10_000)
                    await page.wait_for_timeout(900)
                    log.info("Clicked trades tab by '%s'.", t)
                    return
            except Exception:
                continue

async def _goto_stable(page: Page, url: str) -> None:
    await page.goto(url, wait_until="domcontentloaded", timeout=GOTO_TIMEOUT_MS)
    try:
        await page.wait_for_load_state("networkidle", timeout=20_000)
    except Exception:
        pass
    await page.wait_for_timeout(1_200)

# ───────────────────────── API SNIFFING ─────────────────────────

def _looks_like_trades_json(obj: Any) -> bool:
    # очень мягкая эвристика: массив/поле с массивом, где элементы содержат цену+кол-во/объём и время/ts
    if isinstance(obj, list) and obj:
        sample = obj[0]
        if isinstance(sample, dict):
            keys = {k.lower() for k in sample.keys()}
            return (("price" in keys or "p" in keys) and (("amount" in keys) or ("qty" in keys) or ("volume" in keys) or ("q" in keys))) and (("time" in keys) or ("ts" in keys) or ("timestamp" in keys) or ("t" in keys))
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, list) and v:
                if _looks_like_trades_json(v):
                    return True
    return False

def _extract_trades_from_obj(obj: Any) -> List[Dict[str, Any]]:
    # пытаемся найти “лист сделок” где-нибудь внутри JSON
    def find_list(x: Any) -> Optional[List[dict]]:
        if isinstance(x, list) and x and isinstance(x[0], dict) and _looks_like_trades_json(x):
            return x  # type: ignore
        if isinstance(x, dict):
            for v in x.values():
                res = find_list(v)
                if res is not None:
                    return res
        return None

    lst = find_list(obj)
    if not lst:
        return []

    rows: List[Dict[str, Any]] = []
    for it in lst[:LIMIT]:
        if not isinstance(it, dict):
            continue

        # возможные названия полей
        price_raw = it.get("price", it.get("p"))
        qty_raw = it.get("amount", it.get("qty", it.get("volume", it.get("q"))))
        t_raw = it.get("time", it.get("ts", it.get("timestamp", it.get("t"))))

        if price_raw is None or qty_raw is None or t_raw is None:
            continue

        try:
            price = _q8(_to_decimal_num(str(price_raw)))
            qty = _q8(_to_decimal_num(str(qty_raw)))
            if qty <= 0 or not _price_ok(price):
                continue
        except Exception:
            continue

        # время: если это epoch ms/seconds — нормализуем в HH:MM:SS (локально не критично, нам важно видеть)
        trade_time = None
        try:
            if isinstance(t_raw, (int, float)) or (isinstance(t_raw, str) and t_raw.isdigit()):
                tv = int(t_raw)
                if tv > 10_000_000_000:  # ms
                    tv = tv // 1000
                trade_time = time.strftime("%H:%M:%S", time.gmtime(tv))
            else:
                trade_time = _normalize_time_to_hhmmss(str(t_raw))
        except Exception:
            trade_time = None

        if not trade_time or not _is_valid_time(trade_time):
            # если в JSON нет нормального времени — всё равно покажем как есть
            trade_time = str(t_raw)[:32]

        rows.append({
            "source": SOURCE,
            "symbol": SYMBOL,
            "price": str(price),
            "volume_usdt": str(qty),
            "trade_time": trade_time,
        })

    return rows

@dataclass
class SniffState:
    last_hits: List[Dict[str, Any]]
    hit_url: Optional[str]
    seen_urls: Dict[str, int]

async def install_response_sniffer(page: Page, state: SniffState) -> None:
    async def on_response(resp: Response) -> None:
        try:
            url = resp.url
            ct = (resp.headers.get("content-type") or "").lower()

            # логируем “подозрительные” json эндпоинты
            if "application/json" not in ct and "text/json" not in ct:
                return

            # чтобы не шуметь: ограничим по домену
            if "abcex.io" not in url:
                return

            # считаем частоту
            state.seen_urls[url] = state.seen_urls.get(url, 0) + 1

            # пробуем читать json (бывает большой — тогда скипаем)
            body = await resp.body()
            if not body or len(body) > 2_000_000:
                return

            try:
                obj = json.loads(body.decode("utf-8", errors="ignore"))
            except Exception:
                return

            if not _looks_like_trades_json(obj):
                return

            rows = _extract_trades_from_obj(obj)
            if rows:
                state.last_hits = rows
                state.hit_url = url
                log.warning("API HIT: trades detected from %s | rows=%d", url, len(rows))
                for i, r in enumerate(rows[:3]):
                    log.warning("API SAMPLE[%d]: %s", i, r)

        except Exception:
            return

    page.on("response", on_response)

# ───────────────────────── DOM FALLBACK PARSER ─────────────────────────

async def _extract_trade_lines_from_target(target: Any, limit: int) -> List[str]:
    js = r"""
    (args) => {
      const limit = args.limit ?? 120;
      const timeRe = /\b(\d{1,2}:\d{2}(?::\d{2})?)\b/;
      const numRe = /(?:^|[^\d])(\d[\d\s.,]*\d|\d)(?:[^\d]|$)/g;

      const pickRoot = () => {
        const roots = [
          document.querySelector("[role='tabpanel'][data-state='active']"),
          document.querySelector("[role='tabpanel']:not([hidden])"),
          document.querySelector("[role='tabpanel']"),
          document.querySelector("main"),
          document.body
        ].filter(Boolean);
        return roots[0] || document.body;
      };

      const root = pickRoot();
      const text = (root.innerText || "").replace(/\u00a0/g, " ").trim();
      if (!text) return [];

      const lines = text.split("\n").map(s => s.trim()).filter(Boolean);

      const out = [];
      const seen = new Set();
      for (const line of lines) {
        if (!timeRe.test(line)) continue;
        const nums = (line.match(numRe) || []).map(x => x.replace(/^[^\d]+|[^\d]+$/g, ""));
        if (nums.length < 2) continue;
        const s = line.length > 220 ? line.slice(0, 220) : line;
        if (seen.has(s)) continue;
        seen.add(s);
        out.push(s);
        if (out.length >= limit) break;
      }
      return out;
    }
    """
    return await target.evaluate(js, {"limit": limit})

def _try_parse_from_line(line: str) -> Optional[Tuple[Decimal, Decimal, str]]:
    tt = _normalize_time_to_hhmmss(line)
    if not tt or not _is_valid_time(tt):
        return None

    raw_nums = re.findall(r"\d[\d\s.,]*\d|\d", line.replace("\xa0", " "))
    if len(raw_nums) < 2:
        return None

    a_txt, b_txt = raw_nums[0], raw_nums[1]

    def parse(x: str) -> Optional[Decimal]:
        try:
            v = _q8(_to_decimal_num(x))
            if v <= 0:
                return None
            return v
        except Exception:
            return None

    a = parse(a_txt)
    b = parse(b_txt)
    if not a or not b:
        return None

    if _price_ok(a):
        return a, b, tt
    if _price_ok(b):
        return b, a, tt

    for p_raw, q_raw in ((a, b), (b, a)):
        if p_raw > PRICE_MAX:
            for div in (Decimal("10"), Decimal("100"), Decimal("1000"), Decimal("10000"), Decimal("100000")):
                pp = p_raw / div
                if _price_ok(pp):
                    return _q8(pp), q_raw, tt
    return None

async def extract_trades_dom_anywhere(page: Page, limit: int) -> List[Dict[str, Any]]:
    # пробуем page + все фреймы (вдруг текст есть в blob frame)
    targets: List[Tuple[str, Any]] = [("page", page)]
    for i, fr in enumerate(page.frames):
        if fr.url and fr.url != "about:blank":
            targets.append((f"frame[{i}]", fr))

    all_lines: List[str] = []
    for name, t in targets:
        try:
            lines = await _extract_trade_lines_from_target(t, limit=limit)
            if lines:
                log.info("DOM lines found on %s: %d", name, len(lines))
                all_lines.extend(lines)
        except Exception:
            continue

    # дедуп
    seen = set()
    uniq = []
    for s in all_lines:
        if s in seen:
            continue
        seen.add(s)
        uniq.append(s)

    rows: List[Dict[str, Any]] = []
    for line in uniq[:limit]:
        parsed = _try_parse_from_line(line)
        if not parsed:
            continue
        price, qty, tt = parsed
        rows.append({
            "source": SOURCE,
            "symbol": SYMBOL,
            "price": str(price),
            "volume_usdt": str(qty),
            "trade_time": tt,
        })

    if not rows:
        await log_diag(page, "dom_extract_zero")

    return rows

# ───────────────────────── CORE ─────────────────────────

async def run_once() -> List[Dict[str, Any]]:
    proxy_url = _get_proxy_url()
    pw_proxy = _parse_proxy_for_playwright(proxy_url) if proxy_url else None

    async with async_playwright() as p:
        browser = None
        for attempt in (1, 2, 3):
            try:
                kwargs: Dict[str, Any] = {
                    "headless": True,
                    "args": ["--no-sandbox", "--disable-dev-shm-usage"],
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
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            storage_state=storage_state,
        )

        page = await context.new_page()
        page.set_default_timeout(20_000)
        page.set_default_navigation_timeout(GOTO_TIMEOUT_MS)

        sniff = SniffState(last_hits=[], hit_url=None, seen_urls={})
        await install_response_sniffer(page, sniff)

        try:
            log.info("Opening ABCEX: %s", ABCEX_URL)
            await _goto_stable(page, ABCEX_URL)
            await accept_cookies_if_any(page)
            await login_if_needed(page, ABCEX_EMAIL, ABCEX_PASSWORD)

            # вернёмся на страницу инструмента
            await _goto_stable(page, ABCEX_URL)

            try:
                await context.storage_state(path=STATE_PATH)
                log.info("Saved session state to %s", STATE_PATH)
            except Exception as e:
                log.warning("Could not save storage state: %s", e)

            await click_trades_tab_best_effort(page)

            # подождём немного, чтобы API успело дернуться
            await page.wait_for_timeout(2500)

            await log_diag(page, "after_click_trades_tab")

            # 1) Если уже словили сделки через API — возвращаем их
            if sniff.last_hits:
                log.warning("Using API trades from: %s", sniff.hit_url)
                return sniff.last_hits

            # 2) Если API не словили — попробуем DOM (page + frames)
            dom_rows = await extract_trades_dom_anywhere(page, limit=LIMIT)
            if dom_rows:
                log.warning("Using DOM trades rows=%d", len(dom_rows))
                return dom_rows

            # 3) Последняя попытка: подождать ещё, вдруг API придёт позже
            start = time.time()
            while (time.time() - start) * 1000 < WAIT_TRADES_TIMEOUT_MS:
                if sniff.last_hits:
                    log.warning("Late API HIT. Using API trades from: %s", sniff.hit_url)
                    return sniff.last_hits
                await page.wait_for_timeout(900)

            # В логах покажем топ URL, которые чаще всего дергались (это поможет найти нужный endpoint)
            if sniff.seen_urls:
                top = sorted(sniff.seen_urls.items(), key=lambda x: x[1], reverse=True)[:10]
                log.warning("Top JSON endpoints (count):")
                for u, c in top:
                    log.warning("  %s  (%d)", u, c)

            raise RuntimeError("Не смог получить сделки: ни через API sniff, ни через DOM (включая frames).")

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
    ensure_playwright_browsers(force=False)

    while True:
        try:
            trades = await run_once()
            log.info("Parsed trades: %d", len(trades))
            for i, t in enumerate(trades[:5]):
                log.info("TRADE[%d] %s", i, t)
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
