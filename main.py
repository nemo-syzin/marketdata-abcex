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
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlsplit

import httpx
from playwright.async_api import async_playwright, Page

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
WAIT_TRADES_TIMEOUT_MS = int(os.getenv("WAIT_TRADES_TIMEOUT_MS", "45000"))

# sanity для USDT/RUB
PRICE_MIN = Decimal(os.getenv("PRICE_MIN", "50"))
PRICE_MAX = Decimal(os.getenv("PRICE_MAX", "200"))

# Диагностика в логах
DIAG = os.getenv("DIAG", "1") == "1"
DIAG_TEXT_CHARS = int(os.getenv("DIAG_TEXT_CHARS", "1400"))
DIAG_MAX_SAMPLES = int(os.getenv("DIAG_MAX_SAMPLES", "6"))

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

    js = r"""
    (maxChars, maxSamples) => {
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

      // Сколько вообще "времен" на root
      const allTexts = (rootText || "").split("\n").map(s => s.trim()).filter(Boolean);
      let timeLines = [];
      for (const line of allTexts) {
        if (!timeRe.test(line)) continue;
        timeLines.push(line);
        if (timeLines.length >= 1000) break;
      }

      // Пробуем найти строки, где есть время + 2 числа (очень похоже на сделки)
      const candidates = [];
      for (const line of allTexts) {
        if (!timeRe.test(line)) continue;
        const nums = (line.match(numRe) || []).map(x => x.replace(/^[^\d]+|[^\d]+$/g, ""));
        if (nums.length >= 2) {
          candidates.push(line);
          if (candidates.length >= maxSamples) break;
        }
      }

      // Теперь более "DOM" подход: ищем элементы, внутри которых есть time + 2 числа
      const elements = [];
      const walker = document.createTreeWalker(root, NodeFilter.SHOW_ELEMENT);
      let n = walker.currentNode;
      let safety = 0;
      while (n && safety < 6000) {
        safety++;
        const el = n;
        const txt = (el.innerText || "").replace(/\u00a0/g, " ").trim();
        if (txt && txt.length < 220 && timeRe.test(txt)) {
          const nums = (txt.match(numRe) || []).map(x => x.replace(/^[^\d]+|[^\d]+$/g, ""));
          if (nums.length >= 2) {
            elements.push(txt);
            if (elements.length >= maxSamples) break;
          }
        }
        n = walker.nextNode();
      }

      // tabpanel summary
      const tabpanels = Array.from(document.querySelectorAll("[role='tabpanel']")).slice(0, 8).map(tp => {
        return {
          hidden: tp.hasAttribute("hidden"),
          data_state: tp.getAttribute("data-state"),
          text: (tp.innerText || "").replace(/\u00a0/g, " ").trim().slice(0, 180)
        }
      });

      return {
        rootTag: root.tagName,
        rootTextLen: rootText.length,
        textSnippet,
        timeLinesCount: timeLines.length,
        candidates,
        elements,
        tabpanels
      };
    }
    """
    try:
        out = await page.evaluate(js, DIAG_TEXT_CHARS, DIAG_MAX_SAMPLES)
        log.warning(
            "DIAG[%s] root=%s rootTextLen=%s timeLines=%s",
            tag, out.get("rootTag"), out.get("rootTextLen"), out.get("timeLinesCount")
        )
        if out.get("tabpanels"):
            for i, tp in enumerate(out["tabpanels"]):
                log.warning(
                    "DIAG[%s] tabpanel[%d] hidden=%s state=%s text=%s",
                    tag, i, tp.get("hidden"), tp.get("data_state"), tp.get("text")
                )

        if out.get("candidates"):
            for i, s in enumerate(out["candidates"]):
                log.warning("DIAG[%s] candidateLine[%d]=%s", tag, i, s)

        if out.get("elements"):
            for i, s in enumerate(out["elements"]):
                log.warning("DIAG[%s] candidateEl[%d]=%s", tag, i, s)

        snippet = out.get("textSnippet") or ""
        if snippet:
            log.warning("DIAG[%s] rootTextSnippet:\n%s", tag, snippet)

    except Exception as e:
        log.warning("DIAG[%s] evaluate failed: %s", tag, e)

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
        await log_diag(page, "login_fields_not_visible")
        raise RuntimeError("Не смог найти/увидеть поля email/password.")

    await email_loc.fill(email, timeout=10_000)
    await pw_loc.fill(password, timeout=10_000)

    submit = page.locator(
        "button[type='submit'], button:has-text('Войти'), button:has-text('Login'), button:has-text('Sign in')"
    ).first
    try:
        if await submit.is_visible():
            await submit.click(timeout=10_000)
            log.info("Clicked submit (frame=%s).", page.url)
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
    candidates = ["Сделки", "Trades", "Order history", "История", "Последние сделки"]
    for t in candidates:
        for sel in [
            f"[role='tab']:has-text('{t}')",
            f"button:has-text('{t}')",
            f"a:has-text('{t}')",
            f"text={t}",
        ]:
            try:
                el = page.locator(sel).first
                if await el.count() > 0 and await el.is_visible():
                    await el.click(timeout=8_000)
                    await page.wait_for_timeout(900)
                    log.info("Clicked trades tab (frame=%s).", page.url)
                    return
            except Exception:
                continue

# ───────────────────────── PARSING ─────────────────────────

async def _extract_trade_lines(page: Page, limit: int) -> List[str]:
    """
    Самый устойчивый подход без знания DOM:
    берём innerText активного контейнера (tabpanel/main/body),
    режем на строки и оставляем строки, где есть время и минимум 2 числа.
    """
    js = r"""
    (limit) => {
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

        // лёгкая защита от “слишком длинных” строк
        const s = line.length > 220 ? line.slice(0, 220) : line;

        if (seen.has(s)) continue;
        seen.add(s);
        out.push(s);
        if (out.length >= limit) break;
      }
      return out;
    }
    """
    return await page.evaluate(js, limit)

def _try_parse_from_line(line: str) -> Optional[Tuple[Decimal, Decimal, str]]:
    """
    Ищем в строке:
      - время
      - два числа (первое/второе)
    Дальше решаем, что из них цена.
    """
    tt = _normalize_time_to_hhmmss(line)
    if not tt or not _is_valid_time(tt):
        return None

    # достаём числа
    raw_nums = re.findall(r"\d[\d\s.,]*\d|\d", line.replace("\xa0", " "))
    if len(raw_nums) < 2:
        return None

    # берём первые два как кандидаты
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

    # Вариант 1: a=price, b=qty
    if _price_ok(a):
        return a, b, tt
    # Вариант 2: b=price, a=qty
    if _price_ok(b):
        return b, a, tt

    # Вариант 3: цена могла попасть как “копейки/пункты”
    # пробуем деления для обоих
    for p_raw, q_raw in ((a, b), (b, a)):
        if p_raw > PRICE_MAX:
            for div in (Decimal("10"), Decimal("100"), Decimal("1000"), Decimal("10000"), Decimal("100000")):
                pp = p_raw / div
                if _price_ok(pp):
                    return _q8(pp), q_raw, tt

    return None

async def extract_trades(page: Page, limit: int) -> List[Dict[str, Any]]:
    lines = await _extract_trade_lines(page, limit=limit)
    log.info("Raw trade lines: %d", len(lines))

    rows: List[Dict[str, Any]] = []
    rejected = 0

    for line in lines:
        parsed = _try_parse_from_line(line)
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
        log.info("Rejected lines (failed parse/sanity): %s", rejected)

    # Важно: если ноль — даём диагностику сразу
    if not rows:
        await log_diag(page, "extract_trades_zero")

    return rows

async def wait_trades_parsable(page: Page, timeout_ms: int, min_rows: int = 5) -> None:
    start = time.time()
    last_n = -1
    while (time.time() - start) * 1000 < timeout_ms:
        try:
            probe = await extract_trades(page, limit=60)
            n = len(probe)
            if n != last_n:
                log.info("Trades parsable probe: %d", n)
                last_n = n
            if n >= min_rows:
                return
        except Exception as e:
            log.warning("probe exception: %s", e)
        await page.wait_for_timeout(900)

    await log_diag(page, "trades_not_parsable")
    raise RuntimeError("Не дождался парсабельных сделок (time + 2 numbers).")

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
        log.error("Supabase write failed: %s %s", resp.status_code, resp.text[:2000])

# ───────────────────────── CORE ─────────────────────────

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
    await page.wait_for_timeout(1_200)

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
        page.set_default_timeout(20_000)
        page.set_default_navigation_timeout(GOTO_TIMEOUT_MS)

        try:
            log.info("Opening ABCEX: %s", ABCEX_URL)
            await _goto_stable(page, ABCEX_URL)

            await accept_cookies_if_any(page)
            await login_if_needed(page, ABCEX_EMAIL, ABCEX_PASSWORD)

            # после логина — вернуться на spot, чтобы не остаться на /login
            await _goto_stable(page, ABCEX_URL)

            try:
                await context.storage_state(path=STATE_PATH)
                log.info("Saved session state to %s", STATE_PATH)
            except Exception as e:
                log.warning("Could not save storage state: %s", e)

            await click_trades_tab_best_effort(page)

            # Если вообще не видим времени/табов — сразу логируем состояние
            await log_diag(page, "after_click_trades_tab")

            await wait_trades_parsable(page, timeout_ms=WAIT_TRADES_TIMEOUT_MS, min_rows=5)

            trades = await extract_trades(page, limit=LIMIT)
            log.info("Parsed trades (validated): %d", len(trades))

            # Для контроля: покажем 3 первых строки
            for i, t in enumerate(trades[:3]):
                log.info("Sample trade[%d]: %s", i, t)

            return trades

        except Exception as e:
            await log_diag(page, "cycle_error")
            raise e

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
