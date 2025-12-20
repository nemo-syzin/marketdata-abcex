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
import glob

import httpx
from playwright.async_api import async_playwright, Page, Locator

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
WAIT_TRADES_TIMEOUT_MS = int(os.getenv("WAIT_TRADES_TIMEOUT_MS", "30000"))

# Render cache path
BROWSERS_ROOT = os.getenv("PLAYWRIGHT_BROWSERS_PATH", "/opt/render/.cache/ms-playwright")
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = BROWSERS_ROOT

# ───────────────────────── LOGGING ─────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
log = logging.getLogger("abcex")

# ───────────────────────── INSTALL (строго как у тебя) ─────────────────────────

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
    Именно: python -m playwright install chromium chromium-headless-shell
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
        timeout=15 * 60,
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

# ───────────────────────── UTILS ─────────────────────────

TIME_RE = re.compile(r"\b(\d{1,2}:\d{2}(?::\d{2})?)\b")

def _normalize_num(text: str) -> float:
    t = (text or "").strip().replace("\xa0", " ").replace(" ", "")
    if "," in t and "." in t:
        t = t.replace(",", "")
    else:
        t = t.replace(",", ".")
    return float(t)

def _extract_time(text: str) -> Optional[str]:
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
    return f"{hh}:{mm}:{ss}"

def _trade_time_to_iso_moscow(hhmmss: str) -> str:
    now = datetime.utcnow() + timedelta(hours=3)
    h, m, s = [int(x) for x in hhmmss.split(":")]
    dt = now.replace(hour=h, minute=m, second=s, microsecond=0)
    if dt > now + timedelta(seconds=2):
        dt -= timedelta(days=1)
    return dt.isoformat()

async def save_debug(page: Page, tag: str) -> None:
    """
    Сохраняем скрин и html. На Render их можно посмотреть через Shell.
    """
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
                for i in range(min(cnt, 8)):
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
        "text=Войти", "text=Login", "text=Sign in",
        "a:has-text('Войти')", "button:has-text('Войти')",
        "button:has-text('Login')", "button:has-text('Sign in')",
    ]:
        try:
            btn = page.locator(sel)
            if await btn.count() > 0 and await btn.first.is_visible():
                await btn.first.click(timeout=7_000)
                await page.wait_for_timeout(700)
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

    await page.wait_for_timeout(800)
    for _ in range(40):
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
                await page.wait_for_timeout(600)
                return
        except Exception:
            continue
    for t in ["Сделки", "История", "Order history", "Trades", "Последние сделки"]:
        try:
            tab = page.locator(f"text={t}")
            if await tab.count() > 0 and await tab.first.is_visible():
                await tab.first.click(timeout=8_000)
                await page.wait_for_timeout(600)
                return
        except Exception:
            continue

async def wait_trades_visible(page: Page, timeout_ms: int) -> None:
    """
    Ждём появления времени HH:MM или HH:MM:SS где угодно в DOM.
    """
    start = time.time()
    while (time.time() - start) * 1000 < timeout_ms:
        try:
            ok = await page.evaluate(
                """() => {
                    const re = /\\b\\d{1,2}:\\d{2}(?::\\d{2})?\\b/;
                    const els = Array.from(document.querySelectorAll('p,span,div'));
                    return els.some(e => {
                      const t = (e.textContent||'').trim();
                      return t.length <= 16 && re.test(t);
                    });
                }"""
            )
            if ok:
                log.info("Trades look visible (time tokens detected).")
                return
        except Exception:
            pass
        await page.wait_for_timeout(800)

    await save_debug(page, "trades_not_visible")
    raise RuntimeError("Не дождался появления сделок (time tokens).")

# ───────────────────────── PARSING (НЕ привязано к <p>) ─────────────────────────

async def extract_trades_anywhere(page: Page, limit: int) -> List[Dict[str, Any]]:
    """
    Универсальный DOM-парсер:
    - находим элементы с временем HH:MM(:SS)
    - для каждого времени берём ближайший “ряд” (ancestor div),
      и из текстов в DOM-порядке берём 2 числа перед временем (price/qty)
    """
    raw: List[Dict[str, Any]] = await page.evaluate(
        """(limit) => {
          const isTime = (s) => /\\b\\d{1,2}:\\d{2}(?::\\d{2})?\\b/.test((s||'').trim());
          const normTime = (s) => {
            const m = ((s||'').match(/\\b(\\d{1,2}:\\d{2}(?::\\d{2})?)\\b/)||[])[1];
            if (!m) return null;
            const parts = m.split(':');
            if (parts.length === 2) {
              let [hh, mm] = parts;
              if (hh.length === 1) hh = '0'+hh;
              return `${hh}:${mm}:00`;
            }
            let [hh, mm, ss] = parts;
            if (hh.length === 1) hh = '0'+hh;
            return `${hh}:${mm}:${ss}`;
          };

          const isNum = (s) => /^[0-9][0-9\\s\\u00A0.,]*$/.test((s||'').trim());

          // собираем кандидаты "time elements"
          const timeEls = Array.from(document.querySelectorAll('p,span,div'))
            .filter(e => {
              const t = (e.textContent||'').trim();
              return t && t.length <= 16 && isTime(t);
            });

          const rows = [];
          const seen = new Set();

          for (const te of timeEls) {
            // поднимаемся вверх: ищем div-ряд, где есть хотя бы 2 числа + время
            let node = te;
            for (let up = 0; up < 8 && node; up++) {
              const cand = node.closest ? node.closest('div') : null;
              if (!cand) break;
              node = cand.parentElement;

              const texts = [];
              const walker = document.createTreeWalker(cand, NodeFilter.SHOW_ELEMENT, null);
              let cur = walker.currentNode;
              while (cur) {
                const el = cur;
                const tx = (el.textContent||'').trim();
                // берем только короткие "ячейки", чтобы не схватить весь блок
                if (tx && tx.length <= 32) texts.push(tx);
                cur = walker.nextNode();
              }

              const tnorm = normTime(te.textContent||'');
              if (!tnorm) continue;

              // найдём позицию времени и 2 числа перед ним
              let timeIdx = -1;
              for (let i = 0; i < texts.length; i++) {
                if (normTime(texts[i]) === tnorm) { timeIdx = i; break; }
              }
              if (timeIdx < 0) continue;

              // собираем числовые токены до времени
              const nums = [];
              for (let i = 0; i < timeIdx; i++) {
                const s = texts[i].replace(/\\u00A0/g,' ').trim();
                if (isNum(s)) nums.push(s);
              }
              if (nums.length < 2) continue;

              const price_raw = nums[nums.length - 2];
              const qty_raw = nums[nums.length - 1];

              // side по стилю: ищем элемент, чья textContent == price_raw
              let side = null;
              const priceEl = Array.from(cand.querySelectorAll('*')).find(x => (x.textContent||'').trim() === price_raw);
              if (priceEl) {
                const st = (priceEl.getAttribute('style') || '').toLowerCase();
                if (st.includes('green')) side = 'buy';
                if (st.includes('red')) side = 'sell';
              }

              const key = `${tnorm}|${price_raw}|${qty_raw}`;
              if (seen.has(key)) break;
              seen.add(key);

              rows.push({ time: tnorm, price_raw, qty_raw, side });
              break;
            }

            if (rows.length >= limit) break;
          }

          return rows.slice(0, limit);
        }""",
        limit,
    )

    trades: List[Dict[str, Any]] = []
    for r in raw:
        try:
            price_raw = str(r.get("price_raw", "")).strip()
            qty_raw = str(r.get("qty_raw", "")).strip()
            tt = str(r.get("time", "")).strip()
            side = r.get("side", None)

            price = _normalize_num(price_raw)
            qty = _normalize_num(qty_raw)

            trade_time_iso = _trade_time_to_iso_moscow(tt)

            trades.append(
                {
                    "source": SOURCE,
                    "symbol": SYMBOL,
                    "trade_time": trade_time_iso,
                    "price": str(price),
                    "volume_usdt": str(qty),
                    "turnover_rub": str(price * qty),
                    "side": side if side in ("buy", "sell") else None,
                    "price_raw": price_raw,
                    "qty_raw": qty_raw,
                    "raw_time": tt,
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

async def supabase_upsert_trades(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    if not SUPABASE_URL or not SUPABASE_KEY or not SUPABASE_TABLE:
        log.warning("Supabase env is not fully set; skipping DB write.")
        return

    url = SUPABASE_URL.rstrip("/") + f"/rest/v1/{SUPABASE_TABLE}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }

    async with httpx.AsyncClient(timeout=30.0, headers=headers, trust_env=True) as client:
        resp = await client.post(url, content=json.dumps(rows, ensure_ascii=False).encode("utf-8"))
        if resp.status_code in (200, 201, 204):
            log.info("Supabase upsert OK: %s trades", len(rows))
            return
        log.error("Supabase write failed: %s %s", resp.status_code, resp.text[:1200])

# ───────────────────────── CORE ─────────────────────────

async def run_once() -> List[Dict[str, Any]]:
    proxy_url = _get_proxy_url()
    pw_proxy = _parse_proxy_for_playwright(proxy_url) if proxy_url else None

    async with async_playwright() as p:
        browser = None
        for attempt in (1, 2):
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
                    await asyncio.sleep(2.0)
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
            await page.goto(ABCEX_URL, wait_until="networkidle", timeout=GOTO_TIMEOUT_MS)
            await page.wait_for_timeout(1500)

            await accept_cookies_if_any(page)

            if not ABCEX_EMAIL or not ABCEX_PASSWORD:
                raise RuntimeError("ABCEX_EMAIL/ABCEX_PASSWORD must be set in env.")
            await login_if_needed(page, ABCEX_EMAIL, ABCEX_PASSWORD)

            # После логина сайт иногда уводит на /client/spot без пары — фиксируем.
            await page.goto(ABCEX_URL, wait_until="networkidle", timeout=GOTO_TIMEOUT_MS)
            await page.wait_for_timeout(1200)

            try:
                await context.storage_state(path=STATE_PATH)
                log.info("Saved session state to %s", STATE_PATH)
            except Exception as e:
                log.warning("Could not save storage state: %s", e)

            await click_trades_tab_best_effort(page)
            await wait_trades_visible(page, timeout_ms=WAIT_TRADES_TIMEOUT_MS)

            trades = await extract_trades_anywhere(page, limit=LIMIT)

            log.info("Parsed trades: %d", len(trades))
            if len(trades) == 0:
                # Это ключевой момент: если 0, сохраняем артефакты
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
    ensure_playwright_browsers(force=False)

    seen: Dict[TradeKey, float] = {}
    seen_ttl = 60 * 30

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
                ensure_playwright_browsers(force=True)
            await asyncio.sleep(3.0)

        await asyncio.sleep(POLL_SEC)


if __name__ == "__main__":
    asyncio.run(main())
