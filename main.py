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
WAIT_TRADES_TIMEOUT_MS = int(os.getenv("WAIT_TRADES_TIMEOUT_MS", "25000"))

TIME_RE = re.compile(r"\b(\d{1,2}:\d{2}:\d{2})\b")

# Render cache path
BROWSERS_ROOT = os.getenv("PLAYWRIGHT_BROWSERS_PATH", "/opt/render/.cache/ms-playwright")
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = BROWSERS_ROOT

# ───────────────────────── LOGGING ─────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
log = logging.getLogger("abcex")

# ───────────────────────── INSTALL (как в твоём примере) ─────────────────────────

def _env_without_proxies() -> Dict[str, str]:
    """
    На playwright install убираем прокси — иначе скачивание с CDN часто рвётся.
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
    Устанавливаем браузеры ПРЯМО как в твоём примере, но через python -m playwright.
    force=True — игнорируем проверки и ставим заново (если падал launch).
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
        log.warning("playwright install STDOUT:\n%s", r.stdout[-4000:])
    if r.stderr:
        log.warning("playwright install STDERR:\n%s", r.stderr[-4000:])

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

# ───────────────────────── UTILS (как у тебя) ─────────────────────────

def _normalize_num(text: str) -> float:
    t = (text or "").strip().replace("\xa0", " ").replace(" ", "")
    if "," in t and "." in t:
        t = t.replace(",", "")
    else:
        t = t.replace(",", ".")
    return float(t)


def _looks_like_time(s: str) -> bool:
    return bool(re.fullmatch(r"\d{2}:\d{2}:\d{2}", (s or "").strip()))


def _extract_time(text: str) -> Optional[str]:
    m = TIME_RE.search((text or "").replace("\xa0", " "))
    if not m:
        return None
    hh, mm, ss = m.group(1).split(":")
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

# ───────────────────────── PAGE ACTIONS ─────────────────────────

async def accept_cookies_if_any(page: Page) -> None:
    candidates = ["Принять", "Согласен", "Я согласен", "Accept", "I agree"]
    for txt in candidates:
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

    candidates = [
        "text=Войти", "text=Login", "text=Sign in",
        "a:has-text('Войти')", "button:has-text('Войти')",
        "button:has-text('Login')", "button:has-text('Sign in')",
    ]
    for sel in candidates:
        try:
            btn = page.locator(sel)
            if await btn.count() > 0 and await btn.first.is_visible():
                await btn.first.click(timeout=7_000)
                await page.wait_for_timeout(600)
                break
        except Exception:
            pass


async def login_if_needed(page: Page, email: str, password: str) -> None:
    await _open_login_modal_if_needed(page)

    if not await _is_login_visible(page):
        log.info("Login not required (already in session).")
        return

    log.info("Login detected. Performing sign-in ...")

    email_candidates = [
        "input[type='email']",
        "input[name='email']",
        "input[name*='mail' i]",
        "input[placeholder*='mail' i]",
        "input[placeholder*='email' i]",
        "input[placeholder*='почта' i]",
        "input[autocomplete='username']",
        "input[type='text'][placeholder*='mail' i]",
        "input[type='text'][placeholder*='email' i]",
        "input[type='text'][name*='login' i]",
    ]
    pw_candidates = [
        "input[type='password']",
        "input[autocomplete='current-password']",
        "input[name*='pass' i]",
    ]

    email_loc = await _find_visible_in_frames(page, email_candidates)
    pw_loc = await _find_visible_in_frames(page, pw_candidates)
    if email_loc is None or pw_loc is None:
        raise RuntimeError("Не смог найти поля email/password.")

    await email_loc.fill(email, timeout=10_000)
    await pw_loc.fill(password, timeout=10_000)

    btn_candidates = [
        "button:has-text('Войти')",
        "button:has-text('Вход')",
        "button:has-text('Sign in')",
        "button:has-text('Login')",
        "button[type='submit']",
    ]
    clicked = False
    btn = await _find_visible_in_frames(page, btn_candidates)
    if btn is not None:
        try:
            await btn.click(timeout=10_000)
            clicked = True
        except Exception:
            pass

    if not clicked:
        try:
            await page.keyboard.press("Enter")
        except Exception:
            pass

    await page.wait_for_timeout(800)

    for _ in range(30):
        if not await _is_login_visible(page):
            log.info("Login successful (password field disappeared).")
            return
        await page.wait_for_timeout(400)

    raise RuntimeError("Логин не прошёл (форма логина всё ещё видна). Возможны 2FA/капча/иной флоу.")

# ───────────────────────── NAV/WAIT ─────────────────────────

async def click_trades_tab_best_effort(page: Page) -> None:
    candidates = ["Сделки", "История", "Order history", "Trades", "Последние сделки"]
    for t in candidates:
        try:
            tab = page.locator(f"[role='tab']:has-text('{t}')")
            if await tab.count() > 0 and await tab.first.is_visible():
                await tab.first.click(timeout=8_000)
                await page.wait_for_timeout(500)
                return
        except Exception:
            continue

    for t in candidates:
        try:
            tab = page.locator(f"text={t}")
            if await tab.count() > 0 and await tab.first.is_visible():
                await tab.first.click(timeout=8_000)
                await page.wait_for_timeout(500)
                return
        except Exception:
            continue


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
                log.info("Trades look visible (time cells detected).")
                return
        except Exception:
            pass
        await page.wait_for_timeout(800)

    raise RuntimeError("Не дождался появления сделок (HH:MM:SS).")

# ───────────────────────── PARSING (твой алгоритм, но корень выбираем умнее) ─────────────────────────

async def get_order_history_panel_soft(page: Page) -> Optional[Locator]:
    """
    Как у тебя, но без падения: если нет panel-orderHistory — вернём None.
    """
    panel = page.locator("div[role='tabpanel'][id*='panel-orderHistory']")
    cnt = await panel.count()
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


async def detect_trades_root_by_pattern(page: Page) -> Optional[Locator]:
    """
    Находит корневой контейнер сделок, используя ТВОЙ же паттерн:
      div, у которого прямые дети p,p,p и третий — HH:MM:SS.
    Дальше выбираем контейнер (parent) с максимальным количеством таких строк.
    """
    await page.evaluate(
        """() => {
          // очистим старую метку, если была
          document.querySelectorAll("[data-abcex-trades-root='1']").forEach(n => n.removeAttribute("data-abcex-trades-root"));

          const isTime = (s) => /^\\d{2}:\\d{2}:\\d{2}$/.test((s||'').trim());
          const isNum  = (s) => /^[0-9][0-9\\s\\u00A0.,]*$/.test((s||'').trim());

          const rows = [];
          const divs = Array.from(document.querySelectorAll("div"));
          for (const g of divs) {
            const ps = Array.from(g.querySelectorAll(":scope > p"));
            if (ps.length < 3) continue;

            const t0 = (ps[0].textContent || "").trim();
            const t1 = (ps[1].textContent || "").trim();
            const t2 = (ps[2].textContent || "").trim();

            if (!isTime(t2)) continue;
            if (!isNum(t0) || !isNum(t1)) continue;

            rows.push(g);
          }

          if (!rows.length) return;

          // считаем “голоса” за контейнеры (parentElement)
          const score = new Map();
          for (const r of rows) {
            const p = r.parentElement;
            if (!p) continue;
            score.set(p, (score.get(p) || 0) + 1);
          }

          let best = null;
          let bestScore = 0;
          for (const [node, sc] of score.entries()) {
            if (sc > bestScore) {
              best = node;
              bestScore = sc;
            }
          }

          if (best && bestScore >= 3) {
            best.setAttribute("data-abcex-trades-root", "1");
          }
        }"""
    )
    root = page.locator("[data-abcex-trades-root='1']")
    if await root.count() > 0 and await root.first.is_visible():
        return root.first
    return None


async def extract_trades_from_root(root: Locator, limit: int) -> List[Dict[str, Any]]:
    """
    Это 1-в-1 твой JS-парсер (p,p,p + time + num), только root может быть не panel-orderHistory.
    """
    handle = await root.element_handle()
    if handle is None:
        return []

    raw_rows: List[Dict[str, Any]] = await handle.evaluate(
        """(root, limit) => {
          const isTime = (s) => /^\\d{2}:\\d{2}:\\d{2}$/.test((s||'').trim());
          const isNum = (s) => /^[0-9][0-9\\s\\u00A0.,]*$/.test((s||'').trim());

          const out = [];
          const grids = Array.from(root.querySelectorAll('div'));

          for (const g of grids) {
            const ps = Array.from(g.querySelectorAll(':scope > p'));
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

            out.push({ price_raw: t0, qty_raw: t1, time: t2, side });
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
            time_txt = str(r.get("time", "")).strip()
            side = r.get("side", None)

            if not price_raw or not qty_raw or not time_txt:
                continue
            if not _looks_like_time(time_txt):
                continue

            price = _normalize_num(price_raw)
            qty = _normalize_num(qty_raw)

            tt = _extract_time(time_txt) or time_txt
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

            # Ключевой момент для Render:
            # после логина сайт иногда уводит на /client/spot (без пары).
            # Поэтому ещё раз жёстко открываем нужную пару.
            await page.goto(ABCEX_URL, wait_until="networkidle", timeout=GOTO_TIMEOUT_MS)
            await page.wait_for_timeout(1200)

            try:
                await context.storage_state(path=STATE_PATH)
                log.info("Saved session state to %s", STATE_PATH)
            except Exception as e:
                log.warning("Could not save storage state: %s", e)

            await click_trades_tab_best_effort(page)
            await wait_trades_visible(page, timeout_ms=WAIT_TRADES_TIMEOUT_MS)

            # 1) сначала пробуем ровно как в локальном (panel-orderHistory)
            panel = await get_order_history_panel_soft(page)
            if panel is not None:
                trades = await extract_trades_from_root(panel, limit=LIMIT)
                if trades:
                    log.info("Parsed %d trades from panel-orderHistory", len(trades))
                    return trades

            # 2) если панели нет/пусто — ищем корень по твоему паттерну
            root = await detect_trades_root_by_pattern(page)
            if root is None:
                raise RuntimeError("Не смог определить корневой контейнер сделок по DOM-паттерну.")

            trades = await extract_trades_from_root(root, limit=LIMIT)
            if not trades:
                raise RuntimeError("Корень сделок найден, но сделки не распарсились (0 rows).")

            log.info("Parsed %d trades from detected root", len(trades))
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
    # 1) браузеры (как в твоём примере)
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
