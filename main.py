import asyncio
import logging
import subprocess
import sys
import re

from playwright.async_api import async_playwright


def ensure_playwright_browsers():
    logger = logging.getLogger(__name__)
    logger.info("Ensuring Playwright browsers are installed...")
    try:
        subprocess.run(
            ["playwright", "install", "--with-deps", "chromium", "firefox", "webkit"],
            check=True
        )
    except Exception as e:
        logger.error(f"Failed to install Playwright browsers: {e}")
        sys.exit(1)


async def worker():
    logging.info("Launching browser and navigating to abcex.io...")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        await page.goto("https://abcex.io/", timeout=60000)
        logging.info("Page loaded, waiting for dynamic content...")
        # Wait for the dynamic rates to load
        await page.wait_for_timeout(5000)
        content = await page.content()
        # Extract BTC/USDT and USDT/RUB rates from the page content
        match = re.search(r'BTC/USDT.*?([\d.,]+).*?USDT/RUB.*?([\d.,]+)', content, re.DOTALL)
        if match:
            btc_usdt = match.group(1)
            usdt_rub = match.group(2)
            logging.info(f"BTC/USDT: {btc_usdt}, USDT/RUB: {usdt_rub}")
        else:
            logging.error("Could not find exchange rates on the page.")
        await browser.close()


def main():
    ensure_playwright_browsers()
    asyncio.run(worker())


if __name__ == "__main__":
    main()
