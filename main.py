import asyncio
import logging
import sys
import subprocess
from playwright.async_api import async_playwright

def ensure_playwright_browsers():
    # Ensure Playwright required browsers are installed (identical to the first script)
    subprocess.run([sys.executable, "-m", "playwright", "install"], check=True)

async def main():
    # Ensure Playwright browsers are installed before launching
    ensure_playwright_browsers()
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        # Navigate to the offices page
        await page.goto("https://abcex.io/offices/")
        # Click the button/link to view all offices (if required)
        try:
            await page.click("text=Смотреть все офисы")
        except Exception:
            pass  # Ignore if clicking is not needed or fails
        # Wait for the offices list or support content to load
        await page.wait_for_selector("a[href*='articles']", timeout=10000)
        # Collect links to all office-related articles
        article_links = await page.query_selector_all("a[href*='articles']")
        urls = []
        for link in article_links:
            href = await link.get_attribute("href")
            if href:
                urls.append(href)
        # Visit each article page and print its content
        for url in urls:
            await page.goto(url)
            content = await page.inner_text("body")
            print(content)
        # Close the browser context (will be done automatically by context manager)
    # End of scraping logic

if __name__ == "__main__":
    asyncio.run(main())
