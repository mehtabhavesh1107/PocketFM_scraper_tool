import asyncio
import re
from playwright.async_api import async_playwright

async def debug_book(url):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        print(f"Checking URL: {url}")
        await page.goto(url, wait_until="domcontentloaded")
        await asyncio.sleep(2)
        
        # Try finding series links
        links = await page.query_selector_all('a[href*="/series/"]')
        print(f"Found {len(links)} series links")
        for i, link in enumerate(links):
            href = await link.get_attribute('href')
            text = await link.inner_text()
            print(f"  [{i}] Text: '{text.strip()}' | Href: {href}")
            
        # Try grabbing book title and author to verify match
        title = await page.query_selector('[data-testid="bookTitle"]')
        author = await page.query_selector('[data-testid="namePageCustomizableCharacterName"], .ContributorLinksList')
        
        if title: print(f"Title on page: {await title.inner_text()}")
        if author: print(f"Author on page: {await author.inner_text()}")
        
        await browser.close()

if __name__ == "__main__":
    urls = [
        "https://www.goodreads.com/book/show/246243719-eternal-is-the-night",
        "https://www.goodreads.com/book/show/195155106-crown-me-dead",
        "https://www.goodreads.com/book/show/203492582-runebreaker"
    ]
    for u in urls:
        asyncio.run(debug_book(u))
