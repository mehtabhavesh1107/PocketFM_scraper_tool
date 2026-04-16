import asyncio
from playwright.async_api import async_playwright

async def debug_genres():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        url = "https://www.goodreads.com/book/show/58007522-project-hail-mary" # Example
        print(f"Navigating to Goodreads: {url}")
        await page.goto(url, wait_until="domcontentloaded")
        await asyncio.sleep(3) # Wait for React
        
        # Test selectors for Genres
        selectors = [
            '.BookPageMetadataSection__genre',
            '[data-testid="genresList"] .Button__labelItem',
            'a[href*="/genres/"]'
        ]
        
        for sel in selectors:
            elements = await page.query_selector_all(sel)
            texts = [await e.inner_text() for e in elements]
            print(f"Selector '{sel}': {texts[:5]}")
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(debug_genres())
