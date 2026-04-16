import asyncio
from playwright.async_api import async_playwright

async def debug():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        print("Navigating to Amazon Bestsellers...")
        await page.goto("https://www.amazon.com/best-sellers-books-Amazon/zgbs/books/", wait_until="load")
        await page.wait_for_timeout(5000)
        
        items = await page.query_selector_all('[data-asin]')
        print(f"Found {len(items)} items")
        
        for i, item in enumerate(items[:10]):
            print(f"\n--- Item {i} ---")
            asin = await item.get_attribute('data-asin')
            print(f"ASIN: {asin}")
            
            # Test selectors
            selectors = [
                '.p13n-sc-untruncated-desktop-title',
                '._cDE_gridItem_truncate-title',
                '.zg-grid-general-faceout .a-size-base',
                '[class*="title"]'
            ]
            
            for sel in selectors:
                el = await item.query_selector(sel)
                if el:
                    txt = await el.inner_text()
                    print(f"  [{sel}]: {txt.strip()}")
                else:
                    print(f"  [{sel}]: NOT FOUND")
            
            img = await item.query_selector("img")
            if img:
                alt = await img.get_attribute("alt")
                print(f"  [img alt]: {alt}")
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(debug())
