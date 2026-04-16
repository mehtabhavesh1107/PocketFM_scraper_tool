import asyncio
from playwright.async_api import async_playwright

async def debug_genres_v2():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        # Project Hail Mary
        url = "https://www.goodreads.com/book/show/58007522-project-hail-mary"
        print(f"Navigating to Goodreads: {url}")
        await page.goto(url, wait_until="load")
        
        # Scroll to ensure genres are loaded
        await page.evaluate("window.scrollBy(0, 1000)")
        await asyncio.sleep(5) 
        
        # Check all text on page for 'Genre'
        content = await page.content()
        with open("goodreads_book.html", "w", encoding="utf-8") as f:
            f.write(content)
            
        # Try generic selector for links containing /genres/
        links = await page.query_selector_all('a[href*="/genres/"]')
        print(f"Found {len(links)} genre links")
        for link in links:
            txt = await link.inner_text()
            print(f"  Genre Link: {txt}")
            
        await browser.close()

if __name__ == "__main__":
    asyncio.run(debug_genres_v2())
