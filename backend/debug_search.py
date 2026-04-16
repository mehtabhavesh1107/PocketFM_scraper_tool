import asyncio
from playwright.async_api import async_playwright

async def debug_search():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        query = "Project Hail Mary Andy Weir goodreads"
        url = f"https://search.brave.com/search?q={query.replace(' ', '+')}"
        print(f"Navigating to Brave Search: {url}")
        await page.goto(url, wait_until="load")
        await page.wait_for_timeout(5000)
        
        # Save content for review
        content = await page.content()
        with open("brave_debug.html", "w", encoding="utf-8") as f:
            f.write(content)
        
        links = await page.query_selector_all('a')
        print(f"Found {len(links)} total links")
        for link in links[:50]:
            href = await link.get_attribute('href')
            if href and 'goodreads.com' in href:
                print(f"  FOUND GOODREADS LINK: {href}")
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(debug_search())
