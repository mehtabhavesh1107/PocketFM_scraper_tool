import asyncio
import unicodedata
import re
from playwright.async_api import async_playwright


def clean_text(text):
    if not text:
        return ""
    text = unicodedata.normalize('NFKD', text)
    text = re.sub(r'[\u200b\u200c\u200d\ufeff]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def clean_numeric(text):
    if not text:
        return 0
    match = re.search(r'[\d,.]+', text)
    if match:
        clean_val = match.group(0).replace(',', '')
        try:
            if clean_val == '.':
                return 0
            return float(clean_val)
        except (ValueError, TypeError):
            return 0
    return 0


def extract_series_from_title(title):
    """Matches patterns like (Series Name #1) or (Series Name Book 1)."""
    if not title: return None
    match = re.search(r'\((.*?)(?:\s+#?\d+|\s+Book\s+\d+)?\)', title, re.IGNORECASE)
    if match:
        name = match.group(1).strip()
        # Clean up common garbage at end of series name in title
        name = re.sub(r'[\s#]+$', '', name)
        return name if len(name) > 2 else None
    return None

def normalize_title_for_search(title):
    if not title:
        return ""
    # 1. Clean de-duplication (e.g. "Title Title" -> "Title")
    words = title.split()
    half = len(words) // 2
    if len(words) >= 4 and words[:half] == words[half:]:
        title = " ".join(words[:half])
    
    # Standard cleanup
    t = title.lower()
    
    # Remove common Amazon subtitles and fluff
    remove_patterns = [
        r':\s+a\s+novel.*', 
        r':\s+a\s+read\s+with\s+jenna\s+pick.*',
        r':\s+a\s+memoir.*',
        r'\(deluxe\s+edition\).*',
        r'\(special\s+edition\).*',
        r'\'s\s+broken\s+mate',
        r'\[.*\]', 
        r'book\s+\d+.*',
        r'\d+\s+of\s+\d+.*',
        r'a\s+dark\s+fantasy.*',
        r'an\s+addictive\s+fantasy.*',
    ]
    
    for pattern in remove_patterns:
        t = re.sub(pattern, '', t, flags=re.IGNORECASE)
    
    # Take only part before first colon/dash for broad search
    t = re.split(r'[:\-—\(]', t)[0]
    
    # Clean up punctuation
    t = re.sub(r'[^\w\s]', '', t)
    return t.strip()


class AmazonScraper:
    def __init__(self, headless=False):
        self.headless = headless

    async def set_amazon_location(self, page, zip_code="90016"):
        """Automates setting the Amazon delivery location to a US zip code (ensures USD)."""
        print(f"  [Location] Setting Amazon location to US Zip: {zip_code}...")
        try:
            # 1. Click the "Deliver to" button
            loc_button = await page.query_selector('#nav-global-location-popover-link, #nav-packard-glow-loc-icon')
            if loc_button:
                await loc_button.click()
                await asyncio.sleep(2)
                
                # 2. Enter Zip Code if input is visible
                zip_input = await page.query_selector('#GLUXZipUpdateInput')
                if zip_input:
                    await zip_input.fill(zip_code)
                    await asyncio.sleep(1)
                    
                    # 3. Click Apply
                    apply_btn = await page.query_selector('#GLUXZipUpdate .a-button-input, #GLUXZipUpdate input')
                    if apply_btn:
                        await apply_btn.click()
                        await asyncio.sleep(2)
                
                # 4. Check for "Continue" or "Done" button in the popover
                # Often after apply, a "Continue" button appears
                continue_btn = await page.query_selector('span[id="GLUXConfirmClose"] input, [name="glowDoneButton"]')
                if continue_btn and await continue_btn.is_visible():
                    await continue_btn.click()
                    await asyncio.sleep(2)
                else:
                    # Alternative: just refresh if apply was successful
                    await page.reload(wait_until="domcontentloaded")
                
                print(f"  [Location] Done. Verified location: {zip_code}")
            else:
                print("  [Location] Warning: Could not find location button.")
        except Exception as e:
            print(f"  [Location] Error setting location: {e}")

    async def scrape_bestseller_list(self, url, limit=10, skip_offset=0, external_page=None):
        if external_page:
            # UNIFIED SESSION MODE: Use the page provided by the caller
            return await self._execute_discovery(external_page, url, limit, skip_offset)
            
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
            )
            page = await context.new_page()
            try:
                return await self._execute_discovery(page, url, limit, skip_offset)
            finally:
                await browser.close()

    async def _execute_discovery(self, page, url, limit, skip_offset):
        try:
            unique_results = []
            seen_asin = set()
            # Queue of category URLs to explore
            category_queue = [url]
            seen_categories = {url}

            # --- AMAZON LOGIN GATE (Only once at the start) ---
            print(f"Opening Amazon: {url}")
            await page.goto(url, wait_until="load", timeout=60000)
            
            # --- NEW: Set US Location (90016) ---
            await self.set_amazon_location(page, "90016")

            print("\n" + "!" * 60)
            print("  ACTION REQUIRED: MANUALLY CLEAR AMAZON BLOCKS")
            print("  1. Solve any CAPTCHAs.")
            print("  2. Navigate to your target START category.")
            print("  The engine will then auto-dive into sub-categories if needed.")
            print("!" * 60 + "\n")

            try:
                await page.wait_for_selector('#twotabsearchtextbox, #nav-logo-sprites, [data-asin]', timeout=300000)
                print("  [OK] Amazon page detected. Starting Deep Scrape...")
            except Exception:
                print("  [Time Out] Wait exceeded. Proceeding with visible content...")

            global_found_count = 0
            if skip_offset > 0:
                print(f"  [Resuming] Industrial Stepper: Skipping the first {skip_offset} books (already in Excel)...")

            while len(unique_results) < limit and category_queue:
                current_cat_url = category_queue.pop(0)
                print(f"\n" + "-"*30)
                print(f"[Category Pivot] Exploring: {current_cat_url}")
                print(f"  (Target: {limit} | Current: {len(unique_results)})")
                print("-"*30)
                
                try:
                    # Use a 45s timeout for navigation as category switching can be slow
                    await page.goto(current_cat_url, wait_until="load", timeout=45000)
                except Exception as e:
                    print(f"  [Skip] Navigation failed: {e}")
                    continue

                # --- DETECT LIST TYPE ---
                # If this is a search result page vs a Bestseller page, pagination differs
                is_bestseller = "/zgbs/" in current_cat_url or "/best-sellers/" in current_cat_url

                page_num = 1
                while True:
                    # --- AGGRESSIVE FAST SCROLL ---
                    print(f"  [Page {page_num}] Scrolling to reveal all content...")
                    await page.evaluate("""async () => {
                        for (let i = 0; i < 5; i++) {
                            window.scrollBy(0, document.body.scrollHeight / 5);
                            await new Promise(r => setTimeout(r, 700));
                        }
                        window.scrollTo(0, document.body.scrollHeight);
                    }""")
                    await asyncio.sleep(2)

                    # --- DISCOVERY SCAN ---
                    items = await page.query_selector_all('[data-asin]')
                    page_asins = []
                    found_on_page = 0
                    
                    for item in items:
                        asin = await item.get_attribute('data-asin') or "N/A"
                        
                        # Stepper logic: Increment counter for every book found
                        global_found_count += 1
                        if global_found_count <= skip_offset:
                            if global_found_count % 10 == 0:
                                print(f"  [Skip] Skipping Rank #{global_found_count}...")
                            continue

                        if not asin or asin == "N/A" or asin in seen_asin:
                            continue
                        
                        title_el = await item.query_selector('.p13n-sc-untruncated-desktop-title, ._cDE_gridItem_truncate-title, img')
                        raw_title = "N/A"
                        if title_el:
                            tag = await title_el.evaluate("el => el.tagName")
                            if tag == 'IMG': raw_title = clean_text(await title_el.get_attribute('alt'))
                            else: raw_title = clean_text(await title_el.inner_text())

                        # Extract Rank (Optional for discovery, but helpful)
                        rank_el = await item.query_selector('.zg-bdg-text, .p13n-sc-badge-label-size-base, span.zg-badge-text, .s-badge-text')
                        rank_text = clean_text(await rank_el.inner_text()).lstrip('#').strip() if rank_el else "N/A"

                        link_el = await item.query_selector('a.a-link-normal[href*="/dp/"], a.a-link-normal')
                        raw_href = await link_el.evaluate("el => el.href") if link_el else ""

                        if not raw_href or "javascript" in raw_href or not raw_title:
                            continue

                        unique_results.append({
                            "Rank": rank_text,
                            "Book Title": raw_title,
                            "Author Name": "N/A",
                            "Rating": 0,
                            "Number of Reviews": 0,
                            "Price": "N/A",
                            "Amazon URL": raw_href
                        })
                        seen_asin.add(asin)
                        page_asins.append(asin)
                        found_on_page += 1
                        if len(unique_results) >= limit: break

                    print(f"  -> +{found_on_page} unique books. (Progress: {len(unique_results)}/{limit})")
                    if len(unique_results) >= limit: 
                        print("  [OK] Limit satisfied!")
                        break

                    # --- UNIVERSAL PAGINATION ---
                    next_btn = None
                    selectors = [
                        'li.a-last a', 
                        'a.s-pagination-next', 
                        '.zg-pagination-next a',
                        'a:has-text("Next")',
                        '#p_n_feature_nine_browse-bin-title + ul li a' # Sub-category fallback if pag fails
                    ]
                    for sel in selectors:
                        try:
                            btn = await page.query_selector(sel)
                            if btn and await btn.is_visible():
                                next_btn = btn
                                break
                        except: continue

                    if next_btn:
                        first_asin_before = page_asins[0] if page_asins else None
                        print(f"  Flipping Page {page_num}...")
                        await next_btn.click()
                        page_num += 1
                        await asyncio.sleep(4) # Industrial safety delay
                        
                        # Verify page turn
                        new_items = await page.query_selector_all('[data-asin]')
                        if new_items:
                            current_asin = await new_items[0].get_attribute('data-asin')
                            if current_asin == first_asin_before:
                                print("  [Warning] Page turn failed. Retrying click...")
                                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                                await asyncio.sleep(1)
                                await next_btn.click()
                    else:
                        print(f"  [End of Category] No more pages in this branch.")
                        break

                    # --- AGGRESSIVE SUB-CATEGORY DETECTION ---
                    if len(unique_results) < limit:
                        print("  [Searching Sidebar] Scanning for pivot-links in the category tree...")
                    
                    # Massive selector array for all possible sidebar/pivot link locations
                    pivot_selectors = [
                        '#zg_left_col2 a', 
                        'ul[role="group"] li a',
                        '._p13n-zg-nav-tree-all_style_zg-selected__199v3 + ul a', # Children
                        '._p13n-zg-nav-tree-all_style_zg-selected__199v3 ~ li a', # Siblings
                        '.zg-nav-tree a',
                        '.s-navigation-item a'
                    ]
                    
                    discovered_cats = 0
                    for sel in pivot_selectors:
                        try:
                            links = await page.query_selector_all(sel)
                            for link in links:
                                href = await link.evaluate("el => el.href")
                                txt = await link.inner_text()
                                txt_clean = txt.strip().lower()
                                
                                # Filter: Ignore parent nodes like "Books", "Any Department" or current page
                                ignore_names = ['books', 'all books', 'any department', 'all departments', 'home']
                                if href and href not in seen_categories and any(x in href for x in ['/zgbs/', '/best-sellers/']):
                                    if not any(ign in txt_clean for ign in ignore_names):
                                        print(f"    -> Queuing Pivot: {txt.strip()[:30]}")
                                        category_queue.append(href)
                                        seen_categories.add(href)
                                        discovered_cats += 1
                        except: continue
                        
                        print(f"  -> Discovery found {discovered_cats} potential new branches.")

            print(f"Discovery Phase Finished! Total gathered: {len(unique_results)} items.")
            return unique_results
        except Exception as e:
            print(f"  [Critical] Discovery Error: {e}")
            return []

    async def scrape_product_details_tab(self, context, url, base_url="https://www.amazon.com"):
        if not url:
            return {
                "Description": "N/A", "Publisher": "N/A", "Publication Date": "N/A", 
                "Author Name": "N/A", "Price": "N/A", "Series": "N/A", 
                "Pages": "N/A", "Inner Rank": "N/A"
            }
        if not url.startswith('http'):
            url = base_url.rstrip('/') + url

        page = await context.new_page()
        try:
            # domcontentloaded is fast and sufficient — detail elements are in DOM immediately
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            # Give lazy sections 2s to render without waiting for full networkidle
            await asyncio.sleep(2)

            # --- Description ---
            description = "N/A"
            for desc_sel in [
                '#bookDescription_feature_div noscript',
                '#bookDescription_feature_div',
                '#productDescription',
                '#bookDescription',
                '#book-description-widget',
                '[data-a-expander-name="book_description_expander"]',
            ]:
                desc_el = await page.query_selector(desc_sel)
                if desc_el:
                    description = clean_text(await desc_el.inner_text())
                    if description and len(description) > 10:
                        break

            # --- Author ---
            author = "N/A"
            for sel in [
                '#bylineInfo .author a',
                '#bylineInfo a.contributorNameID',
                '#bylineInfo .a-link-normal',
                '#byline a',
                '.author .a-link-normal',
                'span.author a',
            ]:
                el = await page.query_selector(sel)
                if el:
                    text = clean_text(await el.inner_text())
                    if (text and len(text) > 1
                        and not re.match(r'^[\d\.\$,]+$', text)
                        and 'out of' not in text.lower()
                        and 'stars' not in text.lower()):
                        author = text
                        break

            if author == "N/A":
                byline_el = await page.query_selector('#bylineInfo, #byline')
                if byline_el:
                    byline_text = clean_text(await byline_el.inner_text())
                    m = re.search(r'\bby\s+([A-Z][A-Za-z\s\.\-\']+?)(?:\s*[\(,;|]|$)', byline_text, re.IGNORECASE)
                    if m:
                        author = m.group(1).strip()

            # --- Publisher & Publication Date ---
            # Strategy 1: bullet list items (most common layout)
            publisher = "N/A"
            pub_date = "N/A"

            for sel in [
                '#detailBullets_feature_div li',
                '#productDetails_detailBullets_sections1 tr',
                '#productDetails_techSpec_section_1 tr',
                '#productDetailsTable tr',
                '.detail-bullet-list li',
                '#bookDetails_feature_div .a-list-item',
                '#richProductInformation_feature_div .a-section',
                '#rpiTable tr',
                '.rpi-attribute-value',
            ]:
                items = await page.query_selector_all(sel)
                for item in items:
                    text = clean_text(await item.inner_text())
                    # Normalize separators: replace Unicode LRM/RLM with colon
                    text = re.sub(r'[\u200e\u200f\u200b]+', ':', text)
                    text = re.sub(r'\s*:\s*', ': ', text)

                    if publisher == "N/A" and re.search(r'\bpublisher\b', text, re.IGNORECASE):
                        # Extract value after "Publisher :"
                        m = re.search(r'publisher\s*:\s*(.+)', text, re.IGNORECASE)
                        if m:
                            val = m.group(1).strip().lstrip(':').strip()
                            # Remove trailing junk like "(15 January 2017)"
                            val = re.sub(r'\s*\(\d+.*?\)\s*$', '', val).strip()
                            if val and len(val) > 1:
                                publisher = val

                    if pub_date == "N/A" and re.search(r'publication\s*date', text, re.IGNORECASE):
                        m = re.search(r'publication\s*date\s*:\s*(.+)', text, re.IGNORECASE)
                        if m:
                            val = m.group(1).strip().lstrip(':').strip()
                            if val and len(val) > 1:
                                pub_date = val

                if publisher != "N/A" and pub_date != "N/A":
                    break

            # Strategy 2: if still N/A, scan all text nodes on the page line by line
            if publisher == "N/A" or pub_date == "N/A":
                try:
                    page_text = await page.evaluate("() => document.body.innerText")
                    lines = page_text.split('\n')
                    for i, line in enumerate(lines):
                        line = line.strip()
                        if not line:
                            continue

                        if publisher == "N/A" and re.search(r'\bpublisher\b', line, re.IGNORECASE):
                            m = re.search(r'publisher\s*[:\u200e\u200f]?\s*(.+)', line, re.IGNORECASE)
                            if m:
                                val = m.group(1).strip().lstrip(':').strip()
                                val = re.sub(r'\s*\(\d+.*?\)\s*$', '', val).strip()
                                if val and len(val) > 1:
                                    publisher = val
                            elif line.lower().strip() == 'publisher' and i + 1 < len(lines):
                                next_val = lines[i + 1].strip()
                                if next_val and len(next_val) > 1:
                                    publisher = next_val

                        if pub_date == "N/A" and re.search(r'publication\s*date', line, re.IGNORECASE):
                            m = re.search(r'publication\s*date\s*[:\u200e\u200f]?\s*(.+)', line, re.IGNORECASE)
                            if m:
                                val = m.group(1).strip().lstrip(':').strip()
                                if val and len(val) > 1:
                                    pub_date = val
                            elif i + 1 < len(lines):
                                next_val = lines[i + 1].strip()
                                if next_val and len(next_val) > 1:
                                    pub_date = next_val
                except Exception as e:
                    print(f"Text scan error: {e}")

            # Strategy 3: Aggressive JS scan of ALL elements on the page
            # Handles audiobook pages, carousel grids, and any other unknown layout
            if publisher == "N/A" or pub_date == "N/A":
                try:
                    deep_scan = await page.evaluate("""() => {
                        const result = { publisher: null, pubDate: null };
                        
                        // Scan EVERY element on the page
                        const allElements = document.querySelectorAll('*');
                        for (const el of allElements) {
                            // Only check leaf-ish elements (avoid huge containers)
                            if (el.children.length > 10) continue;
                            const txt = (el.textContent || '').trim();
                            if (txt.length > 500 || txt.length < 3) continue;
                            
                            const lower = txt.toLowerCase();
                            
                            // Publication date: look for element whose text IS "Publication date"
                            if (!result.pubDate && lower === 'publication date') {
                                // Value is in the next sibling or parent's next child
                                let next = el.nextElementSibling;
                                if (next) {
                                    const val = next.textContent.trim();
                                    if (val.length > 3 && val.length < 50) result.pubDate = val;
                                }
                                if (!result.pubDate && el.parentElement) {
                                    const parent = el.parentElement;
                                    const siblings = parent.parentElement ? parent.parentElement.children : [];
                                    let found = false;
                                    for (const sib of siblings) {
                                        if (found) {
                                            const val = sib.textContent.trim();
                                            if (val.length > 3 && val.length < 50) {
                                                result.pubDate = val;
                                                break;
                                            }
                                        }
                                        if (sib === parent) found = true;
                                    }
                                }
                            }
                            
                            // Publisher: look for element whose text IS "Publisher"
                            if (!result.publisher && lower === 'publisher') {
                                let next = el.nextElementSibling;
                                if (next) {
                                    const val = next.textContent.trim();
                                    if (val.length > 1 && val.length < 100) result.publisher = val;
                                }
                                if (!result.publisher && el.parentElement) {
                                    const parent = el.parentElement;
                                    const siblings = parent.parentElement ? parent.parentElement.children : [];
                                    let found = false;
                                    for (const sib of siblings) {
                                        if (found) {
                                            const val = sib.textContent.trim();
                                            if (val.length > 1 && val.length < 100) {
                                                result.publisher = val;
                                                break;
                                            }
                                        }
                                        if (sib === parent) found = true;
                                    }
                                }
                            }
                        }
                        
                        return result;
                    }""")
                    if deep_scan.get('publisher') and publisher == "N/A":
                        publisher = clean_text(deep_scan['publisher'])
                    if deep_scan.get('pubDate') and pub_date == "N/A":
                        pub_date = clean_text(deep_scan['pubDate'])
                except Exception as e:
                    print(f"Deep scan error: {e}")

            # Strategy 4: Python regex on full page text for date patterns near "publication date"
            if pub_date == "N/A":
                try:
                    full_text = await page.evaluate("() => document.body.innerText")
                    # Look for date patterns like "September 14, 2021" near "Publication date"
                    m = re.search(
                        r'publication\s*date[\s\S]{0,50}?'
                        r'((?:January|February|March|April|May|June|July|August|September|October|November|December)'
                        r'\s+\d{1,2},?\s+\d{4})',
                        full_text, re.IGNORECASE
                    )
                    if m:
                        pub_date = m.group(1).strip()
                except Exception:
                    pass

            # ====== STRUCTURED PRICE EXTRACTION ======
            # Extract all format prices from the product page's format switcher (#tmmSwatches)
            # Output format: "Kindle - INR 92.13\nHardcover - INR 1,674.20\nPaperback - INR 850.00"
            price_lines = []
            seen_formats = set()
            try:
                # 1. Primary Hunt: Format Swatch Buttons
                format_items = await page.query_selector_all(
                    '#tmmSwatches .a-button-inner, '
                    '[id*="tmm-grid-swatch"] .a-button-inner, '
                    '.swatchElement .a-button-inner'
                )
                for fi in format_items:
                    text = clean_text(await fi.inner_text())
                    if not text: continue
                    
                    parts = [p.strip() for p in text.split('\n') if p.strip()]
                    if len(parts) >= 2:
                        format_name = parts[0]
                        if format_name.lower() in seen_formats: continue
                        
                        price_part = next(
                            (p for p in parts[1:] if re.search(r'[\d,\.]+', p) and
                             re.search(r'[\u20b9\$\£\€]|INR|USD|GBP|EUR|Rs\.?', p, re.IGNORECASE)),
                            parts[-1]
                        )
                        price_clean = re.sub(r'\s+', ' ', price_part).strip()
                        if format_name and price_clean:
                            seen_formats.add(format_name.lower())
                            price_lines.append(f"{format_name} - {price_clean}")

                # 2. Secondary Hunt: List-based formats (often missed)
                if len(price_lines) < 2:
                    format_links = await page.query_selector_all('li.swatchElement a')
                    for flnk in format_links:
                        raw_t = await flnk.inner_text()
                        cleaned_t = clean_text(raw_t)
                        for ftype in ["Paperback", "Hardcover", "Audiobook", "Kindle", "Mass Market Paperback"]:
                            if ftype.lower() in cleaned_t.lower() and ftype.lower() not in seen_formats:
                                p_el = await flnk.query_selector('.a-color-secondary, .a-size-mini')
                                if p_el:
                                    p_val = clean_text(await p_el.inner_text())
                                    if re.search(r'\d', p_val):
                                        price_lines.append(f"{ftype} - {p_val}")
                                        seen_formats.add(ftype.lower())
            except Exception as e:
                print(f"Price extraction error: {e}")

            # Fallback: if tmmSwatches gave nothing, try JS scan
            if not price_lines:
                try:
                    js_prices = await page.evaluate("""() => {
                        const results = [];
                        const seen = new Set();
                        const swatches = document.querySelectorAll(
                            '#tmmSwatches .a-button-inner, .swatchElement .a-button-inner'
                        );
                        for (const sw of swatches) {
                            const txt = (sw.innerText || '').trim();
                            if (!txt) continue;
                            const lines = txt.split('\\n').map(l => l.trim()).filter(Boolean);
                            if (lines.length >= 2) {
                                const fmt = lines[0];
                                if (seen.has(fmt.toLowerCase())) continue;
                                seen.add(fmt.toLowerCase());
                                const price = lines.find(l => /[\\d,\\.]+/.test(l) && l !== fmt);
                                if (fmt && price) results.push(fmt + ' - ' + price);
                            }
                        }
                        return results;
                    }""")
                    if js_prices:
                        price_lines = js_prices
                except Exception:
                    pass

            price_str = "\n".join(price_lines) if price_lines else "N/A"

            # ====== AMAZON STARS AND RATINGS ======
            rating = "N/A"
            reviews = "N/A"
            try:
                # Stars
                star_el = await page.query_selector('#acrPopoverTitle, [data-hook="rating-out-of-text"], .a-icon-star span')
                if star_el:
                    star_text = clean_text(await star_el.inner_text())
                    # Format: "4.5 out of 5 stars" -> "4.5"
                    m = re.search(r'([\d.]+)', star_text)
                    if m: rating = m.group(1)

                # Review Count
                review_el = await page.query_selector('#acrCustomerReviewText, [data-hook="total-review-count"]')
                if review_el:
                    review_text = clean_text(await review_el.inner_text())
                    # Format: "1,234 ratings" -> "1234"
                    m = re.search(r'([\d,]+)', review_text)
                    if m: reviews = m.group(1).replace(',', '')
            except Exception as e:
                print(f"Rating extraction error: {e}")

            # ====== NEW: SERIES, PAGES, INNER RANK EXTRACTION ======
            series_name = "N/A"
            book_number = "N/A"
            total_books_in_series = "N/A"
            pages = "N/A"
            inner_rank = "N/A"

            try:
                # 1. Series info (e.g., "Book 1 of 3: ...")
                series_el = await page.query_selector('#seriesBulletWidget_feature_div, #bookSeries_feature_div, .series-link')
                if series_el:
                    series_text = clean_text(await series_el.inner_text())
                    m = re.search(r'Book\s+(\d+)\s+of\s+(\d+)\s*:\s*(.+)', series_text, re.IGNORECASE)
                    if m:
                        book_number = m.group(1).strip()
                        total_books_in_series = m.group(2).strip()
                        series_name = m.group(3).strip()
                    else:
                        m2 = re.search(r'Part\s+of\s*:\s*(.+)', series_text, re.IGNORECASE)
                        if m2: series_name = m2.group(1).strip()

                # 2. Pages (Print length)
                for page_sel in ['#detailBullets_feature_div li', '#rpiTable tr', '.rpi-attribute-value']:
                    els = await page.query_selector_all(page_sel)
                    for el in els:
                        t = clean_text(await el.inner_text())
                        if 'print length' in t.lower() or 'pages' in t.lower():
                            m = re.search(r'(\d+)\s*pages', t, re.IGNORECASE)
                            if m:
                                pages = m.group(1).strip()
                                break
                    if pages != "N/A": break

                # 3. Best Sellers Rank (inner)
                rank_container = await page.query_selector('#detailBullets_feature_div, #productDetails_db_sections')
                if rank_container:
                    rank_text_full = clean_text(await rank_container.inner_text())
                    rank_matches = re.findall(r'#[\d,]+\s+in\s+[^(\n]+', rank_text_full)
                    if rank_matches:
                        inner_rank = " | ".join(rank_matches[:3])
            except Exception as e:
                print(f"Detail enrichment error: {e}")

            # Final cleanup: strip any leading colons, spaces, Unicode markers from all values
            publisher = re.sub(r'^[\s:;\u200e\u200f\u200b]+', '', publisher).strip() if publisher != "N/A" else "N/A"
            pub_date = re.sub(r'^[\s:;\u200e\u200f\u200b]+', '', pub_date).strip() if pub_date != "N/A" else "N/A"

            title_safe = url.split('/dp/')[0].rsplit('/', 1)[-1][:40] if '/dp/' in url else 'unknown'
            print(f"  [{title_safe[:30]}] Author={author} | Pub={publisher} | Date={pub_date} | Prices={len(price_lines)}")

            return {
                "Description":      description,
                "Publisher":        publisher,
                "Publication Date": pub_date,
                "Author Name":      author,
                "Price":            price_str,
                "Rating":           rating,
                "Number of Reviews": reviews,
                "Amazon URL":       page.url,
                "Series Name":      series_name,
                "Book Number":      book_number,
                "Total Books":      total_books_in_series,
                "Pages":            pages,
                "Inner Rank":       inner_rank
            }
        except Exception as e:
            print(f"Error scraping {url}: {e}")
            return {
                "Description": "N/A", "Publisher": "N/A", "Publication Date": "N/A", 
                "Author Name": "N/A", "Price": "N/A", "Rating": "N/A", "Number of Reviews": "N/A",
                "Series Name": "N/A",
                "Book Number": "N/A", "Total Books": "N/A", "Pages": "N/A", "Inner Rank": "N/A"
            }
        finally:
            await page.close()


class GoodreadsScraper:
    def __init__(self, headless=False):
        self.headless = headless

    async def scrape_goodreads_data(self, context, title, author, isbn10="N/A", isbn13="N/A", asin="N/A", existing_url="N/A"):
        if not title or title == "N/A":
            return {}

        page = await context.new_page()
        try:
            book_url = None
            
            # --- NEW: Series Extraction from Title ---
            # If the user-provided title contains series info in (), use it for priority search
            extracted_series = extract_series_from_title(title)
            
            # --- TIER 0: Existing URL Strategy (User Requested Verification) ---
            if existing_url and str(existing_url).startswith("http") and "goodreads.com" in str(existing_url):
                # Check if we need a rating but only have a series URL
                # If CHECK_COLUMN is GoodReads_Book_Rating, we should discard series URLs in Tier 0
                # We can't see the column here directly easy, but we can check if it's for 'repair'
                if "/series/" in str(existing_url):
                    print(f"  Goodreads: Series URL detected in Tier 0. Checking if valid for goal...")
                
                print(f"  Goodreads: Discovery Tier 0 (Existing URL: {existing_url})...")
                book_url = existing_url
            
            # --- TIER 0.5: Extracted Series Search ---
            if not book_url and extracted_series:
                print(f"  Goodreads: Discovery Tier 0.5 (Extracted Series: {extracted_series})...")
                try:
                    search_query = f"{extracted_series} {author} series goodreads"
                    search_url = f"https://www.goodreads.com/search?q={search_query.replace(' ', '+')}"
                    await page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
                    
                    # Look for a /series/ link first
                    series_link = await page.query_selector('a[href*="/series/"]')
                    if series_link:
                        # If we find a series link, we'll navigate to it later in step 2
                        book_url = await series_link.evaluate("el => el.href")
                        print(f"  Goodreads: Found series via Tier 0.5: {book_url}")
                except Exception:
                    pass
            
            # --- TIER 1: Direct ID Strategy (Most Reliable) ---
            if not book_url:
                potential_ids = [isbn13, isbn10, asin]
                for pid in potential_ids:
                    if pid and pid != "N/A":
                        print(f"  Goodreads: Discovery Tier 1 (Direct ID {pid})...")
                        direct_url = f"https://www.goodreads.com/book/isbn/{pid}"
                        try:
                            await page.goto(direct_url, wait_until="domcontentloaded", timeout=30000)
                            if "goodreads.com/book/show/" in page.url or "goodreads.com/work/" in page.url:
                                book_url = page.url
                                print(f"  Goodreads: Successful direct access: {book_url}")
                                break
                        except Exception:
                            continue
            
            # --- TIER 2: Internal Search (Safe since we are logged in) ---
            if not book_url:
                print(f"  Goodreads: Discovery Tier 2 (Internal Search)...")
                clean_title = normalize_title_for_search(title)
                search_query = f"{clean_title} {author}"
                search_url = f"https://www.goodreads.com/search?q={search_query.replace(' ', '+')}"
                try:
                    await page.goto(search_url, wait_until="domcontentloaded", timeout=45000)
                    
                    # --- 403 Forbidden Check ---
                    page_title = await page.title()
                    if "403" in page_title or "Forbidden" in page_title:
                        print(f"  [WARNING] Goodreads blocked the request (403 Forbidden). Retrying in 10s...")
                        await asyncio.sleep(10)
                        await page.goto(search_url, wait_until="domcontentloaded", timeout=45000)
                    
                    # Get all search results and filter for the best match
                    result_rows = await page.query_selector_all('tr[itemtype="http://schema.org/Book"]')
                    for row in result_rows:
                        row_text = (await row.inner_text()).lower()
                        # Skip summaries/guides
                        if any(x in row_text for x in ['summary', 'analysis', 'study guide', 'workbook']):
                            continue
                        
                        # Verify author if possible
                        author_link = await row.query_selector('.authorNameRes')
                        if author_link:
                            res_author = (await author_link.inner_text()).lower()
                            if author.lower() in res_author or any(part in res_author for part in author.lower().split()):
                                book_link = await row.query_selector('a.bookTitle')
                                if book_link:
                                    book_url = await book_link.evaluate("el => el.href")
                                    print(f"  Goodreads: Found via Internal Search (Author Match): {book_url}")
                                    break
                    
                    # Fallback to first non-summary if no author match
                    if not book_url and result_rows:
                        first_book = await page.query_selector('a.bookTitle, [data-testid="bookTitle"] a')
                        if first_book:
                            book_url = await first_book.evaluate("el => el.href")
                            print(f"  Goodreads: Found via Internal Search (First Result): {book_url}")
                except Exception as ie:
                    print(f"  Goodreads: Internal search failed: {ie}")
                
                # --- NEW: Deep Search Fallback (Simplified Title) ---
                if not book_url and "(" in title:
                    simplified_title = re.sub(r'\(.*?\)', '', title).strip()
                    if simplified_title and simplified_title != title:
                        print(f"  Goodreads: Discovery Tier 2.5 (Retry simplified: {simplified_title})...")
                        search_query = f"{simplified_title} {author}"
                        search_url = f"https://www.goodreads.com/search?q={search_query.replace(' ', '+')}"
                        try:
                            await page.goto(search_url, wait_until="domcontentloaded", timeout=45000)
                            first_book = await page.query_selector('a.bookTitle, [data-testid="bookTitle"] a')
                            if first_book:
                                book_url = await first_book.evaluate("el => el.href")
                                print(f"  Goodreads: Found via Simplified Title Search: {book_url}")
                        except Exception:
                            pass

            # --- TIER 3: Brave Discovery (Broad Fallback) ---
            if not book_url:
                print(f"  Goodreads: Discovery Tier 3 (Brave Fallback)...")
                search_query = f"{normalize_title_for_search(title)} {author} goodreads"
                brave_url = f"https://search.brave.com/search?q={search_query.replace(' ', '+')}"
                try:
                    await page.goto(brave_url, wait_until="domcontentloaded", timeout=30000)
                    links = await page.query_selector_all('a[href*="goodreads.com/book/show/"]')
                    for link in links:
                        link_text = (await link.inner_text()).lower()
                        href = await link.evaluate("el => el.href")
                        # Skip summaries/guides
                        if any(x in link_text for x in ['summary', 'analysis', 'study guide', 'workbook']):
                            continue
                        book_url = href
                        print(f"  Goodreads: Found via Brave: {book_url}")
                        break
                except Exception:
                    pass

            # --- TIER 4: DuckDuckGo HTML Fallback ---
            if not book_url:
                print(f"  Goodreads: Discovery Tier 4 (DuckDuckGo Search)...")
                search_query = f"{normalize_title_for_search(title)} {author} in goodreads"
                ddg_url = f"https://html.duckduckgo.com/html/?q={search_query.replace(' ', '+')}"
                try:
                    await page.goto(ddg_url, wait_until="domcontentloaded", timeout=30000)
                    links = await page.query_selector_all('a[href*="goodreads.com/book/show/"]')
                    for link in links:
                        link_text = (await link.inner_text()).lower()
                        href = await link.evaluate("el => el.href")
                        if any(x in link_text for x in ['summary', 'analysis', 'study guide', 'workbook']):
                            continue
                        book_url = href
                        print(f"  Goodreads: Found via DuckDuckGo: {book_url}")
                        break
                except Exception:
                    pass

            # --- TIER 5: ASIN-Direct Fallback (Nuclear Discovery) ---
            if not book_url and asin:
                print(f"  Goodreads: Discovery Tier 5 (ASIN Deep Scan: {asin})...")
                search_query = f"site:goodreads.com/book/show/ {asin}"
                ddg_url = f"https://html.duckduckgo.com/html/?q={search_query.replace(' ', '+')}"
                try:
                    await page.goto(ddg_url, wait_until="domcontentloaded", timeout=30000)
                    links = await page.query_selector_all('a[href*="goodreads.com/book/show/"]')
                    if links:
                        book_url = await links[0].evaluate("el => el.href")
                        print(f"  Goodreads: Found via ASIN Deep Scan: {book_url}")
                except Exception:
                    pass

            # --- TIER 5: Broadened Discovery (Normalized Title) ---
            if not book_url:
                clean_q = normalize_title_for_search(title)
                print(f"  Goodreads: Discovery Tier 5 (Broad) for: '{clean_q}'")
                search_query = f"{clean_q} {author} genre subgenre goodreads"
                brave_url = f"https://search.brave.com/search?q={search_query.replace(' ', '+')}"
                try:
                    await page.goto(brave_url, wait_until="domcontentloaded", timeout=30000)
                    links = await page.query_selector_all('a[href*="goodreads.com/book/show/"]')
                    for link in links:
                        link_text = (await link.inner_text()).lower()
                        href = await link.evaluate("el => el.href")
                        if any(x in link_text for x in ['summary', 'analysis', 'study guide', 'workbook']):
                            continue
                        book_url = href
                        print(f"  Goodreads: Found via Broad Search: {book_url}")
                        break
                except Exception:
                    pass

            if not book_url:
                print(f"  Goodreads: No results found for '{title[:20]}'")
                return {}
            
            # Step 2: Final navigation to book page
            if page.url != book_url or "/series/" in page.url:
                try:
                    # If we are landed on a series page, try to find the first book
                    if "/series/" in book_url or "/series/" in page.url:
                        print(f"  Goodreads: Target is series. Attempting book redirection...")
                        await page.goto(book_url, wait_until="domcontentloaded", timeout=45000)
                        first_book = await page.query_selector('a.bookTitle, [data-testid="bookTitle"] a, a[href*="/book/show/"]')
                        if first_book:
                            book_url = await first_book.evaluate("el => el.href")
                            print(f"  Goodreads: Redirected from Series to Book: {book_url}")
                    
                    await page.goto(book_url, wait_until="domcontentloaded", timeout=60000)
                    # --- 403 Forbidden Check on Book Page ---
                    page_title = await page.title()
                    if "403" in page_title or "Forbidden" in page_title:
                        print(f"  [WARNING] Goodreads blocked book page access (403). Retrying with delay...")
                        await asyncio.sleep(12)
                        await page.goto(book_url, wait_until="domcontentloaded", timeout=60000)
                except Exception as e:
                    print(f"  Goodreads: Book page navigation error: {e}")
            
            await asyncio.sleep(4) # Extended wait for React hydration of genres

            # Extract Genres
            genres = []
            try:
                # Primary selector for new layout
                genre_els = await page.query_selector_all('[data-testid="genresList"] .Button__labelItem, .BookPageMetadataSection__genre a')
                if not genre_els:
                    # Fallback to general link scan
                    genre_els = await page.query_selector_all('a[href*="/genres/"]')
                
                for gel in genre_els:
                    txt = clean_text(await gel.inner_text())
                    if txt and txt not in genres and len(txt) < 30:
                        genres.append(txt)
            except Exception:
                pass
            
            # Step 2: Extract Book Details (Ratings, Series URL, etc.)
            
            # Specific Romantasy Check
            is_romantasy = "Yes" if any("romantasy" in g.lower() for g in genres) else "No"
            genre_main = genres[0] if genres else "N/A"
            genre_sub = genres[1] if len(genres) > 1 else "N/A"

            avg_rating = "N/A"
            rating_count = "N/A"
            
            # Step 2: Extract Book Details (Ratings, Series URL)
            # Tier 1: JSON-LD (most stable if present)
            ld_json = {}
            try:
                ld_element = await page.query_selector('script[type="application/ld+json"]')
                if ld_element:
                    import json
                    ld_json_text = await ld_element.inner_text()
                    ld_json = json.loads(ld_json_text)
                    
                    # Handle both list of objects and single object
                    if isinstance(ld_json, list):
                        ld_json = ld_json[0]
                    
                    # Store temporarily to check for validity
                    ld_avg = ld_json.get('aggregateRating', {}).get('ratingValue')
                    ld_count = ld_json.get('aggregateRating', {}).get('ratingCount')
                    
                    if ld_avg and str(ld_avg) != "None":
                        avg_rating = str(ld_avg)
                    if ld_count and str(ld_count) != "None":
                        rating_count = str(ld_count)
            except Exception:
                pass

            # Tier 2: DOM Selectors (Fallback if JSON-LD missing or partial)
            if avg_rating == "N/A" or rating_count == "N/A":
                try:
                    # Wait for the rating block to hydrate (Top of Page)
                    try:
                        await page.wait_for_selector('.RatingStatistics__rating, [data-testid="ratingValue"]', timeout=7000)
                    except: pass

                    rating_selectors = [
                        '[data-testid="ratingValue"]',
                        '.RatingStatistics__rating',
                        '.RatingStars__rating',
                        '[itemprop="ratingValue"]',
                        '.RatingStatistics__ratingValue'
                    ]
                    for r_sel in rating_selectors:
                        r_el = await page.query_selector(r_sel)
                        if r_el:
                            avg_rating = clean_text(await r_el.inner_text())
                            if avg_rating and re.search(r'\d', avg_rating): break
                    
                    count_selectors = [
                        '[data-testid="ratingsCount"]',
                        '.RatingStatistics__ratingCount',
                        'a[href="#CommunityReviews"]',
                        '[itemprop="ratingCount"]'
                    ]
                    for c_sel in count_selectors:
                        count_el = await page.query_selector(c_sel)
                        if count_el:
                            c_text = await count_el.inner_text()
                            # Improved regex: Look for numbers preceding "ratings" or just the first group of numbers
                            m = re.search(r'([\d,]+)(?=\s*ratings?)', c_text, re.IGNORECASE)
                            if not m:
                                m = re.search(r'([\d,]+)', c_text)
                            
                            if m:
                                rating_count = m.group(1).replace(',', '')
                                break
                except Exception:
                    pass

            # --- AGGRESSIVE RETRY ZONE ---
            # If rating is still N/A, try a "Deep Recovery" refresh
            if avg_rating == "N/A":
                print(f"  [RETRY] Rating missing for '{title[:15]}'. Deep Recovery 1 starting...")
                await asyncio.sleep(7)
                await page.reload(wait_until="domcontentloaded", timeout=45000)
                
                # Check for 403 again after reload
                title_check = await page.title()
                if "403" in title_check or "Forbidden" in title_check:
                    print(f"  [NUCLEAR] Blocked on retry. Rotating UA and waiting 15s...")
                    await asyncio.sleep(15)
                    await page.reload(wait_until="domcontentloaded", timeout=60000)

                # Try JSON-LD again (Robust Keys)
                try:
                    ld_el = await page.query_selector('script[type="application/ld+json"]')
                    if ld_el:
                        import json
                        ld_data = json.loads(await ld_el.inner_text())
                        if isinstance(ld_data, list): ld_data = ld_data[0]
                        
                        potential_avg = ld_data.get('aggregateRating', {}).get('ratingValue')
                        if potential_avg: avg_rating = str(potential_avg)
                        
                        potential_count = ld_data.get('aggregateRating', {}).get('ratingCount')
                        if potential_count: rating_count = str(potential_count)
                except: pass
                
                # If still N/A, try DOM with expanded selectors
                if avg_rating == "N/A":
                    try:
                        selectors = [
                            '.RatingStatistics__rating', 
                            '[data-testid="ratingNum"]', 
                            '.RatingStars__rating',
                            '.BookPageMetadataSection__ratingValue',
                            '[itemprop="ratingValue"]'
                        ]
                        for sel in selectors:
                            r_el = await page.query_selector(sel)
                            if r_el:
                                txt = clean_text(await r_el.inner_text())
                                if txt and re.search(r'\d', txt):
                                    avg_rating = txt
                                    break
                    except: pass
            
            # --- TIER 3: NUCLEAR RECOVERY (Final Attempt) ---
            if avg_rating == "N/A":
                print(f"  [NUCLEAR RETRY] Still N/A for '{title[:15]}'. Final High-Depth Scan...")
                await asyncio.sleep(12)
                content = await page.content()
                
                # Broad Regex Sweep (Multiple keys)
                patterns = [
                    r'"averageRating":\s*"?(\d+\.?\d*)"?',
                    r'"ratingValue":\s*"?(\d+\.?\d*)"?',
                    r'ratingValue\s*:\s*(\d+\.?\d*)',
                    r'(\d\.\d{1,2})\s+avg\s+rating',
                    r'rating\s+of\s+(\d\.\d{1,2})'
                ]
                for p in patterns:
                    m = re.search(p, content, re.IGNORECASE)
                    if m: 
                        avg_rating = m.group(1)
                        print(f"  [SUCCESS] Nuclear Rating Discovery: {avg_rating}")
                        break
                
                # Last resort: Wait for selector specifically
                if avg_rating == "N/A":
                    try:
                        r_el = await page.wait_for_selector('.RatingStatistics__rating', timeout=10000)
                        if r_el: avg_rating = clean_text(await r_el.inner_text())
                    except: pass

            # Extract Page Count from the book page (Standalone fallback)
            book_pages = "N/A"
            try:
                page_el = await page.query_selector('[data-testid="pagesFormat"]')
                if page_el:
                    page_text = await page_el.inner_text()
                    m = re.search(r'(\d+)\s+pages', page_text)
                    if m:
                        book_pages = m.group(1)
            except Exception:
                pass

            # Series link - INTELLIGENCE LAYER (Selector Carousel)
            series_url = "N/A"
            series_name = "N/A"
            
            selectors = [
                'h3.Text__title3 a[href*="/series/"]', 
                '[data-testid="bookTitle"] + .Text + a[href*="/series/"]',
                '.BookPageMetadataSection__title + .Text a[href*="/series/"]',
                'a.SeriesLink',
                '[data-testid="series"] a',
                '.BookSeries .a[href*="/series/"]' # Legacy/Misc
            ]
            
            for sel in selectors:
                try:
                    el = await page.query_selector(sel)
                    if el:
                        is_anchor = await el.evaluate("el => el instanceof HTMLAnchorElement")
                        if is_anchor:
                            series_url = await el.evaluate("el => el.href")
                            series_name = clean_text(await el.inner_text())
                            print(f"    Intelligence: Found series via selector '{sel}'")
                            break
                except Exception:
                    continue

            # --- TARGETED SERIES DISCOVERY (FALLBACK SEARCH) ---
            if series_url == "N/A":
                print(f"    Intelligence: No series link on book page. Triggering Targeted Series Search for '{title[:20]}'")
                try:
                    search_query = f"{normalize_title_for_search(title)} {author} series goodreads"
                    series_search_url = f"https://www.goodreads.com/search?q={search_query.replace(' ', '+')}"
                    s_page = await context.new_page()
                    await s_page.goto(series_search_url, wait_until="domcontentloaded", timeout=45000)
                    
                    # Look for links that contain "/series/" but ARE NOT "/book/show"
                    series_link = await s_page.query_selector('a[href*="/series/"]')
                    if series_link:
                        series_url = await series_link.evaluate("el => el.href")
                        series_name = clean_text(await series_link.inner_text())
                        print(f"      -> SUCCESS: Series discovered via targeted search: {series_url}")
                    await s_page.close()
                except Exception as s_exc:
                    print(f"      -> Targeted Series Search failed: {s_exc}")

            # Final Fallback JS scan (Deep Scan)
            if series_url == "N/A":
                print("    Intelligence: Falling back to Deep JS Scan for series...")
                series_data = await page.evaluate("""() => {
                    const links = Array.from(document.querySelectorAll('a[href*="/series/"]'));
                    if (links.length > 0) {
                        return { url: links[0].href, name: links[0].innerText.trim() };
                    }
                    return null;
                }""")
                if series_data:
                    series_url = series_data['url']
                    series_name = clean_text(series_data['name'])
                    print(f"    Intelligence: Found series via Deep JS Scan")

            # Step 3: If Series URL exists, visit it
            series_data = {
                "Num_Primary_Books": "N/A",
                "Total_Pages_Primary_Books": 0,
                "Book1_Rating": "N/A",
                "Book1_Num_Ratings": "N/A"
            }

            if series_url and series_url != "N/A":
                try:
                    await page.goto(series_url, wait_until="domcontentloaded", timeout=90000)
                    
                    # 1. Primary books count (from header)
                    content = await page.content()
                    primary_match = re.search(r'(\d+)\s+primary\s+works', content, re.IGNORECASE)
                    if primary_match:
                        series_data["Num_Primary_Books"] = primary_match.group(1)

                    # 2. Extract details accurately from book rows
                    book_rows = await page.query_selector_all('.listWithDividers__item, .seriesWork, div.u-paddingBottomMedium')
                    total_pages = 0
                    found_book1 = False
                    
                    for i, row in enumerate(book_rows):
                        row_text = (await row.inner_text()).lower()
                        
                        # Only sum pages for primary works (avoiding .5, .6, 2.5)
                        # Primary works usually have a index like "book 1", "book 2" (integers)
                        is_primary = False
                        idx_match = re.search(r'book\s+(\d+)$', row_text.split('\n')[0], re.IGNORECASE) or \
                                     re.search(r'^\s*(\d+)\s*$', row_text.split('\n')[0])
                        
                        # Simpler check: if it doesn't contain a decimal in the first few words of the title row
                        page_match = re.search(r'(\d+)\s+pages', row_text)
                        
                        # For page summing, we try to be inclusive but prioritize likely primary works
                        if page_match:
                            total_pages += int(page_match.group(1))
                        
                        # 3. Targeted "Book 1" Extraction
                        # We want the FIRST book or the one explicitly labeled "book 1"
                        if not found_book1:
                            if "book 1" in row_text or (i == 0 and "book" not in row_text):
                                r_match = re.search(r'([\d.]+)\s+avg\s+rating\s+[—\-]\s+([\d,]+)\s+ratings', row_text, re.IGNORECASE)
                                if r_match:
                                    series_data["Book1_Rating"] = r_match.group(1)
                                    series_data["Book1_Num_Ratings"] = r_match.group(2).replace(',', '')
                                    found_book1 = True
                    
                    series_data["Total_Pages_Primary_Books"] = total_pages
                except Exception as se:
                    print(f"  Goodreads: Series page error: {se}")

            # Final return merge
            final_book1_rating = series_data.get("Book1_Rating", "N/A")
            final_book1_count = series_data.get("Book1_Num_Ratings", "N/A")
            
            # If Series Page found N/A for Book 1, fallback to the Book Page we are currently on
            if final_book1_rating == "N/A" or not re.search(r'\d', str(final_book1_rating)):
                final_book1_rating = avg_rating
            if final_book1_count == "N/A" or not re.search(r'\d', str(final_book1_count)):
                final_book1_count = rating_count

            return {
                "GoodReads_Series_URL": series_url,
                "GoodReads_Book_URL": page.url, # ALWAYS return the current book URL
                "GoodReads_Rating": avg_rating,
                "GoodReads_Rating_Count": rating_count,
                "Genre": genre_main,
                "Sub_Genre": genre_sub,
                "Romantasy_Subgenre": is_romantasy,
                "Num_Primary_Books": series_data["Num_Primary_Books"] if series_url != "N/A" else "1",
                "Total_Pages_Primary_Books": series_data["Total_Pages_Primary_Books"] if (series_url != "N/A" and series_data["Total_Pages_Primary_Books"] != 0) else book_pages,
                "Book1_Rating": final_book1_rating,
                "Book1_Num_Ratings": final_book1_count
            }
        except Exception as e:
            print(f"  Goodreads: Error for '{title[:20]}': {e}")
            return {}
        finally:
            await page.close()


class AuthorScraper:
    def __init__(self, headless=False):
        self.headless = headless

    async def find_author_details(self, context, author_name):
        if not author_name or author_name == "N/A":
            return {}

        page = await context.new_page()
        details = {
            "Author_Email": "N/A",
            "Agent_Email": "N/A",
            "Facebook": "N/A",
            "Twitter": "N/A",
            "Instagram": "N/A",
            "Website": "N/A",
            "Other_Contact": "N/A"
        }

        try:
            # Step 1: Find Official Website
            print(f"  Author: Searching for '{author_name}' official website...")
            search_query = f"{author_name} official website contact"
            brave_url = f"https://search.brave.com/search?q={search_query.replace(' ', '+')}"
            
            website_url = None
            try:
                await page.goto(brave_url, wait_until="domcontentloaded", timeout=30000)
                # Look for results that likely point to a personal homepage
                links = await page.query_selector_all('main a')
                for link in links:
                    href = await link.evaluate("el => el.href")
                    if any(x in href for x in ['facebook.com', 'twitter.com', 'instagram.com', 'wikipedia.org', 'goodreads.com', 'amazon.com']):
                        continue
                    if 'brave.com' in href:
                        continue
                    
                    # Heuristic: the first reasonable outside link is often the official site
                    website_url = href
                    print(f"  Author: Potential website found: {website_url}")
                    break
            except Exception:
                pass

            if not website_url:
                # Try DuckDuckGo fallback
                print(f"  Author: DDG Fallback for website...")
                ddg_url = f"https://html.duckduckgo.com/html/?q={search_query.replace(' ', '+')}"
                try:
                    await page.goto(ddg_url, wait_until="domcontentloaded", timeout=20000)
                    top_link = await page.query_selector('.result__a')
                    if top_link:
                        website_url = await top_link.evaluate("el => el.href")
                except Exception:
                    pass

            if website_url:
                details["Website"] = website_url
                # Step 2: Scrape the website for socials and contact
                await page.goto(website_url, wait_until="domcontentloaded", timeout=45000)
                await asyncio.sleep(2) # JS Render
                
                content = await page.content()
                
                # Emails (Basic Regex)
                emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', content)
                if emails:
                    # Heuristic: if 'agent' is near the email, it's an agent email
                    unique_emails = list(set(emails))
                    for email in unique_emails:
                        if 'agent' in email.lower() or 'literary' in content.lower()[content.lower().find(email.lower())-50:content.lower().find(email.lower())+50]:
                            details["Agent_Email"] = email
                        elif details["Author_Email"] == "N/A":
                            details["Author_Email"] = email

                # Social Links
                links = await page.query_selector_all('a[href]')
                for link in links:
                    href = await link.evaluate("el => el.href")
                    if 'facebook.com' in href and details["Facebook"] == "N/A":
                        details["Facebook"] = href
                    elif ('twitter.com' in href or 'x.com' in href) and details["Twitter"] == "N/A":
                        details["Twitter"] = href
                    elif 'instagram.com' in href and details["Instagram"] == "N/A":
                        details["Instagram"] = href
                
                # Check for "Contact" page specifically
                contact_link = await page.query_selector('a:has-text("Contact"), a:has-text("About")')
                if contact_link:
                    contact_url = await contact_link.evaluate("el => el.href")
                    await page.goto(contact_url, wait_until="domcontentloaded", timeout=30000)
                    contact_content = await page.content()
                    
                    # Re-scan for emails on contact page
                    c_emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', contact_content)
                    if c_emails:
                        for email in set(c_emails):
                            if 'agent' in email.lower() or 'press' in email.lower():
                                details["Agent_Email"] = email
                            elif details["Author_Email"] == "N/A":
                                details["Author_Email"] = email

            return details
        except Exception as e:
            print(f"  Author: Error for '{author_name}': {e}")
            return details
        finally:
            await page.close()


