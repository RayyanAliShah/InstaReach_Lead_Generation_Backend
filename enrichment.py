import re
import asyncio
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from urllib.parse import urljoin, urlparse

# Keywords to find relevant pages
PRIORITY_KEYWORDS = ["contact", "about", "team", "staff", "attorney", "people", "our-firm"]

async def extract_socials_and_email(base_url):
    data = {
        "email": None,
        "instagram": None,
        "facebook": None,
        "linkedin": None,
        "twitter": None
    }
    
    if not base_url:
        return data

    if not base_url.startswith("http"):
        base_url = "https://" + base_url

    print(f"--- [Stealth Scan] Browsing: {base_url} ---")

    try:
        async with async_playwright() as p:
            # STEALTH ARGUMENTS: These hide the fact that we are a bot
            browser = await p.chromium.launch(
                headless=True, # Keep visible so you can debug
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-infobars"
                ]
            )
            
            # Use a real user agent to look like a standard Windows laptop
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                viewport={"width": 1366, "height": 768},
                locale="en-US",
                timezone_id="Europe/London"
            )
            
            # Add extra stealth scripts
            await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            page = await context.new_page()
            
            # 1. Visit Homepage
            try:
                # 'networkidle' waits until all animations/loading is done
                await page.goto(base_url, timeout=15000, wait_until="domcontentloaded")
                
                # Scroll down to trigger lazy-loaded footers (common place for emails)
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(1) 
                
                content = await page.content()
                data = parse_html(content, data)
                
                if data["email"]:
                    print(f"  [✓] Email found on Home: {data['email']}")
                    await browser.close()
                    return data
                    
            except Exception as e:
                print(f"  [x] Failed to load home: {e}")

            # 2. Deep Scraping (Check up to 2 pages)
            if not data["email"]:
                links = await page.query_selector_all("a")
                links_to_visit = []
                
                for link in links:
                    try:
                        href = await link.get_attribute("href")
                        if href and any(k in href.lower() for k in PRIORITY_KEYWORDS):
                            full_url = urljoin(base_url, href)
                            # Only internal links, avoid duplicates
                            if urlparse(full_url).netloc == urlparse(base_url).netloc and full_url not in links_to_visit:
                                links_to_visit.append(full_url)
                    except:
                        continue
                
                # Visit top 2 promising links
                for deep_link in links_to_visit[:2]:
                    print(f"  [>] Clicking deep link: {deep_link}")
                    try:
                        await page.goto(deep_link, timeout=15000, wait_until="domcontentloaded")
                        content = await page.content()
                        data = parse_html(content, data)
                        if data["email"]:
                            print(f"  [✓] Email found on Deep Page: {data['email']}")
                            break # Stop if found
                    except Exception as e:
                        print(f"  [x] Failed deep link: {e}")

            await browser.close()

    except Exception as e:
        print(f"  [!!!] Browser Error: {e}")
            
    return data

def parse_html(html_content, current_data):
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # 1. Email Extraction
    if not current_data["email"]:
        # Mailto links
        mailto = soup.select_one('a[href^="mailto:"]')
        if mailto:
            href = mailto.get('href', '')
            current_data["email"] = href.replace('mailto:', '').split('?')[0]
        
        # Text Search
        if not current_data["email"]:
            # Pattern matches standard emails
            emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', html_content)
            
            clean_emails = []
            for e in emails:
                e_lower = e.lower()
                # Filter junk
                if e_lower.endswith(('.png', '.jpg', '.gif', '.css', '.js', '.webp', '.svg')): continue
                if "example.com" in e_lower or "sentry.io" in e_lower or "wixpress" in e_lower: continue
                clean_emails.append(e)
            
            if clean_emails:
                # Prefer "info" or "contact" over random names
                priority = [e for e in clean_emails if any(x in e for x in ['info', 'contact', 'hello', 'office'])]
                current_data["email"] = priority[0] if priority else clean_emails[0]

    # 2. Social Extraction
    for link in soup.find_all('a', href=True):
        href = link['href']
        if not current_data["instagram"] and "instagram.com" in href: current_data["instagram"] = href
        elif not current_data["facebook"] and "facebook.com" in href: current_data["facebook"] = href
        elif not current_data["linkedin"] and "linkedin.com" in href: current_data["linkedin"] = href
        elif "twitter.com" in href: current_data["twitter"] = href
            
    return current_data