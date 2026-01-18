# ============================================================================
# Importing Required Libraries
# ============================================================================

import asyncio
import aiohttp
import requests
import json
import csv
import re
import base64
import time
import argparse
from pathlib import Path
from bs4 import BeautifulSoup
from tqdm import tqdm
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass, field, asdict
from urllib.parse import urljoin, quote


# ============================================================================
# Configuration
# ============================================================================

ALGOLIA_APP_ID = "45BWZJ1SGC"
ALGOLIA_API_KEY = "MjBjYjRiMzY0NzdhZWY0NjExY2NhZjYxMGIxYjc2MTAwNWFkNTkwNTc4NjgxYjU0YzFhYTY2ZGQ5OGY5NDMxZnJlc3RyaWN0SW5kaWNlcz0lNUIlMjJZQ0NvbXBhbnlfcHJvZHVjdGlvbiUyMiUyQyUyMllDQ29tcGFueV9CeV9MYXVuY2hfRGF0ZV9wcm9kdWN0aW9uJTIyJTVEJnRhZ0ZpbHRlcnM9JTVCJTIyeWNkY19wdWJsaWMlMjIlNUQmYW5hbHl0aWNzVGFncz0lNUIlMjJ5Y2RjJTIyJTVE"
ALGOLIA_INDEX = "YCCompany_production"
ALGOLIA_URL = f"https://{ALGOLIA_APP_ID}-dsn.algolia.net/1/indexes/{ALGOLIA_INDEX}/query"

YC_BASE_URL = "https://www.ycombinator.com"
COMPANIES_URL = f"{YC_BASE_URL}/companies"

# Rate limiting
REQUESTS_PER_SECOND = 5
CONCURRENT_REQUESTS = 10
REQUEST_DELAY = 1.0 / REQUESTS_PER_SECOND

# Cache file
CACHE_FILE = "yc_scraper_cache.json"
OUTPUT_FILE = "yc_startups.csv"


# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class Founder:
    """Represents a startup founder"""
    name: str
    linkedin_url: Optional[str] = None


@dataclass
class Company:
    """Represents a YC startup company"""
    name: str
    slug: str
    batch: str
    short_description: str
    founders: List[Founder] = field(default_factory=list)
    
    @property
    def founder_names(self) -> str:
        """Comma-separated list of founder names"""
        return ", ".join(f.name for f in self.founders)
    
    @property
    def founder_linkedin_urls(self) -> str:
        """Comma-separated list of founder LinkedIn URLs"""
        urls = [f.linkedin_url for f in self.founders if f.linkedin_url]
        return ", ".join(urls)


# ============================================================================
# Algolia Client - Fetches company listings
# ============================================================================

class AlgoliaClient:
    """Client for fetching YC companies from Algolia search API"""
    
    def __init__(self):
        self.session = requests.Session()
        self.headers = {
            "x-algolia-application-id": ALGOLIA_APP_ID,
            "x-algolia-api-key": ALGOLIA_API_KEY,
            "Content-Type": "application/json",
        }
    
    def fetch_companies(self, limit: int = 500, hits_per_page: int = 100) -> List[Company]:
        """
        Fetch companies from Algolia API.
        
        Args:
            limit: Maximum number of companies to fetch
            hits_per_page: Number of results per API request
            
        Returns:
            List of Company objects with basic info (no founders yet)
        """
        companies = []
        page = 0
        total_pages = (limit + hits_per_page - 1) // hits_per_page
        
        print(f"[*] Fetching {limit} companies from Algolia API...")
        
        with tqdm(total=limit, desc="Fetching listings") as pbar:
            while len(companies) < limit:
                payload = {
                    "query": "",
                    "page": page,
                    "hitsPerPage": hits_per_page,
                    "tagFilters": ["ycdc_public"],
                }
                
                try:
                    response = self.session.post(
                        ALGOLIA_URL,
                        headers=self.headers,
                        json=payload,
                        timeout=30
                    )
                    response.raise_for_status()
                    data = response.json()
                    
                    hits = data.get("hits", [])
                    if not hits:
                        print(f"\n[!] No more companies found at page {page}")
                        break
                    
                    for hit in hits:
                        if len(companies) >= limit:
                            break
                        
                        company = Company(
                            name=hit.get("name", "Unknown"),
                            slug=hit.get("slug", ""),
                            batch=hit.get("batch", "Unknown"),
                            short_description=hit.get("one_liner", ""),
                        )
                        companies.append(company)
                        pbar.update(1)
                    
                    page += 1
                    time.sleep(REQUEST_DELAY)  # Rate limiting
                    
                except requests.RequestException as e:
                    print(f"\n[ERROR] Error fetching page {page}: {e}")
                    break
        
        print(f"[OK] Fetched {len(companies)} companies from Algolia")
        return companies


# ============================================================================
# Company Detail Scraper - Gets founder info from company pages
# ============================================================================

class CompanyDetailScraper:
    """Async scraper for company detail pages to extract founder information"""
    
    def __init__(self):
        self.semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    
    async def fetch_page(self, session: aiohttp.ClientSession, url: str) -> Optional[str]:
        """Fetch a single page with rate limiting and error handling"""
        async with self.semaphore:
            try:
                await asyncio.sleep(REQUEST_DELAY)
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    if response.status == 200:
                        return await response.text()
                    else:
                        print(f"\n[!] HTTP {response.status} for {url}")
                        return None
            except Exception as e:
                print(f"\n[ERROR] Error fetching {url}: {e}")
                return None
    
    def parse_founders(self, html: str, company_name: str = "") -> List[Founder]:
        """
        Parse founder information from company detail page HTML.
        
        YC pages use specific CSS classes:
        - ycdc-card-new: Container for each founder card
        - text-xl font-bold: Founder name (desktop)
        - text-lg font-bold: Founder name (mobile)
        - a[aria-label="LinkedIn profile"]: LinkedIn link
        
        Args:
            html: The HTML content of the company page
            company_name: The company name to filter out from founder list
        """
        founders = []
        soup = BeautifulSoup(html, "lxml")
        
        # Helper to validate founder name
        def is_valid_founder_name(name: str) -> bool:
            if not name or len(name) < 3 or len(name) > 50:
                return False
            # Filter out company name matches (case insensitive)
            if company_name and name.lower() == company_name.lower():
                return False
            # Filter out common non-founder entries
            skip_names = ["founders", "team", "active founders", "programs", "tl;dr"]
            if name.lower() in skip_names:
                return False
            return True
        
        # Helper to validate LinkedIn URL (personal profiles only)
        def is_personal_linkedin(url: str) -> bool:
            if not url:
                return False
            # Only accept personal LinkedIn profiles (/in/), not company pages (/company/)
            return "linkedin.com/in/" in url.lower() and "linkedin.com/company/" not in url.lower()
        
        # Strategy 1: Look for founder cards with ycdc-card-new class
        founder_cards = soup.find_all("div", class_=re.compile(r"ycdc-card"))
        
        for card in founder_cards:
            founder_name = None
            linkedin_url = None
            
            # Find founder name in text-xl font-bold or text-lg font-bold divs
            name_elem = card.find("div", class_=re.compile(r"text-xl.*font-bold|font-bold.*text-xl"))
            if not name_elem:
                name_elem = card.find("div", class_=re.compile(r"text-lg.*font-bold|font-bold.*text-lg"))
            
            if name_elem:
                founder_name = name_elem.get_text(strip=True)
            
            # Find LinkedIn link by aria-label or href pattern (personal profiles only)
            linkedin_elem = card.find("a", attrs={"aria-label": re.compile(r"LinkedIn", re.IGNORECASE)})
            if linkedin_elem:
                url = linkedin_elem.get("href", "")
                if is_personal_linkedin(url):
                    linkedin_url = url
            
            if not linkedin_url:
                linkedin_elem = card.find("a", href=re.compile(r"linkedin\.com/in/"))
                if linkedin_elem:
                    url = linkedin_elem.get("href", "")
                    if is_personal_linkedin(url):
                        linkedin_url = url
            
            # Add founder if we found a valid name with a LinkedIn profile
            if is_valid_founder_name(founder_name) and linkedin_url:
                # Avoid duplicates by checking LinkedIn URL
                if not any(f.linkedin_url == linkedin_url for f in founders):
                    founders.append(Founder(name=founder_name, linkedin_url=linkedin_url))
        
        # Strategy 2: Fallback - look for LinkedIn links and find nearby names
        if not founders:
            linkedin_links = soup.find_all("a", href=re.compile(r"linkedin\.com/in/"))
            
            for link in linkedin_links:
                linkedin_url = link.get("href", "")
                
                # Skip company LinkedIn pages
                if not is_personal_linkedin(linkedin_url):
                    continue
                
                # Navigate up to find a container with the name
                parent = link
                for _ in range(5):  # Look up to 5 levels up
                    parent = parent.parent
                    if parent is None:
                        break
                    
                    # Look for name in various div patterns
                    for class_pattern in [r"font-bold", r"font-semibold", r"font-medium"]:
                        name_elem = parent.find("div", class_=re.compile(class_pattern))
                        if name_elem:
                            text = name_elem.get_text(strip=True)
                            # Validate it looks like a name (1-4 words, reasonable length)
                            if is_valid_founder_name(text):
                                words = text.split()
                                if 1 <= len(words) <= 4 and not any(c in text.lower() for c in ["@", "http", "linkedin"]):
                                    # Avoid duplicates
                                    if not any(f.linkedin_url == linkedin_url for f in founders):
                                        founders.append(Founder(name=text, linkedin_url=linkedin_url))
                                    break
                    else:
                        continue
                    break
        
        # Strategy 3: If still no founders, extract names from LinkedIn URL slugs
        if not founders:
            linkedin_links = soup.find_all("a", href=re.compile(r"linkedin\.com/in/"))
            for link in linkedin_links:
                linkedin_url = link.get("href", "")
                
                # Skip company LinkedIn pages
                if not is_personal_linkedin(linkedin_url):
                    continue
                
                # Try to extract name from LinkedIn URL slug
                match = re.search(r"linkedin\.com/in/([^/?]+)", linkedin_url)
                if match:
                    slug = match.group(1)
                    # Convert slug to name-like format
                    name = slug.replace("-", " ").replace("_", " ").title()
                    
                    if is_valid_founder_name(name):
                        if not any(f.linkedin_url == linkedin_url for f in founders):
                            founders.append(Founder(name=name, linkedin_url=linkedin_url))
        
        return founders
    
    async def scrape_company(self, session: aiohttp.ClientSession, company: Company) -> Company:
        """Scrape founder details for a single company"""
        url = f"{COMPANIES_URL}/{company.slug}"
        html = await self.fetch_page(session, url)
        
        if html:
            founders = self.parse_founders(html, company_name=company.name)
            company.founders = founders
        
        return company
    
    async def scrape_all(self, companies: List[Company]) -> List[Company]:
        """Scrape founder details for all companies concurrently"""
        print(f"\n[*] Scraping founder details for {len(companies)} companies...")
        
        connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [self.scrape_company(session, company) for company in companies]
            
            results = []
            for future in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Scraping details"):
                result = await future
                results.append(result)
        
        print(f"[OK] Scraped founder details for {len(results)} companies")
        return results


# ============================================================================
# Cache Manager - Save/restore progress
# ============================================================================

class CacheManager:
    """Manages caching of scraped data to avoid re-scraping on interruption"""
    
    def __init__(self, cache_file: str = CACHE_FILE):
        self.cache_file = Path(cache_file)
    
    def save(self, companies: List[Company]):
        """Save companies to cache file"""
        data = []
        for company in companies:
            company_dict = {
                "name": company.name,
                "slug": company.slug,
                "batch": company.batch,
                "short_description": company.short_description,
                "founders": [
                    {"name": f.name, "linkedin_url": f.linkedin_url}
                    for f in company.founders
                ]
            }
            data.append(company_dict)
        
        with open(self.cache_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"[SAVED] Saved {len(companies)} companies to cache")
    
    def load(self) -> Optional[List[Company]]:
        """Load companies from cache file"""
        if not self.cache_file.exists():
            return None
        
        try:
            with open(self.cache_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            companies = []
            for item in data:
                founders = [
                    Founder(name=f["name"], linkedin_url=f.get("linkedin_url"))
                    for f in item.get("founders", [])
                ]
                company = Company(
                    name=item["name"],
                    slug=item["slug"],
                    batch=item["batch"],
                    short_description=item["short_description"],
                    founders=founders
                )
                companies.append(company)
            
            print(f"[LOADED] Loaded {len(companies)} companies from cache")
            return companies
        except Exception as e:
            print(f"[!] Could not load cache: {e}")
            return None
    
    def clear(self):
        """Clear the cache file"""
        if self.cache_file.exists():
            self.cache_file.unlink()
            print("[CLEARED] Cache cleared")


# ============================================================================
# CSV Exporter
# ============================================================================

def export_to_csv(companies: List[Company], output_file: str = OUTPUT_FILE):
    """Export companies to CSV file"""
    print(f"\n[*] Exporting {len(companies)} companies to CSV...")
    
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        
        # Header row
        writer.writerow([
            "Company Name",
            "Batch",
            "Short Description",
            "Founder Names",
            "Founder LinkedIn URLs"
        ])
        
        # Data rows
        for company in companies:
            writer.writerow([
                company.name,
                company.batch,
                company.short_description,
                company.founder_names,
                company.founder_linkedin_urls
            ])
    
    print(f"[OK] Exported to {output_file}")
    
    # Print summary statistics
    companies_with_founders = sum(1 for c in companies if c.founders)
    companies_with_linkedin = sum(1 for c in companies if c.founder_linkedin_urls)
    total_founders = sum(len(c.founders) for c in companies)
    
    print(f"\n--- Summary ---")
    print(f"   Total companies: {len(companies)}")
    print(f"   Companies with founders: {companies_with_founders}")
    print(f"   Companies with LinkedIn URLs: {companies_with_linkedin}")
    print(f"   Total founders extracted: {total_founders}")


# ============================================================================
# Main Entry Point
# ============================================================================

async def main_async(limit: int = 500, use_cache: bool = True, clear_cache: bool = False, output_file: str = OUTPUT_FILE):
    """Main async scraping workflow"""
    cache = CacheManager()
    
    # Handle cache options
    if clear_cache:
        cache.clear()
    
    companies = None
    
    # Try to load from cache first
    if use_cache:
        companies = cache.load()
    
    if not companies:
        # Phase 1: Fetch company listings from Algolia
        algolia = AlgoliaClient()
        companies = algolia.fetch_companies(limit=limit)
        
        if not companies:
            print("[ERROR] No companies fetched. Exiting.")
            return
        
        # Phase 2: Scrape founder details from company pages
        scraper = CompanyDetailScraper()
        companies = await scraper.scrape_all(companies)
        
        # Save to cache
        cache.save(companies)
    
    # Phase 3: Export to CSV
    export_to_csv(companies, output_file)


def main():
    """CLI entry point"""
    parser = argparse.ArgumentParser(
        description="Scrape YC startup directory for company and founder information"
    )
    parser.add_argument(
        "--limit", "-l",
        type=int,
        default=500,
        help="Maximum number of companies to scrape (default: 500)"
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Ignore cached data and scrape fresh"
    )
    parser.add_argument(
        "--clear-cache",
        action="store_true",
        help="Clear the cache file before starting"
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        default=OUTPUT_FILE,
        help=f"Output CSV file name (default: {OUTPUT_FILE})"
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("YC Startup Directory Scraper")
    print("=" * 60)
    print(f"   Target: {args.limit} companies")
    print(f"   Output: {args.output}")
    print(f"   Cache:  {'disabled' if args.no_cache else 'enabled'}")
    print("=" * 60)
    
    asyncio.run(main_async(
        limit=args.limit,
        use_cache=not args.no_cache,
        clear_cache=args.clear_cache,
        output_file=args.output
    ))
    
    print("\nScraping complete!")


if __name__ == "__main__":
    main()
