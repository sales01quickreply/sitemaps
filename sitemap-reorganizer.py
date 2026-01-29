#!/usr/bin/env python3
"""
Sitemap Reorganizer Tool
Fetches a website's sitemap, categorizes URLs, and generates sub-sitemaps
with actual Last-Modified dates from the server.

Usage:
    python sitemap-reorganizer.py https://www.quickreply.ai
"""

import sys
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple
import time
import re

class SitemapReorganizer:
    def __init__(self, domain_url: str, fetch_lastmod: bool = True, max_workers: int = 10, github_pages_url: str = None):
        """
        Initialize the sitemap reorganizer.

        Args:
            domain_url: The base domain URL (e.g., https://www.quickreply.ai)
            fetch_lastmod: Whether to fetch Last-Modified dates from server (slower but accurate)
            max_workers: Number of concurrent threads for fetching lastmod dates
            github_pages_url: GitHub Pages URL for sitemap hosting (e.g., https://sales01quickreply.github.io/sitemaps)
        """
        self.domain_url = domain_url.rstrip('/')
        self.fetch_lastmod = fetch_lastmod
        self.max_workers = max_workers
        self.github_pages_url = github_pages_url.rstrip('/') if github_pages_url else None
        self.sitemap_url = f"{self.domain_url}/sitemap.xml"
        self.today = datetime.now().strftime('%Y-%m-%d')

        # Categorization rules
        self.blog_prefixes = [
            '/whatsapp-chatbots',
            '/whatsapp-automation',
            '/whatsapp-marketing',
            '/click-to-whatsapp-ads',
            '/whatsapp-api',
            '/whatsapp-bulk-messaging',
            '/whatsapp-retargetings',
            '/whatsapp-drip-campaigns',
            '/whatsapp-catalog',
            '/whatsapp-integrations',
            '/others',
            '/blog',
            '/blogs'
        ]

        # URLs that should be in 'pages' category despite matching blog prefixes
        self.force_pages_urls = [
            '/whatsapp-automation-tool',
            '/whatsapp-marketing-software-2',
            '/whatsapp-marketing-software',
            '/whatsapp-marketing-automation',
            '/whatsapp-automation-for-business'
        ]

        self.categories = {
            'blog': [],
            'pages': [],
            'wa-templates': [],
            'case-studies': [],
            'integrations': [],
            'server': []
        }

    def fetch_sitemap(self) -> str:
        """Fetch the sitemap XML from the domain."""
        print(f"🔍 Fetching sitemap from: {self.sitemap_url}")
        try:
            response = requests.get(self.sitemap_url, timeout=30)
            response.raise_for_status()
            print(f"✅ Successfully fetched sitemap ({len(response.content)} bytes)")
            return response.text
        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching sitemap: {e}")
            sys.exit(1)

    def parse_sitemap(self, xml_content: str) -> List[str]:
        """Parse sitemap XML and extract all URLs."""
        print("📝 Parsing sitemap XML...")
        try:
            root = ET.fromstring(xml_content)
            # Handle namespace
            namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
            urls = [loc.text for loc in root.findall('.//ns:loc', namespace)]
            print(f"✅ Found {len(urls)} URLs in sitemap")
            return urls
        except ET.ParseError as e:
            print(f"❌ Error parsing XML: {e}")
            sys.exit(1)

    def get_lastmod_from_server(self, url: str) -> str:
        """Fetch the Last-Modified date from the server.

        Tries three methods in order:
        1. Extract 'Last Published' date from Webflow HTML comment
        2. Check Last-Modified header
        3. Fall back to today's date
        """
        try:
            # Method 1: Try to get Webflow's "Last Published" date from HTML
            response = requests.get(url, timeout=10, allow_redirects=True)
            html_content = response.text[:1000]  # Only check first 1000 chars for performance

            # Look for: <!-- Last Published: Wed Jan 28 2026 10:30:29 GMT+0000 (Coordinated Universal Time) -->
            import re
            match = re.search(r'<!-- Last Published: (.+?) -->', html_content)

            if match:
                date_str = match.group(1)
                # Parse formats like: "Wed Jan 28 2026 10:30:29 GMT+0000 (Coordinated Universal Time)"
                try:
                    # Remove timezone info in parentheses
                    date_str = re.sub(r'\s*\(.*?\)\s*$', '', date_str)
                    # Parse the datetime
                    dt = datetime.strptime(date_str, '%a %b %d %Y %H:%M:%S %Z')
                    return dt.strftime('%Y-%m-%d')
                except:
                    pass

            # Method 2: Try Last-Modified header
            last_modified = response.headers.get('Last-Modified')
            if last_modified:
                dt = datetime.strptime(last_modified, '%a, %d %b %Y %H:%M:%S %Z')
                return dt.strftime('%Y-%m-%d')

            # Method 3: Fall back to today's date
            return self.today

        except Exception as e:
            # If all methods fail, return today's date
            return self.today

    def fetch_lastmod_dates(self, urls: List[str]) -> Dict[str, str]:
        """Fetch Last-Modified dates for all URLs using concurrent requests."""
        if not self.fetch_lastmod:
            print("⚠️  Skipping Last-Modified fetch (using today's date for all)")
            return {url: self.today for url in urls}

        print(f"🌐 Fetching Last-Modified dates from server ({self.max_workers} concurrent requests)...")
        print("⏳ This may take a few minutes for large sitemaps...")

        lastmod_dates = {}
        completed = 0
        total = len(urls)

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_url = {executor.submit(self.get_lastmod_from_server, url): url for url in urls}

            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    lastmod = future.result()
                    lastmod_dates[url] = lastmod
                    completed += 1

                    # Progress indicator
                    if completed % 50 == 0 or completed == total:
                        print(f"   Progress: {completed}/{total} URLs processed ({completed/total*100:.1f}%)")
                except Exception as e:
                    lastmod_dates[url] = self.today
                    completed += 1

        print(f"✅ Completed fetching Last-Modified dates")
        return lastmod_dates

    def categorize_url(self, url: str) -> str:
        """Categorize a URL based on the grouping rules."""
        parsed = urlparse(url)
        path = parsed.path

        # Check for server URLs (app.quickreply.ai)
        if 'app.quickreply.ai' in parsed.netloc:
            return 'server'

        # Check for case studies
        if path.startswith('/case-studies') or path.startswith('/case-study'):
            return 'case-studies'

        # Check for integrations
        if path.startswith('/integrations'):
            return 'integrations'

        # Check for WhatsApp templates
        if '/whatsapp-template' in path:
            return 'wa-templates'

        # Check if URL should be forced to pages category (before blog check)
        for forced_path in self.force_pages_urls:
            if path == forced_path:
                return 'pages'

        # Check for blog URLs
        for prefix in self.blog_prefixes:
            if path.startswith(prefix):
                return 'blog'

        # Everything else goes to pages
        return 'pages'

    def organize_urls(self, urls: List[str], lastmod_dates: Dict[str, str]) -> None:
        """Organize URLs into categories."""
        print("📂 Categorizing URLs...")

        for url in urls:
            category = self.categorize_url(url)
            lastmod = lastmod_dates.get(url, self.today)
            self.categories[category].append((url, lastmod))

        # Print summary
        print("\n📊 Categorization Summary:")
        print(f"   Blog URLs:           {len(self.categories['blog'])}")
        print(f"   Page URLs:           {len(self.categories['pages'])}")
        print(f"   Template URLs:       {len(self.categories['wa-templates'])}")
        print(f"   Case Study URLs:     {len(self.categories['case-studies'])}")
        print(f"   Integration URLs:    {len(self.categories['integrations'])}")
        print(f"   Server URLs:         {len(self.categories['server'])}")
        print(f"   Total:               {sum(len(v) for v in self.categories.values())}")

    def generate_sitemap_index(self, use_github_pages: bool = False, github_url: str = None) -> str:
        """Generate the main sitemap index XML.

        Args:
            use_github_pages: If True, use GitHub Pages URL instead of domain URL
            github_url: The GitHub Pages base URL (e.g., https://sales01quickreply.github.io/sitemaps)
        """
        xml_parts = ['<?xml version="1.0" encoding="UTF-8"?>']
        xml_parts.append('<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">')

        # Map category names to sitemap filenames
        sitemap_files = {
            'blog': 'sitemap-blog.xml',
            'pages': 'sitemap-pages.xml',
            'wa-templates': 'sitemap-wa-templates.xml',
            'case-studies': 'sitemap-case-studies.xml',
            'integrations': 'sitemap-integrations.xml',
            'server': 'sitemap-server.xml'
        }

        # Determine base URL for sitemap locations
        base_url = github_url if (use_github_pages and github_url) else self.domain_url

        for category, filename in sitemap_files.items():
            # Only include categories that have URLs
            if self.categories[category]:
                xml_parts.append('    <sitemap>')
                xml_parts.append(f'        <loc>{base_url}/{filename}</loc>')
                xml_parts.append(f'        <lastmod>{self.today}</lastmod>')
                xml_parts.append('    </sitemap>')

        xml_parts.append('</sitemapindex>')
        return '\n'.join(xml_parts)

    def generate_sub_sitemap(self, urls_with_lastmod: List[Tuple[str, str]]) -> str:
        """Generate a sub-sitemap XML."""
        xml_parts = ['<?xml version="1.0" encoding="UTF-8"?>']
        xml_parts.append('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">')

        for url, lastmod in urls_with_lastmod:
            xml_parts.append('    <url>')
            xml_parts.append(f'        <loc>{url}</loc>')
            xml_parts.append(f'        <lastmod>{lastmod}</lastmod>')
            xml_parts.append('    </url>')

        xml_parts.append('</urlset>')
        return '\n'.join(xml_parts)

    def save_sitemaps(self, output_dir: str = '.') -> None:
        """Save all sitemaps to files."""
        print(f"\n💾 Saving sitemaps to: {output_dir}/")

        # Save main sitemap index
        use_github = bool(self.github_pages_url)
        index_content = self.generate_sitemap_index(
            use_github_pages=use_github,
            github_url=self.github_pages_url
        )
        index_path = f"{output_dir}/sitemap.xml"
        with open(index_path, 'w', encoding='utf-8') as f:
            f.write(index_content)
        print(f"   ✅ {index_path}")

        # Save complete sitemap with all URLs (uncategorized)
        all_urls = []
        for category_urls in self.categories.values():
            all_urls.extend(category_urls)

        if all_urls:
            complete_content = self.generate_sub_sitemap(all_urls)
            complete_path = f"{output_dir}/sitemap-complete.xml"
            with open(complete_path, 'w', encoding='utf-8') as f:
                f.write(complete_content)
            print(f"   ✅ {complete_path} ({len(all_urls)} URLs - complete uncategorized)")

        # Save sub-sitemaps
        sitemap_files = {
            'blog': 'sitemap-blog.xml',
            'pages': 'sitemap-pages.xml',
            'wa-templates': 'sitemap-wa-templates.xml',
            'case-studies': 'sitemap-case-studies.xml',
            'integrations': 'sitemap-integrations.xml',
            'server': 'sitemap-server.xml'
        }

        for category, filename in sitemap_files.items():
            if self.categories[category]:
                content = self.generate_sub_sitemap(self.categories[category])
                filepath = f"{output_dir}/{filename}"
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(content)
                print(f"   ✅ {filepath} ({len(self.categories[category])} URLs)")

        print("\n✨ All sitemaps generated successfully!")

    def print_sitemaps(self) -> None:
        """Print all sitemaps to console."""
        print("\n" + "="*80)
        print("=== sitemap.xml (Sitemap Index) ===")
        print("="*80)
        use_github = bool(self.github_pages_url)
        print(self.generate_sitemap_index(
            use_github_pages=use_github,
            github_url=self.github_pages_url
        ))

        sitemap_files = {
            'blog': 'sitemap-blog.xml',
            'pages': 'sitemap-pages.xml',
            'wa-templates': 'sitemap-wa-templates.xml',
            'case-studies': 'sitemap-case-studies.xml',
            'integrations': 'sitemap-integrations.xml',
            'server': 'sitemap-server.xml'
        }

        for category, filename in sitemap_files.items():
            if self.categories[category]:
                print("\n" + "="*80)
                print(f"=== {filename} ===")
                print("="*80)
                print(self.generate_sub_sitemap(self.categories[category]))

    def run(self, save_to_files: bool = True, print_to_console: bool = False, output_dir: str = '.') -> None:
        """Run the complete sitemap reorganization process."""
        print("\n🚀 Starting Sitemap Reorganizer")
        print("="*80)

        # Step 1: Fetch sitemap
        xml_content = self.fetch_sitemap()

        # Step 2: Parse URLs
        urls = self.parse_sitemap(xml_content)

        # Step 3: Fetch Last-Modified dates
        lastmod_dates = self.fetch_lastmod_dates(urls)

        # Step 4: Categorize URLs
        self.organize_urls(urls, lastmod_dates)

        # Step 5: Generate and save/print sitemaps
        if save_to_files:
            self.save_sitemaps(output_dir)

        if print_to_console:
            self.print_sitemaps()

        print("\n✅ Process completed successfully!")


def main():
    """Main entry point for the script."""
    if len(sys.argv) < 2:
        print("Usage: python sitemap-reorganizer.py <domain_url> [options]")
        print("\nOptions:")
        print("  --no-fetch-lastmod         Skip fetching Last-Modified dates (faster)")
        print("  --print                    Print sitemaps to console instead of saving")
        print("  --output-dir <dir>         Directory to save sitemaps (default: current directory)")
        print("  --workers <num>            Number of concurrent workers for fetching (default: 10)")
        print("  --github-pages-url <url>   GitHub Pages URL for sitemap hosting")
        print("\nExample:")
        print("  python sitemap-reorganizer.py https://www.quickreply.ai")
        print("  python sitemap-reorganizer.py https://www.quickreply.ai --output-dir ./sitemaps")
        print("  python sitemap-reorganizer.py https://www.quickreply.ai --no-fetch-lastmod --print")
        print("  python sitemap-reorganizer.py https://www.quickreply.ai --github-pages-url https://sales01quickreply.github.io/sitemaps")
        sys.exit(1)

    domain_url = sys.argv[1]

    # Parse optional arguments
    fetch_lastmod = '--no-fetch-lastmod' not in sys.argv
    print_to_console = '--print' in sys.argv
    save_to_files = '--print' not in sys.argv  # Don't save if only printing

    output_dir = '.'
    if '--output-dir' in sys.argv:
        idx = sys.argv.index('--output-dir')
        if idx + 1 < len(sys.argv):
            output_dir = sys.argv[idx + 1]

    max_workers = 10
    if '--workers' in sys.argv:
        idx = sys.argv.index('--workers')
        if idx + 1 < len(sys.argv):
            max_workers = int(sys.argv[idx + 1])

    github_pages_url = None
    if '--github-pages-url' in sys.argv:
        idx = sys.argv.index('--github-pages-url')
        if idx + 1 < len(sys.argv):
            github_pages_url = sys.argv[idx + 1]

    # Run the reorganizer
    reorganizer = SitemapReorganizer(
        domain_url=domain_url,
        fetch_lastmod=fetch_lastmod,
        max_workers=max_workers,
        github_pages_url=github_pages_url
    )
    reorganizer.run(
        save_to_files=save_to_files,
        print_to_console=print_to_console,
        output_dir=output_dir
    )


if __name__ == "__main__":
    main()
