#!/usr/bin/env python3
"""
Webflow Sitemap Generator
Builds the categorized sitemaps for www.quickreply.ai straight from the Webflow API,
so every URL is a live, published page with its own real last-updated date.

How it works:
1. Reads static pages and published CMS items from the Webflow Data API (read-only token)
2. Checks every URL on the live site and keeps only pages that load directly (200),
   are not noindexed, and do not point their canonical somewhere else
3. Refuses to write anything if the result looks broken (safety guard)
4. Writes sitemap.xml (index), sitemap-complete.xml and one sitemap per category

Usage:
    WEBFLOW_API_TOKEN=... python webflow-sitemap.py https://www.quickreply.ai \
        --github-pages-url https://sales01quickreply.github.io/sitemaps [--output-dir out]
"""

import argparse
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse

import requests

API = 'https://api.webflow.com/v2'
USER_AGENT = 'Mozilla/5.0 (compatible; QuickReplySitemapBot/2.0; +https://github.com/sales01quickreply/sitemaps)'

# Webflow system pages that must never be in a sitemap
EXCLUDED_PATHS = {'/404', '/401', '/search', '/password'}

# Safety guard: abort when the new sitemap shrinks too much compared with the last one,
# or when too many Webflow URLs fail the live check (usually means the site blocked us)
MIN_RATIO_VS_PREVIOUS = 0.80
MIN_LIVE_RATIO = 0.85

SITEMAP_FILES = {
    'blog': 'sitemap-blog.xml',
    'pages': 'sitemap-pages.xml',
    'wa-templates': 'sitemap-wa-templates.xml',
    'case-studies': 'sitemap-case-studies.xml',
    'integrations': 'sitemap-integrations.xml',
}

# Categorization rules (same as sitemap-reorganizer.py)
BLOG_PREFIXES = [
    '/whatsapp-chatbots', '/whatsapp-automation', '/whatsapp-marketing', '/click-to-whatsapp-ads',
    '/whatsapp-api', '/whatsapp-bulk-messaging', '/whatsapp-retargetings', '/whatsapp-drip-campaigns',
    '/whatsapp-catalog', '/whatsapp-integrations', '/others', '/blog', '/blogs',
]
FORCE_PAGES_PATHS = [
    '/whatsapp-automation-tool', '/whatsapp-marketing-software-2', '/whatsapp-marketing-software',
    '/whatsapp-marketing-automation', '/whatsapp-automation-for-business',
]


def categorize(path):
    if path.startswith('/case-studies') or path.startswith('/case-study'):
        return 'case-studies'
    if path.startswith('/integrations'):
        return 'integrations'
    if '/whatsapp-template' in path:
        return 'wa-templates'
    if path in FORCE_PAGES_PATHS:
        return 'pages'
    for prefix in BLOG_PREFIXES:
        if path.startswith(prefix):
            return 'blog'
    return 'pages'


class WebflowClient:
    def __init__(self, token):
        self.session = requests.Session()
        self.session.headers.update({'Authorization': f'Bearer {token}', 'accept': 'application/json'})

    def get(self, path, params=None):
        for attempt in range(5):
            r = self.session.get(f'{API}{path}', params=params, timeout=30)
            if r.status_code == 429:
                wait = int(r.headers.get('Retry-After', 60))
                print(f'   ⏳ Webflow rate limit, waiting {wait}s...')
                time.sleep(wait)
                continue
            if r.status_code >= 500:
                time.sleep(5 * (attempt + 1))
                continue
            r.raise_for_status()
            return r.json()
        raise RuntimeError(f'Webflow API kept failing for {path}')

    def paginate(self, path, key):
        offset, results = 0, []
        while True:
            data = self.get(path, {'limit': 100, 'offset': offset})
            batch = data.get(key, [])
            results.extend(batch)
            total = data.get('pagination', {}).get('total', len(results))
            offset += len(batch)
            if not batch or offset >= total:
                return results


def find_site(client, domain):
    sites = client.get('/sites').get('sites', [])
    if not sites:
        raise RuntimeError('The API token cannot see any site. Check it has the "Sites: read" scope.')
    host = urlparse(domain).netloc.lower()
    for site in sites:
        domains = [d.get('url', '').lower() for d in site.get('customDomains', [])]
        if host in domains or host.removeprefix('www.') in domains:
            return site
    if len(sites) == 1:
        return sites[0]
    raise RuntimeError(f'Could not find a Webflow site with domain {host}')


def date_only(timestamp):
    return (timestamp or '')[:10]


def collect_candidates(client, site_id, domain):
    """Return {url: lastmod} for every published static page and CMS item."""
    candidates = {}

    pages = client.paginate(f'/sites/{site_id}/pages', 'pages')
    template_collections = set()
    for page in pages:
        if page.get('collectionId'):
            # CMS template page: its items become URLs below
            if not page.get('draft') and not page.get('archived'):
                template_collections.add(page['collectionId'])
            continue
        if page.get('draft') or page.get('archived'):
            continue
        path = page.get('publishedPath') or ('/' + page.get('slug', '')).rstrip('/')
        if path in ('', '/index'):
            path = '/'
        if path in EXCLUDED_PATHS:
            continue
        candidates[domain + ('' if path == '/' else path)] = date_only(page.get('lastUpdated'))
    print(f'   Static pages: {len(candidates)}')

    collections = client.get(f'/sites/{site_id}/collections').get('collections', [])
    for collection in collections:
        if collection['id'] not in template_collections:
            continue  # collection has no public page per item (e.g. authors, tags)
        items = client.paginate(f"/collections/{collection['id']}/items/live", 'items')
        count = 0
        for item in items:
            if item.get('isDraft') or item.get('isArchived'):
                continue
            slug = item.get('fieldData', {}).get('slug')
            if not slug:
                continue
            url = f"{domain}/{collection['slug']}/{slug}"
            candidates[url] = date_only(item.get('lastUpdated') or item.get('lastPublished'))
            count += 1
        print(f"   CMS /{collection['slug']}: {count}")

    return candidates


CANONICAL_RE = re.compile(r'<link[^>]+rel=["\']canonical["\'][^>]*>', re.I)
HREF_RE = re.compile(r'href=["\']([^"\']+)["\']', re.I)
NOINDEX_RE = re.compile(r'<meta[^>]+name=["\']robots["\'][^>]+content=["\'][^"\']*noindex', re.I)


def normalize(url):
    return url.rstrip('/').lower()


def check_url(session, url):
    """Return None if the URL is fine to list, otherwise a short reason."""
    for attempt in range(3):
        try:
            r = session.get(url, timeout=20, allow_redirects=False)
        except requests.RequestException as e:
            reason = f'error: {type(e).__name__}'
            time.sleep(2 * (attempt + 1))
            continue
        if r.status_code in (429, 500, 502, 503, 504):
            reason = f'HTTP {r.status_code}'
            time.sleep(3 * (attempt + 1))
            continue
        if r.status_code in (301, 302, 307, 308):
            return f"redirects to {r.headers.get('Location', '?')}"
        if r.status_code != 200:
            return f'HTTP {r.status_code}'
        if 'noindex' in r.headers.get('X-Robots-Tag', '').lower() or NOINDEX_RE.search(r.text):
            return 'noindex'
        tag = CANONICAL_RE.search(r.text)
        href = HREF_RE.search(tag.group(0)) if tag else None
        if href and normalize(href.group(1)) != normalize(url):
            return f'canonical points to {href.group(1)}'
        return None
    return reason


def verify_live(candidates, workers):
    session = requests.Session()
    session.headers['User-Agent'] = USER_AGENT
    urls = sorted(candidates)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        reasons = list(pool.map(lambda u: check_url(session, u), urls))
    live = {u: candidates[u] for u, reason in zip(urls, reasons) if reason is None}
    dropped = {u: reason for u, reason in zip(urls, reasons) if reason is not None}
    return live, dropped


def count_previous(output_dir):
    try:
        with open(os.path.join(output_dir, 'sitemap-complete.xml'), encoding='utf-8') as f:
            return f.read().count('<loc>')
    except FileNotFoundError:
        return 0


def urlset(entries):
    lines = ['<?xml version="1.0" encoding="UTF-8"?>',
             '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
    for url, lastmod in entries:
        lines.append('    <url>')
        lines.append(f'        <loc>{url}</loc>')
        if lastmod:
            lines.append(f'        <lastmod>{lastmod}</lastmod>')
        lines.append('    </url>')
    lines.append('</urlset>')
    return '\n'.join(lines) + '\n'


def write_sitemaps(live, domain, github_pages_url, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    categories = {name: [] for name in SITEMAP_FILES}
    for url in sorted(live):
        path = urlparse(url).path or '/'
        categories[categorize(path)].append((url, live[url]))

    index = ['<?xml version="1.0" encoding="UTF-8"?>',
             '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
    for name, filename in SITEMAP_FILES.items():
        entries = categories[name]
        if not entries:
            continue
        with open(os.path.join(output_dir, filename), 'w', encoding='utf-8') as f:
            f.write(urlset(entries))
        newest = max((d for _, d in entries if d), default='')
        index.append('    <sitemap>')
        index.append(f'        <loc>{github_pages_url or domain}/{filename}</loc>')
        if newest:
            index.append(f'        <lastmod>{newest}</lastmod>')
        index.append('    </sitemap>')
        print(f'   ✅ {filename}: {len(entries)} URLs')
    index.append('</sitemapindex>')

    with open(os.path.join(output_dir, 'sitemap.xml'), 'w', encoding='utf-8') as f:
        f.write('\n'.join(index) + '\n')
    with open(os.path.join(output_dir, 'sitemap-complete.xml'), 'w', encoding='utf-8') as f:
        f.write(urlset(sorted(live.items())))
    print(f'   ✅ sitemap-complete.xml: {len(live)} URLs')


def write_summary(lines):
    """Show the report on the GitHub Actions run page."""
    summary = os.environ.get('GITHUB_STEP_SUMMARY')
    if summary:
        with open(summary, 'a', encoding='utf-8') as f:
            f.write('\n'.join(lines) + '\n')


def main():
    parser = argparse.ArgumentParser(description='Generate quickreply.ai sitemaps from the Webflow API')
    parser.add_argument('domain', help='Live site URL, e.g. https://www.quickreply.ai')
    parser.add_argument('--github-pages-url', help='Where the sitemaps are hosted')
    parser.add_argument('--output-dir', default='.', help='Where to write the sitemaps (default: repo root)')
    parser.add_argument('--previous-dir', default='.', help='Where the last published sitemaps are (for the safety guard)')
    parser.add_argument('--workers', type=int, default=10, help='Parallel live checks')
    args = parser.parse_args()

    token = os.environ.get('WEBFLOW_API_TOKEN')
    if not token:
        sys.exit('❌ WEBFLOW_API_TOKEN is not set. Add it under Settings → Secrets and variables → Actions.')

    domain = args.domain.rstrip('/')
    github_pages_url = args.github_pages_url.rstrip('/') if args.github_pages_url else None

    print('🔍 Reading pages and CMS items from Webflow...')
    client = WebflowClient(token)
    site = find_site(client, domain)
    print(f"   Site: {site.get('displayName')} ({site['id']})")
    candidates = collect_candidates(client, site['id'], domain)
    print(f'   Total from Webflow: {len(candidates)}')

    print(f'🌐 Checking every URL on the live site ({args.workers} at a time)...')
    live, dropped = verify_live(candidates, args.workers)
    print(f'   Live and indexable: {len(live)} | Left out: {len(dropped)}')
    for url, reason in sorted(dropped.items()):
        print(f'   - {url} ({reason})')

    previous = count_previous(args.previous_dir)
    live_ratio = len(live) / len(candidates) if candidates else 0
    report = [
        '## Sitemap update',
        f'- URLs from Webflow: **{len(candidates)}**',
        f'- Live and indexable (listed): **{len(live)}**',
        f'- Left out: **{len(dropped)}**',
        f'- Previous sitemap: **{previous}** URLs',
    ]
    if dropped:
        report += ['', '<details><summary>Left out URLs</summary>', '']
        report += [f'- {u} — {r}' for u, r in sorted(dropped.items())]
        report += ['', '</details>']

    problems = []
    if not candidates:
        problems.append('Webflow returned no pages at all.')
    if live_ratio < MIN_LIVE_RATIO:
        problems.append(f'Only {live_ratio:.0%} of Webflow URLs passed the live check '
                        f'(minimum {MIN_LIVE_RATIO:.0%}). The site may be blocking the checker.')
    if previous and len(live) < previous * MIN_RATIO_VS_PREVIOUS:
        problems.append(f'New sitemap has {len(live)} URLs vs {previous} before '
                        f'(dropped more than {1 - MIN_RATIO_VS_PREVIOUS:.0%}).')
    if problems:
        report += ['', '### 🛑 Stopped by safety guard — sitemaps NOT changed'] + [f'- {p}' for p in problems]
        write_summary(report)
        print('\n🛑 Safety guard: sitemaps were NOT changed.')
        for p in problems:
            print(f'   {p}')
        sys.exit(1)

    print(f'💾 Writing sitemaps to {args.output_dir}/')
    write_sitemaps(live, domain, github_pages_url, args.output_dir)
    write_summary(report)
    print('\n✨ Done')


if __name__ == '__main__':
    main()
