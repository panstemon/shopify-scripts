#!/usr/bin/env python3
"""
Clean Shopify product descriptions by

1. Removing blocks that match
   .panel-layout .panel-grid:has(.woonder-products__container)
2. Re-writing Shopify-CDN image URLs so every “.jpeg” → “.jpg”
   (query-strings are preserved).

Now with built-in rate-limit handling:
– Sleeps briefly when the call-limit header shows > 80 % of your quota.
– Retries on 429 responses using the Retry-After value (or 2 s fallback).

USAGE
-----
python clean_shopify_descriptions.py --store mystore --token shpat_XXXXXX
"""
import argparse, re, time
from urllib.parse import urlparse, urlunparse, parse_qs, ParseResult
from requests.utils import parse_header_links

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

API_VERSION = "2024-04"
CDN_SNIPPETS = ("cdn.shopify", ".myshopify", "shopifycdn")  # heuristic


# ───────────────────────────────── rate-limit helpers ────────────────────────
def _sleep_if_near_limit(resp):
    """
    If >80 % of the minute-bucket is used, pause 1 s.
    Shopify’s header format:  'X-Shopify-Shop-Api-Call-Limit: 32/40'
    """
    header = resp.headers.get("X-Shopify-Shop-Api-Call-Limit")
    if not header:
        return
    used, total = map(int, header.split("/"))
    if used / total >= 0.8:
        time.sleep(1)


def _request(session: requests.Session, method: str, url: str, **kwargs):
    """
    Wrapper that handles throttling transparently.
    Retries up to 5× on HTTP 429 using the Retry-After header.
    """
    for attempt in range(6):
        resp = session.request(method, url, timeout=30, **kwargs)
        if resp.status_code != 429:
            _sleep_if_near_limit(resp)
            resp.raise_for_status()
            return resp

        # 429 – Too Many Requests
        retry_after = float(resp.headers.get("Retry-After", "2"))
        if attempt == 5:
            resp.raise_for_status()
        time.sleep(retry_after)


# ───────────────────────────────── URL helpers ───────────────────────────────
def _is_shopify_cdn(url: str) -> bool:
    host = urlparse(url).netloc.lower()
    return any(piece in host for piece in CDN_SNIPPETS)


def _to_jpg(url: str) -> str:
    if not _is_shopify_cdn(url):
        return url
    parsed: ParseResult = urlparse(url)
    if parsed.path.lower().endswith(".jpeg"):
        parsed = parsed._replace(path=re.sub(r"(?i)\.jpeg$", ".jpg", parsed.path))
        return urlunparse(parsed)
    return url


def _fix_srcset(srcset: str) -> str:
    parts = [p.strip() for p in srcset.split(",")]
    fixed = []
    for part in parts:
        if " " in part:
            url_part, descr = part.split(" ", 1)
            fixed.append(f"{_to_jpg(url_part)} {descr}")
        else:
            fixed.append(_to_jpg(part))
    return ", ".join(fixed)


# ───────────────────────────── HTML cleaning ────────────────────────────────
def clean_description(html: str) -> str:
    soup = BeautifulSoup(html or "", "html.parser")

    # 1. Remove unwanted panels
    for panel in soup.select(".panel-layout .panel-grid:has(.woonder-products__container)"):
        panel.decompose()

    # 2. Normalise image URLs
    for tag in soup.find_all(True):
        for attr in ("src", "href"):
            if attr in tag.attrs:
                tag[attr] = _to_jpg(tag[attr])
        if "srcset" in tag.attrs:
            tag["srcset"] = _fix_srcset(tag["srcset"])
        if "style" in tag.attrs and "background-image" in tag["style"]:
            tag["style"] = re.sub(
                r"url\((['\"]?)(.+?)\1\)",
                lambda m: f"url({m.group(1)}{_to_jpg(m.group(2))}{m.group(1)})",
                tag["style"],
                flags=re.I,
            )
    return str(soup)


# ───────────────────────────── product helpers ──────────────────────────────
def get_products(session: requests.Session, store: str):
    """
    Yield all products, page by page, stopping correctly when Link:no-next.
    Requires:
      session.headers already contains {"X-Shopify-Access-Token": token}
    """
    base = f"https://{store}.myshopify.com/admin/api/{API_VERSION}/products.json"
    page_info = None

    with tqdm(desc="Fetching products", unit="prod", colour="cyan") as pbar:
        try:
            while True:
                params = {"limit": 250}
                if page_info:
                    params["page_info"] = page_info

                resp = _request(session, "GET", base, params=params)
                data = resp.json().get("products", [])
                if not data:
                    break

                pbar.update(len(data))
                for prod in data:
                    yield prod

                link_header = resp.headers.get("Link", "")
                if not link_header:
                    break

                # parse_header_links returns a list of dicts with 'url' and 'rel'
                links = parse_header_links(link_header)
                next_link = next((l for l in links if l.get("rel") == "next"), None)
                if not next_link:
                    break

                # Extract page_info query param from the URL
                parsed = urlparse(next_link["url"])
                qs = parse_qs(parsed.query)
                page_info = qs.get("page_info", [None])[0]
                if not page_info:
                    break

        finally:
            pbar.close()


def update_product(session: requests.Session, store: str, product_id: int, body_html: str):
    url = f"https://{store}.myshopify.com/admin/api/{API_VERSION}/products/{product_id}.json"
    payload = {"product": {"id": product_id, "body_html": body_html}}
    _request(session, "PUT", url, json=payload)


# ─────────────────────────────── main ────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--store", required=True, help="mystore or mystore.myshopify.com")
    ap.add_argument("--token", required=True, help="Admin API access token")
    args = ap.parse_args()

    store = args.store.replace(".myshopify.com", "")
    token = args.token

    session = requests.Session()
    session.headers.update({"X-Shopify-Access-Token": token})

    products = list(get_products(session, store))
    if not products:
        print("No products found.")
        return

    updated = skipped = 0
    with tqdm(total=len(products), desc="Cleaning descriptions", unit="prod", colour="green") as bar:
        for prod in products:
            original = prod.get("body_html", "")
            cleaned = clean_description(original)

            if cleaned != original:
                update_product(session, store, prod["id"], cleaned)
                updated += 1
                tqdm.write(f"✏️  Updated: {prod['title']}")
            else:
                skipped += 1
            bar.update(1)

    print(f"\nDone! {updated} updated, {skipped} unchanged, total {len(products)}.")


if __name__ == "__main__":
    main()
