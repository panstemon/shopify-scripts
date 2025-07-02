import argparse
import time
import requests
import sys

API_VERSION = '2024-04'

def rate_limit_sleep(response):
    if "X-Shopify-Shop-Api-Call-Limit" in response.headers:
        used, total = map(int, response.headers["X-Shopify-Shop-Api-Call-Limit"].split("/"))
        if used / total >= 0.8:
            time.sleep(5)
    if response.status_code == 429:
        retry_after = int(response.headers.get("Retry-After", "2"))
        print(f"⚠️  429 Too Many Requests – sleeping {retry_after}s")
        time.sleep(retry_after)

def get_all_resources(store, token, resource):
    items = []
    url = f'https://{store}/admin/api/{API_VERSION}/{resource}.json?limit=250'
    headers = {
        "X-Shopify-Access-Token": token,
        "Content-Type": "application/json"
    }

    while url:
        response = requests.get(url, headers=headers)
        rate_limit_sleep(response)
        if not response.ok:
            print(f"❌ Error fetching {resource}: {response.status_code} – {response.text}")
            break

        data = response.json()
        key = resource.split(".")[0]  # e.g., "products", "custom_collections"
        items.extend(data.get(key, []))

        link = response.headers.get("Link")
        if link and 'rel="next"' in link:
            url = link.split(";")[0].strip("<> ")
        else:
            url = None

    return items

def get_metafields(store, token, resource_type, resource_id):
    url = f'https://{store}/admin/api/{API_VERSION}/{resource_type}/{resource_id}/metafields.json'
    headers = {
        "X-Shopify-Access-Token": token,
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers)
    rate_limit_sleep(response)
    if not response.ok:
        print(f"❌ Failed to get metafields for {resource_type} {resource_id}: {response.status_code}")
        return []
    return response.json().get("metafields", [])

def delete_metafield(store, token, metafield_id):
    url = f'https://{store}/admin/api/{API_VERSION}/metafields/{metafield_id}.json'
    headers = {
        "X-Shopify-Access-Token": token,
        "Content-Type": "application/json"
    }
    response = requests.delete(url, headers=headers)
    rate_limit_sleep(response)
    if response.status_code == 200:
        print(f"✅ Deleted metafield {metafield_id}")
    else:
        print(f"❌ Failed to delete metafield {metafield_id}: {response.status_code} – {response.text}")

def clean_metafields(store, token, resource, resource_type):
    print(f"🔍 Scanning {resource_type} for 'woo.*' metafields...")
    items = get_all_resources(store, token, resource)
    print(f"Found {len(items)} {resource_type}.")

    for item in items:
        metafields = get_metafields(store, token, resource_type, item["id"])
        for mf in metafields:
            if mf["namespace"].startswith("woo"):
                print(f"🗑 Deleting {mf['namespace']}.{mf['key']} from {resource_type} {item['id']}")
                delete_metafield(store, token, mf["id"])

def main():
    parser = argparse.ArgumentParser(description="Delete unstructured Shopify metafields (e.g. woo.*)")
    parser.add_argument("--store", required=True, help="Shopify store domain, e.g. my-store.myshopify.com")
    parser.add_argument("--token", required=True, help="Shopify Admin API access token")
    parser.add_argument("--type", choices=["products", "collections"], required=True, help="Resource type to clean")

    args = parser.parse_args()
    store = args.store
    token = args.token
    resource_type = args.type

    if resource_type == "products":
        clean_metafields(store, token, "products", "products")
    elif resource_type == "collections":
        clean_metafields(store, token, "custom_collections", "custom_collections")
    else:
        print("❌ Unsupported resource type.")
        sys.exit(1)

if __name__ == "__main__":
    main()
