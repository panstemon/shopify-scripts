#!/usr/bin/env python3
"""
Upload every file in a local folder to Shopify Files.

USAGE
-----
python upload_folder_to_shopify.py \
    --store mystore              # “mystore” or “mystore.myshopify.com”
    --token shpat_XXX            # Admin API access token (read/write_files)
    ./assets                     # folder containing files to push
"""

import argparse, json, mimetypes, os, sys, hashlib
from pathlib import Path
import requests
from tqdm import tqdm
from urllib.parse import urlparse

API_VERSION = "2025-04"                       # bump when Shopify releases a new stable version

# ---------- helpers --------------------------------------------------------- #
def graphql(session, store, query, variables=None):
    """Thin wrapper around POST /graphql.json."""
    url = f"https://{store}.myshopify.com/admin/api/{API_VERSION}/graphql.json"
    r = session.post(url, json={"query": query, "variables": variables or {}}, timeout=30)
    r.raise_for_status()
    payload = r.json()
    if payload.get("errors"):
        raise RuntimeError(json.dumps(payload["errors"], indent=2))
    return payload["data"]

def existing_filenames(session, store):
    """Return a set of all basenames already present in Files."""
    query = """
      query($cursor: String) {
        files(first: 250, after: $cursor) {
          pageInfo { hasNextPage endCursor }
          edges {
            node {
              __typename
              ... on GenericFile { url }
              ... on MediaImage  { image { url } }
              alt
            }
          }
        }
      }
    """
    names = set()
    cursor = None
    while True:
        data = graphql(session, store, query, {"cursor": cursor})
        edges = data["files"]["edges"]
        for edge in edges:
            node = edge["node"]
            if node["__typename"] == "GenericFile":
                names.add(Path(urlparse(node["url"]).path).name)
            elif node["__typename"] == "MediaImage":
                names.add(Path(urlparse(node["image"]["url"]).path).name)
        if not data["files"]["pageInfo"]["hasNextPage"]:
            break
        cursor = data["files"]["pageInfo"]["endCursor"]
    return names

def stage_upload(session, store, file_path, mime):
    """Ask Shopify for a staged S3 target."""
    mutation = """
      mutation stagedUploadsCreate($input: [StagedUploadInput!]!) {
        stagedUploadsCreate(input: $input) {
          stagedTargets { url resourceUrl parameters { name value } }
          userErrors { field message }
        }
      }
    """
    input_ = [{
        "filename": file_path.name,
        "mimeType": mime,
        "httpMethod": "POST",
        "resource": "FILE",
        "fileSize"  : str(file_path.stat().st_size),
    }]
    data = graphql(session, store, mutation, {"input": input_})
    errors = data["stagedUploadsCreate"]["userErrors"]
    if errors:
        raise RuntimeError(errors)
    return data["stagedUploadsCreate"]["stagedTargets"][0]

def s3_post(target, file_path):
    """POST the file to the signed S3 URL returned by stagedUploadsCreate."""
    with file_path.open("rb") as f:
        files = {"file": (file_path.name, f, "application/octet-stream")}
        resp = requests.post(target["url"],
                             data={p["name"]: p["value"] for p in target["parameters"]},
                             files=files,
                             timeout=120)
    resp.raise_for_status()

def finalize_file(session, store, target, file_path, mime):
    """Tell Shopify to create a File record that points at the uploaded object."""
    mutation = """
      mutation fileCreate($files: [FileCreateInput!]!) {
        fileCreate(files: $files) {
          files { id }
          userErrors { field message }
        }
      }
    """
    file_input = [{
        "alt"         : file_path.name,
        "contentType" : "IMAGE" if mime.startswith("image/") else "FILE",
        "originalSource": target["resourceUrl"],
    }]
    data = graphql(session, store, mutation, {"files": file_input})
    errs = data["fileCreate"]["userErrors"]
    if errs:
        raise RuntimeError(errs)

# ---------- main ------------------------------------------------------------ #
def main():
    ap = argparse.ArgumentParser(description="Bulk-upload a folder to Shopify Files.")
    ap.add_argument("--store", required=True, help="your-store or your-store.myshopify.com")
    ap.add_argument("--token", required=True, help="Admin API access token")
    ap.add_argument("folder", help="Path to local folder to upload")
    args = ap.parse_args()

    store = args.store.replace(".myshopify.com", "")
    folder = Path(args.folder).expanduser().resolve()
    if not folder.is_dir():
        sys.exit(f"Folder {folder} not found.")

    session = requests.Session()
    session.headers.update({
        "X-Shopify-Access-Token": args.token,
        "Content-Type": "application/json",
    })

    already = existing_filenames(session, store)
    to_upload = [p for p in folder.iterdir() if p.is_file() and p.name not in already]

    if not to_upload:
        print("Nothing new to upload.")
        return

    print(f"{len(to_upload)} of {len(list(folder.iterdir()))} files are new; uploading…")

    for path in tqdm(to_upload, unit="file"):
        mime = mimetypes.guess_type(path)[0] or "application/octet-stream"
        target = stage_upload(session, store, path, mime)
        s3_post(target, path)
        finalize_file(session, store, target, path, mime)

    print("✅  Done!")

if __name__ == "__main__":
    main()
