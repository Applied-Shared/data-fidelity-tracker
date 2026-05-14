"""Serve input data validation DQ reports under one Apps Platform service.

Reports are auto-discovered by scanning an S3 prefix on startup and every
REFRESH_INTERVAL seconds (default 3 h). Legacy local reports can also be
defined in reports.json via a local_dir entry.

Required env vars (for S3 access):
  AWS_ACCESS_KEY_ID
  AWS_SECRET_ACCESS_KEY

Optional env vars:
  S3_REPORTS_URI    — S3 URI prefix to scan (default below)
  REFRESH_INTERVAL  — seconds between scans (default 10800)
"""

from __future__ import annotations

import json
import os
import re
import threading
import time
from pathlib import Path

from flask import Flask, Response, abort, redirect, render_template, request, send_from_directory

app = Flask(__name__)

_BASE = Path(__file__).parent
_STATIC = _BASE / "static"
_SECTIONS_ROOT = _STATIC / "sections"
_S3_CACHE = Path("/tmp/s3_report_cache")

S3_REPORTS_URI = os.environ.get(
    "S3_REPORTS_URI",
    "s3://onroad-perception-datasets/adp_neural_sim/mce106_input_data_reports",
)
REFRESH_INTERVAL = int(os.environ.get("REFRESH_INTERVAL", 3 * 3600))

_lock = threading.RLock()
_s3_reports: list[dict] = []
_local_reports: list[dict] = []
_slug_to_report: dict[str, dict] = {}


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------

def _parse_s3_uri(uri: str) -> tuple[str, str]:
    assert uri.startswith("s3://"), f"Invalid S3 URI: {uri}"
    bucket, _, prefix = uri[5:].partition("/")
    return bucket, prefix.rstrip("/")


_oci_creds: dict | None = None


def _load_oci_creds() -> dict:
    global _oci_creds
    if _oci_creds is not None:
        return _oci_creds

    project_id = os.environ.get("PROJECT_ID", "")

    def _secret(name: str, env_fallback: str) -> str | None:
        val = os.environ.get(env_fallback)
        if val:
            return val
        if not project_id:
            return None
        try:
            from google.cloud import secretmanager
            client = secretmanager.SecretManagerServiceClient()
            path = f"projects/{project_id}/secrets/data-fidelity-tracker-{name}/versions/latest"
            return client.access_secret_version(request={"name": path}).payload.data.decode()
        except Exception as exc:
            app.logger.warning("Could not fetch secret %s: %s", name, exc)
            return None

    _oci_creds = {
        "aws_access_key_id":     _secret("aws-access-key-id",     "AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": _secret("aws-secret-access-key", "AWS_SECRET_ACCESS_KEY"),
        "endpoint_url":          _secret("s3-endpoint-url",       "S3_ENDPOINT_URL"),
        "region_name":           _secret("aws-default-region",    "AWS_DEFAULT_REGION") or "us-phoenix-1",
    }
    return _oci_creds


def _s3_client():
    import boto3
    from botocore.config import Config
    creds = _load_oci_creds()
    return boto3.client(
        "s3",
        aws_access_key_id=creds["aws_access_key_id"],
        aws_secret_access_key=creds["aws_secret_access_key"],
        endpoint_url=creds["endpoint_url"],
        region_name=creds["region_name"],
        config=Config(s3={"addressing_style": "path"}),
    )


def _fetch_s3_html(slug: str, s3_uri: str) -> str | None:
    cache_file = _S3_CACHE / slug / "index.html"
    if cache_file.exists():
        return cache_file.read_text()
    bucket, prefix = _parse_s3_uri(s3_uri)
    key = f"{prefix}/report.html" if prefix else "report.html"
    try:
        body = _s3_client().get_object(Bucket=bucket, Key=key)["Body"].read()
        html = body.decode("utf-8")
    except Exception as exc:
        app.logger.error("S3 fetch failed for %s: %s", slug, exc)
        return None
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    cache_file.write_text(html)
    return html


def _s3_image_url(slug: str, subpath: str, s3_uri: str) -> str | None:
    bucket, prefix = _parse_s3_uri(s3_uri)
    key = f"{prefix}/images/{subpath}" if prefix else f"images/{subpath}"
    try:
        return _s3_client().generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=3600,
        )
    except Exception as exc:
        app.logger.error("S3 presign failed for %s/images/%s: %s", slug, subpath, exc)
        return None


# ---------------------------------------------------------------------------
# S3 discovery
# ---------------------------------------------------------------------------

def _extract_title(html: str) -> str:
    m = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.I | re.S)
    if m:
        return re.sub(r"<[^>]+>", "", m.group(1)).strip()
    m = re.search(r"<title[^>]*>(.*?)</title>", html, re.I | re.S)
    if m:
        return re.sub(r"<[^>]+>", "", m.group(1)).strip()
    return "Untitled Report"


def _scan_s3() -> list[dict]:
    bucket, prefix = _parse_s3_uri(S3_REPORTS_URI)
    scan_prefix = prefix.rstrip("/") + "/"
    try:
        s3 = _s3_client()
        paginator = s3.get_paginator("list_objects_v2")
        uuids = []
        for page in paginator.paginate(Bucket=bucket, Prefix=scan_prefix, Delimiter="/"):
            for cp in page.get("CommonPrefixes", []):
                uuid = cp["Prefix"].rstrip("/").split("/")[-1]
                if uuid:
                    uuids.append(uuid)
    except Exception as exc:
        app.logger.error("S3 list failed: %s", exc)
        return []

    found = []
    for uuid in uuids:
        slug = uuid
        s3_uri = f"s3://{bucket}/{prefix.rstrip('/')}/{uuid}"

        # Reuse already-discovered entry to avoid re-fetching HTML
        with _lock:
            existing = _slug_to_report.get(slug)
        if existing and existing.get("source") == "s3":
            found.append(existing)
            continue

        html = _fetch_s3_html(slug, s3_uri)
        if html is None:
            continue

        date = None
        try:
            obj = s3.head_object(
                Bucket=bucket,
                Key=f"{prefix.rstrip('/')}/{uuid}/report.html",
            )
            date = obj["LastModified"].strftime("%Y-%m-%d")
        except Exception:
            pass

        found.append({
            "slug": slug,
            "label": _extract_title(html),
            "date": date,
            "subtitle": None,
            "note": None,
            "s3_uri": s3_uri,
            "local_dir": None,
            "source": "s3",
        })

    found.sort(key=lambda r: r.get("date") or "", reverse=True)
    return found


def _refresh() -> None:
    discovered = _scan_s3()
    with _lock:
        global _s3_reports, _slug_to_report
        _s3_reports = discovered
        merged: dict[str, dict] = {r["slug"]: r for r in _local_reports}
        merged.update({r["slug"]: r for r in discovered})
        _slug_to_report = merged


def _refresh_loop() -> None:
    while True:
        time.sleep(REFRESH_INTERVAL)
        app.logger.info("Refreshing S3 reports...")
        _refresh()


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

def _load_local_reports() -> None:
    global _local_reports
    reports_path = _BASE / "reports.json"
    if not reports_path.exists():
        return
    entries = json.loads(reports_path.read_text())
    _local_reports = [
        {
            "slug": r["slug"],
            "label": r["label"],
            "date": r.get("date"),
            "subtitle": r.get("subtitle"),
            "note": r.get("note"),
            "s3_uri": r.get("s3_uri"),
            "local_dir": r.get("local_dir"),
            "source": "local",
        }
        for r in entries
    ]
    with _lock:
        _slug_to_report.update({r["slug"]: r for r in _local_reports})


_load_local_reports()
_refresh()
threading.Thread(target=_refresh_loop, daemon=True).start()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

def _all_reports() -> list[dict]:
    with _lock:
        s3_slugs = {r["slug"] for r in _s3_reports}
        local_only = [r for r in _local_reports if r["slug"] not in s3_slugs]
        return list(_s3_reports) + local_only


@app.route("/")
def hub():
    tab = request.args.get("tab", "all")
    all_reports = _all_reports()
    if tab == "mce106":
        reports = [r for r in all_reports if r.get("source") == "s3"]
    else:
        reports = all_reports
    return render_template("hub.html", reports=reports, tab=tab, s3_reports_uri=S3_REPORTS_URI)


@app.route("/api/reports")
def api_reports():
    return {"reports": _all_reports()}


@app.route("/refresh", methods=["POST"])
def manual_refresh():
    _refresh()
    return {"ok": True, "count": len(_all_reports())}


@app.route("/<slug>")
def section_redirect(slug: str):
    with _lock:
        if slug not in _slug_to_report:
            abort(404)
    return redirect(f"/{slug}/", code=302)


@app.route("/<slug>/")
def section_index(slug: str):
    with _lock:
        report = _slug_to_report.get(slug)
    if report is None:
        abort(404)

    if report.get("s3_uri"):
        html = _fetch_s3_html(slug, report["s3_uri"])
        if html is None:
            abort(502)
        return Response(html, mimetype="text/html")

    local_dir = report.get("local_dir")
    if not local_dir:
        abort(404)
    root = str(_SECTIONS_ROOT / local_dir)
    if not os.path.isfile(os.path.join(root, "index.html")):
        abort(404)
    return send_from_directory(root, "index.html")


@app.route("/<slug>/images/<path:subpath>")
def section_images(slug: str, subpath: str):
    with _lock:
        report = _slug_to_report.get(slug)
    if report is None:
        abort(404)

    if report.get("s3_uri"):
        url = _s3_image_url(slug, subpath, report["s3_uri"])
        if url is None:
            abort(502)
        return redirect(url, code=302)

    local_dir = report.get("local_dir")
    if not local_dir:
        abort(404)
    img_root = str(_SECTIONS_ROOT / local_dir / "images")
    if not os.path.isdir(img_root):
        abort(404)
    return send_from_directory(img_root, subpath)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
