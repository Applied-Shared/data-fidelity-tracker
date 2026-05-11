"""Serve input data validation DQ reports under one Apps Platform service.

Directory layout: static/sections/<dir>/ with index.html and images/.
Add a new tab by:
  1. Appending an entry to SECTIONS below.
  2. Dropping the report's index.html + images/ into static/sections/<dir>/.
  3. Redeploying.
"""

from __future__ import annotations

import os

from flask import Flask, abort, redirect, send_from_directory

app = Flask(__name__)

_BASE = os.path.dirname(os.path.abspath(__file__))
_STATIC = os.path.join(_BASE, "static")
_SECTIONS_ROOT = os.path.join(_STATIC, "sections")

# Ordered list of (slug, disk_subdir, display_label) for the tab bar.
SECTIONS: list[tuple[str, str, str]] = [
    ("gen2-mache-baseline", "gen2_mache_baseline", "Baseline"),
    ("dq-0509-76segs", "dq_0509_76segs", "05/09 Evening Session"),
]

_SLUG_TO_DIR: dict[str, str] = {slug: d for slug, d, _ in SECTIONS}


def _section_disk_path(slug: str) -> str | None:
    subdir = _SLUG_TO_DIR.get(slug)
    if not subdir:
        return None
    path = os.path.join(_SECTIONS_ROOT, subdir)
    return path if os.path.isdir(path) else None


@app.route("/")
def hub():
    return send_from_directory(_STATIC, "hub.html")


@app.route("/<slug>")
def section_redirect(slug: str):
    if slug not in _SLUG_TO_DIR:
        abort(404)
    return redirect(f"/{slug}/", code=302)


@app.route("/<slug>/")
def section_index(slug: str):
    root = _section_disk_path(slug)
    if root is None:
        abort(404)
    index_path = os.path.join(root, "index.html")
    if not os.path.isfile(index_path):
        abort(404)
    return send_from_directory(root, "index.html")


@app.route("/<slug>/images/<path:subpath>")
def section_images(slug: str, subpath: str):
    root = _section_disk_path(slug)
    if root is None:
        abort(404)
    img_root = os.path.join(root, "images")
    if not os.path.isdir(img_root):
        abort(404)
    return send_from_directory(img_root, subpath)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
