#!/usr/bin/env python3
"""
Compare OS versions from two local JSON files:
- SNAPSHOT: Ubuntu latest versions per series from Diwa (default: ./ubuntu_releases.json)
  Expected shape:
    {
      "source": "...",
      "series": {
        "25": {"version": "25.10", ...},
        "24": {"version": "24.04.3", ...},
        "22": {"version": "22.04.5", ...}
      }
    }
- HOSTS: Most-recent OS info per host.id from your ES fetch (default: ./hosts_latest.json)
  Expected shape (array of objects):
    [
      {
        "id": "c205f951-...",
        "timestamp": "2025-10-19T06:35:51.475Z",
        "host_name": "PC-01",
        "os_name": "Ubuntu",
        "os_version": "24.04.3 LTS (Noble Numbat)"
      },
      ...
    ]
Outputs OUTFILE (default: ./out_of_date_hosts.json) as an array:
  [
    {"id": "...", "os_name": "Ubuntu", "current_version": "24.04.2", "latest_version": "24.04.3"},
    ...
  ]
"""

import os, sys, json, re
from pathlib import Path

# ---------- config via env (matches your previous scripts) ----------
SNAPSHOT = os.environ.get("SNAPSHOT", "")
HOSTS    = os.environ.get("HOSTS", "")
OUTFILE  = os.environ.get("OUTFILE", "out_of_date_hosts.json")

VERSION_RE = re.compile(r'(\d{2}\.\d{2}(?:\.\d+)?)')

def version_key(v: str):
    parts = [int(p) for p in v.split(".")]
    while len(parts) < 3:
        parts.append(0)
    return tuple(parts)

def extract_ubuntu_version(s: str | None) -> str | None:
    if not s: return None
    m = VERSION_RE.search(s)
    return m.group(1) if m else None

def load_snapshot(path: str) -> dict:
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[ERR] failed to read SNAPSHOT '{path}': {e}", file=sys.stderr)
        sys.exit(1)

    series = data.get("series") or {}
    latest = {}
    for major, info in series.items():
        if isinstance(info, dict) and "version" in info:
            latest[major] = info["version"]
    return latest  # e.g. {"25":"25.10","24":"24.04.3","22":"22.04.5"}

def load_hosts(path: str) -> list:
    try:
        rows = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(rows, list):
            raise ValueError("HOSTS file must be a JSON array")
        return rows
    except Exception as e:
        print(f"[ERR] failed to read HOSTS '{path}': {e}", file=sys.stderr)
        sys.exit(1)

def main():
    latest_series = load_snapshot(SNAPSHOT)
    hosts = load_hosts(HOSTS)

    out = []
    for row in hosts:
        os_name = (row.get("os_name") or "").strip()
        if os_name.lower() != "ubuntu":
            continue  # only compare Ubuntu

        installed_raw = row.get("os_version")
        installed = extract_ubuntu_version(installed_raw)
        if not installed:
            continue  # can't parse version, skip

        major = installed.split(".", 1)[0]  # "24" from "24.04.3"
        expected = latest_series.get(major)
        if not expected:
            continue  # no known latest for this series

        if version_key(installed) < version_key(expected):
            out.append({
                "id": row.get("id"),
                "os_name": os_name,
                "current_version": installed,
                "latest_version": expected,
            })

    # Write output JSON
    outdir = os.path.dirname(os.path.abspath(OUTFILE)) or "."
    os.makedirs(outdir, exist_ok=True)
    Path(OUTFILE).write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[OK] wrote {OUTFILE} ({len(out)} out-of-date host(s))")

if __name__ == "__main__":
    main()