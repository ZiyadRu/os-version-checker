import json
from urllib.request import Request, urlopen
from typing import List

URL = "https://endoflife.date/api/v1/products/macos/"

def get_maintained_macos_latest_simple() -> List[str]:
    """
    Returns a list like ["26.0.1", "15.7.1", "14.8.1"] for all maintained
    macOS releases (e.g., Tahoe, Sequoia, Sonoma).
    """
    req = Request(URL, headers={"Accept": "application/json"})
    with urlopen(req, timeout=20) as resp:
        if resp.status != 200:
            raise RuntimeError(f"Fetch failed: {resp.status} {resp.reason}")
        data = json.load(resp)

    releases = (data.get("result") or {}).get("releases") or []
    versions = []
    for r in releases:
        latest = r.get("latest") or {}
        if r.get("isMaintained") and latest.get("name"):
            v = str(latest["name"]).strip()
            if v not in versions:  # de-dupe, preserve order
                versions.append(v)
    return versions
