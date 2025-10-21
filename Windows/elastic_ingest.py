import os
import sys
import json
from datetime import datetime
import requests
import sys
import re
from config import ES_URL, DEST_INDEX, API_KEY_B64



def _iso_now():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def _sanitize(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name or "unknown")

def ship_json_dir_to_elastic(out_dir="agents_enriched", dest_index=DEST_INDEX, allowed_agents=None):
    """
    Bulk-index all JSON files for agents in `allowed_agents` only.
    - allowed_agents: iterable of agent names from the *current run*.
    - Files for agents not in `allowed_agents` are skipped.
    - Uses auto-generated _id so each run creates separate docs.
    """
    # Build a sanitized allowlist based on current rows
    allowed_sanitized = None
    if allowed_agents is not None:
        allowed_sanitized = {_sanitize(a) for a in allowed_agents if a}

    files = [f for f in os.listdir(out_dir) if f.lower().endswith(".json")]
    if not files:
        print(f"No JSON files found in {out_dir}")
        return

    bulk_lines = []
    used = 0

    for fname in files:
        base = os.path.splitext(fname)[0]  # sanitized agent name in your writer
        if allowed_sanitized is not None and base not in allowed_sanitized:
            # stale file from previous runs; skip
            continue

        path = os.path.join(out_dir, fname)
        with open(path, "r", encoding="utf-8") as f:
            doc = json.load(f)

        # Extra guard: if the payload has agent_name and it's not allowed, skip
        if allowed_sanitized is not None:
            payload_agent = _sanitize(doc.get("agent_name"))
            if payload_agent not in allowed_sanitized:
                continue

        # Ensure @timestamp for Discover
        if "@timestamp" not in doc:
            ts = doc.get("timestamp")
            doc["@timestamp"] = ts if isinstance(ts, str) and ts else _iso_now()

        # Action (no _id => separate doc), then source line
        bulk_lines.append(json.dumps({"index": {"_index": dest_index}}, separators=(",", ":")))
        bulk_lines.append(json.dumps(doc, ensure_ascii=False, separators=(",", ":")))
        used += 1

    if not bulk_lines:
        print("Nothing to index (no files matched current agents).")
        return

    ndjson = "\n".join(bulk_lines) + "\n"
    url = f"{ES_URL.rstrip('/')}/_bulk"
    headers = {
        "Authorization": f"ApiKey {API_KEY_B64}",
        "Content-Type": "application/x-ndjson"
    }

    try:
        resp = requests.post(url, params={"refresh": "wait_for"}, data=ndjson.encode("utf-8"), headers=headers, timeout=90)
        resp.raise_for_status()
        j = resp.json()
        if j.get("errors"):
            fails = [it for it in j.get("items", []) if it.get("index", {}).get("error")]
            print(f"Indexed with errors: {len(fails)} failures out of {used} files")
            for it in fails[:5]:
                err = it["index"]["error"]
                print(f" - {err.get('type')}: {err.get('reason')}")
        else:
            print(f" Bulk indexed {used} docs into {dest_index}")
    except requests.RequestException as e:
        print(f" Bulk index error: {e}", file=sys.stderr)
        sys.exit(1)
