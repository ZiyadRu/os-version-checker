
import os
import sys
import requests
import json
#!/usr/bin/env python3
import os
import sys
import base64
import requests
def getLogs():
    api_key_b64 = ""
    ES_URL = ("").rstrip("/")
    INDEX = ""

    OUTFILE = os.environ.get("OUTFILE", "hosts_latest.json")

    url = f"{ES_URL}/{INDEX}/_search"
    print(url)

    params = {
        "size": 1000,  # fetch enough to cover all hosts; adjust as you like
        "sort": "@timestamp:desc",
        "filter_path": (
            "hits.hits._source.@timestamp,"
            "hits.hits._source.host.id,"
            "hits.hits._source.host.name,"
            "hits.hits._source.host.os.name,"
            "hits.hits._source.host.os.version"
        ),
    }

    headers = {"Authorization": f"ApiKey {api_key_b64}"} if api_key_b64 else {}

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        maybe_hits = data.get("hits", [])
        docs = maybe_hits.get("hits", []) if isinstance(maybe_hits, dict) else maybe_hits

        # Keep only the most recent doc per unique host.id
        latest_by_id = {}  # id -> {"ts": ts, "host_name": ..., "os_name": ..., "os_version": ...}

        for doc in docs:
            src = (doc.get("_source") or {})
            ts = src.get("@timestamp") or src.get("timestamp")
            host = (src.get("host") or {})
            host_id = host.get("id")
            host_name = host.get("name")
            osinfo = (host.get("os") or {})
            os_name = osinfo.get("name")
            os_version = osinfo.get("version")

            if not host_id or not ts:
                continue  # skip incomplete rows

            # ISO8601 'Z' timestamps compare correctly as strings; newest is "greater"
            prev = latest_by_id.get(host_id)
            if (prev is None) or (ts > prev["ts"]):
                latest_by_id[host_id] = {
                    "ts": ts,
                    "host_name": host_name,
                    "os_name": os_name,
                    "os_version": os_version,
                }

        # Build sorted list (newest first) for JSON output
        rows = [
            {
                "id": hid,
                "timestamp": row["ts"],
                "host_name": row["host_name"],
                "os_name": row["os_name"],
                "os_version": row["os_version"],
            }
            for hid, row in sorted(latest_by_id.items(), key=lambda kv: kv[1]["ts"], reverse=True)
        ]

        # --- write OUTFILE just like in the distrowatch script ---
        outdir = os.path.dirname(os.path.abspath(OUTFILE)) or "."
        os.makedirs(outdir, exist_ok=True)
        with open(OUTFILE, "w", encoding="utf-8") as f:
            json.dump(rows, f, indent=2, ensure_ascii=False)
        print(f"[OK] wrote {OUTFILE} ({len(rows)} hosts)")

    except requests.exceptions.RequestException as e:
        print(f"HTTP error: {e}", file=sys.stderr)
        sys.exit(1)
    except ValueError:
        print("Failed to parse JSON response.", file=sys.stderr)
        sys.exit(1)

def main():
    getLogs()

if __name__ == "__main__":
    main()


