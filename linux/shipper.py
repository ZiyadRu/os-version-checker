import os, sys, json, datetime, requests

ES_URL    = os.environ.get("ES_URL", ").rstrip("/")
ES_INDEX  = os.environ.get("ES_INDEX", "")
ES_APIKEY = os.environ.get("ES_API_KEY", "")         # base64 ApiKey
INPUT     = os.environ.get("INPUT", "")

def main():
    if not ES_URL:
        print("[ERR] set ES_URL", file=sys.stderr); return 2

    try:
        rows = json.loads(open(INPUT, "r", encoding="utf-8").read())
    except FileNotFoundError:
        print(f"[OK] '{INPUT}' not found; nothing to ship"); return 0
    except Exception as e:
        print(f"[ERR] reading {INPUT}: {e}", file=sys.stderr); return 1

    if not rows:
        print("[OK] empty file; nothing to ship"); return 0

    now = datetime.datetime.utcnow().isoformat() + "Z"
    ndjson = []
    for r in rows:
        host_id = r.get("id")
        os_name = r.get("os_name")
        cur     = r.get("current_version")
        exp     = r.get("latest_version")

        # ECS-ish document
        doc = {
            "@timestamp": now,
            "status": "out_of_date",
            "source": "comparator",
            "host": {"id": host_id},
            "os": {
                "name": os_name,
                "version": cur,
                "expected": exp,   # custom field alongside os.version
            },
        }

        # deterministic _id so re-running upserts same host+current_version
        ndjson.append(json.dumps({"index": {"_index": ES_INDEX, "_id": f"{host_id}-{cur}"}}))
        ndjson.append(json.dumps(doc))

    headers = {"Content-Type": "application/x-ndjson"}
    if ES_APIKEY: headers["Authorization"] = f"ApiKey {ES_APIKEY}"

    try:
        res = requests.post(f"{ES_URL}/_bulk", data="\n".join(ndjson) + "\n", headers=headers, timeout=30)
        res.raise_for_status()
        body = res.json()
    except requests.exceptions.RequestException as e:
        print(f"[ERR] ES HTTP error: {e}", file=sys.stderr); return 1
    except ValueError:
        print("[ERR] ES returned non-JSON", file=sys.stderr); return 1

    if body.get("errors"):
        fails = sum(1 for it in body.get("items", []) if it.get("index", {}).get("error"))
        print(f"[WARN] shipped with {fails} failure(s)")
    print(f"[OK] shipped {len(rows)} doc(s) to '{ES_INDEX}'")
    return 0

if __name__ == "__main__":
    sys.exit(main())