# shipper.py
import os
import json
import time
from datetime import datetime, timezone
from typing import Optional, List, Tuple

import requests
from config import ES_URL, API_KEY_B64  # uses your existing config


def _bulk_flush(
    actions: List[dict],
    docs: List[dict],
    es_url: str,
    api_key_b64: str,
    refresh: Optional[str],
    max_retries: int,
    retry_backoff_sec: float,
) -> Tuple[int, int]:
    """
    Sends one NDJSON bulk request.
    Returns (num_indexed_attempted, num_failed_items).
    """
    if not actions:
        return (0, 0)

    headers = {
        "Authorization": f"ApiKey {api_key_b64}",
        "Content-Type": "application/x-ndjson",
    }

    bulk_url = f"{es_url.rstrip('/')}/_bulk"
    if refresh is not None:
        bulk_url += f"?refresh={'true' if refresh is True else 'false' if refresh is False else refresh}"

    # Prepare NDJSON payload
    lines = []
    for meta, doc in zip(actions, docs):
        lines.append(json.dumps(meta, separators=(",", ":")))
        lines.append(json.dumps(doc, separators=(",", ":")))
    payload = "\n".join(lines) + "\n"

    # Retry transient issues (429/5xx)
    last_resp = None
    for attempt in range(1, max_retries + 1):
        resp = requests.post(bulk_url, data=payload, headers=headers, timeout=120)
        last_resp = resp
        if resp.status_code == 429 or 500 <= resp.status_code < 600:
            sleep_for = retry_backoff_sec * (2 ** (attempt - 1))
            print(f"[WARN] Bulk HTTP {resp.status_code} attempt {attempt}/{max_retries}; "
                  f"backing off {sleep_for:.1f}s")
            time.sleep(sleep_for)
            continue
        break

    if last_resp is None or not last_resp.ok:
        msg = f"Bulk failed: HTTP {getattr(last_resp, 'status_code', '???')} {getattr(last_resp, 'text', '')[:500]}"
        raise RuntimeError(msg)

    result = last_resp.json()
    failed = 0
    if result.get("errors"):
        # Count + summarize first few failures
        for i, item in enumerate(result.get("items", [])):
            err = (item.get("index") or {}).get("error")
            if err:
                failed += 1
                if failed <= 10:
                    print(f"[ERROR] item #{i} failed: status={item['index'].get('status')} "
                          f"_id={item['index'].get('_id')} error={err}")
    else:
        took = result.get("took")
        print(f"[OK] Bulk indexed {len(actions)} docs in {took} ms")

    return (len(actions), failed)


def ship_dir_to_elastic(
    directory: str,
    dest_index: str,
    *,
    es_url: Optional[str] = None,
    api_key_b64: Optional[str] = None,
    batch_size: int = 500,
    id_field: Optional[str] = "agent_name",     # None → ES auto IDs
    use_filename_as_fallback_id: bool = True,
    refresh: Optional[str] = None,              # e.g., "wait_for" | True | False | None
    max_retries: int = 3,
    retry_backoff_sec: float = 1.0,
) -> None:
    """
    Index all JSON files in `directory` into `dest_index` using the Elasticsearch Bulk API.

    - Each `*.json` file → one document.
    - Adds `ingested_at` (UTC ISO8601) and `source_file` if they don't exist.
    - Uses `_id` from `id_field` when present; otherwise falls back to filename (without .json) if enabled.
    """
    es_url = es_url or ES_URL
    api_key_b64 = api_key_b64 or API_KEY_B64

    directory = os.path.abspath(directory)
    files = [f for f in os.listdir(directory) if f.lower().endswith(".json")]
    files.sort()

    if not files:
        print(f"[INFO] No JSON files found in: {directory}")
        return

    actions, docs = [], []
    ingested_at = datetime.now(timezone.utc).isoformat()

    total = 0
    total_failed = 0

    def flush():
        nonlocal actions, docs, total, total_failed
        n_attempted, n_failed = _bulk_flush(
            actions, docs, es_url, api_key_b64, refresh, max_retries, retry_backoff_sec
        )
        total += n_attempted
        total_failed += n_failed
        actions.clear()
        docs.clear()

    for fname in files:
        fpath = os.path.join(directory, fname)
        try:
            with open(fpath, "r", encoding="utf-8") as fh:
                doc = json.load(fh)
        except Exception as e:
            print(f"[WARN] Skipping {fname}: cannot parse JSON ({e})")
            continue

        if not isinstance(doc, dict):
            print(f"[WARN] Skipping {fname}: root is not a JSON object")
            continue

        doc.setdefault("ingested_at", ingested_at)
        doc.setdefault("source_file", fname)
        doc.setdefault("@timestamp", doc.get("checked_at") or doc.get("ingested_at"))

        _id = None
        if id_field:
            _id = doc.get(id_field)
        if not _id and use_filename_as_fallback_id:
            _id = os.path.splitext(fname)[0]

        meta = {"index": {"_index": dest_index}}
        if _id:
            meta["index"]["_id"] = str(_id)

        actions.append(meta)
        docs.append(doc)

        if len(actions) >= batch_size:
            flush()

    # Flush any remaining docs
    if actions:
        flush()

    print(f"[DONE] Indexed {total} doc(s) from {directory} into '{dest_index}'. "
          f"Failures: {total_failed}")
