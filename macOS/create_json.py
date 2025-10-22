import os
import re
import json
from datetime import datetime, timezone
from fetch_from_elastic import get_elastic_updates
from fetch_latest_version import get_maintained_macos_latest_simple
from config import  SOURCE_INDEX, ES_URL

def normalize_version(v: str) -> str:
    """
    Normalize versions like '14.8.1 (a)' or '13.7.8 (22H730)' -> '14.8.1' / '13.7.8'.
    Keeps only the first 2-3 numeric components.
    """
    if not v:
        return ""
    v = str(v).strip()
    m = re.match(r'^\s*([0-9]+(?:\.[0-9]+){1,2})', v)
    return m.group(1) if m else v

def major_of(version: str) -> str:
    m = re.match(r'^\s*([0-9]+)', version or "")
    return m.group(1) if m else ""

def sanitize_filename(name: str) -> str:
    return re.sub(r'[^A-Za-z0-9._-]+', '_', name or "unknown")

# --- Main function -----------------------------------------------------------

def generate_agent_update_reports(rows,latest_versions, output_dir: str = "agent_update_reports") -> None:
    """
    - Fetches agent macOS versions from Elastic
    - Fetches latest maintained macOS versions from endoflife.date
    - For each agent, writes <output_dir>/<agent_name>.json with enriched fields:
        {
          agent_name, agent_version_raw, agent_version, branch_major,
          is_maintained_major, branch_latest_version,
          is_updated (1/0), reason, observed_at, checked_at, sources
        }
    """
    rows = rows  # [{agent_name, version, timestamp}, ...]
    latest_versions = latest_versions  # ["26.0.1", "15.7.1", "14.8.1"]

    # Normalize latest list, then build quick lookups
    normalized_latest = [normalize_version(v) for v in latest_versions]
    latest_set = set(normalized_latest)
    major_to_latest = {}
    for v in normalized_latest:
        mj = major_of(v)
        if mj:
            major_to_latest[mj] = v  # maintained major -> its latest version

    os.makedirs(output_dir, exist_ok=True)
    checked_at = datetime.now(timezone.utc).isoformat()

    for row in rows:
        agent_name = row.get("agent_name") or "unknown"
        raw_version = row.get("version") or ""
        observed_at = row.get("timestamp")

        agent_version = normalize_version(raw_version)
        agent_major = major_of(agent_version)
        is_maintained_major = agent_major in major_to_latest
        branch_latest = major_to_latest.get(agent_major)

        is_updated = (agent_version in latest_set)

        if not agent_version:
            reason = "No version reported by agent."
        elif is_updated:
            reason = f"{agent_version} is the latest for maintained branch {agent_major}."
        elif is_maintained_major:
            reason = f"{agent_version} is behind the maintained branch {agent_major} (latest is {branch_latest})."
        else:
            maintained_branches = ", ".join(sorted(major_to_latest.keys(), key=int, reverse=True))
            reason = (
                f"{agent_version} is on non-maintained branch {agent_major}; "
                f"maintained branches are {maintained_branches} "
                f"with latest versions {', '.join(normalized_latest)}."
            )

        record = {
            "agent_name": agent_name,
            "agent_version_raw": raw_version,
            "agent_version": agent_version or None,
            "branch_major": agent_major or None,
            "is_maintained_major": bool(is_maintained_major),
            "branch_latest_version": branch_latest,
            "is_updated": is_updated,  
            "reason": reason,
            "observed_at": observed_at,
            "checked_at": checked_at,
            
        }
        outfile = os.path.join(output_dir, f"{sanitize_filename(agent_name)}.json")
        with open(outfile, "w", encoding="utf-8") as fh:
            json.dump(record, fh, indent=2, ensure_ascii=False)

# --- Optional CLI ------------------------------------------------------------
