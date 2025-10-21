import os
import json
import re


def write_enriched_agent_json(rows, ms_latest, out_dir="agents_enriched"):
    """
    rows:      list of {"agent_name", "build", "revision", "timestamp"}
    ms_latest: dict  { build_prefix(int) -> latest_ubr(int) }  e.g. {22631:6060, 26100:6899, 26200:6899}
    out_dir:   output directory for per-agent JSON files

    Returns a small summary dict.
    """
    os.makedirs(out_dir, exist_ok=True)

    summary = {"total": 0, "yes": 0, "no": 0}
    for r in rows:
        agent = r.get("agent_name") or "unknown"
        build = r.get("build")
        rev   = r.get("revision")
        base  = ms_latest.get(build)  # baseline UBR for this build line

        # binary updated + reason
        if build is None or rev is None:
            updated = "no"
            reason  = "missing build or revision"
        elif base is None:
            updated = "no"
            reason  = f"build {build} not found in Microsoft latest table"
        else:
            if rev >= base:
                updated = "yes"
                reason  = f"{build}.{rev} >= {build}.{base}"
            else:
                updated = "no"
                reason  = f"{build}.{rev} < {build}.{base}"

        payload = {
            "agent_name": agent,
            "timestamp": r.get("timestamp"),
            "build": build,
            "revision": rev,
            "baseline_revision": base,
            "updated": updated,      
            "reason": reason
        }

        # filename per agent
        fname = re.sub(r"[^A-Za-z0-9._-]+", "_", agent) + ".json"
        with open(os.path.join(out_dir, fname), "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        summary["total"] += 1
        summary["yes" if updated == "yes" else "no"] += 1

    return summary
