import sys
import requests
import sys
from config import ES_URL, SOURCE_INDEX, API_KEY_B64


def get_elastic_updates():
    url = f"{ES_URL.rstrip('/')}/{SOURCE_INDEX}/_search"
    params = {
        "size": 10000,
        "filter_path": "hits.hits"
    }
    headers = {"Authorization": f"ApiKey {API_KEY_B64}"}

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        hits = data.get("hits", {}).get("hits", [])
        print(f"Retrieved {len(hits)} os_version docs")

        rows = []
        seen_agents = set()

        for doc in hits:
            src = doc.get("_source", {}) or {}
            host = src.get("host", {}) or {}
            os_ = host.get("os", {}) or {}
            os_family = os_.get("family")
            action = src.get("action_data") or {}

            agent = src.get("agent") or {}
            agent_name = agent.get("name")

           
            if os_family != "windows" or not action:
                continue
            if action.get("query") != "SELECT * FROM os_version;":
                continue
            if agent_name in seen_agents:
                continue
            seen_agents.add(agent_name)
            osquery = src.get("osquery") or {}
            build = osquery.get("build")
            revision = osquery.get("revision")
            # normalize to ints if possible
            try:
                build = int(str(build)) if build is not None else None
            except ValueError:
                build = None
            try:
                revision = int(str(revision)) if revision is not None else None
            except ValueError:
                revision = None
            
            
            rows.append({
                "agent_name": agent_name,
                "build": build,           # e.g., 22631
                "revision": revision,     # e.g., 6060
                "timestamp": src.get("@timestamp")
            })

        return rows

    except requests.exceptions.RequestException as e:
        print(f" Elastic HTTP error: {e}", file=sys.stderr)
        sys.exit(1)
    except ValueError:
        print(" Failed to parse Elastic JSON.", file=sys.stderr)
        sys.exit(1)

