from fetch_from_elastic import get_elastic_updates
from scrape_latest_build import fetch_ms_latest_builds
from create_json import write_enriched_agent_json
from elastic_ingest import ship_json_dir_to_elastic
from shipper import ship_dir_to_elastic
from config import DEST_INDEX, SUPPORTED_BUILDS



if __name__ == "__main__":
    hosts = get_elastic_updates()
    ms_latest = fetch_ms_latest_builds()
    print("Microsoft latest (build → UBR):", ms_latest)
    print("Current supported builds: ", SUPPORTED_BUILDS)
    summary = write_enriched_agent_json(hosts, ms_latest, out_dir="agents_enriched")
    ship_dir_to_elastic(
        directory="agents_enriched",
        dest_index=DEST_INDEX,
        refresh="wait_for",   # optional: make searchable before returning
        batch_size=500,
        id_field="agent_name",  # or None to let ES autogenerate IDs
    )
