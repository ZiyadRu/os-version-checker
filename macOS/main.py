from fetch_from_elastic import get_elastic_updates
from fetch_latest_version import get_maintained_macos_latest_simple
from create_json import generate_agent_update_reports
from shipper import ship_dir_to_elastic
from config import DEST_INDEX

if __name__ == "__main__":
    rows = get_elastic_updates()
    version_list = get_maintained_macos_latest_simple()
    outdir = "agent_update_reports"
    generate_agent_update_reports(rows, version_list, output_dir=outdir)

    # Ship them to Elasticsearch
    ship_dir_to_elastic(
        directory=outdir,
        dest_index=DEST_INDEX,
        refresh="wait_for",   # optional: make searchable before returning
        batch_size=500,
        id_field="agent_name",  # or None to let ES autogenerate IDs
    )