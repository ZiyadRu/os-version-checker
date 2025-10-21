# Windows 11 LCU Compliance (Elastic + Osquery)

Detect—per machine—whether it’s on the **latest Windows 11 cumulative update (LCU)**.

---

## What it does

1. **Fetch** each agent’s `build` and `revision` (UBR) from **Elasticsearch** (`osquery` → `os_version`).
2. **Scrape** Microsoft’s **Windows 11 release information** to get the **Latest build** per release line.
3. **Compare** and write **one JSON per agent** with:
   - `updated: "yes" | "no"`
   - `reason`
4. **Ingest** the generated JSONs back into Elasticsearch as **separate documents**.

---

