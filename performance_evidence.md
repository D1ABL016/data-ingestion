# Performance Evidence

## Test File
- **File:** `stores_master_500k.csv`
- **Total Rows:** 500,000

## Results

| Metric | Value |
|--------|-------|
| Total Ingestion Time | 150 seconds |
| Rows Succeeded | 492,846 |
| Rows Failed | 7,154 |
| Success Rate | 98.57% |

## Approach

File ingestion was handled asynchronously via a FastAPI `BackgroundTask`, ensuring the HTTP response was returned immediately (202 Accepted) without blocking the request cycle.

The CSV was processed in chunks of 1,000 rows. Each chunk went through the full pipeline — validation, lookup resolution via batch get-or-create, and bulk insert using `bulk_insert_mappings()` — before committing and moving to the next chunk. This kept memory usage constant regardless of file size.

An in-memory lookup cache was maintained across all chunks, warming over time and reducing redundant DB queries for repeated geographic values (city, state, country, etc.).
