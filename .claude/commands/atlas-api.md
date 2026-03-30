Load the full Atlas Metastore API reference docs as context before answering.

## Instructions

Read all three API reference files and use them as the source of truth for endpoint signatures, request/response shapes, query params, auth requirements, and curl examples. Do not guess or invent endpoint details — only use what is in these files.

Read the following files now:
1. `docs/api/repair-endpoints.md` — All `/api/meta` repair endpoints (classification, index, lineage, access control, migration)
2. `docs/api/entity-bulk.md` — `POST /api/meta/entity/bulk` create/update reference
3. `docs/api/search-indexsearch.md` — `POST /api/meta/search/indexsearch` reference

After reading, confirm which docs were loaded and answer the user's question using only the loaded content.

## Usage

`/atlas-api <your question about the API>`

**Examples:**
- `/atlas-api how do I repair classifications for a list of GUIDs?`
- `/atlas-api what query params does repairAllClassifications accept?`
- `/atlas-api show me a bulk entity update example with business attributes`
- `/atlas-api what privilege is needed to call repairindex?`
- `/atlas-api how do I paginate indexsearch results?`
