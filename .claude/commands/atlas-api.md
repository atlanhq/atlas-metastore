Answer questions about Atlas Metastore API endpoints using the official API reference docs.

## Instructions

**HARD RULE: Always read the relevant docs file before using, suggesting, or calling any repair endpoint. Never rely on memory or ticket comments for endpoint signatures, body format, or auth requirements — the local file is the source of truth.**

If information is missing from the local file, fetch the live docs at `https://k.atlan.dev/metastore/master/api/<section>/` to supplement.

**Do NOT load all docs upfront.** Read only the file(s) relevant to the user's question:

| If the question is about… | Read this file |
|---|---|
| Tag / classification desync, `__traitNames`, `__classificationNames`, `repairClassifications` | `docs/api/repair-endpoints.md` |
| Index repair, JanusGraph, `repairindex`, missing from search, composite/single index | `docs/api/repair-endpoints.md` |
| `hasLineage` flag wrong, lineage tab issues, `repairhaslineage` | `docs/api/repair-endpoints.md` |
| Persona alias missing, ES alias, `accesscontrolAlias` | `docs/api/repair-endpoints.md` |
| `outputPorts`, `repairattributes`, `qualifiedName` migration | `docs/api/repair-endpoints.md` |
| Creating or updating entities, `entity/bulk`, relationships, classifications, business metadata | `docs/api/entity-bulk.md` |
| Searching assets, `indexsearch`, DSL queries, pagination, aggregations, `relationAttributes` | `docs/api/search-indexsearch.md` |

If the question spans multiple areas, read the relevant files for each area — but only those.

After reading, answer using only the loaded content. Do not guess or invent endpoint signatures, params, or behaviour not present in the docs.

## Usage

`/atlas-api <question>`

**Examples:**
- `/atlas-api how do I repair classifications for a list of GUIDs?`
- `/atlas-api what privilege is needed to call repairindex?`
- `/atlas-api show me a bulk entity update with business attributes`
- `/atlas-api how do I paginate indexsearch results?`
- `/atlas-api what params does repairAllClassifications accept?`
