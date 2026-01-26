# Versioned Metadata

This feature records every mutation as a versioned entry in Cassandra. It can optionally create the entity in the graph, or skip graph persistence entirely. The versioned store is designed for append-only workloads where every event must be preserved.

## Overview

- Write endpoint (versioned write + optional graph persistence):  
  `POST /api/atlas/v2/entity/bulk?versionedLookup=true&skipEntityStore={true|false}`
- Read endpoint (list):  
  `GET /api/atlas/v2/entity/versioned/{typeName}/{baseQN}`
- Read endpoint (specific version):  
  `GET /api/atlas/v2/entity/versioned/{typeName}/{baseQN}/version/{timeuuid}`
- Storage: Cassandra table `versioned_entries` (configurable).
- Version strategy: server-generated `timeuuid` per entry.
- Qualified name strategy: `baseQN + "." + timeuuid`.
- BaseQN derivation: from the entity type’s **unique** attributes (`isUnique: true`).

## API Summary

```
POST /api/atlas/v2/entity/bulk?versionedLookup={true|false}&skipEntityStore={true|false}
  - Body: Atlas bulk entity payload
  - 200: EntityMutationResponse (empty if skipEntityStore=true)
  - 500: Missing unique attributes for baseQN

GET /api/atlas/v2/entity/versioned/{typeName}/{baseQN}
  - 200: VersionedEntry[]
  - 400: Unknown typeName

GET /api/atlas/v2/entity/versioned/{typeName}/{baseQN}/version/{timeuuid}
  - 200: VersionedEntry
  - 400: Invalid timeuuid format
  - 404: Not found for typeName/baseQN/version
```

## Configuration

### Request parameters

- `versionedLookup=true`  
  Persist a versioned entry for every entity in the bulk request.
- `skipEntityStore=true|false`  
  When `true`, the entity is not created/updated in the graph.  
  When `false`, the entity is created/updated normally, and the versioned entry is written in addition.

### Server configuration

The only server configuration required is the table name (optional):

```
# Versioned Cassandra table name (stored in metastore keyspace)
# atlas.versioned.table.name=versioned_entries
```

All other behavior is driven by request parameters and the type definition.

## Base Qualified Name Strategy

The base qualified name (baseQN) is derived from the entity type’s unique attributes:

- All attributeDefs with `isUnique: true` are considered.
- Attribute names are sorted in ascending order for determinism.
- Values are joined with `.` to form the baseQN.
- `qualifiedName` is ignored for baseQN derivation.

Examples:

- Single unique attribute:  
  `productId = product_42` → `baseQN = product_42`
- Multiple unique attributes:  
  `db = analytics`, `table = sales` → `baseQN = analytics.sales`

Note: Atlas does not treat multiple unique attributes as a compound key.

If any unique attribute value is missing in the payload, the request fails.

## Version Strategy

Each versioned entry uses a server-generated `timeuuid`.

The persisted qualified name is:

```
<baseQN>.<timeuuid>
```

This makes every event a distinct append-only record.

## Create a Typedef

Define a type with at least one unique attribute:

```
curl -sS -u admin:admin -X POST \
  -H 'Content-Type: application/json' \
  'http://localhost:21000/api/atlas/v2/types/typedefs' \
  -d '{
    "entityDefs": [
      {
        "name": "ProductViewMutation",
        "description": "Versioned mutation event for product views.",
        "superTypes": ["Asset"],
        "serviceType": "example",
        "typeVersion": "1.0",
        "attributeDefs": [
          {
            "name": "productId",
            "typeName": "string",
            "cardinality": "SINGLE",
            "isIndexable": true,
            "isOptional": false,
            "isUnique": true
          },
          {
            "name": "viewName",
            "typeName": "string",
            "cardinality": "SINGLE",
            "isIndexable": true,
            "isOptional": false,
            "isUnique": false
          },
          {
            "name": "operation",
            "typeName": "string",
            "cardinality": "SINGLE",
            "isIndexable": true,
            "isOptional": true,
            "isUnique": false
          }
        ]
      }
    ]
  }'
```

## Write Versioned Entries

### A) Versioned write only (skip graph persistence)

```
curl -sS -u admin:admin -X POST \
  -H 'Content-Type: application/json' \
  'http://localhost:21000/api/atlas/v2/entity/bulk?versionedLookup=true&skipEntityStore=true' \
  -d '{
    "entities": [
      {
        "typeName": "ProductViewMutation",
        "attributes": {
          "qualifiedName": "ignored-by-versioned",
          "name": "Example",
          "productId": "product_42",
          "viewName": "daily_summary",
          "operation": "UPDATE"
        }
      }
    ]
  }'
```

Notes:
- The entity is **not** created in the graph.
- A versioned entry is written to Cassandra.

### B) Versioned write + graph persistence

```
curl -sS -u admin:admin -X POST \
  -H 'Content-Type: application/json' \
  'http://localhost:21000/api/atlas/v2/entity/bulk?versionedLookup=true&skipEntityStore=false' \
  -d '{
    "entities": [
      {
        "typeName": "ProductViewMutation",
        "attributes": {
          "qualifiedName": "ignored-by-versioned",
          "name": "Example",
          "productId": "product_42",
          "viewName": "daily_summary",
          "operation": "UPDATE"
        }
      }
    ]
  }'
```

Notes:
- The entity **is** created/updated in the graph.
- A versioned entry is written to Cassandra in addition to graph persistence.

## Read Versioned Entries

### List versions for a baseQN

```
curl -sS -u admin:admin \
  'http://localhost:21000/api/atlas/v2/entity/versioned/ProductViewMutation/product_42'
```

Example response:

```
[
  {
    "typeName": "ProductViewMutation",
    "baseQualifiedName": "product_42",
    "version": "61cc0020-fafb-11f0-b7a9-d1d97b8b27c0",
    "qualifiedName": "product_42.61cc0020-fafb-11f0-b7a9-d1d97b8b27c0",
    "createdAt": 1769461803303,
    "originalJson": {
      "typeName": "ProductViewMutation",
      "attributes": {
        "qualifiedName": "ignored-by-versioned",
        "name": "Example",
        "productId": "product_42",
        "viewName": "daily_summary",
        "operation": "UPDATE"
      }
    }
  }
]
```

Fields:
- `createdAt` is a millisecond epoch timestamp generated on write.
- `originalJson` is the original entity payload (typeName + attributes).

### Fetch a specific version

```
curl -sS -u admin:admin \
  'http://localhost:21000/api/atlas/v2/entity/versioned/ProductViewMutation/product_42/version/61cc0020-fafb-11f0-b7a9-d1d97b8b27c0'
```

Response is a single versioned entry:

```
{
  "typeName": "ProductViewMutation",
  "baseQualifiedName": "product_42",
  "version": "61cc0020-fafb-11f0-b7a9-d1d97b8b27c0",
  "qualifiedName": "product_42.61cc0020-fafb-11f0-b7a9-d1d97b8b27c0",
  "createdAt": 1769461803303,
  "originalJson": { ... }
}
```

## Summary

- Versioned entries are always append-only.
- `timeuuid` ensures every event is unique and time-ordered.
- BaseQN is derived from unique attributes; join with `.`.
- Use `skipEntityStore=true` to skip graph persistence.
