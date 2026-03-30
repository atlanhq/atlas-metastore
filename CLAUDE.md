# Atlas Metastore - Claude Development Notes



This file contains important context for Claude Code sessions working on this repository.



## Quick Build Command



```bash

JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home /opt/homebrew/bin/mvn compile -pl repository -am -DskipTests -Drat.skip=true 2>&1 | tail -100

```



**Full build command (for distribution/packaging):**

```bash

JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home mvn clean -Dos.detected.classifier=osx-x86_64 -Dmaven.test.skip -DskipTests -Drat.skip=true -DskipOverlay -DskipEnunciate=true install package -Pdist

```



**Build specific modules:**

```bash

# Repository module only (most common for backend changes)

JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home /opt/homebrew/bin/mvn compile -pl repository -am -DskipTests -Drat.skip=true

 

# Webapp module

JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home /opt/homebrew/bin/mvn compile -pl webapp -am -DskipTests -Drat.skip=true

 

# Integration module

JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home /opt/homebrew/bin/mvn compile -pl intg -am -DskipTests -Drat.skip=true

```



## Project Structure



```

atlas-metastore/

├── addons/                    # Bootstrap models, policies, elasticsearch configs

│   ├── models/               # Type definitions (base_model.json)

│   ├── policies/             # Bootstrap policies

│   ├── elasticsearch/        # ES mappings and settings

│   └── static/templates/     # Policy templates

├── repository/               # Core business logic (most changes go here)

│   └── src/main/java/org/apache/atlas/

│       ├── repository/store/graph/v2/    # Entity/Type storage

│       │   ├── EntityGraphMapper.java    # Entity CRUD operations

│       │   ├── AtlasTypeDefGraphStoreV2.java  # TypeDef storage

│       │   ├── AtlasRelationshipStoreV2.java  # Relationship storage

│       │   └── preprocessor/             # Entity preprocessors (QN generation)

│       ├── glossary/                     # Glossary service

│       ├── tasks/                        # Async task management

│       └── util/                         # Utilities (NanoIdUtils, etc.)

├── intg/                     # API models, client libraries

├── webapp/                   # REST API layer

├── graphdb/                  # JanusGraph database layer

├── notification/             # Kafka notification layer

└── common/                   # Shared utilities

```



## TypeDef System - IMPORTANT



### Available TypeDefs in GitHub Repo



The GitHub repository contains **minimal/bare-bone type definitions**. The base model at `addons/models/0000-Area0/0010-base_model.json` only includes:

- Core enums (atlas_operation, AuthPolicyType, AuthPolicyCategory, etc.)

- Base entity types (Referenceable, Asset, DataSet, Process, Infrastructure)

- Access control types (AuthPolicy, Persona, Purpose, Stakeholder)

- Glossary types (AtlasGlossary, AtlasGlossaryTerm, AtlasGlossaryCategory)

- Data mesh types (DataDomain, DataProduct)

- Query types (Collection, Folder, Query)



### Missing Common TypeDefs



The following common types are **NOT available** in the default GitHub definitions and require external `minimal.json` or production typedefs:

- `Table`

- `Column`

- `Schema`

- `Database`

- `Connection`

- Connector-specific types (Snowflake, BigQuery, Postgres, etc.)



**To work with these types**, you need the `minimal.json` file from Atlan's internal documentation or a production typedef export.



### TypeDef Categories



| Category | Description | Examples |

|----------|-------------|----------|

| `PRIMITIVE` | Basic types | string, int, boolean |

| `ENUM` | Enumerated values | AuthPolicyType, atlas_operation |

| `STRUCT` | Composite structures | SourcesAndSinksCriteria |

| `CLASSIFICATION` | Tags/Labels | Confidential, PII |

| `ENTITY` | Main entity types | Table, Column, AtlasGlossaryTerm |

| `RELATIONSHIP` | Entity relationships | AtlasGlossaryTermAnchor |

| `BUSINESS_METADATA` | Custom attributes | Business metadata definitions |



## Key Classes for ID Generation



When working on GUID/QualifiedName generation:



| Component | File | Method |

|-----------|------|--------|

| Entity GUID | `EntityGraphMapper.java:240` | `createVertex()` |

| Shell Entity GUID | `EntityGraphMapper.java:249` | `createShellEntityVertex()` |

| TypeDef GUID | `AtlasTypeDefGraphStoreV2.java:194` | `createTypeVertex()` |

| Relationship GUID | `AtlasRelationshipStoreV2.java:508` | `createRelationship()` |

| Task GUID | `AtlasTaskService.java:209` | `createAtlasTask()` |

| Glossary QN | `GlossaryPreProcessor.java:130` | `createQualifiedName()` |

| Term QN | `TermPreProcessor.java:238` | `createQualifiedName()` |

| Category QN | `CategoryPreProcessor.java:518` | `createQualifiedName()` |

| Domain QN | `DataDomainPreProcessor.java:395` | `createQualifiedName()` |

| Product QN | `DataProductPreProcessor.java:310` | `createQualifiedName()` |

| Persona QN | `PersonaPreProcessor.java:150` | `processCreatePersona()` |

| Purpose QN | `PurposePreProcessor.java:114` | `processCreatePurpose()` |



**Random ID Utilities:**

- `NanoIdUtils.java` - Generates 21-char random NanoIds (used for QualifiedNames)

- `UUID.randomUUID()` - Generates standard UUIDs (used for GUIDs)



## Preprocessor Pattern



Entity preprocessors handle entity creation/update logic before persistence:



```

repository/src/main/java/org/apache/atlas/repository/store/graph/v2/preprocessor/

├── PreProcessorUtils.java          # Common utilities (getUUID, etc.)

├── glossary/

│   ├── GlossaryPreProcessor.java   # AtlasGlossary

│   ├── TermPreProcessor.java       # AtlasGlossaryTerm

│   └── CategoryPreProcessor.java   # AtlasGlossaryCategory

├── datamesh/

│   ├── DataDomainPreProcessor.java

│   ├── DataProductPreProcessor.java

│   ├── StakeholderPreProcessor.java

│   └── StakeholderTitlePreProcessor.java

├── accesscontrol/

│   ├── PersonaPreProcessor.java

│   └── PurposePreProcessor.java

├── sql/

│   ├── QueryPreProcessor.java

│   ├── QueryFolderPreProcessor.java

│   └── QueryCollectionPreProcessor.java

└── AuthPolicyPreProcessor.java

```



## Running Atlas Locally



See `LOCAL_SETUP.md` for detailed setup instructions. Quick reference:



**Dependencies:**

- Redis (port 6379)

- Cassandra (port 9042)

- Elasticsearch (port 9200)

- Kafka (optional, port 9092)



**Start command:**

```bash

java -Datlas.home=deploy/ -Datlas.conf=deploy/conf -Datlas.data=deploy/data \

  -Datlas.log.dir=deploy/logs -Dlogback.configurationFile=file:./deploy/conf/atlas-logback.xml \

  --add-opens java.base/java.lang=ALL-UNNAMED -Xms512m \

  org.apache.atlas.Atlas

```



**Access:** http://localhost:21000 (admin/admin)



## QualifiedName Patterns



| Entity Type | QualifiedName Pattern | Example |

|-------------|----------------------|---------|

| Glossary | `{nanoId}` | `abc123XYZ` |

| Term | `{nanoId}@{glossaryQN}` | `term123@glossaryQN` |

| Category | `{parentPath}.{nanoId}@{glossaryQN}` | `parent.cat123@glossaryQN` |

| Domain | `default/domain/{nanoId}/super` or `{parentQN}/domain/{nanoId}` | `default/domain/abc123/super` |

| Product | `{domainQN}/product/{nanoId}` | `default/domain/xyz/super/product/abc123` |

| Persona | `{tenantId}/{nanoId}` | `default/persona123` |

| Purpose | `{tenantId}/{nanoId}` | `default/purpose123` |

| Policy | `{parentEntityQN}/{nanoId}` | `default/persona123/policy456` |

| Collection | `{userName}/{nanoId}` | `admin/col123` |

| Query | `{collectionQN}/{userName}/{nanoId}` | `admin/col123/admin/query456` |



## Common Issues & Solutions



### Build Failures



1. **Missing GitHub PAT**: Configure `~/.m2/settings.xml` with GitHub credentials for private package access

2. **Wrong Java version**: Must use Java 17 (Zulu recommended)

3. **Rat check failures**: Add `-Drat.skip=true` to skip license header checks



### TypeDef Not Found



If you get "type not found" errors for common types like Table/Column:

1. Check if you're using the production typedef set

2. Load `minimal.json` typedef definitions

3. Or create the typedef via REST API first



### Entity Creation Fails



Common causes:

1. Missing required attributes (qualifiedName is always required)

2. Invalid relationship references (anchor glossary must exist before creating terms)

3. TypeDef not loaded



## Testing

**Important:** Running tests directly on a module may fail with dependency resolution errors. Use the two-step approach:

```bash
# Step 1: Build dependencies first (skip tests)
JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home /opt/homebrew/bin/mvn install -pl repository -am -DskipTests -Drat.skip=true

# Step 2: Run the specific test
JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home /opt/homebrew/bin/mvn test -pl repository -Dtest=EntityGraphMapperTest -Drat.skip=true
```

**Run tests with pattern:**
```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home /opt/homebrew/bin/mvn test -pl repository -Dtest=*Glossary* -Drat.skip=true
```

**Alternative (single command, slower):**
```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home /opt/homebrew/bin/mvn test -pl repository -am -Dtest=EntityGraphMapperTest -Drat.skip=true -Dsurefire.failIfNoSpecifiedTests=false
```

**Note:** Tests can also be run directly from IntelliJ IDEA without these issues.



## Git Workflow



This project uses standard GitHub flow with PR-based merges to `master` branch.

**CRITICAL — PR target repo:** This repo (`atlanhq/atlas-metastore`) is a fork of `apache/atlas`. The `gh` CLI defaults to the upstream parent. **ALWAYS** use `--repo atlanhq/atlas-metastore` when running `gh pr create`, `gh pr view`, `gh pr edit`, or any other `gh` command. Example:
```bash
gh pr create --repo atlanhq/atlas-metastore --title "..." --body "..."
```



**Branch naming**: Feature branches typically use descriptive names (e.g., `ms-366-staging-smarter`, `fix-record-entity-update`)



## API Reference

Full API docs live at **https://k.atlan.dev/metastore/master** (VPN required).
Source files: `docs/api/` — use `/atlas-api` skill to load full content as context.

| Endpoint | Purpose | Auth |
|---|---|---|
| `POST /api/meta/entity/bulk` | Create / update entities | authenticated |
| `POST /api/meta/search/indexsearch` | Search assets via ES DSL | authenticated |
| `POST /api/meta/entity/repairClassificationsMappings/{guid}` | Fix tag desync for one entity | any user |
| `POST /api/meta/entity/bulk/repairClassificationsMappings` | Fix tag desync for multiple GUIDs | any user |
| `POST /api/meta/entity/repairAllClassifications` | Tenant-wide tag desync repair | `ADMIN_REPAIR_INDEX` |
| `POST /api/meta/entity/guid/{guid}/repairindex` | Re-index single entity | `ADMIN_REPAIR_INDEX` |
| `POST /api/meta/entity/guid/bulk/repairindex` | Re-index multiple entities | `ADMIN_REPAIR_INDEX` |
| `POST /api/meta/entity/repairindex/{typename}` | Re-index all entities of a type | `ADMIN_REPAIR_INDEX` |
| `POST /api/meta/repair/single-index` | Fix single JanusGraph index by qualifiedName | `ADMIN_REPAIR_INDEX` |
| `POST /api/meta/repair/composite-index` | Fix composite JanusGraph index | `ADMIN_REPAIR_INDEX` |
| `POST /api/meta/repair/batch` | Batch repair JanusGraph indices | `ADMIN_REPAIR_INDEX` |
| `POST /api/meta/entity/repairhaslineage` | Fix stale `hasLineage` flag (by GUID) | any user |
| `POST /api/meta/entity/repairhaslineagebyids` | Fix stale `hasLineage` flag (by vertex ID) | any user |
| `POST /api/meta/entity/repair/accesscontrolAlias/{guid}` | Rebuild ES alias for a Persona | any user |
| `POST /api/meta/entity/guid/bulk/repairattributes` | Repair specific attribute on entities | `ADMIN_REPAIR_INDEX` |
| `POST /api/meta/migration/repair-unique-qualified-name` | Re-derive qualifiedName for assets | authenticated |
| `POST /api/meta/migration/repair-stakeholder-qualified-name` | Re-derive qualifiedName for Stakeholders | authenticated |

## External Resources



- [Apache Atlas Documentation](https://atlas.apache.org/documentation.html)

- [Internal Setup Guide](https://atlanhq.atlassian.net/wiki/spaces/c873aeb606dd4834a95d9909a757bfa6/pages/800424446/How+to+run+Atlas+on+the+local+machine