# Bulk Purge E2E Test

End-to-end test for the Bulk Purge by Connection feature. Creates 10K+ assets, tests standard delete, triggers bulk purge, and verifies cleanup.

## Prerequisites

- Docker (for Cassandra, Elasticsearch, Redis)
- Java 17 (Zulu recommended)
- Python 3.6+ with `requests` (`pip install requests`)
- Atlas built (see main CLAUDE.md)

## Quick Start

```bash
# 1. Start Docker services
docker-compose -f local-dev/docker-compose.yaml up -d

# 2. Wait for services to be ready (~30s for Cassandra)
sleep 30

# 3. Set up local deploy directory
bash dev-support/bulk-purge-test/setup_local_env.sh

# 4. Build Atlas (if not already built)
JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home \
  mvn clean -Dos.detected.classifier=osx-x86_64 -Dmaven.test.skip -DskipTests \
  -Drat.skip=true -DskipOverlay -DskipEnunciate=true install package -Pdist

# 5. Start Atlas (from repo root)
java -Datlas.home=deploy/ -Datlas.conf=deploy/conf \
  -Datlas.data=deploy/data -Datlas.log.dir=deploy/logs \
  -Dlogback.configurationFile=file:./deploy/conf/atlas-logback.xml \
  --add-opens java.base/java.lang=ALL-UNNAMED -Xms512m \
  org.apache.atlas.Atlas

# 6. Wait for Atlas to become ACTIVE (~60-100s), then run the test
python3 dev-support/bulk-purge-test/test_bulk_purge.py
```

## Configuration

Via environment variables or CLI args:

| Variable | CLI Flag | Default | Description |
|---|---|---|---|
| `ATLAS_URL` | `--url` | `http://localhost:21000` | Atlas base URL |
| `ATLAS_USER` | `--user` | `admin` | Username |
| `ATLAS_PASS` | `--password` | `admin` | Password |
| `ASSET_COUNT` | `--count` | `10000` | Number of Table assets to create |
| `PARALLEL_WORKERS` | `--workers` | `20` | Thread pool size |
| `BATCH_SIZE` | `--batch-size` | `50` | Entities per bulk-create request |

### Smaller test run

```bash
python3 dev-support/bulk-purge-test/test_bulk_purge.py --count 100 --workers 5
```

## Test Phases

| Phase | Description |
|---|---|
| 0 | Preflight — verify Atlas is reachable and ACTIVE |
| 1 | Check types — verify Connection, Table, Process types exist |
| 2 | Create test Connection entity |
| 3 | Create N Table assets in parallel (batches of 50) |
| 4 | Create external Connection + 10 tables + 5 lineage Processes |
| 5 | Wait for Elasticsearch to sync all entities |
| 6 | Verify entity counts match expectations |
| 7 | Standard delete of 50 entities (baseline comparison) |
| 8 | Trigger bulk purge and poll until completion |
| 9 | Post-purge verification (assets gone, connections preserved, lineage repaired) |

## Expected Output

```
[HH:MM:SS] [INFO] === Phase 0: Preflight ===
[HH:MM:SS] [INFO] Atlas status: ACTIVE
...
[HH:MM:SS] [INFO] === Phase 8: Trigger Bulk Purge ===
[HH:MM:SS] [INFO] Purge submitted: requestId=..., purgeKey=..., mode=CONNECTION
[HH:MM:SS] [INFO]   Status: RUNNING | deleted: 5000/10000 | ...
[HH:MM:SS] [INFO] Purge COMPLETED in 45.2s
...
[HH:MM:SS] [INFO] BULK PURGE E2E TEST SUMMARY
[HH:MM:SS] [INFO]   Assets created:    10000
[HH:MM:SS] [INFO]   Purge duration:    45.2s
[HH:MM:SS] [INFO]   Entities purged:   10005
[HH:MM:SS] [INFO]   Peak delete rate:  250 entities/s
[HH:MM:SS] [INFO]   Overall result:    PASS
```

## What the Setup Script Does

`setup_local_env.sh` creates the `deploy/` directory (gitignored) with:

- `conf/atlas-application.properties` — generated config pointing to local Docker services
- `conf/atlas-logback.xml` — copied from `local-dev/`
- `conf/users-credentials.properties` — copied from test resources
- Symlinks to `webapp/src/test/resources/deploy/` for models, elasticsearch, policies, static

The generated properties are modeled on `AtlasInProcessBaseIT.java` integration test config.
