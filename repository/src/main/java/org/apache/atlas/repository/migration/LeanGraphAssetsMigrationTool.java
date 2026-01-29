package org.apache.atlas.repository.migration;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.cassandra.CassandraConfig;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.janus.AtlasJanusVertex;
import org.apache.atlas.repository.graphdb.janus.cassandra.DynamicVertexService;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class LeanGraphAssetsMigrationTool {
    private static final Logger LOG = LoggerFactory.getLogger(LeanGraphAssetsMigrationTool.class);

    private static final class Options {
        private int batchSize = 1000;
        private long start = 0L;
        private long limit = -1L;
        private boolean dryRun = false;
    }

    public static void main(String[] args) {
        Options options = parseArgs(args);

        RequestContext.get();
        CqlSession session = null;
        AtlasGraph graph = null;

        try {
            ApplicationProperties.get();
            graph = AtlasGraphProvider.getGraphInstance();
            session = new CassandraConfig().cqlSession();
            DynamicVertexService dynamicVertexService = new DynamicVertexService(session);

            migrateAssets(graph, dynamicVertexService, options);
        } catch (Exception e) {
            LOG.error("Lean graph assets migration failed", e);
            System.exit(1);
        } finally {
            if (session != null) {
                session.close();
            }
            RequestContext.clear();
            if (graph != null) {
                try {
                    graph.rollback();
                } catch (Exception e) {
                    LOG.warn("Failed to rollback graph after migration", e);
                }
            }
            System.exit(1);
        }
    }

    private static void migrateAssets(AtlasGraph graph, DynamicVertexService dynamicVertexService, Options options)
            throws AtlasBaseException {
        LOG.info("Starting lean graph assets migration: batchSize={}, start={}, limit={}, dryRun={}",
                options.batchSize, options.start, options.limit, options.dryRun);

        Iterable<AtlasVertex> assets = graph.query()
                .has(Constants.SUPER_TYPES_PROPERTY_KEY, Constants.ASSET_ENTITY_TYPE)
                .vertices();

        long scanned = 0L;
        long migrated = 0L;
        long skipped = 0L;

        Map<String, Map<String, Object>> batchProperties = new HashMap<>(options.batchSize);
        Map<String, Long> updatedAtById = new HashMap<>(options.batchSize);

        for (AtlasVertex vertex : assets) {
            if (scanned < options.start) {
                scanned++;
                continue;
            }

            if (options.limit >= 0 && migrated >= options.limit) {
                break;
            }

            String vertexId = vertex.getIdForDisplay();
            if (StringUtils.isBlank(vertexId)) {
                skipped++;
                scanned++;
                continue;
            }

            Map<String, Object> properties = buildPropertiesMap(vertex);
            if (properties.isEmpty()) {
                skipped++;
                scanned++;
                continue;
            }

            batchProperties.put(vertexId, properties);
            Long updatedAt = getUpdatedAt(vertex);
            if (updatedAt != null) {
                updatedAtById.put(vertexId, updatedAt);
            }

            scanned++;
            migrated++;

            if (batchProperties.size() >= options.batchSize) {
                flushBatch(dynamicVertexService, batchProperties, updatedAtById, options.dryRun);
            }
        }

        if (!batchProperties.isEmpty()) {
            flushBatch(dynamicVertexService, batchProperties, updatedAtById, options.dryRun);
        }

        LOG.info("Lean graph assets migration completed: scanned={}, migrated={}, skipped={}.", scanned, migrated, skipped);
    }

    private static void flushBatch(DynamicVertexService dynamicVertexService,
                                   Map<String, Map<String, Object>> batchProperties,
                                   Map<String, Long> updatedAtById,
                                   boolean dryRun) throws AtlasBaseException {
        if (!dryRun) {
            dynamicVertexService.upsertVerticesIfNotNewer(batchProperties, updatedAtById);
        }
        batchProperties.clear();
        updatedAtById.clear();
    }

    private static Map<String, Object> buildPropertiesMap(AtlasVertex vertex) {
        Map<String, Object> properties = new HashMap<>();

        if (vertex instanceof AtlasJanusVertex) {
            Vertex wrapped = vertex.getWrappedElement();
            Iterator<VertexProperty<Object>> it = wrapped.properties();
            while (it.hasNext()) {
                VertexProperty<Object> property = it.next();
                String key = property.key();
                Object value = property.value();

                if (properties.containsKey(key)) {
                    Object existing = properties.get(key);
                    if (existing instanceof List) {
                        ((List<Object>) existing).add(value);
                    } else {
                        List<Object> values = new ArrayList<>();
                        values.add(existing);
                        values.add(value);
                        properties.put(key, values);
                    }
                } else {
                    properties.put(key, value);
                }
            }

            return properties;
        }

        for (String key : vertex.getPropertyKeys()) {
            Collection<Object> values = vertex.getPropertyValues(key, Object.class);
            if (values == null || values.isEmpty()) {
                continue;
            }
            if (values.size() == 1) {
                properties.put(key, values.iterator().next());
            } else {
                properties.put(key, new ArrayList<>(values));
            }
        }

        return properties;
    }

    private static Long getUpdatedAt(AtlasVertex vertex) {
        Object value = vertex.getProperty(Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY, Object.class);
        if (value instanceof Long) {
            return (Long) value;
        }
        if (value instanceof java.util.Date) {
            return ((java.util.Date) value).getTime();
        }

        Object created = vertex.getProperty(Constants.TIMESTAMP_PROPERTY_KEY, Object.class);
        if (created instanceof Long) {
            return (Long) created;
        }
        if (created instanceof java.util.Date) {
            return ((java.util.Date) created).getTime();
        }

        return null;
    }

    private static Options parseArgs(String[] args) {
        Options options = new Options();
        if (args == null) {
            return options;
        }

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if ("--batch-size".equals(arg) && i + 1 < args.length) {
                options.batchSize = Integer.parseInt(args[++i]);
            } else if ("--start".equals(arg) && i + 1 < args.length) {
                options.start = Long.parseLong(args[++i]);
            } else if ("--limit".equals(arg) && i + 1 < args.length) {
                options.limit = Long.parseLong(args[++i]);
            } else if ("--dry-run".equals(arg)) {
                options.dryRun = true;
            } else if ("--help".equals(arg) || "-h".equals(arg)) {
                printUsageAndExit();
            }
        }

        return options;
    }

    private static void printUsageAndExit() {
        System.out.println("LeanGraphAssetsMigrationTool options:\n"
                + "  --batch-size <n>  Batch size for Cassandra writes (default 1000)\n"
                + "  --start <n>       Number of asset vertices to skip before migrating\n"
                + "  --limit <n>       Maximum number of assets to migrate\n"
                + "  --dry-run         Do not write to Cassandra\n");
        System.exit(0);
    }
}
