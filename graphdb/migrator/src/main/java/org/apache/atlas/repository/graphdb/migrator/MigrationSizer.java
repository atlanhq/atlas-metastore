package org.apache.atlas.repository.graphdb.migrator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Auto-sizes migrator thread pools based on estate size (vertex count from ES)
 * and available CPU cores.
 *
 * Called once before component creation in MigratorMain. Queries ES _count
 * on the source vertex index, computes optimal thread counts, and applies
 * them to MigratorConfig via applyAutoSizing().
 *
 * Users can opt out of auto-sizing for any parameter by explicitly setting
 * it in the properties file. Auto-sizing only overrides parameters that
 * were left at their defaults (i.e., not present in the config file).
 */
public class MigrationSizer {

    private static final Logger LOG = LoggerFactory.getLogger(MigrationSizer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Tier boundaries (vertex count)
    static final long TIER_SMALL_MAX  = 500_000L;
    static final long TIER_MEDIUM_MAX = 5_000_000L;
    static final long TIER_LARGE_MAX  = 20_000_000L;

    // ES bulk size threshold: below 1M vertices use 5000, above use 10000
    static final long ES_BULK_THRESHOLD = 1_000_000L;

    public enum EstateTier { SMALL, MEDIUM, LARGE, XLARGE }

    /**
     * Query ES for estate size and apply auto-sizing to the config.
     * If the ES query fails, logs a warning and leaves config unchanged.
     */
    public static void autoSize(MigratorConfig config) {
        long vertexCount;

        RestClient client = null;
        try {
            client = createEsClient(config);
            vertexCount = getEsCount(client, config.getSourceEsIndex());
        } catch (Exception e) {
            LOG.warn("Auto-sizing: could not query ES for estate size ({}). Using config defaults.", e.getMessage());
            return;
        } finally {
            closeQuietly(client);
        }

        int cores = Runtime.getRuntime().availableProcessors();
        EstateTier tier = classifyEstate(vertexCount);

        int threads             = computeThreads(tier, cores);
        int esBulkSize          = computeEsBulkSize(vertexCount);
        int queueCapacity       = 20_000;
        int maxInflightPerThread = 50;

        config.applyAutoSizing(threads, threads, esBulkSize, queueCapacity, maxInflightPerThread);

        LOG.info("========================================");
        LOG.info("=== Migration Auto-Sizing ===");
        LOG.info("  Estate: {} vertices -> tier {}",
                 String.format("%,d", vertexCount), tier);
        LOG.info("  Available CPU cores: {}", cores);
        LOG.info("  Scanner threads: {} {}",
                 config.getScannerThreads(),
                 config.isExplicitlySet("migration.scanner.threads")
                     ? "(user-specified)" : "(auto-sized)");
        LOG.info("  Writer threads:  {} {}",
                 config.getWriterThreads(),
                 config.isExplicitlySet("migration.writer.threads")
                     ? "(user-specified)" : "(auto-sized)");
        LOG.info("  ES bulk size:    {} {}",
                 config.getEsBulkSize(),
                 config.isExplicitlySet("migration.es.bulk.size")
                     ? "(user-specified)" : "(auto-sized)");
        LOG.info("  Queue capacity:  {} {}",
                 config.getQueueCapacity(),
                 config.isExplicitlySet("migration.queue.capacity")
                     ? "(user-specified)" : "(auto-sized)");
        LOG.info("  Max inflight/thread: {} {}",
                 config.getMaxInflightPerThread(),
                 config.isExplicitlySet("migration.writer.max.inflight.per.thread")
                     ? "(user-specified)" : "(auto-sized)");
        LOG.info("========================================");
    }

    static EstateTier classifyEstate(long vertexCount) {
        if (vertexCount < TIER_SMALL_MAX)  return EstateTier.SMALL;
        if (vertexCount < TIER_MEDIUM_MAX) return EstateTier.MEDIUM;
        if (vertexCount < TIER_LARGE_MAX)  return EstateTier.LARGE;
        return EstateTier.XLARGE;
    }

    /**
     * Compute thread count for both scanner and writer (equal).
     * SMALL: cores (matches small pod), MEDIUM/LARGE/XLARGE: cores * 2 (I/O-bound).
     */
    static int computeThreads(EstateTier tier, int cores) {
        switch (tier) {
            case SMALL:  return Math.max(2, cores);
            case MEDIUM: return Math.max(4, cores * 2);
            case LARGE:  return Math.max(8, cores * 2);
            case XLARGE: return Math.max(16, cores * 2);
            default:     return 16;
        }
    }

    /**
     * ES bulk size: 5000 for small estates (< 1M), 10000 for larger ones.
     */
    static int computeEsBulkSize(long vertexCount) {
        return vertexCount < ES_BULK_THRESHOLD ? 5000 : 10000;
    }

    // --- ES helpers ---

    @SuppressWarnings("unchecked")
    private static long getEsCount(RestClient client, String index) throws Exception {
        Request request = new Request("GET", "/" + index + "/_count");
        Response response = client.performRequest(request);
        String body = EntityUtils.toString(response.getEntity());
        Map<String, Object> result = MAPPER.readValue(body, Map.class);
        return ((Number) result.get("count")).longValue();
    }

    private static RestClient createEsClient(MigratorConfig config) {
        RestClientBuilder builder = RestClient.builder(
            new HttpHost(config.getTargetEsHostname(), config.getTargetEsPort(),
                         config.getTargetEsProtocol()));

        String username = config.getTargetEsUsername();
        String password = config.getTargetEsPassword();
        if (username != null && !username.isEmpty()) {
            BasicCredentialsProvider creds = new BasicCredentialsProvider();
            creds.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));
            builder.setHttpClientConfigCallback(
                b -> b.setDefaultCredentialsProvider(creds));
        }

        builder.setRequestConfigCallback(b -> b
            .setConnectTimeout(10_000)
            .setSocketTimeout(30_000));

        return builder.build();
    }

    private static void closeQuietly(RestClient client) {
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }
}
