package org.apache.atlas.repository.graphdb.migrator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * Pushes graph analysis metrics to Mixpanel via direct HTTP API.
 *
 * Two endpoints:
 *   1. /engage with $set — idempotent profile update (re-running overwrites properties, no duplicates)
 *   2. /import with $insert_id — event tracking with dedup (same domain + same date = one event)
 *
 * Required env vars:
 *   MIXPANEL_TOKEN      — project token (required for both /engage and /import)
 *   MIXPANEL_API_SECRET — API secret for /import auth (optional; event send is skipped if missing)
 *   DOMAIN_NAME         — tenant short name, used as $distinct_id
 */
public class MixpanelReporter {

    private static final Logger LOG = LoggerFactory.getLogger(MixpanelReporter.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String ENGAGE_URL = "https://api.mixpanel.com/engage";
    private static final String IMPORT_URL = "https://api.mixpanel.com/import";

    private final String projectToken;
    private final String apiSecret;
    private final HttpClient httpClient;

    public MixpanelReporter(String projectToken, String apiSecret) {
        this.projectToken = Objects.requireNonNull(projectToken, "projectToken");
        this.apiSecret = apiSecret; // nullable — /import is skipped if null
        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();
    }

    /**
     * Push analysis report to Mixpanel.
     * 1. Updates user profile via /engage $set (idempotent — always overwrites)
     * 2. Sends event via /import with $insert_id (deduped by domain + date)
     */
    public void send(AnalysisReport report) {
        Map<String, Object> properties = report.toFlatMap();
        String distinctId = report.getDomainName();

        if (distinctId == null || distinctId.isEmpty()) {
            LOG.error("Cannot send to Mixpanel: domain name is null/empty");
            return;
        }

        LOG.info("Sending analysis report to Mixpanel for domain '{}'...", distinctId);

        // 1. Profile update via /engage $set (idempotent — always overwrites)
        boolean profileOk = sendProfileUpdate(distinctId, properties);

        // 2. Event via /import with $insert_id (deduped by domain + date)
        boolean eventOk = false;
        if (apiSecret != null && !apiSecret.isEmpty()) {
            eventOk = sendEvent(distinctId, "metastore_graph_analysis_report", properties);
        } else {
            LOG.info("Skipping Mixpanel event — MIXPANEL_API_SECRET not set (profile update only)");
        }

        LOG.info("Mixpanel send complete: profile={}, event={}",
            profileOk ? "OK" : "FAILED", eventOk ? "OK" : "SKIPPED/FAILED");
    }

    /**
     * Update user profile via /engage $set.
     * Idempotent — re-running overwrites all properties with latest values.
     * Auth: project token in payload (no header needed).
     */
    private boolean sendProfileUpdate(String distinctId, Map<String, Object> properties) {
        try {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("$token", projectToken);
            payload.put("$distinct_id", distinctId);
            payload.put("$set", properties);

            String body = MAPPER.writeValueAsString(List.of(payload));

            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(ENGAGE_URL))
                .header("Content-Type", "application/json")
                .header("Accept", "text/plain")
                .timeout(Duration.ofSeconds(30))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

            HttpResponse<String> response = httpClient.send(request,
                HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200 && "1".equals(response.body().trim())) {
                LOG.info("Mixpanel profile updated for '{}'", distinctId);
                return true;
            } else {
                LOG.error("Mixpanel /engage failed: status={}, response={}",
                    response.statusCode(), response.body());
                return false;
            }
        } catch (Exception e) {
            LOG.error("Failed to send Mixpanel profile update: {}", e.getMessage(), e);
            return false;
        }
    }

    /**
     * Send event via /import with $insert_id for dedup.
     * Same domain + same date = same event (no duplicates on re-run).
     * Auth: Basic auth with API secret as username, empty password.
     */
    private boolean sendEvent(String distinctId, String eventName,
                              Map<String, Object> properties) {
        try {
            // Dedup key: domain only — one event per tenant, latest wins
            // (within Mixpanel's 5-day $insert_id dedup window)
            String insertId = distinctId + "_graph_analysis";

            Map<String, Object> eventProps = new LinkedHashMap<>(properties);
            eventProps.put("distinct_id", distinctId);
            eventProps.put("time", Instant.now().getEpochSecond());
            eventProps.put("$insert_id", insertId);
            eventProps.put("token", projectToken);

            Map<String, Object> event = new LinkedHashMap<>();
            event.put("event", eventName);
            event.put("properties", eventProps);

            String body = MAPPER.writeValueAsString(List.of(event));

            // Basic auth: API secret as username, empty password
            String auth = Base64.getEncoder().encodeToString(
                (apiSecret + ":").getBytes(StandardCharsets.UTF_8));

            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(IMPORT_URL))
                .header("Content-Type", "application/json")
                .header("Authorization", "Basic " + auth)
                .header("Accept", "application/json")
                .timeout(Duration.ofSeconds(30))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

            HttpResponse<String> response = httpClient.send(request,
                HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                LOG.info("Mixpanel event '{}' sent for '{}' (insert_id={})",
                    eventName, distinctId, insertId);
                return true;
            } else {
                LOG.error("Mixpanel /import failed: status={}, response={}",
                    response.statusCode(), response.body());
                return false;
            }
        } catch (Exception e) {
            LOG.error("Failed to send Mixpanel event: {}", e.getMessage(), e);
            return false;
        }
    }

    /**
     * Create from environment variables. Returns null if MIXPANEL_TOKEN is not set.
     */
    public static MixpanelReporter fromEnv() {
        String token = System.getenv("MIXPANEL_TOKEN");
        if (token == null || token.isEmpty()) {
            return null;
        }
        String secret = System.getenv("MIXPANEL_API_SECRET");
        return new MixpanelReporter(token, secret);
    }
}
