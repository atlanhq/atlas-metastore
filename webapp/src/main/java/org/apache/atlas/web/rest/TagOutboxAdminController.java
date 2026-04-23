package org.apache.atlas.web.rest;

import org.apache.atlas.repository.assetsync.OutboxEntry;
import org.apache.atlas.repository.assetsync.OutboxEntryId;
import org.apache.atlas.repository.assetsync.EntityGuidRef;
import org.apache.atlas.repository.tagoutbox.TagOutbox;
import org.apache.atlas.repository.tagoutbox.TagOutboxConfig;
import org.apache.atlas.repository.tagoutbox.TagOutboxService;
import org.apache.atlas.web.util.Servlets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Admin/operations REST surface for the tag-outbox subsystem.
 *
 * <p>Three endpoints under {@code /api/meta/tag-outbox/}:</p>
 * <ul>
 *     <li>{@code GET  /status} — JSON summary of subsystem state (enabled, started,
 *         keyspace/table names, storage counts, oldest pending age).</li>
 *     <li>{@code GET  /entries?status={PENDING|FAILED}&limit=N} — list of outbox
 *         entries in a given partition with their metadata.</li>
 *     <li>{@code POST /entries/{guid}/retry} — promote a FAILED row back to PENDING
 *         for immediate relay pickup. Idempotent.</li>
 * </ul>
 *
 * <p>Auth: inherits the same DLQAdminController-style pattern — controller is
 * reachable via the standard webapp authorization pipeline. No additional
 * per-method auth annotations; deployment handles admin gating externally.</p>
 *
 * <p>Null-safe: every endpoint short-circuits with a clear JSON payload when
 * the subsystem hasn't started (e.g. disabled via
 * {@code atlas.tag.outbox.enabled=false} or startup failed).</p>
 */
@Path("meta/tag-outbox")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class TagOutboxAdminController {

    /** Default result page size for list endpoints. */
    private static final int DEFAULT_LIMIT = 50;
    /** Hard cap on list endpoint page size to keep memory bounded. */
    private static final int MAX_LIMIT     = 500;

    @Autowired
    private TagOutboxService service;

    /**
     * Overall subsystem health + storage snapshot. Cheap (single Cassandra
     * partition scan for counts) — safe to poll from dashboards.
     */
    @GET
    @Path("/status")
    public Map<String, Object> getStatus() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("subsystem", "tag-outbox");
        out.put("started", service.isStarted());

        TagOutbox outbox = service.getOutbox();
        TagOutboxConfig cfg = service.getActiveConfig();

        if (cfg != null) {
            Map<String, Object> config = new LinkedHashMap<>();
            config.put("enabled",            cfg.enabled());
            config.put("reconcilerEnabled", cfg.reconcilerEnabled());
            config.put("keyspace",           cfg.keyspace());
            config.put("outboxTableName",    cfg.outboxTableName());
            config.put("leaseTableName",     cfg.leaseTableName());
            config.put("leaseName",          cfg.leaseName());
            config.put("maxAttempts",        cfg.maxAttempts());
            out.put("config", config);
        }

        if (outbox != null) {
            TagOutbox.StorageStats stats = outbox.computeStorageStats();
            Map<String, Object> storage = new LinkedHashMap<>();
            storage.put("pendingCount",            stats.pendingCount);
            storage.put("processingCount",         stats.processingCount);
            storage.put("failedCount",             stats.failedCount);
            storage.put("oldestPendingAgeSeconds", stats.oldestPendingAgeSeconds);
            out.put("storage", storage);
        }

        return out;
    }

    /**
     * List outbox entries in a given partition.
     *
     * @param status {@code "PENDING"} or {@code "FAILED"}; defaults to {@code FAILED}
     *               since that's the partition operators normally inspect.
     * @param limit  page size, clamped to {@value #MAX_LIMIT}.
     */
    @GET
    @Path("/entries")
    public Map<String, Object> listEntries(@QueryParam("status") @DefaultValue("FAILED") String status,
                                           @QueryParam("limit")  @DefaultValue("50") int limit) {
        TagOutbox outbox = service.getOutbox();
        if (outbox == null) {
            return notStartedResponse();
        }
        int clampedLimit = Math.max(1, Math.min(limit, MAX_LIMIT));

        List<OutboxEntry<EntityGuidRef>> entries;
        String normalized = status == null ? "FAILED" : status.trim().toUpperCase();
        if (TagOutbox.STATUS_FAILED.equals(normalized)) {
            entries = outbox.scanFailed(clampedLimit);
        } else if (TagOutbox.STATUS_PENDING.equals(normalized)) {
            // "Stuck PENDING" view — rows whose last-attempted is older than
            // the reconciler's stuck-threshold. For PENDING in general this
            // partition is typically empty so scan with a near-zero threshold.
            long stuckSec = service.getActiveConfig() != null
                    ? service.getActiveConfig().reconcilerStuckPendingThresholdSeconds() : 0L;
            entries = outbox.scanStuckPending(Duration.ofSeconds(stuckSec), clampedLimit);
        } else {
            Map<String, Object> err = new LinkedHashMap<>();
            err.put("error", "Invalid status — must be PENDING or FAILED (got '" + status + "')");
            return err;
        }

        List<Map<String, Object>> rows = new ArrayList<>(entries.size());
        for (OutboxEntry<EntityGuidRef> e : entries) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("entityGuid",      e.getPayload() != null ? e.getPayload().getEntityGuid() : null);
            row.put("attemptCount",    e.getAttemptCount());
            row.put("createdAt",       e.getCreatedAt() != null ? e.getCreatedAt().toString() : null);
            row.put("lastAttemptedAt", e.getLastAttemptedAt() != null ? e.getLastAttemptedAt().toString() : null);
            rows.add(row);
        }

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("status",  normalized);
        out.put("count",   rows.size());
        out.put("limit",   clampedLimit);
        out.put("entries", rows);
        return out;
    }

    /**
     * Promote a FAILED row back to PENDING so the relay picks it up. Safe to
     * call for non-existent GUIDs (a fresh PENDING row is created; relay
     * replay is idempotent).
     */
    @POST
    @Path("/entries/{guid}/retry")
    public Response retryEntry(@PathParam("guid") String guid) {
        TagOutbox outbox = service.getOutbox();
        if (outbox == null) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(notStartedResponse()).build();
        }
        if (guid == null || guid.isEmpty()) {
            Map<String, Object> err = new LinkedHashMap<>();
            err.put("error", "guid path parameter is required");
            return Response.status(Response.Status.BAD_REQUEST).entity(err).build();
        }

        boolean hadFailedRow = outbox.retryFailed(new OutboxEntryId(guid, ""));

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("guid",           guid);
        out.put("hadFailedRow",   hadFailedRow);
        out.put("queuedForRetry", true);
        return Response.ok(out).build();
    }

    private static Map<String, Object> notStartedResponse() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("subsystem", "tag-outbox");
        out.put("started",   false);
        out.put("message",   "TagOutboxService is not running on this pod — check atlas.tag.outbox.enabled and pod logs");
        return out;
    }
}
