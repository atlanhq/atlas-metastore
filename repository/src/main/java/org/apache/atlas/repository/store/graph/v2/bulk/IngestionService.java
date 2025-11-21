package org.apache.atlas.repository.store.graph.v2.bulk;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v2.BulkRequestContext;
import org.apache.atlas.repository.store.graph.v2.EntityMutationService;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.utils.AtlasJson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.apache.atlas.utils.AtlasPerfTracer;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.apache.atlas.service.metrics.MetricUtils;
import org.apache.atlas.RequestContext;
import java.util.Set;

@Service
public class IngestionService {
    private static final Logger LOG = LoggerFactory.getLogger(IngestionService.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("service.IngestionService");
    private static final ObjectMapper mapper = new ObjectMapper();

    private final IngestionDAO ingestionDAO;
    private final EntityMutationService entityMutationService;
    private final ThreadPoolExecutor executorService;

    @Inject
    public IngestionService(IngestionDAO ingestionDAO, EntityMutationService entityMutationService) {
        this.ingestionDAO = ingestionDAO;
        this.entityMutationService = entityMutationService;

        int threadCount = AtlasConfiguration.INGESTION_DUMP_THREADS.getInt();
        int queueSize = AtlasConfiguration.INGESTION_DUMP_QUEUE_SIZE.getInt();

        this.executorService = new ThreadPoolExecutor(
                threadCount,
                threadCount,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueSize),
                new ThreadPoolExecutor.AbortPolicy()
        );
        
        LOG.info("IngestionService initialized with {} threads (cores/2) and queue size {}", threadCount, queueSize);
        MetricUtils.getMeterRegistry().gauge("ingestion.queue.size", executorService.getQueue(), Collection::size);
    }

    public String submit(InputStream payloadStream, Map<String, String> queryParams) throws AtlasBaseException {
        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "IngestionService.submit()");
            }
            String requestId = UUID.randomUUID().toString();
            
            final RequestContext parentContext = RequestContext.get();
            RequestContextDTO contextDTO = new RequestContextDTO(parentContext);
            
            byte[] optionsBytes = AtlasJson.toBytes(queryParams);
            byte[] contextBytes = AtlasJson.toBytes(contextDTO);

            byte[] payload;
            try {
                payload = payloadStream.readAllBytes();
            } catch (IOException e) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Failed to read request body");
            }

            // 1. Persist to Cassandra
            ingestionDAO.save(requestId, payload, optionsBytes, contextBytes);
            LOG.info("Ingestion request {} persisted with status PENDING", requestId);

            // 2. Submit to ThreadPool
            try {
                executorService.submit(() -> processTask(requestId));
                MetricUtils.getMeterRegistry().counter("ingestion.tasks.submitted").increment();
            } catch (Exception e) {
                MetricUtils.getMeterRegistry().counter("ingestion.tasks.rejected").increment();
                LOG.error("Failed to submit task to executor, updating status to FAILED", e);
                ingestionDAO.updateStatus(requestId, "FAILED", "System overloaded: " + e.getMessage());
                throw e;
            }
            
            return requestId;
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    public IngestionDAO.IngestionRequest getStatus(String requestId) throws AtlasBaseException {
        return ingestionDAO.getStatus(requestId);
    }

    private void processTask(String requestId) {
        try {
            // 1. Load context and payload
            IngestionDAO.IngestionPayloadAndContext payloadAndContext = ingestionDAO.getPayloadAndContext(requestId);
            if (payloadAndContext == null || payloadAndContext.getPayload() == null) {
                LOG.error("Payload for request {} not found", requestId);
                ingestionDAO.updateStatus(requestId, "FAILED", "Payload not found");
                return;
            }

            RequestContextDTO contextDTO;
            try {
                contextDTO = AtlasJson.fromBytes(payloadAndContext.getRequestContext(), RequestContextDTO.class);
            } catch (IOException e) {
                LOG.error("Failed to deserialize RequestContextDTO for request {}", requestId, e);
                ingestionDAO.updateStatus(requestId, "FAILED", "Invalid request context format");
                return;
            }

            RequestContext.clear(); // Clean up any stale context from previous runs
            RequestContext.get().setUser(contextDTO.getUser(), contextDTO.getUserGroups());
            RequestContext.get().setClientIPAddress(contextDTO.getClientIPAddress());

            LOG.info("Processing ingestion request {}", requestId);
            ingestionDAO.updateStatus(requestId, "IN_PROGRESS", null);

            byte[] payloadBytes = payloadAndContext.getPayload();
            
            String payloadJson = new String(payloadBytes, StandardCharsets.UTF_8);
            AtlasEntitiesWithExtInfo entities = AtlasType.fromJson(payloadJson, AtlasEntitiesWithExtInfo.class);
            
            // 2. Load Options
            byte[] optionsBytes = payloadAndContext.getRequestOptions();
            Map<String, String> queryParams;
            try {
                queryParams = AtlasJson.fromBytes(optionsBytes, Map.class);
            } catch (IOException e) {
                LOG.error("Failed to deserialize query parameters for request {}", requestId, e);
                ingestionDAO.updateStatus(requestId, "FAILED", "Invalid query parameters format");
                return;
            }

            boolean replaceClassifications = Boolean.parseBoolean(queryParams.getOrDefault("replaceClassifications", "false"));
            boolean replaceTags = Boolean.parseBoolean(queryParams.getOrDefault("replaceTags", "false"));
            boolean appendTags = Boolean.parseBoolean(queryParams.getOrDefault("appendTags", "false"));
            boolean replaceBusinessAttributes = Boolean.parseBoolean(queryParams.getOrDefault("replaceBusinessAttributes", "false"));
            boolean isOverwriteBusinessAttributes = Boolean.parseBoolean(queryParams.getOrDefault("overwriteBusinessAttributes", "false"));

            BulkRequestContext context = new BulkRequestContext.Builder()
                    .setReplaceClassifications(replaceClassifications)
                    .setReplaceTags(replaceTags)
                    .setAppendTags(appendTags)
                    .setReplaceBusinessAttributes(replaceBusinessAttributes)
                    .setOverwriteBusinessAttributes(isOverwriteBusinessAttributes)
                    .build();

            AtlasEntityStream entityStream = new AtlasEntityStream(entities);
            EntityMutationResponse response = entityMutationService.createOrUpdate(entityStream, context);

            // 4. Update success
            byte[] responseBytes = AtlasType.toJson(response).getBytes(StandardCharsets.UTF_8);
            ingestionDAO.updateStatus(requestId, "COMPLETED", responseBytes, null);
            MetricUtils.getMeterRegistry().counter("ingestion.tasks.completed").increment();
            LOG.info("Ingestion request {} completed successfully", requestId);

        } catch (Exception e) {
            MetricUtils.getMeterRegistry().counter("ingestion.tasks.failed").increment();
            LOG.error("Failed to process ingestion request {}", requestId, e);
            try {
                ingestionDAO.updateStatus(requestId, "FAILED", e.getMessage());
            } catch (AtlasBaseException ex) {
                LOG.error("Failed to update failure status for request {}", requestId, ex);
            }
        } finally {
            RequestContext.clear();
        }
    }

    @PreDestroy
    public void shutdown() {
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    // DTO for serializing RequestContext
    private static class RequestContextDTO {
        private String user;
        private Set<String> userGroups;
        private String clientIPAddress;

        // For Jackson
        public RequestContextDTO() {}

        public RequestContextDTO(RequestContext context) {
            this.user = context.getUser();
            this.userGroups = context.getUserGroups();
            this.clientIPAddress = context.getClientIPAddress();
        }

        public String getUser() { return user; }
        public Set<String> getUserGroups() { return userGroups; }
        public String getClientIPAddress() { return clientIPAddress; }
    }
}
