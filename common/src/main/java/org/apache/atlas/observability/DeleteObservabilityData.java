package org.apache.atlas.observability;

/**
 * Data model for Atlas Delete observability metrics.
 */
public class DeleteObservabilityData {

    // Request info
    private String traceId;
    private String xAtlanAgentId;
    private String xAtlanClientOrigin;
    private long timestamp;
    private long duration;
    private String implementation = "v1"; // v1 or v2
    
    // Status info
    private String operationStatus;
    private String errorType;
    private String operationName;

    // Delete Timing
    private long lookupVerticesTime;
    private long authorizationCheckTime;
    private long preprocessingTime;
    private long removeHasLineageTime;
    private long deleteEntitiesTime;
    private long deleteTypeVertexTime;
    private long termCleanupTime;
    private long esIndexingTime;
    private long notificationTime;
    
    private int deleteEntitiesCount;

    public DeleteObservabilityData() {
        this.timestamp = System.currentTimeMillis();
    }

    public DeleteObservabilityData(String traceId, String xAtlanAgentId, String xAtlanClientOrigin) {
        this();
        this.traceId = traceId;
        this.xAtlanAgentId = xAtlanAgentId;
        this.xAtlanClientOrigin = xAtlanClientOrigin;
    }

    // Common getters and setters
    public String getTraceId() { return traceId; }
    public void setTraceId(String traceId) { this.traceId = traceId; }
    
    public String getXAtlanAgentId() { return xAtlanAgentId; }
    public void setXAtlanAgentId(String xAtlanAgentId) { this.xAtlanAgentId = xAtlanAgentId; }
    
    public String getXAtlanClientOrigin() { return xAtlanClientOrigin; }
    public void setXAtlanClientOrigin(String xAtlanClientOrigin) { this.xAtlanClientOrigin = xAtlanClientOrigin; }
    
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    
    public long getDuration() { return duration; }
    public void setDuration(long duration) { this.duration = duration; }

    public String getImplementation() { return implementation; }
    public void setImplementation(String implementation) { this.implementation = implementation; }
    
    public String getOperationStatus() { return operationStatus; }
    public void setOperationStatus(String operationStatus) { this.operationStatus = operationStatus; }
    
    public String getErrorType() { return errorType; }
    public void setErrorType(String errorType) { this.errorType = errorType; }
    
    public String getOperationName() { return operationName; }
    public void setOperationName(String operationName) { this.operationName = operationName; }

    // Delete metrics getters and setters
    public long getLookupVerticesTime() { return lookupVerticesTime; }
    public void setLookupVerticesTime(long lookupVerticesTime) { this.lookupVerticesTime = lookupVerticesTime; }

    public long getAuthorizationCheckTime() { return authorizationCheckTime; }
    public void setAuthorizationCheckTime(long authorizationCheckTime) { this.authorizationCheckTime = authorizationCheckTime; }

    public long getPreprocessingTime() { return preprocessingTime; }
    public void setPreprocessingTime(long preprocessingTime) { this.preprocessingTime = preprocessingTime; }

    public long getRemoveHasLineageTime() { return removeHasLineageTime; }
    public void setRemoveHasLineageTime(long removeHasLineageTime) { this.removeHasLineageTime = removeHasLineageTime; }

    public long getDeleteEntitiesTime() { return deleteEntitiesTime; }
    public void setDeleteEntitiesTime(long deleteEntitiesTime) { this.deleteEntitiesTime = deleteEntitiesTime; }

    public long getDeleteTypeVertexTime() { return deleteTypeVertexTime; }
    public void setDeleteTypeVertexTime(long deleteTypeVertexTime) { this.deleteTypeVertexTime = deleteTypeVertexTime; }
    public void addDeleteTypeVertexTime(long time) { this.deleteTypeVertexTime += time; }

    public long getTermCleanupTime() { return termCleanupTime; }
    public void setTermCleanupTime(long termCleanupTime) { this.termCleanupTime = termCleanupTime; }

    public long getNotificationTime() { return notificationTime; }
    public void setNotificationTime(long notificationTime) { this.notificationTime = notificationTime; }

    public int getDeleteEntitiesCount() { return deleteEntitiesCount; }
    public void setDeleteEntitiesCount(int deleteEntitiesCount) { this.deleteEntitiesCount = deleteEntitiesCount; }
}
