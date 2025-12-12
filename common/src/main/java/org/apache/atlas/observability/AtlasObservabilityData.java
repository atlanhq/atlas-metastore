package org.apache.atlas.observability;

import java.util.List;
import java.util.Map;

/**
 * Simple data model for Atlas observability metrics.
 */
public class AtlasObservabilityData {
    
    // Request info
    private String traceId;
    private String xAtlanAgentId;
    private String xAtlanClientOrigin;
    private long timestamp;
    private long duration;
    private String implementation = "v1"; // v1 or v2
    
    // Payload info
    private int payloadAssetSize;
    private long payloadRequestBytes;
    private List<String> assetGuids;
    private List<String> vertexIds;
    
    // Array relationships
    private Map<String, Integer> relationshipAttributes;
    private Map<String, Integer> appendRelationshipAttributes;
    private Map<String, Integer> removeRelationshipAttributes;
    private int totalArrayRelationships;
    
    // Array attributes (non-relationship arrays)
    private Map<String, Integer> arrayAttributes;
    private int totalArrayAttributes;
    
    // Timing
    private long diffCalcTime;
    private long lineageCalcTime;
    private long validationTime;
    private long ingestionTime;
    private long notificationTime;
    private long auditLogTime;
    
    // Delete Timing
    private long lookupVerticesTime;
    private long authorizationCheckTime;
    private long preprocessingTime;
    private long removeHasLineageTime;
    private long deleteEntitiesTime;
    private long deleteTypeVertexTime;
    private long termCleanupTime;
    private long esIndexingTime;
    private int deleteEntitiesCount;

    public AtlasObservabilityData() {
        this.timestamp = System.currentTimeMillis();
    }
    
    public AtlasObservabilityData(String traceId, String xAtlanAgentId, String xAtlanClientOrigin) {
        this();
        this.traceId = traceId;
        this.xAtlanAgentId = xAtlanAgentId;
        this.xAtlanClientOrigin = xAtlanClientOrigin;
    }
    
    // Getters and setters
    public String getTraceId() { return traceId; }
    
    public String getXAtlanClientOrigin() { return xAtlanClientOrigin; }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    
    public long getDuration() { return duration; }
    public void setDuration(long duration) { this.duration = duration; }

    public String getImplementation() { return implementation; }
    public void setImplementation(String implementation) { this.implementation = implementation; }
    
    public int getPayloadAssetSize() { return payloadAssetSize; }
    public void setPayloadAssetSize(int payloadAssetSize) { this.payloadAssetSize = payloadAssetSize; }
    
    public long getPayloadRequestBytes() { return payloadRequestBytes; }
    public void setPayloadRequestBytes(long payloadRequestBytes) { this.payloadRequestBytes = payloadRequestBytes; }
    
    public List<String> getAssetGuids() { return assetGuids; }
    public void setAssetGuids(List<String> assetGuids) { this.assetGuids = assetGuids; }
    
    public List<String> getVertexIds() { return vertexIds; }
    public void setVertexIds(List<String> vertexIds) { this.vertexIds = vertexIds; }
    
    public Map<String, Integer> getRelationshipAttributes() { return relationshipAttributes; }
    public void setRelationshipAttributes(Map<String, Integer> relationshipAttributes) { this.relationshipAttributes = relationshipAttributes; }
    
    public Map<String, Integer> getAppendRelationshipAttributes() { return appendRelationshipAttributes; }
    public void setAppendRelationshipAttributes(Map<String, Integer> appendRelationshipAttributes) { this.appendRelationshipAttributes = appendRelationshipAttributes; }
    
    public Map<String, Integer> getRemoveRelationshipAttributes() { return removeRelationshipAttributes; }
    public void setRemoveRelationshipAttributes(Map<String, Integer> removeRelationshipAttributes) { this.removeRelationshipAttributes = removeRelationshipAttributes; }
    
    public int getTotalArrayRelationships() { return totalArrayRelationships; }
    public void setTotalArrayRelationships(int totalArrayRelationships) { this.totalArrayRelationships = totalArrayRelationships; }
    
    public long getDiffCalcTime() { return diffCalcTime; }
    public void setDiffCalcTime(long diffCalcTime) { this.diffCalcTime = diffCalcTime; }
    
    public long getLineageCalcTime() { return lineageCalcTime; }
    public void setLineageCalcTime(long lineageCalcTime) { this.lineageCalcTime = lineageCalcTime; }
    
    public long getValidationTime() { return validationTime; }
    public void setValidationTime(long validationTime) { this.validationTime = validationTime; }
    
    public long getIngestionTime() { return ingestionTime; }
    public void setIngestionTime(long ingestionTime) { this.ingestionTime = ingestionTime; }
    
    public long getNotificationTime() { return notificationTime; }
    public void setNotificationTime(long notificationTime) { this.notificationTime = notificationTime; }
    
    public long getAuditLogTime() { return auditLogTime; }
    public void setAuditLogTime(long auditLogTime) { this.auditLogTime = auditLogTime; }
    
    public Map<String, Integer> getArrayAttributes() { return arrayAttributes; }
    public void setArrayAttributes(Map<String, Integer> arrayAttributes) { this.arrayAttributes = arrayAttributes; }
    
    public int getTotalArrayAttributes() { return totalArrayAttributes; }
    public void setTotalArrayAttributes(int totalArrayAttributes) { this.totalArrayAttributes = totalArrayAttributes; }

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

    public long getEsIndexingTime() { return esIndexingTime; }
    public void setEsIndexingTime(long esIndexingTime) { this.esIndexingTime = esIndexingTime; }

    public int getDeleteEntitiesCount() { return deleteEntitiesCount; }
    public void setDeleteEntitiesCount(int deleteEntitiesCount) { this.deleteEntitiesCount = deleteEntitiesCount; }
}
