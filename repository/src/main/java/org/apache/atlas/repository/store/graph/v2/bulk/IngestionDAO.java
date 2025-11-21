package org.apache.atlas.repository.store.graph.v2.bulk;

import org.apache.atlas.exception.AtlasBaseException;

public interface IngestionDAO {

    void save(String requestId, byte[] payload, byte[] requestOptions, byte[] requestContext) throws AtlasBaseException;

    void updateStatus(String requestId, String status, String errorMessage) throws AtlasBaseException;

    void updateStatus(String requestId, String status, byte[] resultSummary, String errorMessage) throws AtlasBaseException;

    IngestionRequest getStatus(String requestId) throws AtlasBaseException;

    IngestionPayloadAndContext getPayloadAndContext(String requestId) throws AtlasBaseException;

    class IngestionRequest {
        private String requestId;
        private String status;
        private byte[] resultSummary;
        private String errorMessage;
        private long updatedAt;

        public String getRequestId() { return requestId; }
        public void setRequestId(String requestId) { this.requestId = requestId; }
        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }
        public byte[] getResultSummary() { return resultSummary; }
        public void setResultSummary(byte[] resultSummary) { this.resultSummary = resultSummary; }
        public String getErrorMessage() { return errorMessage; }
        public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
        public long getUpdatedAt() { return updatedAt; }
        public void setUpdatedAt(long updatedAt) { this.updatedAt = updatedAt; }
    }

    class IngestionPayloadAndContext {
        private byte[] payload;
        private byte[] requestOptions;
        private byte[] requestContext;

        public byte[] getPayload() { return payload; }
        public void setPayload(byte[] payload) { this.payload = payload; }
        public byte[] getRequestOptions() { return requestOptions; }
        public void setRequestOptions(byte[] requestOptions) { this.requestOptions = requestOptions; }
        public byte[] getRequestContext() { return requestContext; }
        public void setRequestContext(byte[] requestContext) { this.requestContext = requestContext; }
    }
}
