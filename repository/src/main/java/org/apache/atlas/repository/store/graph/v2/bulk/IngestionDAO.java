package org.apache.atlas.repository.store.graph.v2.bulk;

import org.apache.atlas.exception.AtlasBaseException;

public interface IngestionDAO {

    void save(String requestId, byte[] payload, String requestOptions) throws AtlasBaseException;

    void updateStatus(String requestId, String status, String errorMessage) throws AtlasBaseException;

    void updateStatus(String requestId, String status, byte[] resultSummary, String errorMessage) throws AtlasBaseException;

    IngestionRequest getStatus(String requestId) throws AtlasBaseException;

    byte[] getPayload(String requestId) throws AtlasBaseException;

    String getRequestOptions(String requestId) throws AtlasBaseException;

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
}
