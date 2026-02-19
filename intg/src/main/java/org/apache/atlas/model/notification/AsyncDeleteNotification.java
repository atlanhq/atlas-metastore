/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.model.notification;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.apache.atlas.model.instance.AtlasObjectId;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * Notification message for async delete requests.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class AsyncDeleteNotification implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Type of delete endpoint.
     */
    public enum DeleteEndpointType {
        GUID_DELETE,              // DELETE /entity/guid/{guid}
        BULK_GUID_DELETE,         // DELETE /entity/bulk?guid=...
        UNIQUE_ATTR_DELETE,       // DELETE /entity/uniqueAttribute/type/{type}
        BULK_UNIQUE_ATTR_DELETE   // DELETE /entity/bulk/uniqueAttribute
    }

    // Request identification
    private String requestId;
    private DeleteEndpointType endpointType;
    private long timestamp;
    private String requestedBy;

    // Retry tracking
    private int retryCount;
    private int maxRetries;
    private String lastError;

    // Request payload
    private DeletePayload payload;

    public AsyncDeleteNotification() {
    }

    public AsyncDeleteNotification(String requestId, DeleteEndpointType endpointType, long timestamp,
                                   String requestedBy, int maxRetries, DeletePayload payload) {
        this.requestId = requestId;
        this.endpointType = endpointType;
        this.timestamp = timestamp;
        this.requestedBy = requestedBy;
        this.retryCount = 0;
        this.maxRetries = maxRetries;
        this.payload = payload;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public DeleteEndpointType getEndpointType() {
        return endpointType;
    }

    public void setEndpointType(DeleteEndpointType endpointType) {
        this.endpointType = endpointType;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getRequestedBy() {
        return requestedBy;
    }

    public void setRequestedBy(String requestedBy) {
        this.requestedBy = requestedBy;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public String getLastError() {
        return lastError;
    }

    public void setLastError(String lastError) {
        this.lastError = lastError;
    }

    public DeletePayload getPayload() {
        return payload;
    }

    public void setPayload(DeletePayload payload) {
        this.payload = payload;
    }

    public void normalize() { }

    @Override
    public String toString() {
        return toString(new StringBuilder()).toString();
    }

    public StringBuilder toString(StringBuilder sb) {
        if (sb == null) {
            sb = new StringBuilder();
        }

        sb.append("AsyncDeleteNotification{");
        sb.append("requestId='").append(requestId).append('\'');
        sb.append(", endpointType=").append(endpointType);
        sb.append(", timestamp=").append(timestamp);
        sb.append(", requestedBy='").append(requestedBy).append('\'');
        sb.append(", retryCount=").append(retryCount);
        sb.append(", maxRetries=").append(maxRetries);
        sb.append(", lastError='").append(lastError).append('\'');
        sb.append(", payload=").append(payload);
        sb.append('}');

        return sb;
    }

    /**
     * Payload for delete request containing entity identifiers.
     */
    @JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown=true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.PROPERTY)
    public static class DeletePayload implements Serializable {
        private static final long serialVersionUID = 1L;

        // For GUID_DELETE and BULK_GUID_DELETE
        private List<String> guids;

        // For UNIQUE_ATTR_DELETE
        private String typeName;
        private Map<String, Object> uniqueAttributes;

        // For BULK_UNIQUE_ATTR_DELETE
        private List<AtlasObjectId> objectIds;

        // Optional flags
        private boolean skipHasLineageCalculation;

        public DeletePayload() {
        }

        public List<String> getGuids() {
            return guids;
        }

        public void setGuids(List<String> guids) {
            this.guids = guids;
        }

        public String getTypeName() {
            return typeName;
        }

        public void setTypeName(String typeName) {
            this.typeName = typeName;
        }

        public Map<String, Object> getUniqueAttributes() {
            return uniqueAttributes;
        }

        public void setUniqueAttributes(Map<String, Object> uniqueAttributes) {
            this.uniqueAttributes = uniqueAttributes;
        }

        public List<AtlasObjectId> getObjectIds() {
            return objectIds;
        }

        public void setObjectIds(List<AtlasObjectId> objectIds) {
            this.objectIds = objectIds;
        }

        public boolean isSkipHasLineageCalculation() {
            return skipHasLineageCalculation;
        }

        public void setSkipHasLineageCalculation(boolean skipHasLineageCalculation) {
            this.skipHasLineageCalculation = skipHasLineageCalculation;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("DeletePayload{");
            sb.append("guids=").append(guids);
            sb.append(", typeName='").append(typeName).append('\'');
            sb.append(", uniqueAttributes=").append(uniqueAttributes);
            sb.append(", objectIds=").append(objectIds);
            sb.append(", skipHasLineageCalculation=").append(skipHasLineageCalculation);
            sb.append('}');
            return sb.toString();
        }
    }
}
