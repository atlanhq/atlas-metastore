package org.apache.atlas.audit.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Date;

public class ExternalAuditEvent extends AuditEventBase {

    @JsonProperty("event.type")
    private String eventType = "authz";

    @JsonProperty("event.scope")
    private String eventScope;

    @JsonProperty("event.action")
    private String action;

    @JsonProperty("user.id")
    private String userId;

    @JsonProperty("net.peer.ip")
    private String clientIP;

    @JsonProperty("event.details")
    private EventDetails eventDetails = new EventDetails();

    @JsonProperty("timestamp")
    private Date eventTime = new Date();

    private long eventDurationMS = 0;
    private long eventCount;

    public static class EventDetails {
        @JsonProperty("user.name")
        private String userName;

        @JsonProperty("event.asset")
        private String asset;

        @JsonProperty("event.operation")
        private String operation;

        @JsonProperty("event.policyId")
        private String policyId;

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }

        public String getAsset() {
            return asset;
        }

        public void setAsset(String asset) {
            this.asset = asset;
        }

        public String getOperation() {
            return operation;
        }

        public void setOperation(String operation) {
            this.operation = operation;
        }

        public String getPolicyId() {
            return policyId;
        }

        public void setPolicyId(String policyId) {
            this.policyId = policyId;
        }
    }

    public ExternalAuditEvent() {
        super();
    }

    /**
     * Creates an ExternalAuditEvent from an AuthzAuditEvent
     * @param authzEvent The AuthzAuditEvent to convert from
     */
    public ExternalAuditEvent(AuthzAuditEvent authzEvent) {
        super();
        if (authzEvent != null) {
            this.clientIP = authzEvent.getClientIP();
            this.eventTime = authzEvent.getEventTime();
            this.eventDurationMS = authzEvent.getEventDurationMS();
            this.action = authzEvent.getAccessResult() == 1 ? "AUTHORIZED" : "UNAUTHORIZED";
            this.userId = authzEvent.getUser();
            this.eventScope = authzEvent.getAgentId();
            
            // Set event details
            EventDetails details = new EventDetails();
            details.setUserName(authzEvent.getUser());
            details.setAsset(authzEvent.getEntityGuid() != null ? authzEvent.getEntityGuid() : authzEvent.getResourcePath());
            details.setOperation(authzEvent.getAction());
            details.setPolicyId(authzEvent.getPolicyId());
            this.eventDetails = details;
        }
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getEventScope() {
        return eventScope;
    }

    public void setEventScope(String eventScope) {
        this.eventScope = eventScope;
    }

    public long getEventCount() {
        return eventCount;
    }

    public String getAction() {
        return action;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getClientIP() {
        return clientIP;
    }

    public void setClientIP(String clientIP) {
        this.clientIP = clientIP;
    }

    public EventDetails eventDetails() {
        return eventDetails;
    }

    public void setEventDetails(EventDetails eventDetails) {
        this.eventDetails = eventDetails;
    }

    @Override
    public String getEventKey() {
        return userId + "^" + action + "^" + eventDetails.getAsset() + "^" + eventDetails.getOperation() + "^" + eventDetails.getPolicyId() + "^" + clientIP;
    }

    @Override
    public Date getEventTime() {
        return eventTime;
    }

    @Override
    public void setEventCount(long eventCount) {
        this.eventCount = eventCount;
    }

    public void setEventTime(Date eventTime) {
        this.eventTime = eventTime;
    }

    @Override
    public void setEventDurationMS(long eventDurationMS) {
        this.eventDurationMS = eventDurationMS;
    }

    public long getEventDurationMS() {
        return eventDurationMS;
    }
}
