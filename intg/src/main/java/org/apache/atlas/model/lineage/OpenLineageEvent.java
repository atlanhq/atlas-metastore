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
package org.apache.atlas.model.lineage;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.io.Serializable;
import java.util.Date;
import java.util.Objects;
import java.util.UUID;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * Simple POJO representing an OpenLineage event stored in Cassandra.
 * This is a standalone model for Cassandra-only storage - no JanusGraph or Elasticsearch.
 */
@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class OpenLineageEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    private UUID eventId;      // UUID - primary key component
    private String source;       // Producer/source system URI
    private String jobName;      // Job name from the event
    private String runId;        // Run ID - partition key
    private Date   eventTime;    // Event timestamp - clustering column
    private String event;        // Full event JSON payload
    private String status;       // Event status (START, RUNNING, COMPLETE, FAIL, ABORT)

    public OpenLineageEvent() {
    }

    public OpenLineageEvent(UUID eventId, String source, String jobName, String runId,
                           Date eventTime, String event, String status) {
        this.eventId = eventId;
        this.source = source;
        this.jobName = jobName;
        this.runId = runId;
        this.eventTime = eventTime;
        this.event = event;
        this.status = status;
    }

    public UUID getEventId() {
        return eventId;
    }

    public void setEventId(UUID eventId) {
        this.eventId = eventId;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getRunId() {
        return runId;
    }

    public void setRunId(String runId) {
        this.runId = runId;
    }

    public Date getEventTime() {
        return eventTime;
    }

    public void setEventTime(Date eventTime) {
        this.eventTime = eventTime;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OpenLineageEvent that = (OpenLineageEvent) o;
        return Objects.equals(eventId, that.eventId) &&
               Objects.equals(runId, that.runId) &&
               Objects.equals(eventTime, that.eventTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, runId, eventTime);
    }

    @Override
    public String toString() {
        return "OpenLineageEvent{" +
                "eventId='" + eventId + '\'' +
                ", source='" + source + '\'' +
                ", jobName='" + jobName + '\'' +
                ", runId='" + runId + '\'' +
                ", eventTime=" + eventTime +
                ", status='" + status + '\'' +
                '}';
    }
}
