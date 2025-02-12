package org.apache.atlas.model.instance;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import java.io.Serializable;
import java.util.List;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class TaskV2Request implements Serializable {

    String parentTaskVertexId;
    String tagVertexId;
    String assetVertexId;
    ClassificationPropagationType action;

    public TaskV2Request(String parentTaskVertexId, String tagVertexId, String assetVertexId, ClassificationPropagationType action) {
        this.parentTaskVertexId = parentTaskVertexId;
        this.tagVertexId = tagVertexId;
        this.assetVertexId = assetVertexId;
        this.action = action;
    }

    public TaskV2Request() {
    }

    public String getParentTaskVertexId() {
        return parentTaskVertexId;
    }

    public void setParentTaskVertexId(String parentTaskVertexId) {
        this.parentTaskVertexId = parentTaskVertexId;
    }

    public String getTagVertexId() {
        return tagVertexId;
    }

    public void setTagVertexId(String tagVertexId) {
        this.tagVertexId = tagVertexId;
    }

    public String getAssetVertexId() {
        return assetVertexId;
    }

    public void setAssetVertexId(String assetVertexId) {
        this.assetVertexId = assetVertexId;
    }

    public ClassificationPropagationType getAction() {
        return action;
    }

    public void setAction(ClassificationPropagationType action) {
        this.action = action;
    }

    public enum ClassificationPropagationType {

        CLASSIFICATION_PROPAGATION_TEXT_UPDATE("CLASSIFICATION_PROPAGATION_TEXT_UPDATE"),
        CLASSIFICATION_PROPAGATION_ADD("CLASSIFICATION_PROPAGATION_ADD"),
        CLASSIFICATION_PROPAGATION_DELETE("CLASSIFICATION_PROPAGATION_DELETE"),
        CLASSIFICATION_ONLY_PROPAGATION_DELETE("CLASSIFICATION_ONLY_PROPAGATION_DELETE"),
        CLASSIFICATION_ONLY_PROPAGATION_DELETE_ON_HARD_DELETE("CLASSIFICATION_ONLY_PROPAGATION_DELETE_ON_HARD_DELETE"),
        CLASSIFICATION_REFRESH_PROPAGATION("CLASSIFICATION_REFRESH_PROPAGATION"),
        CLASSIFICATION_PROPAGATION_RELATIONSHIP_UPDATE("CLASSIFICATION_PROPAGATION_RELATIONSHIP_UPDATE"),
        CLEANUP_CLASSIFICATION_PROPAGATION("CLEANUP_CLASSIFICATION_PROPAGATION");

        private final String value;

        ClassificationPropagationType(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return value;
        }
    }
}
