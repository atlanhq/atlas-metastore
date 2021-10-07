/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.model.glossary;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.atlas.model.AtlasBaseModelObject;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Map;
import java.util.Set;


public abstract class AtlasGlossaryBaseObject extends AtlasBaseModelObject {

    protected static Set<String> excludeFromOtherAttributes = new HashSet<String>() {{
        add("qualifiedName");
        add("name");
        add("shortDescription");
        add("longDescription");
        add("additionalAttributes");
        add("examples");
        add("abbreviation");
        add("usage");
        add("language");
        add("guid");
    }};

    private Map<String, Object> otherAttributes;

    // Core attributes
    private String qualifiedName;
    protected String name;
    protected String shortDescription;
    protected String longDescription;
    private Map<String, Object> additionalAttributes;

    // Classifications
    protected List<AtlasClassification> classifications;

    public AtlasGlossaryBaseObject() {
    }

    public AtlasGlossaryBaseObject(final AtlasGlossaryBaseObject other) {
        super(other);
        this.name = other.name;
        this.shortDescription = other.shortDescription;
        this.longDescription = other.longDescription;
        this.classifications = other.classifications;
        this.qualifiedName = other.qualifiedName;
        this.additionalAttributes = other.additionalAttributes;
        this.otherAttributes = other.otherAttributes;
    }

    @JsonAnyGetter
    public Map<String, Object> getOtherAttributes() {
        return otherAttributes;
    }

    @JsonAnySetter
    public void setOtherAttribute(String propertyKey, Object value) {
        if (this.otherAttributes == null) {
            this.otherAttributes = new HashMap<>();
        }
        this.otherAttributes.put(propertyKey, value);
    }

    public void setOtherAttributes(Map<String, Object> attributes) {
        if (this.otherAttributes == null) {
            this.otherAttributes = new HashMap<>();
        }

        for (String key : attributes.keySet()) {
            if (!excludeFromOtherAttributes.contains(key)) {
                this.otherAttributes.put(key, attributes.get(key));
            }
        }
    }

    public void setEntityAttributes(AtlasEntity atlasEntity, Map<String, Object> attributes) {
        if (this.otherAttributes == null) {
            this.otherAttributes = new HashMap<>();
        }

        if (MapUtils.isNotEmpty(attributes)) {
            for (String key : attributes.keySet()) {
                if (!atlasEntity.getAttributes().containsKey(key)) {
                    atlasEntity.setAttribute(key, attributes.get(key));
                }
            }
        }
    }

    public Map<String, Object> getAdditionalAttributes() {
        return additionalAttributes;
    }

    public void setAdditionalAttributes(Map<String, Object> additionalAttributes) {
        this.additionalAttributes = additionalAttributes;
    }

    public String getQualifiedName() {
        return qualifiedName;
    }

    public void setQualifiedName(final String qualifiedName) {
        this.qualifiedName = qualifiedName;
    }

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public String getShortDescription() {
        return shortDescription;
    }

    public void setShortDescription(final String shortDescription) {
        this.shortDescription = shortDescription;
    }

    public String getLongDescription() {
        return longDescription;
    }

    public void setLongDescription(final String longDescription) {
        this.longDescription = longDescription;
    }

    abstract public void setAttribute(String attrName, String attrVal);

    public List<AtlasClassification> getClassifications() {
        return classifications;
    }

    public void setClassifications(final List<AtlasClassification> classifications) {
        this.classifications = classifications;
    }

    @JsonIgnore
    public void addClassification(AtlasClassification classification) {
        List<AtlasClassification> classifications = this.classifications;
        if (classifications == null) {
            classifications = new ArrayList<>();
        }
        classifications.add(classification);
        setClassifications(classifications);
    }

    @JsonIgnore
    public void removeClassification(AtlasClassification classification) {
        if (CollectionUtils.isNotEmpty(classifications)) {
            classifications.remove(classification);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        AtlasGlossaryBaseObject that = (AtlasGlossaryBaseObject) o;
        return Objects.equals(qualifiedName, that.qualifiedName) &&
                Objects.equals(name, that.name) &&
                Objects.equals(shortDescription, that.shortDescription) &&
                Objects.equals(longDescription, that.longDescription) &&
                Objects.equals(additionalAttributes, that.additionalAttributes) &&
                Objects.equals(classifications, that.classifications) &&
                Objects.equals(otherAttributes, that.otherAttributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), qualifiedName, name, shortDescription, longDescription, additionalAttributes, classifications, otherAttributes);
    }

    @Override
    protected StringBuilder toString(final StringBuilder sb) {
        sb.append(", qualifiedName='").append(qualifiedName).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", shortDescription='").append(shortDescription).append('\'');
        sb.append(", longDescription='").append(longDescription).append('\'');
        sb.append(", classifications=").append(classifications).append('\'');
        sb.append(", additionalAttributes=").append(additionalAttributes);
        sb.append(", otherAttributes=").append(otherAttributes);

        return sb;
    }
}
