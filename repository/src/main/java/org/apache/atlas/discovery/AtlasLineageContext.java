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
package org.apache.atlas.discovery;

import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.lineage.AtlasLineageInfo;
import org.apache.atlas.model.lineage.AtlasLineageRequest;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static org.apache.atlas.model.lineage.AtlasLineageInfo.LineageDirection.BOTH;

public class AtlasLineageContext {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasLineageContext.class);

    private int depth;
    private int limit;
    private String guid;
    private boolean isDataset;
    private boolean isProcess;
    private boolean hideProcess;
    private boolean skipDeleted;
    private AtlasLineageInfo.LineageDirection direction = BOTH;

    private Set<String> attributes;
    private Predicate predicate;

    private AtlasVertex startDatasetVertex = null;

    public AtlasLineageContext(AtlasLineageRequest lineageRequest, AtlasTypeRegistry typeRegistry) {
        this.guid = lineageRequest.getGuid();
        this.limit = lineageRequest.getLimit();
        this.depth = lineageRequest.getDepth();
        this.direction = lineageRequest.getDirection();
        this.skipDeleted = lineageRequest.isSkipDeleted();
        this.hideProcess = lineageRequest.isHideProcess();
        this.attributes = lineageRequest.getAttributes();

        predicate = constructInMemoryPredicate(typeRegistry, lineageRequest.getEntityFilters());
    }

    public int getDepth() {
        return depth;
    }

    public void setDepth(int depth) {
        this.depth = depth;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public String getGuid() {
        return guid;
    }

    public void setGuid(String guid) {
        this.guid = guid;
    }

    public boolean isDataset() {
        return isDataset;
    }

    public void setDataset(boolean dataset) {
        isDataset = dataset;
    }

    public boolean isProcess() {
        return isProcess;
    }

    public void setProcess(boolean process) {
        isProcess = process;
    }

    public boolean isSkipDeleted() {
        return skipDeleted;
    }

    public void setSkipDeleted(boolean skipDeleted) {
        this.skipDeleted = skipDeleted;
    }

    public AtlasLineageInfo.LineageDirection getDirection() {
        return direction;
    }

    public void setDirection(AtlasLineageInfo.LineageDirection direction) {
        this.direction = direction;
    }

    public boolean isHideProcess() {
        return hideProcess;
    }

    public void setHideProcess(boolean hideProcess) {
        this.hideProcess = hideProcess;
    }

    public Set<String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Set<String> attributes) {
        this.attributes = attributes;
    }

    public AtlasVertex getStartDatasetVertex() {
        return startDatasetVertex;
    }

    public void setStartDatasetVertex(AtlasVertex startDatasetVertex) {
        this.startDatasetVertex = startDatasetVertex;
    }

    protected Predicate constructInMemoryPredicate(AtlasTypeRegistry typeRegistry, SearchParameters.FilterCriteria filterCriteria) {
        LineageSearchProcessor lineageSearchProcessor = new LineageSearchProcessor();
        return lineageSearchProcessor.constructInMemoryPredicate(typeRegistry, filterCriteria);
    }

    protected boolean evaluate(AtlasVertex vertex) {
        if (predicate != null) {
            return predicate.evaluate(vertex);
        }
        return true;
    }

    public boolean shouldApplyLimit() {
        return limit > 0;
    }

    @Override
    public String toString() {
        return "LineageRequestContext{" +
                "depth=" + depth +
                ", guid='" + guid + '\'' +
                ", isDataset=" + isDataset +
                ", isProcess=" + isProcess +
                ", skipDeleted=" + skipDeleted +
                ", direction=" + direction +
                ", attributes=" + attributes +
                '}';
    }
}
