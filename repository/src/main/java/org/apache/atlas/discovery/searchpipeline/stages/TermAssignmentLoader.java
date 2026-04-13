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
package org.apache.atlas.discovery.searchpipeline.stages;

import org.apache.atlas.discovery.searchpipeline.EnrichmentStage;
import org.apache.atlas.discovery.searchpipeline.SearchEnrichmentContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.glossary.relations.AtlasTermAssignmentHeader;
import org.apache.atlas.model.glossary.enums.AtlasTermAssignmentStatus;
import org.apache.atlas.repository.EdgeVertexReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.atlas.glossary.GlossaryUtils.*;
import static org.apache.atlas.repository.Constants.*;

/**
 * Stage 5: Build term assignments from edges already in context — 0 CQL.
 *
 * <p>Current code at {@code mapAssignedTerms()} (EntityGraphRetriever:2582) does:
 * per-entity edge query + per-edge vertex load = N sync edge queries + N×T vertex loads.
 * Code has TODO: "This should be optimized to use vertexEdgePropertiesCache".</p>
 *
 * <p>This stage reads TERM_ASSIGNMENT edges from Stage 2 (already fetched because
 * {@code TypeAwareEdgeLabelResolver} includes {@code TERM_ASSIGNMENT_LABEL → IN}
 * when {@code includeMeanings=true}) and referenced term vertex properties from
 * Stage 3. Assembles {@code AtlasTermAssignmentHeader} entirely from context data.</p>
 */
public class TermAssignmentLoader implements EnrichmentStage {

    private static final Logger LOG = LoggerFactory.getLogger(TermAssignmentLoader.class);

    @Override
    public String name() {
        return "termAssignmentLoad";
    }

    @Override
    public void enrich(SearchEnrichmentContext context) throws AtlasBaseException {
        if (!context.isIncludeMeanings()) {
            return;
        }

        for (String vertexId : context.getOrderedVertexIds()) {
            List<EdgeVertexReference> termEdges = context.getEdgesForLabel(vertexId, TERM_ASSIGNMENT_LABEL);

            if (termEdges.isEmpty()) {
                context.getTermAssignmentMap().put(vertexId, Collections.emptyList());
                continue;
            }

            List<AtlasTermAssignmentHeader> headers = new ArrayList<>(termEdges.size());
            for (EdgeVertexReference edge : termEdges) {
                AtlasTermAssignmentHeader header = new AtlasTermAssignmentHeader();
                String termVertexId = edge.getReferenceVertexId();

                // Term GUID — from referenced vertex properties (Stage 3)
                String termGuid = context.getVertexProperty(termVertexId, GUID_PROPERTY_KEY, String.class);
                if (termGuid != null) {
                    header.setTermGuid(termGuid);
                }

                // Display text — from referenced vertex properties
                // GLOSSARY_TERM_DISPLAY_NAME_ATTR = "name" (private in EntityGraphRetriever)
                String displayName = context.getVertexProperty(termVertexId, "name", String.class);
                if (displayName != null) {
                    header.setDisplayText(displayName);
                }

                // Edge properties — all from edge valueMap (already in memory)
                Map<String, Object> edgeProps = edge.getProperties();
                if (edgeProps != null) {
                    Object relGuid = edgeProps.get(RELATIONSHIP_GUID_PROPERTY_KEY);
                    if (relGuid != null) {
                        header.setRelationGuid(String.valueOf(relGuid));
                    }

                    Object description = edgeProps.get(TERM_ASSIGNMENT_ATTR_DESCRIPTION);
                    if (description != null) {
                        header.setDescription(String.valueOf(description));
                    }

                    Object expression = edgeProps.get(TERM_ASSIGNMENT_ATTR_EXPRESSION);
                    if (expression != null) {
                        header.setExpression(String.valueOf(expression));
                    }

                    Object status = edgeProps.get(TERM_ASSIGNMENT_ATTR_STATUS);
                    if (status != null) {
                        try {
                            header.setStatus(AtlasTermAssignmentStatus.valueOf(String.valueOf(status)));
                        } catch (IllegalArgumentException e) {
                            LOG.warn("Invalid term assignment status '{}' for edge {}", status, edge.getEdgeLabel());
                        }
                    }

                    Object confidence = edgeProps.get(TERM_ASSIGNMENT_ATTR_CONFIDENCE);
                    if (confidence instanceof Integer) {
                        header.setConfidence((Integer) confidence);
                    } else if (confidence instanceof Number) {
                        header.setConfidence(((Number) confidence).intValue());
                    }

                    Object createdBy = edgeProps.get(TERM_ASSIGNMENT_ATTR_CREATED_BY);
                    if (createdBy != null) {
                        header.setCreatedBy(String.valueOf(createdBy));
                    }

                    Object steward = edgeProps.get(TERM_ASSIGNMENT_ATTR_STEWARD);
                    if (steward != null) {
                        header.setSteward(String.valueOf(steward));
                    }

                    Object source = edgeProps.get(TERM_ASSIGNMENT_ATTR_SOURCE);
                    if (source != null) {
                        header.setSource(String.valueOf(source));
                    }
                }

                headers.add(header);
            }

            context.getTermAssignmentMap().put(vertexId, headers);
        }

        if (LOG.isDebugEnabled()) {
            int totalTerms = context.getTermAssignmentMap().values().stream().mapToInt(List::size).sum();
            LOG.debug("TermAssignmentLoader: {} total term assignments across {} vertices (0 CQL)",
                    totalTerms, context.getOrderedVertexIds().size());
        }
    }
}
