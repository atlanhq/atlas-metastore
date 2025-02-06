package org.apache.atlas.repository.migration;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Set;

import static org.apache.atlas.model.instance.AtlasEntity.Status.ACTIVE;
import static org.apache.atlas.model.instance.AtlasEntity.Status.DELETED;
import static org.apache.atlas.repository.Constants.EDGE_LABELS_FOR_HARD_DELETION;
import static org.apache.atlas.repository.Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY;
import static org.apache.atlas.repository.graph.GraphHelper.getStatus;

public class ValidateProductEdgesMigrationService {

    private static final Logger LOG = LoggerFactory.getLogger(ValidateProductEdgesMigrationService.class);

    private final Set<String> productGuids;
    private final GraphHelper graphHelper;

    public ValidateProductEdgesMigrationService(Set<String> productGuids, GraphHelper graphHelper) {
        this.productGuids = productGuids;
        this.graphHelper = graphHelper;
    }

    public boolean validateEdgeMigration() throws AtlasBaseException {
        try {
            int count = 0;
            int totalProductChecked = 0;
            boolean redundantEdgesFound = false;

            for (String productGuid: productGuids) {
                LOG.info("Validating edges for Product: {}", productGuid);

                if (productGuid != null && !productGuid.trim().isEmpty()) {
                    AtlasVertex productVertex = graphHelper.getVertexForGUID(productGuid);

                    if (productVertex == null) {
                        LOG.info("ProductGUID with no vertex found: {}", productGuid);
                    } else {
                        AtlasEntity.Status vertexStatus = getStatus(productVertex);

                        if (ACTIVE.equals(vertexStatus)) {
                            boolean softDeletedEdgesFound = validateEdgeForActiveProduct(productVertex);
                            if (softDeletedEdgesFound) {
                                count++;
                                totalProductChecked++;
                            } else {
                                totalProductChecked++;
                            }
                        }

                        if (DELETED.equals(vertexStatus)) {
                            boolean edgeWithDifferentTimeStampFound = validateEdgeForArchivedProduct(productVertex);
                            if (edgeWithDifferentTimeStampFound) {
                                count++;
                                totalProductChecked++;
                            } else {
                                totalProductChecked++;
                            }
                        }
                    }
                }
            }

            if (count > 0) {
                redundantEdgesFound = true;
                LOG.info("Found {} products with redundant edges....", count);
            }

            LOG.info("Total products checked: {}", totalProductChecked);

            return redundantEdgesFound;
        } catch (Exception e) {
            LOG.error("Error while validating edges for Products: {}", productGuids, e);
            throw new AtlasBaseException(e);
        }
    }

    public boolean validateEdgeForActiveProduct (AtlasVertex productVertex) {
        boolean softDeletedEdgesFound = false;

        try {
            Iterator<AtlasEdge> existingEdges = productVertex.getEdges(AtlasEdgeDirection.BOTH, EDGE_LABELS_FOR_HARD_DELETION).iterator();

            if (existingEdges == null || !existingEdges.hasNext()) {
                LOG.info("No edges found for Product: {}", productVertex);
                return softDeletedEdgesFound;
            }

            while (existingEdges.hasNext()) {
                AtlasEdge edge = existingEdges.next();

                AtlasEntity.Status edgeStatus = getStatus(edge);

                if (DELETED.equals(edgeStatus)) {
                    softDeletedEdgesFound = true;
                }
            }
        } catch (Exception e) {
            LOG.error("Error while validating edges for Active Product: {}", productVertex, e);
            throw new RuntimeException(e);
        }

        return softDeletedEdgesFound;
    }

    public boolean validateEdgeForArchivedProduct (AtlasVertex productVertex) {
        boolean edgeWithDifferentTimeStampFound = false;
        try {
            Long updatedTime = productVertex.getProperty(MODIFICATION_TIMESTAMP_PROPERTY_KEY, Long.class);
            Iterator<AtlasEdge> existingEdges = productVertex.getEdges(AtlasEdgeDirection.BOTH, EDGE_LABELS_FOR_HARD_DELETION).iterator();

            if (existingEdges == null || !existingEdges.hasNext()) {
                LOG.info("No edges found for Product: {}", productVertex);
                return edgeWithDifferentTimeStampFound;
            }

            while (existingEdges.hasNext()) {
                AtlasEdge edge = existingEdges.next();
                Long modifiedEdgeTimestamp = edge.getProperty(MODIFICATION_TIMESTAMP_PROPERTY_KEY, Long.class);

                if (!updatedTime.equals(modifiedEdgeTimestamp)) {
                    LOG.info("Found edge with different timestamp: {}", edge);
                    edgeWithDifferentTimeStampFound = true;
                }
            }
        } catch (Exception e) {
            LOG.error("Error while validating edges for Archived Product: {}", productVertex, e);
            throw new RuntimeException(e);
        }
        return edgeWithDifferentTimeStampFound;
    }
}
