package org.apache.atlas.repository.store.graph.v2.tags;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasClassification;

import java.util.List;
import java.util.Set;

public interface TagDAO {
    List<AtlasClassification> getTagsForVertex(String vertexId) throws AtlasBaseException;
    AtlasClassification findTagByVertexIdAndTagTypeName(String assetVertexId, String tagTypeName) throws AtlasBaseException;
    void putPropagatedTags(String sourceAssetId, String tagTypeName, Set<String> propagatedAssetVertexIds);
    void deletePropagatedTags(String sourceAssetId, String tagTypeName) throws AtlasBaseException;
}