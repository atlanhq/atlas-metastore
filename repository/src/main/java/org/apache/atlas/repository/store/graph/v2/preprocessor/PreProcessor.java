package org.apache.atlas.repository.store.graph.v2.preprocessor;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;

import java.util.HashSet;
import java.util.Set;

import static org.apache.atlas.repository.Constants.*;


public interface PreProcessor {

    Set<String> skipInitialAuthCheckTypes = new HashSet<String>() {{
        add(ATLAS_GLOSSARY_TERM_ENTITY_TYPE);
        add(ATLAS_GLOSSARY_CATEGORY_ENTITY_TYPE);
        add(STAKEHOLDER_ENTITY_TYPE);
        add(STAKEHOLDER_TITLE_ENTITY_TYPE);
        add(DATA_DOMAIN_ENTITY_TYPE);
        add(DATA_PRODUCT_ENTITY_TYPE);
    }};

    Set<String> skipUpdateAuthCheckTypes = new HashSet<String>() {{
        add(DATA_DOMAIN_ENTITY_TYPE);
        add(DATA_PRODUCT_ENTITY_TYPE);
    }};

    void processAttributes(AtlasStruct entity, EntityMutationContext context, EntityMutations.EntityOperation operation) throws AtlasBaseException;

    default void processDelete(AtlasVertex vertex) throws AtlasBaseException {
        //override this method for implementation
    }
}
