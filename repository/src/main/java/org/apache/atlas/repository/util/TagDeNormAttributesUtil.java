package org.apache.atlas.repository.util;

import joptsimple.internal.Strings;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.repository.graph.IFullTextMapper;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.repository.Constants.CLASSIFICATION_NAMES_KEY;
import static org.apache.atlas.repository.Constants.CLASSIFICATION_NAME_DELIMITER;
import static org.apache.atlas.repository.Constants.CLASSIFICATION_TEXT_KEY;
import static org.apache.atlas.repository.Constants.PROPAGATED_CLASSIFICATION_NAMES_KEY;
import static org.apache.atlas.repository.Constants.PROPAGATED_TRAIT_NAMES_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.TRAIT_NAMES_PROPERTY_KEY;
import static org.apache.atlas.repository.graph.FullTextMapperV2.FULL_TEXT_DELIMITER;
import static org.apache.atlas.repository.graph.GraphHelper.getDelimitedClassificationNames;

public class TagDeNormAttributesUtil {

    private static final Logger LOG = LoggerFactory.getLogger(TagDeNormAttributesUtil.class);

    public static Map<String, Object> getAllAttributesForAllTagsForRepair(String sourceAssetGuid,
                                                                        List<AtlasClassification> currentTags,
                                                                        AtlasTypeRegistry typeRegistry,
                                                                        IFullTextMapper fullTextMapperV2) throws AtlasBaseException {
        Map<String, Object> deNormAttrs = new HashMap<>();

        String classificationTextKey = Strings.EMPTY;
        String classificationNamesKey = Strings.EMPTY;
        String propagatedClassificationNamesKey = Strings.EMPTY;

        List<String> traitNames= Collections.EMPTY_LIST;
        List<String> propagatedTraitNames = Collections.EMPTY_LIST;

        if (CollectionUtils.isNotEmpty(currentTags)) {
            // filter attachments
            traitNames = new ArrayList<>(0);
            propagatedTraitNames = new ArrayList<>(0);

            for (AtlasClassification tag : currentTags) {
                if (sourceAssetGuid.equals(tag.getEntityGuid())) {
                    traitNames.add(tag.getTypeName());
                } else {
                    propagatedTraitNames.add(tag.getTypeName());
                }
            }

            classificationTextKey = getClassificationTextKey(currentTags, typeRegistry, fullTextMapperV2);

            if (!traitNames.isEmpty()) {
                classificationNamesKey = getDelimitedClassificationNames(traitNames);
            }

            if (!propagatedTraitNames.isEmpty()) {
                StringBuilder finalTagNames = new StringBuilder();
                propagatedTraitNames.forEach(tagName -> finalTagNames.append(CLASSIFICATION_NAME_DELIMITER).append(tagName));

                propagatedClassificationNamesKey = finalTagNames.toString();
            }
        }

        deNormAttrs.put(CLASSIFICATION_TEXT_KEY, classificationTextKey);

        deNormAttrs.put(TRAIT_NAMES_PROPERTY_KEY, traitNames);
        deNormAttrs.put(CLASSIFICATION_NAMES_KEY, classificationNamesKey);

        deNormAttrs.put(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, propagatedTraitNames);
        deNormAttrs.put(PROPAGATED_CLASSIFICATION_NAMES_KEY, propagatedClassificationNamesKey);

        return deNormAttrs;
    }

    private static String getClassificationTextKey(List<AtlasClassification> tags, AtlasTypeRegistry typeRegistry, IFullTextMapper fullTextMapperV2) throws AtlasBaseException {
        if (typeRegistry == null) {
            LOG.error("typeRegistry can not be null");
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "typeRegistry can not be null");
        }
        if (fullTextMapperV2 == null) {
            LOG.error("fullTextMapperV2 can not be null");
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "fullTextMapperV2 can not be null");
        }

        StringBuilder sb = new StringBuilder();
        for (AtlasClassification currentTag : tags) {
            final AtlasClassificationType classificationType = typeRegistry.getClassificationTypeByName(currentTag.getTypeName());

            sb.append(currentTag.getTypeName()).append(FULL_TEXT_DELIMITER);
            fullTextMapperV2.mapAttributes(classificationType, currentTag.getAttributes(), null, sb, null, new HashSet<>(), true);
        }

        return sb.toString();
    }
}
