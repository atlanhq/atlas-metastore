package org.apache.atlas.web.rest;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.Timed;
import org.apache.atlas.discovery.AtlasDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.IndexSearchParams;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.EntityGraphDiscoveryContext;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.repository.store.graph.v2.preprocessor.model.AbstractModelPreProcessor;
import org.apache.atlas.repository.store.graph.v2.preprocessor.model.DMEntityPreProcessor;
import org.apache.atlas.repository.store.graph.v2.preprocessor.model.ModelResponse;
import org.apache.atlas.searchlog.SearchLoggingManagement;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.util.ModelUtil;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.*;

import static org.apache.atlas.repository.Constants.*;

@Path("model")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class ModelREST {


    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.ModelREST");
    private static final Logger LOG = LoggerFactory.getLogger(DiscoveryREST.class);

    @Context
    private HttpServletRequest httpServletRequest;
    private final boolean enableSearchLogging;
    private final AtlasTypeRegistry typeRegistry;
    private final AbstractModelPreProcessor entityPreProcessor;
    private final AtlasDiscoveryService discoveryService;
    private final SearchLoggingManagement loggerManagement;
    private final EntityGraphMapper entityGraphMapper;
    private final EntityGraphRetriever graphRetriever;

    //private final Entity

    private static final String INDEXSEARCH_TAG_NAME = "indexsearch";
    private static final Set<String> TRACKING_UTM_TAGS = new HashSet<>(Arrays.asList("ui_main_list", "ui_popup_searchbar"));
    private static final String UTM_TAG_FROM_PRODUCT = "project_webapp";

    @Inject
    public ModelREST(AtlasTypeRegistry typeRegistry, AtlasDiscoveryService discoveryService, SearchLoggingManagement loggerManagement, EntityGraphMapper entityGraphMapper, EntityGraphRetriever graphRetriever, DMEntityPreProcessor entityPreProcessor) {
        this.typeRegistry = typeRegistry;
        this.discoveryService = discoveryService;
        this.loggerManagement = loggerManagement;
        this.entityGraphMapper = entityGraphMapper;
        this.graphRetriever = graphRetriever;
        this.entityPreProcessor = entityPreProcessor;
        this.enableSearchLogging = AtlasConfiguration.ENABLE_SEARCH_LOGGER.getBoolean();
    }

    @Path("/search")
    @POST
    @Timed
    public AtlasSearchResult dataSearch(@QueryParam("namespace") String namespace, @QueryParam("businessDate") String businessDate, @QueryParam("systemDate") String systemDate, @Context HttpServletRequest servletRequest, IndexSearchParams parameters) throws AtlasBaseException {

        Servlets.validateQueryParamLength("namespace", namespace);
        Servlets.validateQueryParamLength("businessDate", businessDate);
        AtlasPerfTracer perf = null;
        long startTime = System.currentTimeMillis();

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "ModelREST.dataSearch(" + parameters + ")");
            }

            parameters = parameters == null ? new IndexSearchParams() : parameters;

            String queryStringUsingFiltersAndUserDSL = ModelUtil.createQueryStringUsingFiltersAndUserDSL(namespace, businessDate, systemDate, parameters.getQuery());

            if (StringUtils.isEmpty(queryStringUsingFiltersAndUserDSL)) {
                AtlasBaseException abe = new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Invalid model search query");
                throw abe;
            }

            parameters.setQuery(queryStringUsingFiltersAndUserDSL);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Performing indexsearch for the params ({})", parameters);
            }

            AtlasSearchResult result = discoveryService.directIndexSearch(parameters);
            if (result == null) {
                return null;
            }
            return result;
        } catch (AtlasBaseException abe) {
            if (enableSearchLogging && parameters.isSaveSearchLog()) {
                // logSearchLog(parameters, servletRequest, abe, System.currentTimeMillis() - startTime);
            }
            throw abe;
        } catch (Exception e) {
            AtlasBaseException abe = new AtlasBaseException(e.getMessage(), e.getCause());
            if (enableSearchLogging && parameters.isSaveSearchLog()) {
                //logSearchLog(parameters, servletRequest, abe, System.currentTimeMillis() - startTime);
            }
            throw abe;
        } finally {
            if (CollectionUtils.isNotEmpty(parameters.getUtmTags())) {
                AtlasPerfMetrics.Metric indexsearchMetric = new AtlasPerfMetrics.Metric(INDEXSEARCH_TAG_NAME);
                indexsearchMetric.addTag("utmTag", "other");
                indexsearchMetric.addTag("source", "other");
                for (String utmTag : parameters.getUtmTags()) {
                    if (TRACKING_UTM_TAGS.contains(utmTag)) {
                        indexsearchMetric.addTag("utmTag", utmTag);
                        break;
                    }
                }
                if (parameters.getUtmTags().contains(UTM_TAG_FROM_PRODUCT)) {
                    indexsearchMetric.addTag("source", UTM_TAG_FROM_PRODUCT);
                }
                indexsearchMetric.addTag("name", INDEXSEARCH_TAG_NAME);
                indexsearchMetric.setTotalTimeMSecs(System.currentTimeMillis() - startTime);
                RequestContext.get().addApplicationMetrics(indexsearchMetric);
            }
            AtlasPerfTracer.log(perf);
        }
    }

    @DELETE
    @Path("/entity")
    @Timed
    public EntityMutationResponse deleteByQualifiedNamePrefix(@QueryParam("qualifiedNamePrefix") String qualifiedNamePrefix,
                                                              @QueryParam("entityType") String entityType,
                                                              @Context HttpServletRequest servletRequest) throws AtlasBaseException {

        Servlets.validateQueryParamLength("qualifiedNamePrefix", qualifiedNamePrefix);
        Servlets.validateQueryParamLength("entityType", entityType);
        AtlasPerfTracer perf = null;

        EntityGraphDiscoveryContext graphDiscoveryContext = new EntityGraphDiscoveryContext(typeRegistry, null);
        EntityMutationContext entityMutationContext = new EntityMutationContext(graphDiscoveryContext);

        try {
            if (StringUtils.isEmpty(qualifiedNamePrefix) || StringUtils.isEmpty(entityType)) {
                throw new AtlasBaseException(AtlasErrorCode.QUALIFIED_NAME_PREFIX_NOT_EXIST);
            }

            // check with chris how the label look like
            // accordingly capitalize letters
            AtlasEntityType atlasEntityType = typeRegistry.getEntityTypeByName(entityType);

            if (atlasEntityType == null) {
                throw new AtlasBaseException(AtlasErrorCode.INVALID_ENTITY_TYPE);
            }

            AtlasVertex latestEntityVertex = AtlasGraphUtilsV2.findLatestEntityAttributeVerticesByType(entityType, qualifiedNamePrefix);

            if (latestEntityVertex == null) {
                throw new AtlasBaseException(AtlasErrorCode.NO_TYPE_EXISTS_FOR_QUALIFIED_NAME_PREFIX, qualifiedNamePrefix);
            }

            String modelQualifiedName;

            if (entityType.equals(MODEL_ENTITY)) {
                int lastIndex = qualifiedNamePrefix.lastIndexOf("/");
                modelQualifiedName = qualifiedNamePrefix.substring(0, lastIndex);
                String entityGuid = AtlasGraphUtilsV2.getIdFromVertex(latestEntityVertex);
                replicateModelVersionAndExcludeEntity(modelQualifiedName, entityGuid, entityMutationContext);

            } else if (entityType.equals(MODEL_ATTRIBUTE)) {
                int lastIndex = qualifiedNamePrefix.lastIndexOf("/");
                String entityQualifiedNamePrefix = qualifiedNamePrefix.substring(0, lastIndex);
                String attributeGuid = AtlasGraphUtilsV2.getIdFromVertex(latestEntityVertex);
                replicateModelVersionAndEntityAndExcludeAttribute(entityQualifiedNamePrefix,
                        attributeGuid, "", entityMutationContext);
            } else {
                throw new AtlasBaseException(AtlasErrorCode.INVALID_ENTITY_TYPE);
            }
            return entityGraphMapper.mapAttributesAndClassifications(entityMutationContext,
                    false, false, false, false);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    private void replicateModelVersionAndEntityAndExcludeAttribute(final String entityQualifiedNamePrefix, String deleteAttributeGuid, String deleteEntityGuid, EntityMutationContext entityMutationContext) throws AtlasBaseException {
        int lastIndex = entityQualifiedNamePrefix.lastIndexOf("/");
        String modelQualifiedName = entityQualifiedNamePrefix.substring(0, lastIndex);

        // get entity
        // replicate entity
        AtlasVertex latestEntityVertex = AtlasGraphUtilsV2.findLatestEntityAttributeVerticesByType(MODEL_ENTITY, entityQualifiedNamePrefix);
        AtlasEntity latestEntity = graphRetriever.toAtlasEntity(latestEntityVertex);

        if (latestEntityVertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.DATA_ENTITY_NOT_EXIST);
        }

        long now = RequestContext.get().getRequestTime();

        ModelResponse modelResponse = entityPreProcessor.replicateModelEntity(latestEntity,
                latestEntityVertex, entityQualifiedNamePrefix, now);

        AtlasEntity replicaEntity = modelResponse.getReplicaEntity();
        AtlasVertex replicaVertex = modelResponse.getReplicaVertex();

        // exclude attribute from entity
        Map<String, Object> relationshipAttributes = excludeEntityFromRelationshipAttribute(deleteAttributeGuid,
                replicaEntity.getRelationshipAttributes());
        replicaEntity.setRelationshipAttributes(relationshipAttributes);


        //replicate modelVersion
        ModelResponse modelVersionResponse = replicateModelVersionAndExcludeEntity(
                modelQualifiedName, "", entityMutationContext);

        // create entity-modelVersion relationship
        entityPreProcessor.createModelVersionModelEntityRelationship(
                modelVersionResponse.getReplicaVertex(),
                replicaVertex);

        entityMutationContext.addCreated(replicaEntity.getGuid(),
                replicaEntity,
                typeRegistry.getEntityTypeByName(MODEL_ENTITY),
                replicaVertex);

    }

    private ModelResponse replicateModelVersionAndExcludeEntity(final String modelQualifiedName, String deleteEntityGuid, EntityMutationContext entityMutationContext) throws AtlasBaseException {
        Map<String, Object> attrValues = new HashMap<>();
        attrValues.put(QUALIFIED_NAME, modelQualifiedName);

        AtlasVertex modelVertex = AtlasGraphUtilsV2.findByUniqueAttributes(
                typeRegistry.getEntityTypeByName(MODEL_DATA_MODEL), attrValues);

        if (modelVertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.DATA_MODEL_NOT_EXIST);
        }

        String modelGuid = AtlasGraphUtilsV2.getIdFromVertex(modelVertex);

        long now = RequestContext.get().getRequestTime();

        ModelResponse modelResponse = entityPreProcessor.replicateModelVersion(modelGuid, modelQualifiedName, now);
        AtlasEntity replicaModelVersionEntity = modelResponse.getReplicaEntity();
        AtlasVertex replicaModelVersionVertex = modelResponse.getReplicaVertex();
        String modelVersionGuid = replicaModelVersionEntity.getGuid();

        Map<String, Object> relationshipAttributes = excludeEntityFromRelationshipAttribute(deleteEntityGuid,
                replicaModelVersionEntity.getRelationshipAttributes());
        replicaModelVersionEntity.setRelationshipAttributes(relationshipAttributes);
        entityPreProcessor.createModelModelVersionRelation(modelGuid, modelVersionGuid);

        entityMutationContext.addCreated(modelVersionGuid, replicaModelVersionEntity,
                typeRegistry.getEntityTypeByName(MODEL_VERSION), replicaModelVersionVertex);

        entityMutationContext.getDiscoveryContext().addResolvedGuid(modelGuid, modelVertex);
        return modelResponse;
    }

    private Map<String, Object> excludeEntityFromRelationshipAttribute(String entityGuid, Map<String, Object> relationshipAttributes) throws AtlasBaseException {
        if (StringUtils.isEmpty(entityGuid)) {
            return relationshipAttributes;
        }
        Map<String, Object> appendAttributesDestination = new HashMap<>();
        if (relationshipAttributes != null) {
            Map<String, Object> appendAttributesSource = (Map<String, Object>) relationshipAttributes;

            String guid = "";

            for (String attribute : appendAttributesSource.keySet()) {

                if (appendAttributesSource.get(attribute) instanceof List) {

                    List<Map<String, Object>> destList = new ArrayList<>();
                    Map<String, Object> destMap = null;

                    List<Map<String, Object>> attributeList = (List<Map<String, Object>>) appendAttributesSource.get(attribute);

                    for (Map<String, Object> relationAttribute : attributeList) {
                        guid = (String) relationAttribute.get("guid");

                        if (guid.equals(entityGuid)) {
                            continue;
                        }

                        destMap = new HashMap<>(relationAttribute);
                        destList.add(destMap);
                    }
                    appendAttributesDestination.put(attribute, destList);
                } else {
                    if (appendAttributesSource.get(attribute) instanceof Map) {
                        LinkedHashMap<String, Object> attributeList = (LinkedHashMap<String, Object>) appendAttributesSource.get(attribute);
                        guid = (String) attributeList.get("guid");

                        // update end2
                        if (guid.equals(entityGuid)) {
                            continue;
                        }

                        Map<String, Object> destMap = new HashMap<>(attributeList);
                        appendAttributesDestination.put(attribute, destMap);
                    }
                }
            }
        }
        return appendAttributesDestination;
    }
}