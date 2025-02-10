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
package org.apache.atlas.web.adapters;

import static org.apache.atlas.TestUtilsV2.CLASSIFICATION;
import static org.apache.atlas.TestUtilsV2.COLUMN_TYPE;
import static org.apache.atlas.TestUtilsV2.DATABASE_TYPE;
import static org.apache.atlas.TestUtilsV2.FETL_CLASSIFICATION;
import static org.apache.atlas.TestUtilsV2.PHI;
import static org.apache.atlas.TestUtilsV2.PII;
import static org.apache.atlas.TestUtilsV2.TABLE_TYPE;
import static org.apache.atlas.model.discovery.SearchParameters.FilterCriteria.Condition.AND;
import static org.apache.atlas.repository.Constants.CLASSIFICATION_NAMES_KEY;
import static org.apache.atlas.repository.Constants.CUSTOM_ATTRIBUTES_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.STATE_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.TIMESTAMP_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.TYPE_NAME_PROPERTY_KEY;
import static org.apache.atlas.utils.TestLoadModelUtils.createTypesAsNeeded;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.RequestContext;
import org.apache.atlas.TestModules;
import org.apache.atlas.TestUtilsV2;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.glossary.GlossaryService;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.discovery.SearchParameters.FilterCriteria;
import org.apache.atlas.model.glossary.AtlasGlossary;
import org.apache.atlas.model.glossary.AtlasGlossaryTerm;
import org.apache.atlas.model.glossary.relations.AtlasGlossaryHeader;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.ClassificationAssociateRequest;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.store.bootstrap.AtlasTypeDefStoreInitializer;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.web.rest.DiscoveryREST;
import org.apache.atlas.web.rest.EntityREST;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Guice(modules = {TestModules.TestOnlyModule.class})
public class TestEntitiesREST {

    private static final Logger LOG = LoggerFactory.getLogger(TestEntitiesREST.class);

    @Inject
    private AtlasTypeRegistry typeRegistry;
    @Inject
    private GlossaryService   glossaryService;
    @Inject
    private AtlasTypeDefStore typeStore;
    @Inject
    private DiscoveryREST     discoveryREST;
    @Inject
    private EntityREST        entityREST;

    private AtlasGlossary                     glossary;
    private AtlasGlossaryTerm                 term1;
    private AtlasEntity                       dbEntity;
    private AtlasEntity                       tableEntity;
    private AtlasEntity                       tableEntity2;
    private List<AtlasEntity>                 columns;
    private List<AtlasEntity>                 columns2;
    private SearchParameters                  searchParameters;
    private Map<String, List<String>>         createdGuids     = new HashMap<>();
    private Map<String, AtlasClassification>  tagMap           = new HashMap<>();

    @BeforeClass
    public void setUp() throws Exception {
        AtlasTypesDef[] testTypesDefs = new AtlasTypesDef[] {  TestUtilsV2.defineHiveTypes() };

        for (AtlasTypesDef typesDef : testTypesDefs) {
            AtlasTypesDef typesToCreate = AtlasTypeDefStoreInitializer.getTypesToCreate(typesDef, typeRegistry);

            if (!typesToCreate.isEmpty()) {
                typeStore.createTypesDef(typesToCreate);
            }
        }

        loadGlossaryType();

        createEntities();

        createGlossary();

        createTerms();

        initTagMap();

        registerEntities();

        addTagTo(CLASSIFICATION, TABLE_TYPE);
    }

    @AfterMethod
    public void cleanup() throws Exception {
        RequestContext.clear();
    }

    @Test
    public void testGetEntities() throws Exception {

        final AtlasEntitiesWithExtInfo response = entityREST.getByGuids(createdGuids.get(DATABASE_TYPE), false, false);
        final List<AtlasEntity> entities = response.getEntities();

        Assert.assertNotNull(entities);
        Assert.assertEquals(entities.size(), 1);
        verifyAttributes(entities);
    }

    @Test
    public void testCustomAttributesSearch() throws Exception {
        AtlasEntity dbWithCustomAttr = new AtlasEntity(dbEntity);
        Map         customAttr       = new HashMap<String, String>() {{ put("key1", "value1"); }};

        dbWithCustomAttr.setCustomAttributes(customAttr);

        AtlasEntitiesWithExtInfo atlasEntitiesWithExtInfo = new AtlasEntitiesWithExtInfo(dbWithCustomAttr);
        EntityMutationResponse   response                 = entityREST.createOrUpdate(atlasEntitiesWithExtInfo, false, false, false);

        Assert.assertNotNull(response.getUpdatedEntitiesByTypeName(DATABASE_TYPE));

        searchParameters = new SearchParameters();
        searchParameters.setTypeName("_ALL_ENTITY_TYPES");

        SearchParameters.FilterCriteria filter = new SearchParameters.FilterCriteria();

        filter.setAttributeName(CUSTOM_ATTRIBUTES_PROPERTY_KEY);
        filter.setOperator(SearchParameters.Operator.CONTAINS);
        filter.setAttributeValue("key1=value1");

        searchParameters.setEntityFilters(filter);

        AtlasSearchResult res = discoveryREST.searchWithParameters(searchParameters);

        Assert.assertNotNull(res.getEntities());
        Assert.assertEquals(res.getEntities().size(), 1);
    }

    @Test
    public void testBasicSearch() throws Exception {
        // search entities with classification named classification
        searchParameters = new SearchParameters();
        searchParameters.setIncludeSubClassifications(true);
        searchParameters.setClassification(TestUtilsV2.CLASSIFICATION);

        AtlasSearchResult res = discoveryREST.searchWithParameters(searchParameters);
        Assert.assertNotNull(res.getEntities());
        Assert.assertEquals(res.getEntities().size(), 2);
    }

    @Test
    public void testSearchByMultiSystemAttributes() throws Exception {

        searchParameters = new SearchParameters();
        searchParameters.setTypeName("_ALL_ENTITY_TYPES");
        SearchParameters.FilterCriteria fc = new SearchParameters.FilterCriteria();
        SearchParameters.FilterCriteria subFc1 = new SearchParameters.FilterCriteria();
        SearchParameters.FilterCriteria subFc2 = new SearchParameters.FilterCriteria();

        subFc1.setAttributeName(MODIFICATION_TIMESTAMP_PROPERTY_KEY);
        subFc1.setOperator(SearchParameters.Operator.LT);
        subFc1.setAttributeValue(String.valueOf(System.currentTimeMillis()));

        subFc2.setAttributeName(TIMESTAMP_PROPERTY_KEY);
        subFc2.setOperator(SearchParameters.Operator.LT);
        subFc2.setAttributeValue(String.valueOf(System.currentTimeMillis()));

        fc.setCriterion(Arrays.asList(subFc1, subFc2));
        fc.setCondition(AND);
        searchParameters.setEntityFilters(fc);

        AtlasSearchResult res = discoveryREST.searchWithParameters(searchParameters);
        Assert.assertNotNull(res.getEntities());
        Assert.assertTrue(res.getEntities().size() > 5);
    }

    @Test
    public void testWildCardBasicSearch() throws Exception {

        //table - classification
        searchParameters = new SearchParameters();

        searchParameters.setClassification("*");
        AtlasSearchResult res = discoveryREST.searchWithParameters(searchParameters);
        Assert.assertNotNull(res.getEntities());
        Assert.assertEquals(res.getEntities().size(), 2);

        searchParameters.setClassification("_CLASSIFIED");
        res = discoveryREST.searchWithParameters(searchParameters);
        Assert.assertNotNull(res.getEntities());
        Assert.assertEquals(res.getEntities().size(), 2);

        // Test wildcard usage of basic search
        searchParameters.setClassification("cl*");
        res = discoveryREST.searchWithParameters(searchParameters);
        Assert.assertNotNull(res.getEntities());
        Assert.assertEquals(res.getEntities().size(), 2);

        searchParameters.setClassification("*ion");
        res = discoveryREST.searchWithParameters(searchParameters);
        Assert.assertNotNull(res.getEntities());
        Assert.assertEquals(res.getEntities().size(), 2);

        searchParameters.setClassification("*l*");
        res = discoveryREST.searchWithParameters(searchParameters);
        Assert.assertNotNull(res.getEntities());
        Assert.assertEquals(res.getEntities().size(), 2);

        searchParameters.setClassification("_NOT_CLASSIFIED");
        searchParameters.setTypeName(DATABASE_TYPE);
        res = discoveryREST.searchWithParameters(searchParameters);
        Assert.assertNotNull(res.getEntities());
        Assert.assertEquals(res.getEntities().size(), 1);
    }

    @Test(dependsOnMethods = "testWildCardBasicSearch")
    public void testBasicSearchWithAttr() throws Exception{
        searchParameters = new SearchParameters();
        searchParameters.setClassification("cla*");
        searchParameters.setTypeName(TABLE_TYPE);

        AtlasSearchResult res = discoveryREST.searchWithParameters(searchParameters);

        Assert.assertNotNull(res.getEntities());
        Assert.assertEquals(res.getEntities().size(), 2);

        // table - classification
        // column - phi
        addTagTo(PHI, COLUMN_TYPE);

        FilterCriteria filterCriteria = new FilterCriteria();
        filterCriteria.setAttributeName("stringAttr");
        filterCriteria.setOperator(SearchParameters.Operator.CONTAINS);
        filterCriteria.setAttributeValue("sample");

        // basic search with tag filterCriteria
        searchParameters = new SearchParameters();
        searchParameters.setClassification(PHI);
        searchParameters.setTagFilters(filterCriteria);

        res = discoveryREST.searchWithParameters(searchParameters);
        Assert.assertNotNull(res.getEntities());
        Assert.assertEquals(res.getEntities().size(), 2);

        filterCriteria.setAttributeName("stringAttr");
        filterCriteria.setOperator(SearchParameters.Operator.EQ);
        filterCriteria.setAttributeValue("sample_string");

        res = discoveryREST.searchWithParameters(searchParameters);
        Assert.assertNotNull(res.getEntities());
        Assert.assertEquals(res.getEntities().size(), 2);

        filterCriteria.setAttributeValue("SAMPLE_STRING");
        res = discoveryREST.searchWithParameters(searchParameters);
        Assert.assertNull(res.getEntities());
    }

    @Test(dependsOnMethods = "testBasicSearchWithAttr")
    public void testBasicSearchWithSubTypes() throws Exception{

        // table - classification
        // column - phi
        searchParameters = new SearchParameters();
        searchParameters.setClassification(TestUtilsV2.CLASSIFICATION);
        searchParameters.setIncludeSubClassifications(true);

        AtlasSearchResult res = discoveryREST.searchWithParameters(searchParameters);

        Assert.assertNotNull(res.getEntities());
        Assert.assertEquals(res.getEntities().size(), 2);

        // table - classification
        // database - fetl_classification
        // column - phi
        addTagTo(FETL_CLASSIFICATION, DATABASE_TYPE);

        final AtlasClassification result_tag = entityREST.getClassification(createdGuids.get(DATABASE_TYPE).get(0), TestUtilsV2.FETL_CLASSIFICATION);
        Assert.assertNotNull(result_tag);
        Assert.assertEquals(result_tag.getTypeName(), FETL_CLASSIFICATION);

        // basic search with subtypes
        res = discoveryREST.searchWithParameters(searchParameters);
        Assert.assertNotNull(res.getEntities());
        Assert.assertEquals(res.getEntities().size(), 3);

        // basic search without subtypes
        searchParameters.setIncludeSubClassifications(false);
        res = discoveryREST.searchWithParameters(searchParameters);

        Assert.assertNotNull(res.getEntities());
        Assert.assertEquals(res.getEntities().size(), 2);
    }

    @Test(dependsOnMethods = "testBasicSearchWithSubTypes")
    public void testGraphQueryFilter() throws Exception {

        // table - classification
        // database - fetl_classification
        // column - phi
        searchParameters = new SearchParameters();
        searchParameters.setQuery("sample_string");
        searchParameters.setClassification(PHI);

        SearchParameters.FilterCriteria fc = new SearchParameters.FilterCriteria();
        fc.setOperator(SearchParameters.Operator.EQ);
        fc.setAttributeName("booleanAttr");
        fc.setAttributeValue("true");

        searchParameters.setTagFilters(fc);
        AtlasSearchResult res = discoveryREST.searchWithParameters(searchParameters);
        Assert.assertNotNull(res.getEntities());
        Assert.assertEquals(res.getEntities().size(), 2);
        Assert.assertEquals(res.getEntities().get(0).getTypeName(), COLUMN_TYPE);

        AtlasClassification cls = new AtlasClassification(TestUtilsV2.PHI, new HashMap<String, Object>() {{
            put("stringAttr", "sample_string");
            put("booleanAttr", false);
            put("integerAttr", 100);
        }});

        ClassificationAssociateRequest clsAssRequest = new ClassificationAssociateRequest(Collections.singletonList(createdGuids.get(TABLE_TYPE).get(0)), cls);
        entityREST.addClassification(clsAssRequest);

        final AtlasClassification result_tag = entityREST.getClassification(createdGuids.get(TABLE_TYPE).get(0), TestUtilsV2.PHI);
        Assert.assertNotNull(result_tag);
        Assert.assertEquals(result_tag.getTypeName(), PHI);

        // table - phi, classification
        // database - fetl_classification
        // column - phi
        fc.setAttributeValue("false");
        res = discoveryREST.searchWithParameters(searchParameters);

        Assert.assertNotNull(res.getEntities());
        Assert.assertEquals(res.getEntities().size(), 1);
        Assert.assertEquals(res.getEntities().get(0).getTypeName(), TABLE_TYPE);
    }

    @Test(dependsOnMethods = "testGraphQueryFilter")
    public void testSearchByMultiTags() throws Exception {

        addTagTo(PHI, DATABASE_TYPE);

        // database - phi, felt_classification
        // table1 - phi, classification, table2 - classification,
        // column - phi
        searchParameters = new SearchParameters();
        searchParameters.setIncludeSubClassifications(false);
        searchParameters.setTypeName("_ALL_ENTITY_TYPES");
        SearchParameters.FilterCriteria fc = new SearchParameters.FilterCriteria();
        SearchParameters.FilterCriteria subFc1 = new SearchParameters.FilterCriteria();
        SearchParameters.FilterCriteria subFc2 = new SearchParameters.FilterCriteria();
