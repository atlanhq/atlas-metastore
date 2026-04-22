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
package org.apache.atlas.model.typedef;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link StringMapDeserializer} verifying that non-string JSON
 * primitives in options maps are coerced to strings during deserialization.
 */
public class StringMapDeserializerTest {

    private static final ObjectMapper mapper = new ObjectMapper();

    // =========================================================================
    // AtlasBaseTypeDef options (type-def level)
    // =========================================================================

    @Test
    public void testTypeDefOptionsWithAllStringValues() throws Exception {
        String json = "{\"name\":\"test\",\"options\":{\"key1\":\"value1\",\"key2\":\"value2\"}}";
        AtlasEntityDef def = mapper.readValue(json, AtlasEntityDef.class);

        assertNotNull(def.getOptions());
        assertEquals("value1", def.getOptions().get("key1"));
        assertEquals("value2", def.getOptions().get("key2"));
    }

    @Test
    public void testTypeDefOptionsWithBooleanValues() throws Exception {
        String json = "{\"name\":\"test\",\"options\":{\"enabled\":true,\"disabled\":false}}";
        AtlasEntityDef def = mapper.readValue(json, AtlasEntityDef.class);

        assertNotNull(def.getOptions());
        assertEquals("true", def.getOptions().get("enabled"));
        assertEquals("false", def.getOptions().get("disabled"));
    }

    @Test
    public void testTypeDefOptionsWithNumericValues() throws Exception {
        String json = "{\"name\":\"test\",\"options\":{\"maxLength\":100,\"ratio\":3.14}}";
        AtlasEntityDef def = mapper.readValue(json, AtlasEntityDef.class);

        assertNotNull(def.getOptions());
        assertEquals("100", def.getOptions().get("maxLength"));
        assertEquals("3.14", def.getOptions().get("ratio"));
    }

    @Test
    public void testTypeDefOptionsWithMixedValues() throws Exception {
        String json = "{\"name\":\"test\",\"options\":{\"str\":\"hello\",\"bool\":true,\"num\":42,\"flt\":1.5}}";
        AtlasEntityDef def = mapper.readValue(json, AtlasEntityDef.class);

        assertNotNull(def.getOptions());
        assertEquals("hello", def.getOptions().get("str"));
        assertEquals("true", def.getOptions().get("bool"));
        assertEquals("42", def.getOptions().get("num"));
        assertEquals("1.5", def.getOptions().get("flt"));
    }

    @Test
    public void testTypeDefOptionsWithNullEntry() throws Exception {
        String json = "{\"name\":\"test\",\"options\":{\"key1\":\"value1\",\"key2\":null}}";
        AtlasEntityDef def = mapper.readValue(json, AtlasEntityDef.class);

        assertNotNull(def.getOptions());
        assertEquals("value1", def.getOptions().get("key1"));
        assertNull(def.getOptions().get("key2"));
        assertTrue(def.getOptions().containsKey("key2"));
    }

    @Test
    public void testTypeDefOptionsNull() throws Exception {
        String json = "{\"name\":\"test\",\"options\":null}";
        AtlasEntityDef def = mapper.readValue(json, AtlasEntityDef.class);

        assertNull(def.getOptions());
    }

    @Test
    public void testTypeDefOptionsEmpty() throws Exception {
        String json = "{\"name\":\"test\",\"options\":{}}";
        AtlasEntityDef def = mapper.readValue(json, AtlasEntityDef.class);

        assertNotNull(def.getOptions());
        assertTrue(def.getOptions().isEmpty());
    }

    @Test
    public void testTypeDefOptionsMissing() throws Exception {
        String json = "{\"name\":\"test\"}";
        AtlasEntityDef def = mapper.readValue(json, AtlasEntityDef.class);

        assertNull(def.getOptions());
    }

    // =========================================================================
    // AtlasAttributeDef options (attribute-def level) -- the failing field
    // =========================================================================

    @Test
    public void testAttributeDefOptionsWithBooleans() throws Exception {
        String json = "{\"name\":\"attr1\",\"typeName\":\"string\",\"options\":{\"isRichText\":true,\"allowFiltering\":false}}";
        AtlasStructDef.AtlasAttributeDef attrDef = mapper.readValue(json, AtlasStructDef.AtlasAttributeDef.class);

        assertNotNull(attrDef.getOptions());
        assertEquals("true", attrDef.getOptions().get("isRichText"));
        assertEquals("false", attrDef.getOptions().get("allowFiltering"));
    }

    @Test
    public void testAttributeDefOptionsWithMixedTypes() throws Exception {
        String json = "{\"name\":\"attr1\",\"typeName\":\"string\",\"options\":{" +
                "\"applicableEntityTypes\":\"[\\\"Asset\\\"]\"," +
                "\"showAsFeatured\":true," +
                "\"showInOverview\":false," +
                "\"multiValueSelect\":false," +
                "\"isRichText\":\"true\"," +
                "\"allowSearch\":false," +
                "\"allowFiltering\":true," +
                "\"maxStrLength\":\"100000000\"," +
                "\"isEnum\":\"false\"" +
                "}}";
        AtlasStructDef.AtlasAttributeDef attrDef = mapper.readValue(json, AtlasStructDef.AtlasAttributeDef.class);

        assertNotNull(attrDef.getOptions());
        assertEquals("[\"Asset\"]", attrDef.getOptions().get("applicableEntityTypes"));
        assertEquals("true", attrDef.getOptions().get("showAsFeatured"));
        assertEquals("false", attrDef.getOptions().get("showInOverview"));
        assertEquals("false", attrDef.getOptions().get("multiValueSelect"));
        assertEquals("true", attrDef.getOptions().get("isRichText"));
        assertEquals("false", attrDef.getOptions().get("allowSearch"));
        assertEquals("true", attrDef.getOptions().get("allowFiltering"));
        assertEquals("100000000", attrDef.getOptions().get("maxStrLength"));
        assertEquals("false", attrDef.getOptions().get("isEnum"));
    }

    @Test
    public void testAttributeDefOptionsWithNestedArray() throws Exception {
        String json = "{\"name\":\"attr1\",\"typeName\":\"string\",\"options\":{\"types\":[\"Table\",\"Column\"]}}";
        AtlasStructDef.AtlasAttributeDef attrDef = mapper.readValue(json, AtlasStructDef.AtlasAttributeDef.class);

        assertNotNull(attrDef.getOptions());
        String types = attrDef.getOptions().get("types");
        assertNotNull(types);
        assertTrue(types.contains("Table"));
        assertTrue(types.contains("Column"));
    }

    @Test
    public void testAttributeDefOptionsWithNestedObject() throws Exception {
        String json = "{\"name\":\"attr1\",\"typeName\":\"string\",\"options\":{\"config\":{\"a\":1,\"b\":\"x\"}}}";
        AtlasStructDef.AtlasAttributeDef attrDef = mapper.readValue(json, AtlasStructDef.AtlasAttributeDef.class);

        assertNotNull(attrDef.getOptions());
        String config = attrDef.getOptions().get("config");
        assertNotNull(config);
        assertTrue(config.contains("\"a\""));
        assertTrue(config.contains("\"b\""));
    }

    // =========================================================================
    // Roundtrip test: deserialize with booleans -> serialize -> deserialize
    // =========================================================================

    @Test
    public void testSerializationRoundtrip() throws Exception {
        String json = "{\"name\":\"test\",\"options\":{\"isRichText\":true,\"count\":5,\"label\":\"hello\"}}";
        AtlasEntityDef def = mapper.readValue(json, AtlasEntityDef.class);

        assertEquals("true", def.getOptions().get("isRichText"));
        assertEquals("5", def.getOptions().get("count"));
        assertEquals("hello", def.getOptions().get("label"));

        // Serialize back
        String serialized = mapper.writeValueAsString(def);

        // Serialized form should have string values
        assertTrue(serialized.contains("\"isRichText\":\"true\""));
        assertTrue(serialized.contains("\"count\":\"5\""));

        // Deserialize again from the serialized form (all strings now)
        AtlasEntityDef def2 = mapper.readValue(serialized, AtlasEntityDef.class);
        assertEquals(def.getOptions().get("isRichText"), def2.getOptions().get("isRichText"));
        assertEquals(def.getOptions().get("count"), def2.getOptions().get("count"));
        assertEquals(def.getOptions().get("label"), def2.getOptions().get("label"));
    }

    // =========================================================================
    // BusinessMetadataDef (the exact type from the failing request)
    // =========================================================================

    @Test
    public void testBusinessMetadataDefWithBooleanOptions() throws Exception {
        String json = "{\"category\":\"BUSINESS_METADATA\",\"name\":\"TestBM\",\"options\":{\"logoType\":\"icon\",\"imageId\":null}}";
        AtlasBusinessMetadataDef bmDef = mapper.readValue(json, AtlasBusinessMetadataDef.class);

        assertNotNull(bmDef.getOptions());
        assertEquals("icon", bmDef.getOptions().get("logoType"));
        assertNull(bmDef.getOptions().get("imageId"));
    }
}
