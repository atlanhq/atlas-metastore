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
package org.apache.atlas.glossary;

import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.NanoIdUtils;


public abstract class GlossaryUtils {

    public static final String TERM_ASSIGNMENT_ATTR_DESCRIPTION = "description";
    public static final String TERM_ASSIGNMENT_ATTR_EXPRESSION  = "expression";
    public static final String TERM_ASSIGNMENT_ATTR_STATUS      = "status";
    public static final String TERM_ASSIGNMENT_ATTR_CONFIDENCE  = "confidence";
    public static final String TERM_ASSIGNMENT_ATTR_CREATED_BY  = "createdBy";
    public static final String TERM_ASSIGNMENT_ATTR_STEWARD     = "steward";
    public static final String TERM_ASSIGNMENT_ATTR_SOURCE      = "source";

    public static final String ATLAS_GLOSSARY_TERM_TYPENAME     = "AtlasGlossaryTerm";
    public static final String ATLAS_GLOSSARY_CATEGORY_TYPENAME = "AtlasGlossaryCategory";

    public static final String NAME                         = "name";
    public static final String QUALIFIED_NAME               = "qualifiedName";

    protected final AtlasRelationshipStore relationshipStore;
    protected final AtlasTypeRegistry      typeRegistry;

    protected GlossaryUtils(final AtlasRelationshipStore relationshipStore, final AtlasTypeRegistry typeRegistry) {
        this.relationshipStore = relationshipStore;
        this.typeRegistry = typeRegistry;
    }

    protected static String getUUID(){
        return NanoIdUtils.randomNanoId();
    }
}
