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
package org.apache.atlas.policytransformer;

import org.apache.atlas.exception.AtlasBaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.atlas.repository.Constants.getStaticFileAsString;

public class CacheTransformerTemplateHelper {
    private static final Logger LOG = LoggerFactory.getLogger(CacheTransformerTemplateHelper.class);

    static final String RESOURCE_POLICY_TRANSFORMER = "templates/policy_cache_transformer_%s.json";

    public static PolicyTransformerTemplate getTemplate(String fileSuffix) throws AtlasBaseException {
        PolicyTransformerTemplate templates;
        String jsonTemplate = null;
        String fileName = String.format(RESOURCE_POLICY_TRANSFORMER, fileSuffix);

        try {
            jsonTemplate = getStaticFileAsString(fileName);
        } catch (IOException e) {
            LOG.error("Failed to load template for policies: {}", RESOURCE_POLICY_TRANSFORMER);
            throw new AtlasBaseException(e);
        }
        templates = new PolicyTransformerTemplate();
        templates.fromJsonString(jsonTemplate);

        return templates;
    }
}
