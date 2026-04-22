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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Custom Jackson deserializer for {@code Map<String, String>} that tolerates
 * non-string JSON primitives (booleans, numbers, nulls) by coercing them
 * to their string representations.
 * <p>
 * This handles the case where API clients send:
 * <pre>{"options": {"isRichText": true, "maxLength": 100}}</pre>
 * instead of:
 * <pre>{"options": {"isRichText": "true", "maxLength": "100"}}</pre>
 */
public class StringMapDeserializer extends JsonDeserializer<Map<String, String>> {

    @Override
    public Map<String, String> deserialize(JsonParser parser, DeserializationContext ctxt)
            throws IOException {
        if (parser.currentToken() != JsonToken.START_OBJECT) {
            ctxt.reportWrongTokenException(this, JsonToken.START_OBJECT,
                    "Expected JSON object for Map<String, String>");
            return null;
        }

        Map<String, String> result = new HashMap<>();

        while (parser.nextToken() != JsonToken.END_OBJECT) {
            String key = parser.currentName();
            parser.nextToken();

            JsonToken valueToken = parser.currentToken();
            String value;

            switch (valueToken) {
                case VALUE_STRING:
                    value = parser.getText();
                    break;
                case VALUE_NUMBER_INT:
                case VALUE_NUMBER_FLOAT:
                    value = parser.getText();
                    break;
                case VALUE_TRUE:
                    value = "true";
                    break;
                case VALUE_FALSE:
                    value = "false";
                    break;
                case VALUE_NULL:
                    value = null;
                    break;
                case START_OBJECT:
                case START_ARRAY:
                    value = parser.readValueAsTree().toString();
                    break;
                default:
                    value = parser.getText();
                    break;
            }

            result.put(key, value);
        }

        return result;
    }

    @Override
    public Map<String, String> getNullValue(DeserializationContext ctxt) {
        return null;
    }
}
