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
package org.apache.atlas.query;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class AtlasDSL {

    public static class Parser {
        private static final Logger LOG = LoggerFactory.getLogger(Parser.class);

        private static final Set<String> RESERVED_KEYWORDS =
                new HashSet<>(Arrays.asList("[", "]", "(", ")", "=", "<", ">", "!=", "<=", ">=", ",", "and", "or", "+", "-",
                                            "*", "/", ".", "select", "from", "where", "groupby", "loop", "isa", "is", "has",
                                            "as", "times", "withPath", "limit", "offset", "orderby", "count", "max", "min",
                                            "sum", "by", "order", "like"));

        public static boolean isKeyword(String word) {
            return RESERVED_KEYWORDS.contains(word);
        }

    }

    static class Validator extends BaseErrorListener {
        private boolean isValid = true;
        private String errorMsg = "";

        @Override
        public void syntaxError(final Recognizer<?, ?> recognizer, final Object offendingSymbol, final int line, final int charPositionInLine, final String msg, final RecognitionException e) {
            isValid = false;
            errorMsg = msg;
        }

        public boolean isValid() {
            return isValid;
        }

        public String getErrorMsg() {
            return errorMsg;
        }
    }

}
