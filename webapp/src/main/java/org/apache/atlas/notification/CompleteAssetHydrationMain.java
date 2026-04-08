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
package org.apache.atlas.notification;

import org.apache.atlas.ApplicationProperties;
import org.apache.commons.configuration.Configuration;

public final class CompleteAssetHydrationMain {
    private static final String ATLAS_HOME = "atlas.home";
    private static final String ATLAS_DATA = "atlas.data";
    private static final String ATLAS_LOG_DIR = "atlas.log.dir";

    private CompleteAssetHydrationMain() {
    }

    public static void main(String[] args) throws Exception {
        setApplicationHomeDefaults();

        Configuration configuration = ApplicationProperties.get();
        try (CompleteAssetHydrator hydrator = new CompleteAssetHydrator(configuration)) {
            Runtime.getRuntime().addShutdownHook(new Thread(hydrator::close, "complete-asset-hydrator-shutdown"));
            hydrator.runUntilStopped();
        }
    }

    private static void setApplicationHomeDefaults() {
        if (System.getProperty(ATLAS_HOME) == null) {
            System.setProperty(ATLAS_HOME, "target");
        }

        if (System.getProperty(ATLAS_DATA) == null) {
            System.setProperty(ATLAS_DATA, "target/data");
        }

        if (System.getProperty(ATLAS_LOG_DIR) == null) {
            System.setProperty(ATLAS_LOG_DIR, "target/logs");
        }
    }
}
