/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.web.listeners;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class capable of performing a simple login.
 * Kerberos authentication has been removed - only simple authentication is supported.
 */
public class LoginProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(LoginProcessor.class);

    /**
     * Perform a SIMPLE login based on established OS identity.
     * Kerberos login has been removed.
     */
    public void login() {
        LOG.info("LoginProcessor: Using simple authentication (Kerberos disabled)");
    }
}
