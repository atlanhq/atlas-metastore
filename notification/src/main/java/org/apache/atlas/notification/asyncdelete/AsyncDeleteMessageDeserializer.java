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

package org.apache.atlas.notification.asyncdelete;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.atlas.model.notification.AtlasNotificationMessage;
import org.apache.atlas.model.notification.AsyncDeleteNotification;
import org.apache.atlas.notification.AbstractMessageDeserializer;
import org.apache.atlas.notification.AbstractNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Async delete notification message deserializer.
 */
public class AsyncDeleteMessageDeserializer extends AbstractMessageDeserializer<AsyncDeleteNotification> {

    /**
     * Logger for async delete notification messages.
     */
    private static final Logger NOTIFICATION_LOGGER = LoggerFactory.getLogger(AsyncDeleteMessageDeserializer.class);

    /**
     * Create an async delete notification message deserializer.
     */
    public AsyncDeleteMessageDeserializer() {
        super(new TypeReference<AsyncDeleteNotification>() {},
              new TypeReference<AtlasNotificationMessage<AsyncDeleteNotification>>() {},
              AbstractNotification.CURRENT_MESSAGE_VERSION, NOTIFICATION_LOGGER);
    }

    @Override
    public AsyncDeleteNotification deserialize(String messageJson) {
        final AsyncDeleteNotification ret = super.deserialize(messageJson);

        if (ret != null) {
            ret.normalize();
        }

        return ret;
    }
}
