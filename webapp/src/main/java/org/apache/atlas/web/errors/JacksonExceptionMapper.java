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

package org.apache.atlas.web.errors;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.atlas.web.util.Servlets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

@Provider
@Component
public class JacksonExceptionMapper implements ExceptionMapper<JsonProcessingException> {
    private static final Logger LOG = LoggerFactory.getLogger(JacksonExceptionMapper.class);

    @Context
    private HttpServletRequest httpServletRequest;

    @Override
    public Response toResponse(JsonProcessingException exception) {
        LOG.error("Malformed json passed to server", exception);
        ExceptionMapperUtil.logRequestBodyOnError(httpServletRequest);

        String message = exception.getOriginalMessage();
        if (message == null || message.isEmpty()) {
            message = exception.getMessage();
        }

        return Servlets.getErrorResponse(message, Response.Status.BAD_REQUEST);
    }
}
