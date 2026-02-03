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
package org.apache.atlas.repository.store.graph.v2.lineage;

import org.apache.atlas.model.lineage.OpenLineageEvent;

import java.util.Collections;
import java.util.List;

public class OpenLineageEventPage {
    private final List<OpenLineageEvent> events;
    private final String pagingState;
    private final int pageSize;

    public OpenLineageEventPage(List<OpenLineageEvent> events, String pagingState, int pageSize) {
        this.events = events == null ? Collections.emptyList() : events;
        this.pagingState = pagingState;
        this.pageSize = pageSize;
    }

    public List<OpenLineageEvent> getEvents() {
        return events;
    }

    public String getPagingState() {
        return pagingState;
    }

    public int getPageSize() {
        return pageSize;
    }
}
