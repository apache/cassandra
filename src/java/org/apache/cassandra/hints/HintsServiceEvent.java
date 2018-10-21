/*
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

package org.apache.cassandra.hints;

import java.io.Serializable;
import java.util.HashMap;

import org.apache.cassandra.diag.DiagnosticEvent;

/**
 * DiagnosticEvent implementation for HintService.
 */
final class HintsServiceEvent extends DiagnosticEvent
{
    enum HintsServiceEventType
    {
        DISPATCHING_STARTED,
        DISPATCHING_PAUSED,
        DISPATCHING_RESUMED,
        DISPATCHING_SHUTDOWN
    }

    private final HintsServiceEventType type;
    private final HintsService service;
    private final boolean isDispatchPaused;
    private final boolean isShutdown;
    private final boolean dispatchExecutorIsPaused;
    private final boolean dispatchExecutorHasScheduledDispatches;

    HintsServiceEvent(HintsServiceEventType type, HintsService service)
    {
        this.type = type;
        this.service = service;
        this.isDispatchPaused = service.isDispatchPaused.get();
        this.isShutdown = service.isShutDown();
        this.dispatchExecutorIsPaused = service.dispatchExecutor.isPaused();
        this.dispatchExecutorHasScheduledDispatches = service.dispatchExecutor.hasScheduledDispatches();
    }

    public Enum<HintsServiceEventType> getType()
    {
        return type;
    }

    public HashMap<String, Serializable> toMap()
    {
        // be extra defensive against nulls and bugs
        HashMap<String, Serializable> ret = new HashMap<>();
        ret.put("isDispatchPaused", isDispatchPaused);
        ret.put("isShutdown", isShutdown);
        ret.put("dispatchExecutorIsPaused", dispatchExecutorIsPaused);
        ret.put("dispatchExecutorHasScheduledDispatches", dispatchExecutorHasScheduledDispatches);
        return ret;
    }
}
