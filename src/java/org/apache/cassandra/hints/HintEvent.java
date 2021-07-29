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
import java.util.UUID;
import javax.annotation.Nullable;

import org.apache.cassandra.diag.DiagnosticEvent;
import org.apache.cassandra.locator.InetAddressAndPort;

/**
 * DiagnosticEvent implementation for hinted handoff.
 */
final class HintEvent extends DiagnosticEvent
{
    enum HintEventType
    {
        DISPATCHING_STARTED,
        DISPATCHING_PAUSED,
        DISPATCHING_RESUMED,
        DISPATCHING_SHUTDOWN,

        DISPATCHER_CREATED,
        DISPATCHER_CLOSED,

        DISPATCHER_PAGE,
        DISPATCHER_HINT_RESULT,

        ABORT_REQUESTED
    }

    enum HintResult
    {
        PAGE_SUCCESS, PAGE_FAILURE
    }

    private final HintEventType type;
    private final HintsDispatcher dispatcher;
    private final UUID targetHostId;
    private final InetAddressAndPort targetAddress;
    @Nullable
    private final HintResult dispatchResult;
    @Nullable
    private final Long pageHintsSuccessful;
    @Nullable
    private final Long pageHintsFailed;
    @Nullable
    private final Long pageHintsTimeout;

    HintEvent(HintEventType type, HintsDispatcher dispatcher, UUID targetHostId, InetAddressAndPort targetAddress,
              @Nullable HintResult dispatchResult, @Nullable Long pageHintsSuccessful,
              @Nullable Long pageHintsFailed, @Nullable Long pageHintsTimeout)
    {
        this.type = type;
        this.dispatcher = dispatcher;
        this.targetHostId = targetHostId;
        this.targetAddress = targetAddress;
        this.dispatchResult = dispatchResult;
        this.pageHintsSuccessful = pageHintsSuccessful;
        this.pageHintsFailed = pageHintsFailed;
        this.pageHintsTimeout = pageHintsTimeout;
    }

    public Enum<HintEventType> getType()
    {
        return type;
    }

    public HashMap<String, Serializable> toMap()
    {
        // be extra defensive against nulls and bugs
        HashMap<String, Serializable> ret = new HashMap<>();
        ret.put("targetHostId", targetHostId);
        ret.put("targetAddress", targetAddress.getHostAddressAndPort());
        if (dispatchResult != null) ret.put("dispatchResult", dispatchResult.name());
        if (pageHintsSuccessful != null || pageHintsFailed != null || pageHintsTimeout != null)
        {
            ret.put("hint.page.hints_succeeded", pageHintsSuccessful);
            ret.put("hint.page.hints_failed", pageHintsFailed);
            ret.put("hint.page.hints_timed_out", pageHintsTimeout);
        }
        return ret;
    }
}
