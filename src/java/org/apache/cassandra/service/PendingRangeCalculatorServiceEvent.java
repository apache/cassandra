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

package org.apache.cassandra.service;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.diag.DiagnosticEvent;

/**
 * Events related to {@link PendingRangeCalculatorService}.
 */
final class PendingRangeCalculatorServiceEvent extends DiagnosticEvent
{
    private final PendingRangeCalculatorServiceEventType type;
    private final int taskCount;

    public enum PendingRangeCalculatorServiceEventType
    {
        TASK_STARTED,
        TASK_FINISHED_SUCCESSFULLY,
        TASK_EXECUTION_REJECTED,
        TASK_COUNT_CHANGED
    }

    PendingRangeCalculatorServiceEvent(PendingRangeCalculatorServiceEventType type)
    {
        this(type, -1);
    }

    PendingRangeCalculatorServiceEvent(PendingRangeCalculatorServiceEventType type,
                                       int taskCount)
    {
        this.type = type;
        this.taskCount = taskCount;
    }

    public int getTaskCount()
    {
        return taskCount;
    }

    public PendingRangeCalculatorServiceEventType getType()
    {
        return type;
    }

    public Map<String, Serializable> toMap()
    {
        // be extra defensive against nulls and bugs
        if (taskCount < 0)
            return Collections.emptyMap();
        HashMap<String, Serializable> ret = new HashMap<>();
        ret.put("taskCount", taskCount);
        return ret;
    }
}
