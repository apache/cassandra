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

import org.apache.cassandra.diag.DiagnosticEventService;
import org.apache.cassandra.service.PendingRangeCalculatorServiceEvent.PendingRangeCalculatorServiceEventType;

/**
 * Utility methods for diagnostic events related to {@link PendingRangeCalculatorService}.
 */
final class PendingRangeCalculatorServiceDiagnostics
{
    private static final DiagnosticEventService service = DiagnosticEventService.instance();

    private PendingRangeCalculatorServiceDiagnostics()
    {
    }
    
    static void taskStarted(int taskCount)
    {
        if (isEnabled(PendingRangeCalculatorServiceEventType.TASK_STARTED))
            service.publish(new PendingRangeCalculatorServiceEvent(PendingRangeCalculatorServiceEventType.TASK_STARTED,
                                                                   taskCount));
    }

    static void taskFinished()
    {
        if (isEnabled(PendingRangeCalculatorServiceEventType.TASK_FINISHED_SUCCESSFULLY))
            service.publish(new PendingRangeCalculatorServiceEvent(PendingRangeCalculatorServiceEventType.TASK_FINISHED_SUCCESSFULLY));
    }

    static void taskRejected(int taskCount)
    {
        if (isEnabled(PendingRangeCalculatorServiceEventType.TASK_EXECUTION_REJECTED))
            service.publish(new PendingRangeCalculatorServiceEvent(PendingRangeCalculatorServiceEventType.TASK_EXECUTION_REJECTED,
                                                                   taskCount));
    }

    static void taskCountChanged(int taskCount)
    {
        if (isEnabled(PendingRangeCalculatorServiceEventType.TASK_COUNT_CHANGED))
            service.publish(new PendingRangeCalculatorServiceEvent(PendingRangeCalculatorServiceEventType.TASK_COUNT_CHANGED,
                                                                   taskCount));
    }

    private static boolean isEnabled(PendingRangeCalculatorServiceEventType type)
    {
        return service.isEnabled(PendingRangeCalculatorServiceEvent.class, type);
    }
}
