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


import org.apache.cassandra.diag.DiagnosticEventService;
import org.apache.cassandra.hints.HintsServiceEvent.HintsServiceEventType;

/**
 * Utility methods for DiagnosticEvents around the HintService.
 */
final class HintsServiceDiagnostics
{
    private static final DiagnosticEventService service = DiagnosticEventService.instance();

    private HintsServiceDiagnostics()
    {
    }
    
    static void dispatchingStarted(HintsService hintsService)
    {
        if (isEnabled(HintsServiceEventType.DISPATCHING_STARTED))
            service.publish(new HintsServiceEvent(HintsServiceEventType.DISPATCHING_STARTED, hintsService));
    }

    static void dispatchingShutdown(HintsService hintsService)
    {
        if (isEnabled(HintsServiceEventType.DISPATCHING_SHUTDOWN))
            service.publish(new HintsServiceEvent(HintsServiceEventType.DISPATCHING_SHUTDOWN, hintsService));
    }

    static void dispatchingPaused(HintsService hintsService)
    {
        if (isEnabled(HintsServiceEventType.DISPATCHING_PAUSED))
            service.publish(new HintsServiceEvent(HintsServiceEventType.DISPATCHING_PAUSED, hintsService));
    }

    static void dispatchingResumed(HintsService hintsService)
    {
        if (isEnabled(HintsServiceEventType.DISPATCHING_RESUMED))
            service.publish(new HintsServiceEvent(HintsServiceEventType.DISPATCHING_RESUMED, hintsService));
    }

    private static boolean isEnabled(HintsServiceEventType type)
    {
        return service.isEnabled(HintsServiceEvent.class, type);
    }

}
