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
import org.apache.cassandra.hints.HintEvent.HintEventType;
import org.apache.cassandra.hints.HintEvent.HintResult;

/**
 * Utility methods for DiagnosticEvents around hinted handoff.
 */
final class HintDiagnostics
{
    private static final DiagnosticEventService service = DiagnosticEventService.instance();

    private HintDiagnostics()
    {
    }

    static void dispatcherCreated(HintsDispatcher dispatcher)
    {
        if (isEnabled(HintEventType.DISPATCHER_CREATED))
            service.publish(new HintEvent(HintEventType.DISPATCHER_CREATED, dispatcher,
                                          dispatcher.hostId, dispatcher.address, null, null, null, null));
    }

    static void dispatcherClosed(HintsDispatcher dispatcher)
    {
        if (isEnabled(HintEventType.DISPATCHER_CLOSED))
            service.publish(new HintEvent(HintEventType.DISPATCHER_CLOSED, dispatcher,
                                          dispatcher.hostId, dispatcher.address, null, null, null, null));
    }

    static void dispatchPage(HintsDispatcher dispatcher)
    {
        if (isEnabled(HintEventType.DISPATCHER_PAGE))
            service.publish(new HintEvent(HintEventType.DISPATCHER_PAGE, dispatcher,
                                          dispatcher.hostId, dispatcher.address, null, null, null, null));
    }

    static void abortRequested(HintsDispatcher dispatcher)
    {
        if (isEnabled(HintEventType.ABORT_REQUESTED))
            service.publish(new HintEvent(HintEventType.ABORT_REQUESTED, dispatcher,
                                          dispatcher.hostId, dispatcher.address, null, null, null, null));
    }

    static void pageSuccessResult(HintsDispatcher dispatcher, long success, long failures, long timeouts)
    {
        if (isEnabled(HintEventType.DISPATCHER_HINT_RESULT))
            service.publish(new HintEvent(HintEventType.DISPATCHER_HINT_RESULT, dispatcher,
                                          dispatcher.hostId, dispatcher.address, HintResult.PAGE_SUCCESS,
                                          success, failures, timeouts));
    }

    static void pageFailureResult(HintsDispatcher dispatcher, long success, long failures, long timeouts)
    {
        if (isEnabled(HintEventType.DISPATCHER_HINT_RESULT))
            service.publish(new HintEvent(HintEventType.DISPATCHER_HINT_RESULT, dispatcher,
                                          dispatcher.hostId, dispatcher.address, HintResult.PAGE_FAILURE,
                                          success, failures, timeouts));
    }

    private static boolean isEnabled(HintEventType type)
    {
        return service.isEnabled(HintEvent.class, type);
    }

}
