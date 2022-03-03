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

package org.apache.cassandra.db.guardrails;

import org.apache.cassandra.db.guardrails.GuardrailEvent.GuardrailEventType;
import org.apache.cassandra.diag.DiagnosticEventService;

/**
 * Utility methods for {@link GuardrailEvent} activities.
 */
final class GuardrailsDiagnostics
{
    private static final DiagnosticEventService service = DiagnosticEventService.instance();

    private GuardrailsDiagnostics()
    {
    }

    /**
     * Creates a new diagnostic event for the activation of the soft/warn limit of a guardrail.
     *
     * @param name    The name that identifies the activated guardrail.
     * @param message The warning message emitted by the activated guardrail.
     */
    static void warned(String name, String message)
    {
        if (isEnabled(GuardrailEventType.WARNED))
            service.publish(new GuardrailEvent(GuardrailEventType.WARNED, name, message));
    }

    /**
     * Creates a new diagnostic event for the activation of the hard/fail limit of a guardrail.
     *
     * @param name    The name that identifies the activated guardrail.
     * @param message The failure message emitted by the activated guardrail.
     */
    static void failed(String name, String message)
    {
        if (isEnabled(GuardrailEventType.FAILED))
            service.publish(new GuardrailEvent(GuardrailEventType.FAILED, name, message));
    }

    private static boolean isEnabled(GuardrailEventType type)
    {
        return service.isEnabled(GuardrailEvent.class, type);
    }
}
