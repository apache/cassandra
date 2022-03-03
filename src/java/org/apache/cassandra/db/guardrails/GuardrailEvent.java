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

import java.io.Serializable;
import java.util.HashMap;

import org.apache.cassandra.diag.DiagnosticEvent;

/**
 * {@link DiagnosticEvent} implementation for guardrail activation events.
 */
final class GuardrailEvent extends DiagnosticEvent
{
    enum GuardrailEventType
    {
        WARNED, FAILED
    }

    /** The type of activation, which is a warning or a failure. */
    private final GuardrailEventType type;

    /** The name that identifies the activated guardrail. */
    private final String name;

    /** The warn/fail message emitted by the activated guardrail. */
    private final String message;

    /**
     * Creates new guardrail activation event.
     *
     * @param type    The type of activation, which is warning or a failure.
     * @param name    The name that identifies the activated guardrail.
     * @param message The warn/fail message emitted by the activated guardrail.
     */
    GuardrailEvent(GuardrailEventType type, String name, String message)
    {
        this.type = type;
        this.name = name;
        this.message = message;
    }

    @Override
    public Enum<GuardrailEventType> getType()
    {
        return type;
    }

    @Override
    public HashMap<String, Serializable> toMap()
    {
        HashMap<String, Serializable> ret = new HashMap<>();
        ret.put("name", name);
        ret.put("message", message);
        return ret;
    }
}
