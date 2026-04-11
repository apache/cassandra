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

import java.util.function.Supplier;

import javax.annotation.Nullable;

import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.tracing.Tracing;

public abstract class AbstractCustomGuardrail<T> extends CustomGuardrail<T>
{
    /**
     * @param name                name of the custom guardrail
     * @param reason              guardrail reason
     * @param configSupplier      configuration supplier of the custom guardrail
     * @param guardWhileSuperuser when true, the guardrail will be executed even the caller is a superuser. If
     *                            false, this guardrail will be called only in case a caller is not a superuser.
     */
    public AbstractCustomGuardrail(String name, String reason, Supplier<CustomGuardrailConfig> configSupplier, boolean guardWhileSuperuser)
    {
        super(name, reason, configSupplier, guardWhileSuperuser);
    }

    @Override
    String decorateMessage(String message)
    {
        return String.format("Guardrail %s violated: %s", name, message);
    }

    @Override
    protected void warn(String message, String redactedMessage)
    {
        String msg = decorateMessage(message);
        String redactedMsg = decorateMessage(redactedMessage);

        ClientWarn.instance.warn(msg);
        Tracing.trace(redactedMsg);
        GuardrailsDiagnostics.warned(name, redactedMsg);
    }

    @Override
    protected void fail(String message, String redactedMessage, @Nullable ClientState state)
    {
        String msg = decorateMessage(message);
        String redactedMsg = decorateMessage(redactedMessage);

        ClientWarn.instance.warn(msg);
        Tracing.trace(redactedMsg);
        GuardrailsDiagnostics.failed(name, redactedMsg);

        if (state != null || throwOnNullClientState)
            throwException(message, redactedMessage);
    }

    protected abstract void throwException(String message, String redactedMessage);
}
