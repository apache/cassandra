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

import java.util.Map;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.tracing.Tracing;

public class PasswordGuardrail extends CustomGuardrail<String>
{
    private static final Logger logger = LoggerFactory.getLogger(PasswordGuardrail.class);

    /**
     * @param configSupplier configuration supplier of the custom guardrail
     */
    public PasswordGuardrail(Supplier<CustomGuardrailConfig> configSupplier)
    {
        super("password", null, configSupplier, true);
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
            throw new PasswordGuardrailException(message, redactedMessage);
    }

    @Override
    String decorateMessage(String message)
    {
        return String.format("Guardrail %s violated: %s", name, message);
    }

    @Override
    void reconfigure(@Nullable Map<String, Object> newConfig)
    {
        if (!DatabaseDescriptor.isPasswordValidatorReconfigurationEnabled())
        {
            logger.warn("It is not possible to reconfigure password guardrail because " +
                        "property 'password_validator_reconfiguration_enabled' is set to false.");
            return;
        }

        super.reconfigure(newConfig);
    }

    public static class PasswordGuardrailException extends GuardrailViolatedException
    {
        public final String redactedMessage;

        PasswordGuardrailException(String message, String redactedMessage)
        {
            super(message);
            this.redactedMessage = redactedMessage;
        }
    }
}
