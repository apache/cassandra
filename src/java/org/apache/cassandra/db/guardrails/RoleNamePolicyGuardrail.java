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

public class RoleNamePolicyGuardrail extends AbstractCustomGuardrail<String>
{
    private static final Logger logger = LoggerFactory.getLogger(RoleNamePolicyGuardrail.class);

    /**
     * @param configSupplier configuration supplier of the custom guardrail
     */
    public RoleNamePolicyGuardrail(Supplier<CustomGuardrailConfig> configSupplier)
    {
        super("role_name_policy", null, configSupplier, true);
    }

    @Override
    void reconfigure(@Nullable Map<String, Object> newConfig)
    {
        if (!DatabaseDescriptor.isRoleNamePolicyReconfigurationEnabled())
        {
            logger.warn("It is not possible to reconfigure role_name_policy guardrail because " +
                        "property 'role_name_policy_reconfiguration_enabled' is set to false.");
            return;
        }

        super.reconfigure(newConfig);
    }

    @Override
    protected void throwException(String message, String redactedMessage)
    {
        throw new RoleNamePolicyGuardrailException(message, redactedMessage);
    }

    public static class RoleNamePolicyGuardrailException extends GuardrailViolatedException
    {
        public final String redactedMessage;

        RoleNamePolicyGuardrailException(String message, String redactedMessage)
        {
            super(message);
            this.redactedMessage = redactedMessage;
        }
    }
}
