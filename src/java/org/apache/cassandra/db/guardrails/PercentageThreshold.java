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

import java.util.function.ToLongFunction;

import javax.annotation.Nullable;

import org.apache.cassandra.service.ClientState;

/**
 * A {@link Threshold} guardrail whose values represent a percentage
 * <p>
 * This works exactly as a {@link Threshold}, but provides slightly more convenient error messages for percentage
 */
public class PercentageThreshold extends MaxThreshold
{
    /**
     * Creates a new threshold guardrail.
     *
     * @param name            the identifying name of the guardrail
     * @param reason          the optional description of the reason for guarding the operation
     * @param warnThreshold   a {@link ClientState}-based provider of the value above which a warning should be triggered.
     * @param failThreshold   a {@link ClientState}-based provider of the value above which the operation should be aborted.
     * @param messageProvider a function to generate the warning or error message if the guardrail is triggered
     */
    public PercentageThreshold(String name,
                               @Nullable String reason,
                               ToLongFunction<ClientState> warnThreshold,
                               ToLongFunction<ClientState> failThreshold,
                               ErrorMessageProvider messageProvider)
    {
        super(name, reason, warnThreshold, failThreshold, messageProvider);
    }

    @Override
    protected String errMsg(boolean isWarning, String what, long value, long thresholdValue)
    {
        return messageProvider.createMessage(isWarning,
                                             what,
                                             String.format("%d%%", value),
                                             String.format("%d%%", thresholdValue));
    }
}
