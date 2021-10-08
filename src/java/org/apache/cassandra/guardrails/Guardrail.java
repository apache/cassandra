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

package org.apache.cassandra.guardrails;

import javax.annotation.Nullable;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.QueryState;

/**
 * General class defining a given guardrail (that guards against some particular usage/condition).
 *
 * <p>Some guardrails only emit warnings when triggered, while other fail the query that trigger them. Some may do one
 * or the other based on specific threshold.
 *
 * <p>Note that all the defined class support live updates, which is why each guardrail class ctor takes suppliers of
 * the condition the guardrail acts on rather than the condition itself. Which does imply that said suppliers should
 * be fast and non-blocking to avoid surprises. Note that this does not mean live updates are exposed to the user,
 * just that the implementation is up to it if we ever want to expose it.
 */
public interface Guardrail
{
    /**
     * Checks whether this guardrail is enabled or not. This will be enabled if guardrails are
     * ({@link Guardrails#ready()} ()}) and if the authenticated user (if specified) is not system nor superuser.
     *
     * @param state the query state, used to skip the check if the query is internal or is done by a superuser.
     *              A {@code null} value means that the check should be done regardless of the query.
     * @return {@code true} if this guardrail is enabled, {@code false} otherwise.
     */
    boolean enabled(@Nullable QueryState state);

    /**
     * do no throw {@link InvalidRequestException} if guardrail failure is triggered.
     * <p>
     * Note: this method is not thread safe and should only be used during guardrail initialization
     *
     * @return current guardrail
     */
    Guardrail setNoExceptionOnFailure();

    /**
     * Note: this method is not thread safe and should only be used during guardrail initialization
     *
     * @param minNotifyIntervalInMs minimal interval between subsequent warnings or failures
     *                              default 0 means always log
     *                              failure throws (unless {@link Guardrail#setNoExceptionOnFailure()} is used),
     *                              regardless of this minimal time
     * @return current guardrail
     */
    Guardrail setMinNotifyIntervalInMs(long minNotifyIntervalInMs);
}

