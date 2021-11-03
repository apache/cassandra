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

import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.NoSpamLogger;

/**
 * General class defining a given guardrail that guards against some particular usage/condition.
 * <p>
 * Some guardrails only emit warnings when triggered, while others abort the query that triggers them. Some may do one
 * or the other based on specific threshold. The queries are aborted with an {@link InvalidRequestException}.
 * <p>
 * Note that all the defined classes support live updates, which is why each guardrail class constructor takes
 * suppliers of the condition the guardrail acts on rather than the condition itself. This implies that said suppliers
 * should be fast and non-blocking to avoid surprises.
 */
public abstract class Guardrail
{
    protected static final NoSpamLogger logger = NoSpamLogger.getLogger(LoggerFactory.getLogger(Guardrail.class),
                                                                        10, TimeUnit.MINUTES);

    /**
     * Checks whether this guardrail is enabled or not. This will be enabled if guardrails are enabled
     * ({@link Guardrails#enabled(ClientState)}) and if the authenticated user (if specified) is not system nor
     * superuser.
     *
     * @param state the client state, used to skip the check if the query is internal or is done by a superuser.
     *              A {@code null} value means that the check should be done regardless of the query.
     * @return {@code true} if this guardrail is enabled, {@code false} otherwise.
     */
    public boolean enabled(@Nullable ClientState state)
    {
        return Guardrails.enabled(state) && (state == null || state.isOrdinaryUser());
    }

    protected void warn(String message)
    {
        logger.warn(message);
        // Note that ClientWarn will simply ignore the message if we're not running this as part of a user query
        // (the internal "state" will be null)
        ClientWarn.instance.warn(message);
        // Similarly, tracing will also ignore the message if we're not running tracing on the current thread.
        Tracing.trace(message);
    }

    protected void abort(String message)
    {
        logger.error(message);
        // Note that ClientWarn will simply ignore the message if we're not running this as part of a user query
        // (the internal "state" will be null)
        ClientWarn.instance.warn(message);
        // Similarly, tracing will also ignore the message if we're not running tracing on the current thread.
        Tracing.trace(message);

        throw new InvalidRequestException(message);
    }
}
