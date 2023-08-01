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

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.Clock;
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
    protected static final String REDACTED = "<redacted>";

    /** A name identifying the guardrail (mainly for shipping with diagnostic events). */
    public final String name;

    /** An optional description of the reason for guarding the operation. */
    @Nullable
    public final String reason;

    /** Minimum logging and triggering interval to avoid spamming downstream. */
    private long minNotifyIntervalInMs = 0;

    /** Time of last warning in milliseconds. */
    private volatile long lastWarnInMs = 0;

    /** Time of last failure in milliseconds. */
    private volatile long lastFailInMs = 0;

    Guardrail(String name, @Nullable String reason)
    {
        this.name = name;
        this.reason = reason;
    }

    /**
     * Checks whether this guardrail is enabled or not when the check is done for a background opperation that is not
     * associated to a specific {@link ClientState}, such as compaction or other background processes. Operations that
     * are associated to a {@link ClientState}, such as CQL queries, should use {@link Guardrail#enabled(ClientState)}.
     *
     * @return {@code true} if this guardrail is enabled, {@code false} otherwise.
     */
    public boolean enabled()
    {
        return enabled(null);
    }

    /**
     * Checks whether this guardrail is enabled or not. This will be enabled if the database is initialized and the
     * authenticated user (if specified) is not system nor superuser.
     *
     * @param state the client state, used to skip the check if the query is internal or is done by a superuser.
     *              A {@code null} value means that the check should be done regardless of the query.
     * @return {@code true} if this guardrail is enabled, {@code false} otherwise.
     */
    public boolean enabled(@Nullable ClientState state)
    {
        return DatabaseDescriptor.isDaemonInitialized() && (state == null || state.isOrdinaryUser());
    }

    protected void warn(String message)
    {
        warn(message, message);
    }

    protected void warn(String message, String redactedMessage)
    {
        if (skipNotifying(true))
            return;

        message = decorateMessage(message);

        logger.warn(message);
        // Note that ClientWarn will simply ignore the message if we're not running this as part of a user query
        // (the internal "state" will be null)
        ClientWarn.instance.warn(message);
        // Similarly, tracing will also ignore the message if we're not running tracing on the current thread.
        Tracing.trace(message);
        GuardrailsDiagnostics.warned(name, decorateMessage(redactedMessage));
    }

    protected void fail(String message, @Nullable ClientState state)
    {
        fail(message, message, state);
    }

    protected void fail(String message, String redactedMessage, @Nullable ClientState state)
    {
        message = decorateMessage(message);

        if (!skipNotifying(false))
        {
            logger.error(message);
            // Note that ClientWarn will simply ignore the message if we're not running this as part of a user query
            // (the internal "state" will be null)
            ClientWarn.instance.warn(message);
            // Similarly, tracing will also ignore the message if we're not running tracing on the current thread.
            Tracing.trace(message);
            GuardrailsDiagnostics.failed(name, decorateMessage(redactedMessage));
        }

        if (state != null)
            throw new GuardrailViolatedException(message);
    }

    @VisibleForTesting
    String decorateMessage(String message)
    {
        // Add a prefix to error message so user knows what threw the warning or cause the failure.
        String decoratedMessage = String.format("Guardrail %s violated: %s", name, message);

        // Add the reason for the guardrail triggering, if there is any.
        if (reason != null)
        {
            decoratedMessage += (message.endsWith(".") ? ' ' : ". ") + reason;
        }

        return decoratedMessage;
    }

    /**
     * Note: this method is not thread safe and should only be used during guardrail initialization
     *
     * @param minNotifyIntervalInMs frequency of logging and triggering listener to avoid spamming,
     *                              default 0 means always log and trigger listeners.
     * @return current guardrail
     */
    Guardrail minNotifyIntervalInMs(long minNotifyIntervalInMs)
    {
        assert minNotifyIntervalInMs >= 0;
        this.minNotifyIntervalInMs = minNotifyIntervalInMs;
        return this;
    }

    /**
     * reset last notify time to make sure it will notify downstream when {@link this#warn(String, String)}
     * or {@link this#fail(String, ClientState)} is called next time.
     */
    @VisibleForTesting
    void resetLastNotifyTime()
    {
        lastFailInMs = 0;
        lastWarnInMs = 0;
    }

    /**
     * @return true if guardrail should not log message and trigger listeners; otherwise, update lastWarnInMs or
     * lastFailInMs respectively.
     */
    private boolean skipNotifying(boolean isWarn)
    {
        if (minNotifyIntervalInMs == 0)
            return false;

        long nowInMs = Clock.Global.currentTimeMillis();
        long timeElapsedInMs = nowInMs - (isWarn ? lastWarnInMs : lastFailInMs);

        boolean skip = timeElapsedInMs < minNotifyIntervalInMs;

        if (!skip)
        {
            if (isWarn)
                lastWarnInMs = nowInMs;
            else
                lastFailInMs = nowInMs;
        }

        return skip;
    }
}
