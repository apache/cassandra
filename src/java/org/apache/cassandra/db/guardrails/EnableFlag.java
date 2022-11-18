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

import java.util.function.Predicate;
import javax.annotation.Nullable;

import org.apache.cassandra.service.ClientState;

/**
 * A guardrail that enables the use of a particular feature.
 *
 * <p>Note that this guardrail only aborts operations (if the feature is not enabled) so is only meant for query-based
 * guardrails (we're happy to reject queries deemed dangerous, but we don't want to create a guardrail that breaks
 * compaction for instance).
 */
public class EnableFlag extends Guardrail
{
    private final Predicate<ClientState> warned;
    private final Predicate<ClientState> enabled;
    private final String featureName;

    /**
     * Creates a new {@link EnableFlag} guardrail.
     *
     * @param name        the identifying name of the guardrail
     * @param reason      the optional description of the reason for guarding the operation
     * @param enabled     a {@link ClientState}-based supplier of boolean indicating whether the feature guarded by this
     *                    guardrail is enabled.
     * @param featureName The feature that is guarded by this guardrail (for reporting in error messages), {@link
     *                    EnableFlag#ensureEnabled(String, ClientState)} can specify a different {@code featureName}.
     */
    public EnableFlag(String name, @Nullable String reason, Predicate<ClientState> enabled, String featureName)
    {
        this(name, reason, (state) -> false, enabled, featureName);
    }

    /**
     * Creates a new {@link EnableFlag} guardrail.
     *
     * @param name        the identifying name of the guardrail
     * @param reason      the optional description of the reason for guarding the operation
     * @param warned      a {@link ClientState}-based supplier of boolean indicating whether warning should be
     *                    emitted even guardrail as such has passed. If guardrail fails, the warning will not be
     *                    emitted. This might be used for cases when we want to warn a user regardless of successful
     *                    guardrail execution.
     * @param enabled     a {@link ClientState}-based supplier of boolean indicating whether the feature guarded by this
     *                    guardrail is enabled.
     * @param featureName The feature that is guarded by this guardrail (for reporting in error messages), {@link
     *                    EnableFlag#ensureEnabled(String, ClientState)} can specify a different {@code featureName}.
     */
    public EnableFlag(String name,
                      @Nullable String reason,
                      Predicate<ClientState> warned,
                      Predicate<ClientState> enabled,
                      String featureName)
    {
        super(name, reason);
        this.warned = warned;
        this.enabled = enabled;
        this.featureName = featureName;
    }

    /**
     * Returns whether the guarded feature is enabled or not.
     *
     * @param state The client state, used to skip the check if the query is internal or is done by a superuser.
     *              A {@code null} value means that the check should be done regardless of the query.
     * @return {@code true} is the feature is enabled, {@code false} otherwise.
     */
    public boolean isEnabled(@Nullable ClientState state)
    {
        return !enabled(state) || enabled.test(state);
    }

    /**
     * Aborts the operation if this guardrail is not enabled.
     *
     * <p>This must be called when the feature guarded by this guardrail is used to ensure such use is in fact
     * allowed.
     *
     * @param state The client state, used to skip the check if the query is internal or is done by a superuser.
     *              A {@code null} value means that the check should be done regardless of the query.
     */
    public void ensureEnabled(@Nullable ClientState state)
    {
        ensureEnabled(featureName, state);
    }

    /**
     * Aborts the operation if this guardrail is not enabled.
     *
     * <p>This must be called when the feature guarded by this guardrail is used to ensure such use is in fact
     * allowed.
     *
     * @param featureName The feature that is guarded by this guardrail (for reporting in error messages).
     * @param state       The client state, used to skip the check if the query is internal or is done by a superuser. A
     *                    {@code null} value means that the check should be done regardless of the query, although it
     *                    won't throw any exception if the failure threshold is exceeded. This is so because checks
     *                    without an associated client come from asynchronous processes such as compaction, and we don't
     *                    want to interrupt such processes.
     */
    public void ensureEnabled(String featureName, @Nullable ClientState state)
    {
        if (!enabled(state))
            return;

        if (!enabled.test(state))
        {
            fail(featureName + " is not allowed", state);
            return;
        }

        if (warned.test(state))
            warn(featureName + " is not recommended");
    }
}
