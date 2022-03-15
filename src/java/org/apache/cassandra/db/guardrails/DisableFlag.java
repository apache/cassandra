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
 * A guardrail that completely disables the use of a particular feature.
 *
 * <p>Note that this guardrail only aborts operations (if the feature is disabled) so is only meant for
 * query-based guardrails (we're happy to reject queries deemed dangerous, but we don't want to create a guardrail
 * that breaks compaction for instance).
 */
public class DisableFlag extends Guardrail
{
    private final Predicate<ClientState> disabled;
    private final String what;

    /**
     * Creates a new {@link DisableFlag} guardrail.
     *
     * @param name     the identifying name of the guardrail
     * @param disabled a {@link ClientState}-based supplier of boolean indicating whether the feature guarded by this
     *                 guardrail must be disabled.
     * @param what     The feature that is guarded by this guardrail (for reporting in error messages),
     *                 {@link DisableFlag#ensureEnabled(String, ClientState)} can specify a different {@code what}.
     */
    public DisableFlag(String name, Predicate<ClientState> disabled, String what)
    {
        super(name);
        this.disabled = disabled;
        this.what = what;
    }

    /**
     * Aborts the operation if this guardrail is disabled.
     *
     * <p>This must be called when the feature guarded by this guardrail is used to ensure such use is in fact
     * allowed.
     *
     * @param state The client state, used to skip the check if the query is internal or is done by a superuser.
     *              A {@code null} value means that the check should be done regardless of the query.
     */
    public void ensureEnabled(@Nullable ClientState state)
    {
        ensureEnabled(what, state);
    }

    /**
     * Aborts the operation if this guardrail is disabled.
     *
     * <p>This must be called when the feature guarded by this guardrail is used to ensure such use is in fact
     * allowed.
     *
     * @param what  The feature that is guarded by this guardrail (for reporting in error messages).
     * @param state The client state, used to skip the check if the query is internal or is done by a superuser.
     *              A {@code null} value means that the check should be done regardless of the query, although it won't
     *              throw any exception if the failure threshold is exceeded. This is so because checks without an
     *              associated client come from asynchronous processes such as compaction, and we don't want to
     *              interrupt such processes.
     */
    public void ensureEnabled(String what, @Nullable ClientState state)
    {
        if (enabled(state) && disabled.test(state))
            fail(what + " is not allowed", state);
    }
}
