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
 * A guardrail based on numeric threshold(s).
 *
 * <p>A {@link MaxThreshold} guardrail defines (up to) 2 thresholds, one at which a warning is issued, and a higher one
 * at which the operation is aborted with an exception. Only one of those thresholds can be activated if desired.
 *
 * <p>This guardrail only handles guarding positive values.
 */
public class MaxThreshold extends Threshold
{
    private final ToLongFunction<ClientState> warnThreshold;
    private final ToLongFunction<ClientState> failThreshold;

    /**
     * Creates a new threshold guardrail.
     *
     * @param name            the identifying name of the guardrail
     * @param warnThreshold   a {@link ClientState}-based provider of the value above which a warning should be triggered.
     * @param failThreshold   a {@link ClientState}-based provider of the value above which the operation should be aborted.
     * @param messageProvider a function to generate the warning or error message if the guardrail is triggered
     */
    public MaxThreshold(String name,
                        ToLongFunction<ClientState> warnThreshold,
                        ToLongFunction<ClientState> failThreshold,
                        Threshold.ErrorMessageProvider messageProvider)
    {
        super(name, warnThreshold, failThreshold, messageProvider);
        this.warnThreshold = warnThreshold;
        this.failThreshold = failThreshold;
    }

    private long failValue(ClientState state)
    {
        long failValue = failThreshold.applyAsLong(state);
        return failValue <= 0 ? Long.MAX_VALUE : failValue;
    }

    private long warnValue(ClientState state)
    {
        long warnValue = warnThreshold.applyAsLong(state);
        return warnValue <= 0 ? Long.MAX_VALUE : warnValue;
    }

    /**
     * Checks whether the provided value would trigger a warning or failure if passed to {@link #guard}.
     *
     * <p>This method is optional (does not have to be called) but can be used in the case where the "what"
     * argument to {@link #guard} is expensive to build to save doing so in the common case (of the guardrail
     * not being triggered).
     *
     * @param value the value to test.
     * @param state The client state, used to skip the check if the query is internal or is done by a superuser.
     *              A {@code null} value means that the check should be done regardless of the query.
     * @return {@code true} if {@code value} is above the warning or failure thresholds of this guardrail,
     * {@code false otherwise}.
     */
    public boolean triggersOn(long value, @Nullable ClientState state)
    {
        return enabled(state) && (value > Math.min(failValue(state), warnValue(state)));
    }

    /**
     * Apply the guardrail to the provided value, warning or failing if appropriate.
     *
     * @param value            The value to check.
     * @param what             A string describing what {@code value} is a value of. This is used in the error message
     *                         if the guardrail is triggered. For instance, say the guardrail guards the size of column
     *                         values, then this argument must describe which column of which row is triggering the
     *                         guardrail for convenience.
     * @param containsUserData whether the {@code what} contains user data that should be redacted on external systems.
     * @param state            The client state, used to skip the check if the query is internal or is done by a superuser.
     *                         A {@code null} value means that the check should be done regardless of the query.
     */
    public void guard(long value, String what, boolean containsUserData, @Nullable ClientState state)
    {
        if (!enabled(state))
            return;

        long failValue = failValue(state);
        if (value > failValue)
        {
            triggerFail(value, failValue, what, containsUserData, state);
            return;
        }

        long warnValue = warnValue(state);
        if (value > warnValue)
            triggerWarn(value, warnValue, what, containsUserData);
    }

}
