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
 * <p>A {@link Threshold} guardrail defines (up to) 2 thresholds, one at which a warning is issued, and a higher one
 * at which the operation is aborted with an exception. Only one of those thresholds can be activated if desired.
 *
 * <p>This guardrail only handles guarding positive values.
 */
public class Threshold extends Guardrail
{
    private final ToLongFunction<ClientState> warnThreshold;
    private final ToLongFunction<ClientState> failThreshold;
    private final ErrorMessageProvider messageProvider;

    /**
     * Creates a new threshold guardrail.
     *
     * @param warnThreshold   a {@link ClientState}-based provider of the value above which a warning should be triggered.
     * @param failThreshold   a {@link ClientState}-based provider of the value above which the operation should be aborted.
     * @param messageProvider a function to generate the warning or error message if the guardrail is triggered
     */
    public Threshold(ToLongFunction<ClientState> warnThreshold,
                     ToLongFunction<ClientState> failThreshold,
                     ErrorMessageProvider messageProvider)
    {
        this.warnThreshold = warnThreshold;
        this.failThreshold = failThreshold;
        this.messageProvider = messageProvider;
    }

    private String errMsg(boolean isWarning, String what, long value, long thresholdValue)
    {
        return messageProvider.createMessage(isWarning,
                                             what,
                                             value,
                                             thresholdValue);
    }

    private long failValue(ClientState state)
    {
        long failValue = failThreshold.applyAsLong(state);
        return failValue < 0 ? Long.MAX_VALUE : failValue;
    }

    private long warnValue(ClientState state)
    {
        long warnValue = warnThreshold.applyAsLong(state);
        return warnValue < 0 ? Long.MAX_VALUE : warnValue;
    }

    @Override
    public boolean enabled(@Nullable ClientState state)
    {
        if (!super.enabled(state))
            return false;

        return failThreshold.applyAsLong(state) >= 0 || warnThreshold.applyAsLong(state) >= 0;
    }

    /**
     * Apply the guardrail to the provided value, warning or failing if appropriate.
     *
     * @param value The value to check.
     * @param what  A string describing what {@code value} is a value of. This is used in the error message if the
     *              guardrail is triggered. For instance, say the guardrail guards the size of column values, then this
     *              argument must describe which column of which row is triggering the guardrail for convenience.
     * @param state The client state, used to skip the check if the query is internal or is done by a superuser.
     *              A {@code null} value means that the check should be done regardless of the query.
     */
    public void guard(long value, String what, @Nullable ClientState state)
    {
        if (!enabled(state))
            return;

        long failValue = failValue(state);
        if (value > failValue)
        {
            triggerFail(value, failValue, what);
            return;
        }

        long warnValue = warnValue(state);
        if (value > warnValue)
            triggerWarn(value, warnValue, what);
    }

    private void triggerFail(long value, long failValue, String what)
    {
        fail(errMsg(false, what, value, failValue));
    }

    private void triggerWarn(long value, long warnValue, String what)
    {
        warn(errMsg(true, what, value, warnValue));
    }

    /**
     * A function used to build the error message of a triggered {@link Threshold} guardrail.
     */
    interface ErrorMessageProvider
    {
        /**
         * Called when the guardrail is triggered to build the corresponding error message.
         *
         * @param isWarning Whether the trigger is a warning one; otherwise it is a failure one.
         * @param what      A string, provided by the call to the {@link #guard} method, describing what the guardrail
         *                  has been applied to (and that has triggered it).
         * @param value     The value that triggered the guardrail (as a string).
         * @param threshold The threshold that was passed to trigger the guardrail (as a string).
         */
        String createMessage(boolean isWarning, String what, long value, long threshold);
    }
}
