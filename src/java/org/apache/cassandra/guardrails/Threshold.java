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

import java.util.function.Supplier;
import javax.annotation.Nullable;

import org.apache.cassandra.service.QueryState;

/**
 * A guardrail based on numeric threshold(s).
 *
 * <p>A {@link Threshold} guardrail defines (up to) 2 threshold, one at which a warning is issued, and a higher one
 * at which a failure is triggered. Only one of those threshold can be activated if desired.
 *
 * <p>This guardrail only handles guarding positive values.
 */
public interface Threshold extends Guardrail
{
    /**
     * A function used to build the error message of a triggered {@link Threshold} guardrail.
     */
    interface ErrorMessageProvider
    {
        /**
         * Called when the guardrail is triggered to build the corresponding error message.
         *
         * @param isWarning whether the trigger is a warning one; otherwise it is failure one.
         * @param what      a string, provided by the call to the {@link #guard} method, describing what the guardrail
         *                  has been applied to (and that has triggered it).
         * @param value     the value that triggered the guardrail (as a string).
         * @param threshold the threshold that was passed to trigger the guardrail (as a string).
         */
        String createMessage(boolean isWarning, String what, long value, long threshold);
    }

    /**
     * Checks whether the provided value would trigger a warning or failure if passed to {@link #guard}.
     *
     * <p>This method is optional (does not have to be called) but can be used in the case where the "what"
     * argument to {@link #guard} is expensive to build to save doing so in the common case (of the guardrail
     * not being triggered).
     *
     * @param value the value to test.
     * @param state the query state, used to skip the check if the query is internal or is done by a superuser.
     *              A {@code null} value means that the check should be done regardless of the query.
     * @return {@code true} if {@code value} is above the warning or failure thresholds of this guardrail,
     * {@code false otherwise}.
     */
    boolean triggersOn(long value, @Nullable QueryState state);

    /**
     * Apply the guardrail to the provided value, triggering a warning or failure if appropriate.
     *
     * @param value            the value to check.
     * @param what             a string describing what {@code value} is a value of used in the error message if the
     *                         guardrail is triggered (for instance, say the guardrail guards the size of column values, then this
     *                         argument must describe which column of which row is triggering the guardrail for convenience). Note that
     *                         this is only used if the guardrail triggers, so if it is expensive to build, you can put the call to
     *                         this method behind a {@link #triggersOn} call.
     * @param containsUserData a boolean describing if {@code what} contains user data. If this is the case,
     *                         {@code what} will only be included in the log messages and client warning. It will not be included in the
     *                         error messages that are passed to listeners and exceptions. We have to exclude the user data from
     *                         exceptions because they are sent to Insights.
     * @param state            the query state, used to skip the check if the query is internal or is done by a superuser.
     *                         A {@code null} value means that the check should be done regardless of the query.
     */
    void guard(long value, String what, boolean containsUserData, @Nullable QueryState state);

    /**
     * Creates a new {@link DefaultGuardrail.DefaultThreshold.DefaultGuardedCounter} guarded by this threshold guardrail.
     *
     * @param whatFct          a function called when either a warning or failure is triggered by the created counter to
     *                         describe the value. This is equivalent to the {@code what} argument of {@link #guard} but is a function to
     *                         allow the output string to be compute lazily (only if a failure/warn ends up being triggered).
     * @param containsUserData if a warning or failure is triggered by the created counter and the {@code whatFct}
     *                         is called, indicates whether the create string contains user data. This is the exact equivalent to the
     *                         similarly named argument of {@link #guard}.
     * @param state            the query state, used to skip the check if the query is internal or is done by a superuser.
     *                         A {@code null} value means that the check should be done regardless of the query.
     * @return the newly created guarded counter.
     */
    GuardedCounter newCounter(Supplier<String> whatFct, boolean containsUserData, @Nullable QueryState state);

    /**
     * A facility for when the value to guard is built incrementally, but we want to trigger failures as soon
     * as the failure threshold is reached, but only trigger the warning on the final value (and so only if the
     * failure threshold hasn't also been reached).
     * <p>
     * Note that instances are neither thread safe nor reusable.
     */
    interface GuardedCounter
    {
        /**
         * The currently accumulated value of the counter.
         */
        long get();

        /**
         * Add the provided increment to the counter, triggering a failure if the counter after this addition
         * crosses the failure threshold.
         *
         * @param increment the increment to add.
         */
        void add(long increment);

        /**
         * Trigger the warn if the currently accumulated counter value crosses warning threshold and the failure
         * has not been triggered yet.
         * <p>
         * This is generally meant to be called when the guarded value is complete.
         *
         * @return {@code true} and trigger a warning if the current counter value is greater than the warning
         * threshold and less than or equal to the failure threshold, {@code false} otherwise.
         */
        boolean checkAndTriggerWarning();
    }
}
