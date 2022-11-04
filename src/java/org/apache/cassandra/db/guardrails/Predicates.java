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

import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.Nullable;

import org.apache.cassandra.service.ClientState;

/**
 * A guardrail based on two predicates.
 *
 * <p>A {@link Predicates} guardrail defines (up to) 2 predicates, one at which a warning is issued, and another one
 * at which a failure is triggered. If failure is triggered, warning is skipped.
 *
 * @param <T> the type of the values to be tested against predicates.
 */
public class Predicates<T> extends Guardrail
{
    private final Function<ClientState, Predicate<T>> warnPredicate;
    private final Function<ClientState, Predicate<T>> failurePredicate;
    private final MessageProvider<T> messageProvider;

    /**
     * A function used to build the warning or error message of a triggered {@link Predicates} guardrail.
     */
    public interface MessageProvider<T>
    {
        /**
         * Called when the guardrail is triggered to build the corresponding message.
         *
         * @param isWarning whether the trigger is a warning one; otherwise it is failure one.
         * @param value     the value that triggers guardrail.
         */
        String createMessage(boolean isWarning, T value);
    }

    /**
     * Creates a new {@link Predicates} guardrail.
     *
     * @param name             the identifying name of the guardrail
     * @param reason           the optional description of the reason for guarding the operation
     * @param warnPredicate    a {@link ClientState}-based predicate provider that is used to check if given value should trigger a warning.
     * @param failurePredicate a {@link ClientState}-based predicate provider that is used to check if given value should trigger a failure.
     * @param messageProvider  a function to generate the warning or error message if the guardrail is triggered
     */
    Predicates(String name,
               @Nullable String reason,
               Function<ClientState, Predicate<T>> warnPredicate,
               Function<ClientState, Predicate<T>> failurePredicate,
               MessageProvider<T> messageProvider)
    {
        super(name, reason);
        this.warnPredicate = warnPredicate;
        this.failurePredicate = failurePredicate;
        this.messageProvider = messageProvider;
    }

    /**
     * Apply the guardrail to the provided value, triggering a warning or failure if appropriate.
     *
     * @param value the value to check.
     */
    public void guard(T value, @Nullable ClientState state)
    {
        if (!enabled(state))
            return;

        if (failurePredicate.apply(state).test(value))
        {
            fail(messageProvider.createMessage(false, value), state);
        }
        else if (warnPredicate.apply(state).test(value))
        {
            warn(messageProvider.createMessage(true, value));
        }
    }
}
