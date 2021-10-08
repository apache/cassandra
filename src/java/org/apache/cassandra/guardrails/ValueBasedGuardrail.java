/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.guardrails;

import javax.annotation.Nullable;

import org.apache.cassandra.service.QueryState;

/**
 * A guardrail based on two predicates.
 *
 * <p>A {@link ValueBasedGuardrail} guardrail defines a basic guardrail that may warn or fail,
 * depending on provided value
 *
 * @param <T> the type of the values to be tested.
 */
public interface ValueBasedGuardrail<T> extends Guardrail
{
    /**
     * A function used to build the warning or error message of a triggered {@link ValueBasedGuardrail} guardrail.
     */
    interface MessageProvider<T>
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
     * Apply the guardrail to the provided value, triggering a warning or failure if appropriate.
     *
     * @param value the value to check.
     * @param state the query state, used to skip the check if the query is internal or is done by a superuser.
     *              A {@code null} value means that the check should be done regardless of the query.
     */
    void guard(T value, @Nullable QueryState state);
}
