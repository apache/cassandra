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

import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.collect.Sets;

import org.apache.cassandra.service.ClientState;

import static java.lang.String.format;

/**
 * A guardrail that warns about some specific values, warns about but ignores some other values, and/or rejects the use
 * of some other values.
 *
 * @param <T> The type of the values of which certain are warned, ignored and/or disallowed.
 */
public class Values<T> extends Guardrail
{
    private final Function<ClientState, Set<T>> warnedValues;
    private final Function<ClientState, Set<T>> ignoredValues;
    private final Function<ClientState, Set<T>> disallowedValues;
    private final String what;

    /**
     * Creates a new values guardrail.
     *
     * @param name             the identifying name of the guardrail
     * @param reason           the optional description of the reason for guarding the operation
     * @param warnedValues     a {@link ClientState}-based provider of the values for which a warning is triggered.
     * @param ignoredValues    a {@link ClientState}-based provider of the values that are ignored.
     * @param disallowedValues a {@link ClientState}-based provider of the values that are disallowed.
     * @param what             The feature that is guarded by this guardrail (for reporting in error messages).
     */
    public Values(String name,
                  @Nullable String reason,
                  Function<ClientState, Set<T>> warnedValues,
                  Function<ClientState, Set<T>> ignoredValues,
                  Function<ClientState, Set<T>> disallowedValues,
                  String what)
    {
        super(name, reason);
        this.warnedValues = warnedValues;
        this.ignoredValues = ignoredValues;
        this.disallowedValues = disallowedValues;
        this.what = what;
    }

    /**
     * Triggers a warning for each of the provided values that is discouraged by this guardrail. If any of the values
     * is disallowed it will abort the operation.
     * <p>
     * This assumes that there aren't any values to be ignored, thus it doesn't require an ignore action. If this is
     * not the case and the provided value is set up to be ignored this will throw an assertion error.
     *
     * @param values The values to check.
     * @param state  The client state, used to skip the check if the query is internal or is done by a superuser.
     *               A {@code null} value means that the check should be done regardless of the query.
     */
    public void guard(Set<T> values, @Nullable ClientState state)
    {
        guard(values, x -> {
            throw new AssertionError(format("There isn't an ignore action for %s, but value %s is setup to be ignored",
                                            what, x));
        }, state);
    }

    /**
     * Triggers a warning for each of the provided values that is discouraged by this guardrail. Also triggers a warning
     * for each of the provided values that is ignored by this guardrail and triggers the provided action to ignore it.
     * If any of the values is disallowed it will abort the operation.
     *
     * @param values       The values to check.
     * @param ignoreAction An action called on the subset of {@code values} that should be ignored. This action
     *                     should do whatever is necessary to make sure the value is ignored.
     * @param state        The client state, used to skip the check if the query is internal or is done by a superuser.
     *                     A {@code null} value means that the check should be done regardless of the query, although it
     *                     won't throw any exception if the failure threshold is exceeded. This is so because checks
     *                     without an associated client come from asynchronous processes such as compaction, and we
     *                     don't want to interrupt such processes.
     */
    public void guard(Set<T> values, Consumer<T> ignoreAction, @Nullable ClientState state)
    {
        if (!enabled(state))
            return;

        Set<T> disallowed = disallowedValues.apply(state);
        Set<T> toDisallow = Sets.intersection(values, disallowed);
        if (!toDisallow.isEmpty())
            fail(format("Provided values %s are not allowed for %s (disallowed values are: %s)",
                        toDisallow.stream().sorted().collect(Collectors.toList()), what, disallowed), state);

        Set<T> ignored = ignoredValues.apply(state);
        Set<T> toIgnore = Sets.intersection(values, ignored);
        if (!toIgnore.isEmpty())
        {
            warn(format("Ignoring provided values %s as they are not supported for %s (ignored values are: %s)",
                        toIgnore.stream().sorted().collect(Collectors.toList()), what, ignored));
            toIgnore.forEach(ignoreAction);
        }

        Set<T> warned = warnedValues.apply(state);
        Set<T> toWarn = Sets.intersection(values, warned);
        if (!toWarn.isEmpty())
            warn(format("Provided values %s are not recommended for %s (warned values are: %s)",
                        toWarn.stream().sorted().collect(Collectors.toList()), what, warned));
    }
}
