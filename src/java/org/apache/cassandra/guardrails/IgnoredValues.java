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

import java.util.Set;
import java.util.function.Consumer;
import javax.annotation.Nullable;

import org.apache.cassandra.service.QueryState;

/**
 * A guardrail that warns but ignore some specific values.
 *
 * @param <T> the type of the values of which certain are ignored.
 */
public interface IgnoredValues<T> extends Guardrail
{
    /**
     * Checks whether the provided value would trigger this guardrail.
     *
     * <p>This method is optional (does not have to be called) but can be used in the case some of the arguments
     * to the actual guardrail method is expensive to build to save doing so in the common case (of the
     * guardrail not being triggered).
     *
     * @param value the value to test.
     * @param state the query state, used to skip the check if the query is internal or is done by a superuser.
     * @return {@code true} if {@code value} is not allowed by this guardrail,
     * {@code false otherwise}.
     */
    boolean triggersOn(T value, @Nullable QueryState state);

    /**
     * Checks for ignored values by this guardrail and when it found some, log a warning and trigger an action
     * to ignore them.
     *
     * @param values       the values to check.
     * @param ignoreAction an action called on the subset of {@code values} that should be ignored. This action
     *                     should do whatever is necessary to make sure the value is ignored.
     * @param state        the query state, used to skip the check if the query is internal or is done by a superuser.
     *                     A {@code null} value means that the check should be done regardless of the query.
     */
    void maybeIgnoreAndWarn(Set<T> values, Consumer<T> ignoreAction, @Nullable QueryState state);
}
