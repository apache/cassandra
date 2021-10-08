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
import javax.annotation.Nullable;

import org.apache.cassandra.service.QueryState;

/**
 * A guardrail that rejects the use of specific values.
 *
 * <p>Note that like {@link DisableFlag}, this guardrail only triggers failures and is thus only for query-based
 * guardrails.
 *
 * @param <T> the type of the values of which certain are disallowed.
 */
public interface DisallowedValues<T> extends Guardrail
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
     * Triggers a failure if the provided value is disallowed by this guardrail.
     *
     * @param value the value to check.
     * @param state the query state, used to skip the check if the query is internal or is done by a superuser.
     *              A {@code null} value means that the check should be done regardless of the query.
     */
    void ensureAllowed(T value, @Nullable QueryState state);

    /**
     * Triggers a failure if any of the provided values is disallowed by this guardrail.
     *
     * @param values the values to check.
     * @param state  the query state, used to skip the check if the query is internal or is done by a superuser.
     *               A {@code null} value means that the check should be done regardless of the query.
     */
    void ensureAllowed(Set<T> values, @Nullable QueryState state);
}
