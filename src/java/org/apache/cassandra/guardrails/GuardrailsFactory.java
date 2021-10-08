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
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.cassandra.service.QueryState;

public interface GuardrailsFactory
{
    /**
     * Creates a new {@link Threshold} guardrail.
     *
     * @param name                 the name of the guardrail
     * @param warnThreshold        a supplier of the threshold above which a warning should be triggered. This cannot be
     *                             null, but {@code () -> -1L} can be provided if no warning threshold is desired.
     * @param failThreshold        a supplier of the threshold above which a failure should be triggered. This cannot be
     *                             null, but {@code () -> -1L} can be provided if no failure threshold is desired.
     * @param errorMessageProvider a function to generate the error message if the guardrail is triggered
     *                             (being it for a warning or a failure).
     */
    Threshold threshold(String name,
            LongSupplier warnThreshold,
            LongSupplier failThreshold,
            Threshold.ErrorMessageProvider errorMessageProvider);

    /**
     * Creates a new {@link DisableFlag} guardrail.
     *
     * @param name     the name of the guardrail
     * @param disabled a supplier of boolean indicating whether the feature guarded by this guardrail must be
     *                 disabled.
     * @param what     the feature that is guarded by this guardrail (for reporting in error messages),
     *                 {@link DisableFlag#ensureEnabled(String, QueryState)} can specify a different {@code what}.
     */
    DisableFlag disableFlag(String name, BooleanSupplier disabled, String what);

    /**
     * Creates a new {@link DisallowedValues} guardrail.
     *
     * @param name          the name of the guardrail
     * @param disallowedRaw a supplier of the values that are disallowed in raw (string) form. The set returned by
     *                      this supplier <b>must not</b> be mutated (we don't use {@code ImmutableSet} because we
     *                      want to feed values from {@link GuardrailsConfig} directly and having ImmutableSet
     *                      there would currently be annoying (because populated automatically by snakeYaml)).
     * @param parser        a function to parse the value to disallow from string.
     * @param what          what represents the value disallowed (for reporting in error messages).
     */
    <T> DisallowedValues<T> disallowedValues(String name, Supplier<Set<String>> disallowedRaw, Function<String, T> parser, String what);

    /**
     * Creates a new {@link IgnoredValues} guardrail.
     *
     * @param name       the name of the guardrail
     * @param ignoredRaw a supplier of the values that are ignored in raw (string) form. The set returned by
     *                   this supplier <b>must not</b> be mutated (we don't use {@code ImmutableSet} because we
     *                   want to feed values from {@link GuardrailsConfig} directly and having ImmutableSet
     *                   there would currently be annoying (because populated automatically by snakeYaml)).
     * @param parser     a function to parse the value to ignore from string.
     * @param what       what represents the value ignored (for reporting in error messages).
     */
    <T> IgnoredValues<T> ignoredValues(String name, Supplier<Set<String>> ignoredRaw, Function<String, T> parser, String what);

    /**
     * Creates a new {@link ValueBasedGuardrail} guardrail which warns or fails on basis of provided Predicates
     *
     * @param name             the name of the guardrail (for identification in {@link Guardrails.Listener} events).
     * @param warnPredicate    a predicate that is used to check if given value should trigger a warning.
     * @param failurePredicate a predicate that is used to check if given value should trigger a failure.
     * @param messageProvider  a function to generate the warning or error message if the guardrail is triggered
     */
    <T> ValueBasedGuardrail<T> predicates(String name, Predicate<T> warnPredicate, Predicate<T> failurePredicate, ValueBasedGuardrail.MessageProvider<T> messageProvider);
}
