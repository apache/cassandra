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

public class DefaultGuardrailsFactory implements GuardrailsFactory
{
    @Override
    public Threshold threshold(String name,
            LongSupplier warnThreshold,
            LongSupplier failThreshold,
            Threshold.ErrorMessageProvider errorMessageProvider)
    {
        return new DefaultGuardrail.DefaultThreshold(name, warnThreshold, failThreshold, errorMessageProvider);
    }

    @Override
    public DisableFlag disableFlag(String name, BooleanSupplier disabled, String what)
    {
        return new DefaultGuardrail.DefaultDisableFlag(name, disabled, what);
    }

    @Override
    public <T> DisallowedValues<T> disallowedValues(String name, Supplier<Set<String>> disallowedRaw, Function<String, T> parser, String what)
    {
        return new DefaultGuardrail.DefaultDisallowedValues<>(name, disallowedRaw, parser, what);
    }

    @Override
    public <T> IgnoredValues<T> ignoredValues(String name, Supplier<Set<String>> ignoredRaw, Function<String, T> parser, String what)
    {
        return new DefaultGuardrail.DefaultIgnoredValues<>(name, ignoredRaw, parser, what);
    }

    @Override
    public <T> ValueBasedGuardrail<T> predicates(String name, Predicate<T> warnPredicate, Predicate<T> failurePredicate, ValueBasedGuardrail.MessageProvider<T> messageProvider)
    {
        return new DefaultGuardrail.Predicates<>(name, warnPredicate, failurePredicate, messageProvider);
    }
}
