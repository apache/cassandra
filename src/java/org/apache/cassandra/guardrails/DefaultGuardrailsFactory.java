/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
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
            DefaultGuardrail.DefaultThreshold.ErrorMessageProvider errorMessageProvider)
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
