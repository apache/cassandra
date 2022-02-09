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

import java.util.function.BiConsumer;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.assertj.core.api.Assertions;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Utility class for testing a {@link Threshold} guardrail.
 */
public abstract class ThresholdTester extends GuardrailTester
{
    private final String name;
    private final long warnThreshold;
    private final long failThreshold;
    private final TriConsumer<Guardrails, Long, Long> setter;
    private final ToLongFunction<Guardrails> warnGetter;
    private final ToLongFunction<Guardrails> failGetter;
    private final long maxValue = Integer.MAX_VALUE;

    protected ThresholdTester(long warnThreshold,
                              long failThreshold,
                              String name,
                              TriConsumer<Guardrails, Long, Long> setter,
                              ToLongFunction<Guardrails> warnGetter,
                              ToLongFunction<Guardrails> failGetter)
    {
        this.name = name;
        this.warnThreshold = warnThreshold;
        this.failThreshold = failThreshold;
        this.setter = setter;
        this.warnGetter = warnGetter;
        this.failGetter = failGetter;
    }

    protected ThresholdTester(int warnThreshold,
                              int failThreshold,
                              String name,
                              TriConsumer<Guardrails, Integer, Integer> setter,
                              ToIntFunction<Guardrails> warnGetter,
                              ToIntFunction<Guardrails> failGetter)
    {
        this.name = name;
        this.warnThreshold = warnThreshold;
        this.failThreshold = failThreshold;
        this.setter = (g, w, a) -> setter.accept(g, w.intValue(), a.intValue());
        this.warnGetter = g -> (long) warnGetter.applyAsInt(g);
        this.failGetter = g -> (long) failGetter.applyAsInt(g);
    }

    protected abstract long currentValue();

    protected void assertCurrentValue(int count)
    {
        assertEquals(count, currentValue());
    }

    @Before
    public void before()
    {
        setter.accept(guardrails(), warnThreshold, failThreshold);
    }

    @Test
    public void testConfigValidation()
    {
        testValidationOfThresholdProperties(name + "_warn_threshold", name + "_fail_threshold");
    }

    protected void testValidationOfThresholdProperties(String warnName, String failName)
    {
        setter.accept(guardrails(), -1L, -1L);

        testValidationOfStrictlyPositiveProperty((g, a) -> setter.accept(g, -1L, a), failName);
        testValidationOfStrictlyPositiveProperty((g, w) -> setter.accept(g, w, -1L), warnName);

        setter.accept(guardrails(), -1L, -1L);
        Assertions.assertThatThrownBy(() -> setter.accept(guardrails(), 2L, 1L))
                  .hasMessageContaining(format("The warn threshold 2 for %s should be lower than the fail threshold 1",
                                               name + "_warn_threshold"));
    }

    protected void assertThresholdValid(String query) throws Throwable
    {
        assertValid(query);

        Assertions.assertThat(currentValue())
                  .isLessThanOrEqualTo(warnGetter.applyAsLong(guardrails()))
                  .isLessThanOrEqualTo(failGetter.applyAsLong(guardrails()));
    }

    protected void assertThresholdWarns(String message, String query) throws Throwable
    {
        assertWarns(message, query);

        Assertions.assertThat(currentValue())
                  .isGreaterThan(warnGetter.applyAsLong(guardrails()))
                  .isLessThanOrEqualTo(failGetter.applyAsLong(guardrails()));
    }

    protected void assertThresholdFails(String message, String query) throws Throwable
    {
        assertFails(message, query);

        Assertions.assertThat(currentValue())
                  .isGreaterThanOrEqualTo(warnGetter.applyAsLong(guardrails()))
                  .isEqualTo(failGetter.applyAsLong(guardrails()));
    }

    private void assertInvalidPositiveProperty(BiConsumer<Guardrails, Long> setter,
                                               long value,
                                               long maxValue,
                                               boolean allowZero,
                                               String name)
    {
        try
        {
            assertValidProperty(setter, value);
            fail(format("Expected exception for guardrails.%s value: %d", name, value));
        }
        catch (IllegalArgumentException e)
        {
            String expectedMessage = null;

            if (value > maxValue)
                expectedMessage = format("Invalid value %d for %s: maximum allowed value is %d",
                                         value, name, maxValue);
            if (value == 0 && !allowZero)
                expectedMessage = format("Invalid value for %s: 0 is not allowed; if attempting to disable use %s",
                                         name, Config.DISABLED_GUARDRAIL);

            if (value < Config.DISABLED_GUARDRAIL)
                expectedMessage = format("Invalid value %d for %s: negative values are not "
                                         + "allowed, outside of %s which disables the guardrail",
                                         value, name, Config.DISABLED_GUARDRAIL);

            assertEquals(format("Exception message '%s' does not contain '%s'", e.getMessage(), expectedMessage),
                         expectedMessage, e.getMessage());
        }
    }

    private void assertInvalidStrictlyPositiveProperty(BiConsumer<Guardrails, Long> setter, long value, String name)
    {
        assertInvalidPositiveProperty(setter, value, maxValue, false, name);
    }

    protected void testValidationOfStrictlyPositiveProperty(BiConsumer<Guardrails, Long> setter, String name)
    {
        assertInvalidStrictlyPositiveProperty(setter, Integer.MIN_VALUE, name);
        assertInvalidStrictlyPositiveProperty(setter, -2, name);
        assertValidProperty(setter, (long) Config.DISABLED_GUARDRAIL); // disabled
        assertInvalidStrictlyPositiveProperty(setter, 0, name);
        assertValidProperty(setter, 1L);
        assertValidProperty(setter, 2L);
        assertValidProperty(setter, maxValue);
    }

    @FunctionalInterface
    public interface TriConsumer<T, U, V>
    {
        void accept(T t, U u, V v);
    }
}
