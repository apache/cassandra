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
import org.apache.cassandra.exceptions.ConfigurationException;
import org.assertj.core.api.Assertions;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Utility class for testing a {@link Threshold} guardrail.
 */
public abstract class ThresholdTester extends GuardrailTester
{
    private final long warnThreshold;
    private final long failThreshold;
    private final TriConsumer<Guardrails, Long, Long> setter;
    private final ToLongFunction<Guardrails> warnGetter;
    private final ToLongFunction<Guardrails> failGetter;
    private final long maxValue;
    private final long disabledValue;

    protected ThresholdTester(int warnThreshold,
                              int failThreshold,
                              Threshold threshold,
                              TriConsumer<Guardrails, Integer, Integer> setter,
                              ToIntFunction<Guardrails> warnGetter,
                              ToIntFunction<Guardrails> failGetter)
    {
        super(threshold);
        this.warnThreshold = warnThreshold;
        this.failThreshold = failThreshold;
        this.setter = (g, w, a) -> setter.accept(g, w.intValue(), a.intValue());
        this.warnGetter = g -> (long) warnGetter.applyAsInt(g);
        this.failGetter = g -> (long) failGetter.applyAsInt(g);
        maxValue = Integer.MAX_VALUE;
        disabledValue = Config.DISABLED_GUARDRAIL;
    }

    protected ThresholdTester(long warnThreshold,
                              long failThreshold,
                              Threshold threshold,
                              TriConsumer<Guardrails, Long, Long> setter,
                              ToLongFunction<Guardrails> warnGetter,
                              ToLongFunction<Guardrails> failGetter)
    {
        super(threshold);
        this.warnThreshold = warnThreshold;
        this.failThreshold = failThreshold;
        this.setter = setter;
        this.warnGetter = warnGetter;
        this.failGetter = failGetter;
        maxValue = Long.MAX_VALUE;
        disabledValue = Config.DISABLED_SIZE_GUARDRAIL.toBytes();
    }

    protected long currentValue()
    {
        throw new UnsupportedOperationException();
    }

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
        assertNotNull(guardrail);
        testValidationOfThresholdProperties(guardrail.name + "_warn_threshold", guardrail.name + "_fail_threshold");
    }

    protected void testValidationOfThresholdProperties(String warnName, String failName)
    {
        setter.accept(guardrails(), disabledValue, disabledValue);

        testValidationOfStrictlyPositiveProperty((g, a) -> setter.accept(g, disabledValue, a), failName);
        testValidationOfStrictlyPositiveProperty((g, w) -> setter.accept(g, w, disabledValue), warnName);

        setter.accept(guardrails(), disabledValue, disabledValue);
        Assertions.assertThatThrownBy(() -> setter.accept(guardrails(), 2L, 1L))
                  .hasMessageContaining(guardrail.name + "_warn_threshold should be lower than the fail threshold");
    }

    protected void assertThresholdValid(String query) throws Throwable
    {
        assertValid(query);

        Assertions.assertThat(currentValue())
                  .isLessThanOrEqualTo(warnGetter.applyAsLong(guardrails()))
                  .isLessThanOrEqualTo(failGetter.applyAsLong(guardrails()));
    }

    protected void assertThresholdWarns(String query, String... messages) throws Throwable
    {
        assertWarns(query, messages);

        Assertions.assertThat(currentValue())
                  .isGreaterThan(warnGetter.applyAsLong(guardrails()))
                  .isLessThanOrEqualTo(failGetter.applyAsLong(guardrails()));
    }

    protected void assertThresholdFails(String query, String... messages) throws Throwable
    {
        assertFails(query, messages);

        Assertions.assertThat(currentValue())
                  .isGreaterThanOrEqualTo(warnGetter.applyAsLong(guardrails()))
                  .isEqualTo(failGetter.applyAsLong(guardrails()));
    }

    private void assertInvalidPositiveProperty(BiConsumer<Guardrails, Long> setter,
                                               long value,
                                               long maxValue,
                                               String name)
    {
        try
        {
            assertValidProperty(setter, value);
            fail(format("Expected exception for guardrails.%s value: %d", name, value));
        }
        catch (ConfigurationException e)
        {
            String expectedMessage = null;

            if (value < 0)
                expectedMessage = "Invalid data storage: value must be positive";

            Assertions.assertThat(e.getMessage()).contains(expectedMessage);
        }
        catch (IllegalArgumentException e)
        {
            String expectedMessage = null;

            if (value > maxValue)
                expectedMessage = format("Invalid value %d for %s: maximum allowed value is %d",
                                         value, name, maxValue);
            if (value == 0 && value != disabledValue)
                expectedMessage = format("Invalid value for %s: 0 is not allowed; if attempting to disable use %s",
                                         name, disabledValue);

            if (value < 0 && value != disabledValue)
                expectedMessage = format("Invalid value %d for %s: negative values are not "
                                         + "allowed, outside of %s which disables the guardrail",
                                         value, name, disabledValue);

            assertEquals(format("Exception message '%s' does not contain '%s'", e.getMessage(), expectedMessage),
                         expectedMessage, e.getMessage());
        }
    }

    private void assertInvalidStrictlyPositiveProperty(BiConsumer<Guardrails, Long> setter, long value, String name)
    {
        assertInvalidPositiveProperty(setter, value, maxValue, name);
    }

    protected void testValidationOfStrictlyPositiveProperty(BiConsumer<Guardrails, Long> setter, String name)
    {
        assertInvalidStrictlyPositiveProperty(setter, Integer.MIN_VALUE, name);
        assertInvalidStrictlyPositiveProperty(setter, -2, name);
        assertValidProperty(setter, disabledValue);
        assertInvalidStrictlyPositiveProperty(setter, disabledValue == 0 ? -1 : 0, name);
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
