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

import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

import org.junit.Before;
import org.junit.Test;

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
    private final Long disabledValue;

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
        disabledValue = -1L;
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
        disabledValue = -1L;
    }

    protected ThresholdTester(String warnThreshold,
                              String failThreshold,
                              Threshold threshold,
                              TriConsumer<Guardrails, String, String> setter,
                              Function<Guardrails, String> warnGetter,
                              Function<Guardrails, String> failGetter,
                              Function<Long, String> stringFormatter,
                              ToLongFunction<String> stringParser)
    {
        super(threshold);
        this.warnThreshold = stringParser.applyAsLong(warnThreshold);
        this.failThreshold = stringParser.applyAsLong(failThreshold);
        this.setter = (g, w, f) -> setter.accept(g,
                                                 w == null ? null : stringFormatter.apply(w),
                                                 f == null ? null : stringFormatter.apply(f));
        this.warnGetter = g -> stringParser.applyAsLong(warnGetter.apply(g));
        this.failGetter = g -> stringParser.applyAsLong(failGetter.apply(g));
        maxValue = Long.MAX_VALUE - 1;
        disabledValue = null;
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

    protected void assertMaxThresholdValid(String query) throws Throwable
    {
        assertValid(query);

        long warnValue = warnGetter.applyAsLong(guardrails());
        long failValue = failGetter.applyAsLong(guardrails());
        long current = currentValue();

        if (warnValue != disabledValue)
            Assertions.assertThat(current)
                      .isLessThanOrEqualTo(warnValue);

        if (failValue != disabledValue)
            Assertions.assertThat(current)
                      .isLessThanOrEqualTo(failValue);
    }

    protected void assertMinThresholdValid(String query) throws Throwable
    {
        assertValid(query);

        long warnValue = warnGetter.applyAsLong(guardrails());
        long failValue = failGetter.applyAsLong(guardrails());
        long current = currentValue();

        if (warnValue != disabledValue)
            Assertions.assertThat(current)
                      .isGreaterThanOrEqualTo(warnValue);

        if (failValue != disabledValue)
            Assertions.assertThat(current)
                      .isGreaterThanOrEqualTo(failValue);
    }


    protected void assertThresholdWarns(String query, String message) throws Throwable
    {
        assertThresholdWarns(query, message, message);
    }

    protected void assertThresholdWarns(String query, String message, String redactedMessage) throws Throwable
    {
        assertThresholdWarns(query, Collections.singletonList(message), Collections.singletonList(redactedMessage));
    }

    protected void assertThresholdWarns(String query, List<String> messages) throws Throwable
    {
        assertThresholdWarns(query, messages, messages);
    }

    protected void assertThresholdWarns(String query, List<String> messages, List<String> redactedMessages) throws Throwable
    {
        assertWarns(query, messages, redactedMessages);

        Assertions.assertThat(currentValue())
                  .isGreaterThan(warnGetter.applyAsLong(guardrails()))
                  .isLessThanOrEqualTo(failGetter.applyAsLong(guardrails()));
    }

    protected void assertThresholdFails(String query, String message) throws Throwable
    {
        assertThresholdFails(query, message, message);
    }

    protected void assertThresholdFails(String query, String message, String redactedMessage) throws Throwable
    {
        assertThresholdFails(query, Collections.singletonList(message), Collections.singletonList(redactedMessage));
    }

    protected void assertThresholdFails(String query, List<String> messages) throws Throwable
    {
        assertThresholdFails(query, messages, messages);
    }

    protected void assertThresholdFails(String query, List<String> messages, List<String> redactedMessages) throws Throwable
    {
        assertFails(query, messages, redactedMessages);

        Assertions.assertThat(currentValue())
                  .isGreaterThanOrEqualTo(warnGetter.applyAsLong(guardrails()))
                  .isEqualTo(failGetter.applyAsLong(guardrails()));
    }

    protected void assertInvalidPositiveProperty(BiConsumer<Guardrails, Long> setter,
                                               long value,
                                               long maxValue,
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

            if (value == 0 && value != disabledValue)
                expectedMessage = format("Invalid value for %s: 0 is not allowed; if attempting to disable use %s",
                                         name, disabledValue);

            if (value < 0 && disabledValue != null && value != disabledValue)
                expectedMessage = format("Invalid value %d for %s: negative values are not "
                                         + "allowed, outside of %s which disables the guardrail",
                                         value, name, disabledValue);

            if (expectedMessage == null && value < 0)
                expectedMessage = "value must be non-negative";

            Assertions.assertThat(e).hasMessageContaining(expectedMessage);
        }
    }

    private void assertInvalidStrictlyPositiveProperty(BiConsumer<Guardrails, Long> setter, long value, String name)
    {
        assertInvalidPositiveProperty(setter, value, maxValue, name);
    }

    protected void assertInvalidPositiveIntProperty (BiConsumer<Guardrails, Integer> setter, int value,
                                                     int maxValue,
                                                     String name)
    {
        assertInvalidPositiveProperty((g, l) -> setter.accept(g, l.intValue()), (long) value, maxValue, name);
    }

    protected void testValidationOfStrictlyPositiveProperty(BiConsumer<Guardrails, Long> setter, String name)
    {
        assertInvalidStrictlyPositiveProperty(setter, Integer.MIN_VALUE, name);
        assertInvalidStrictlyPositiveProperty(setter, -2, name);
        assertValidProperty(setter, disabledValue);
        assertInvalidStrictlyPositiveProperty(setter, disabledValue == null ? -1 : 0, name);
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
