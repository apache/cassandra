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
package org.apache.cassandra.config;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.*;
import static org.quicktheories.QuickTheory.qt;

public class DurationSpecTest
{
    private static final long MAX_INT_CONFIG_VALUE = Integer.MAX_VALUE - 1;

    @SuppressWarnings("AssertBetweenInconvertibleTypes")
    @Test
    public void testConversions()
    {
        assertEquals(10000000000L, new DurationSpec.LongNanosecondsBound ("10s").toNanoseconds());
        assertEquals(MAX_INT_CONFIG_VALUE, new DurationSpec.IntSecondsBound(MAX_INT_CONFIG_VALUE + "s").toSeconds());
        assertEquals(MAX_INT_CONFIG_VALUE, new DurationSpec.LongMillisecondsBound(MAX_INT_CONFIG_VALUE + "ms").toMilliseconds());
        assertEquals(600000000000L, new DurationSpec.LongNanosecondsBound ("10m").toNanoseconds());
        assertEquals(MAX_INT_CONFIG_VALUE, new DurationSpec.IntMinutesBound(MAX_INT_CONFIG_VALUE + "m").toMinutes());
        assertEquals(MAX_INT_CONFIG_VALUE, new DurationSpec.IntSecondsBound(MAX_INT_CONFIG_VALUE + "s").toSeconds());
        assertEquals(new DurationSpec.IntMillisecondsBound(0.7, TimeUnit.MILLISECONDS), new DurationSpec.LongNanosecondsBound("1ms"));
        assertEquals(new DurationSpec.IntMillisecondsBound(0.33, TimeUnit.MILLISECONDS), new DurationSpec.LongNanosecondsBound("0ms"));
        assertEquals(new DurationSpec.IntMillisecondsBound(0.333, TimeUnit.MILLISECONDS), new DurationSpec.LongNanosecondsBound("0ms"));
    }

    @Test
    public void testFromSymbol()
    {
        assertEquals(DurationSpec.fromSymbol("ms"), TimeUnit.MILLISECONDS);
        assertEquals(DurationSpec.fromSymbol("d"), TimeUnit.DAYS);
        assertEquals(DurationSpec.fromSymbol("h"), TimeUnit.HOURS);
        assertEquals(DurationSpec.fromSymbol("m"), TimeUnit.MINUTES);
        assertEquals(DurationSpec.fromSymbol("s"), TimeUnit.SECONDS);
        assertEquals(DurationSpec.fromSymbol("us"), TimeUnit.MICROSECONDS);
        assertEquals(DurationSpec.fromSymbol("µs"), TimeUnit.MICROSECONDS);
        assertEquals(DurationSpec.fromSymbol("ns"), NANOSECONDS);
        assertThatThrownBy(() -> DurationSpec.fromSymbol("n")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testGetSymbol()
    {
        assertEquals(DurationSpec.symbol(TimeUnit.MILLISECONDS), "ms");
        assertEquals(DurationSpec.symbol(TimeUnit.DAYS), "d");
        assertEquals(DurationSpec.symbol(TimeUnit.HOURS), "h");
        assertEquals(DurationSpec.symbol(TimeUnit.MINUTES), "m");
        assertEquals(DurationSpec.symbol(TimeUnit.SECONDS), "s");
        assertEquals(DurationSpec.symbol(TimeUnit.MICROSECONDS), "us");
        assertEquals(DurationSpec.symbol(NANOSECONDS), "ns");
    }

    @Test
    public void testInvalidInputs()
    {
        assertThatThrownBy(() -> new DurationSpec.LongNanosecondsBound("10")).isInstanceOf(IllegalArgumentException.class)
                                                                             .hasMessageContaining("Invalid duration: 10");
        assertThatThrownBy(() -> new DurationSpec.LongNanosecondsBound("-10s")).isInstanceOf(IllegalArgumentException.class)
                                                                               .hasMessageContaining("Invalid duration: -10s");
        assertThatThrownBy(() -> new DurationSpec.LongNanosecondsBound(-10, SECONDS)).isInstanceOf(IllegalArgumentException.class)
                                                                               .hasMessageContaining("Invalid duration: value must be non-negativ");
        assertThatThrownBy(() -> new DurationSpec.LongNanosecondsBound("10xd")).isInstanceOf(IllegalArgumentException.class)
                                                                               .hasMessageContaining("Invalid duration: 10xd");
        assertThatThrownBy(() -> new DurationSpec.LongNanosecondsBound("0.333555555ms")).isInstanceOf(IllegalArgumentException.class)
                                                                                        .hasMessageContaining("Invalid duration: 0.333555555ms");
    }

    @Test
    public void testInvalidForConversion()
    {
        assertThatThrownBy(() -> new DurationSpec.LongNanosecondsBound(Long.MAX_VALUE + "ns")).isInstanceOf(IllegalArgumentException.class)
                                                                           .hasMessageContaining("Invalid duration: 9223372036854775807ns. " +
                                                                                                 "It shouldn't be more than 9223372036854775806 in nanoseconds");
        assertThatThrownBy(() -> new DurationSpec.LongNanosecondsBound(Long.MAX_VALUE + "ms")).isInstanceOf(IllegalArgumentException.class)
                                                                             .hasMessageContaining("Invalid duration: 9223372036854775807ms. " +
                                                                                                   "It shouldn't be more than 9223372036854775806 in nanoseconds");
        assertThatThrownBy(() -> new DurationSpec.LongNanosecondsBound(Long.MAX_VALUE-5 + "µs")).isInstanceOf(IllegalArgumentException.class)
                                                                               .hasMessageContaining("Invalid duration: 9223372036854775802µs. " +
                                                                                                     "It shouldn't be more than 9223372036854775806 in nanoseconds");
        assertThatThrownBy(() -> new DurationSpec.LongNanosecondsBound(Long.MAX_VALUE-5 + "us")).isInstanceOf(IllegalArgumentException.class)
                                                                               .hasMessageContaining("Invalid duration: 9223372036854775802us. " +
                                                                                                     "It shouldn't be more than 9223372036854775806 in nanoseconds");
        assertThatThrownBy(() -> new DurationSpec.LongNanosecondsBound(Long.MAX_VALUE-5 + "s")).isInstanceOf(IllegalArgumentException.class)
                                                                           .hasMessageContaining("Invalid duration: 9223372036854775802s. " +
                                                                                                 "It shouldn't be more than 9223372036854775806 in nanoseconds");
        assertThatThrownBy(() -> new DurationSpec.LongNanosecondsBound(Long.MAX_VALUE-5 + "h")).isInstanceOf(IllegalArgumentException.class)
                                                                          .hasMessageContaining("Invalid duration: 9223372036854775802h. " +
                                                                                                "It shouldn't be more than 9223372036854775806 in nanoseconds");
        assertThatThrownBy(() -> new DurationSpec.LongNanosecondsBound(Long.MAX_VALUE-5 + "d")).isInstanceOf(IllegalArgumentException.class)
                                                                          .hasMessageContaining("Invalid duration: 9223372036854775802d. " +
                                                                                                "It shouldn't be more than 9223372036854775806 in nanoseconds");
    }

    @Test
    public void testOverflowingConversion()
    {
        assertThatThrownBy(() -> new DurationSpec.IntMillisecondsBound("2147483648ms")).isInstanceOf(IllegalArgumentException.class)
                                                                                       .hasMessageContaining("Invalid duration: 2147483648ms." +
                                                                                                             " It shouldn't be more than 2147483646 in milliseconds");
        assertThatThrownBy(() -> new DurationSpec.IntMillisecondsBound(2147483648L)).isInstanceOf(IllegalArgumentException.class)
                                                                                    .hasMessageContaining("Invalid duration: 2147483648 milliseconds. " +
                                                                                                          "It shouldn't be more than 2147483646 in milliseconds");
        assertThatThrownBy(() -> new DurationSpec.IntMillisecondsBound("2147483648s")).isInstanceOf(IllegalArgumentException.class)
                                                                                      .hasMessageContaining("Invalid duration: 2147483648s. " +
                                                                                                            "It shouldn't be more than 2147483646 in milliseconds");
        assertThatThrownBy(() -> new DurationSpec.IntMillisecondsBound("35791395m")).isInstanceOf(IllegalArgumentException.class)
                                                                                    .hasMessageContaining("Invalid duration: 35791395m. " +
                                                                                                          "It shouldn't be more than 2147483646 in milliseconds");
        assertThatThrownBy(() -> new DurationSpec.IntMillisecondsBound("597h")).isInstanceOf(IllegalArgumentException.class)
                                                                               .hasMessageContaining("Invalid duration: 597h. " +
                                                                                                     "It shouldn't be more than 2147483646 in milliseconds");
        assertThatThrownBy(() -> new DurationSpec.IntMillisecondsBound("24856d")).isInstanceOf(IllegalArgumentException.class)
                                                                                 .hasMessageContaining("Invalid duration: 24856d. " +
                                                                                                       "It shouldn't be more than 2147483646 in milliseconds");

        assertThatThrownBy(() -> new DurationSpec.IntSecondsBound("2147483648s")).isInstanceOf(IllegalArgumentException.class)
                                                                                 .hasMessageContaining("Invalid duration: 2147483648s. " +
                                                                                                       "It shouldn't be more than 2147483646 in seconds");
        assertThatThrownBy(() -> new DurationSpec.IntSecondsBound(2147483648L)).isInstanceOf(IllegalArgumentException.class)
                                                                               .hasMessageContaining("Invalid duration: 2147483648 seconds. " +
                                                                                                     "It shouldn't be more than 2147483646 in seconds");
        assertThatThrownBy(() -> DurationSpec.IntSecondsBound.inSecondsString("2147483648")).isInstanceOf(NumberFormatException.class)
                                                                                            .hasMessageContaining("For input string: \"2147483648\"");
        assertThatThrownBy(() -> DurationSpec.IntSecondsBound.inSecondsString("2147483648s")).isInstanceOf(IllegalArgumentException.class)
                                                                                             .hasMessageContaining("Invalid duration: 2147483648s. " +
                                                                                                                   "It shouldn't be more than 2147483646 in seconds");
        assertThatThrownBy(() -> new DurationSpec.IntSecondsBound("35791395m")).isInstanceOf(IllegalArgumentException.class)
                                                                               .hasMessageContaining("Invalid duration: 35791395m. " +
                                                                                                     "It shouldn't be more than 2147483646 in seconds");
        assertThatThrownBy(() -> new DurationSpec.IntSecondsBound("596524h")).isInstanceOf(IllegalArgumentException.class)
                                                                             .hasMessageContaining("Invalid duration: 596524h. " +
                                                                                                   "It shouldn't be more than 2147483646 in seconds");
        assertThatThrownBy(() -> new DurationSpec.IntSecondsBound("24856d")).isInstanceOf(IllegalArgumentException.class)
                                                                            .hasMessageContaining("Invalid duration: 24856d. " +
                                                                                                  "It shouldn't be more than 2147483646 in seconds");

        assertThatThrownBy(() -> new DurationSpec.IntMinutesBound("2147483648s")).isInstanceOf(IllegalArgumentException.class)
                                                                                 .hasMessageContaining("Invalid duration: 2147483648s " +
                                                                                                       "Accepted units:[MINUTES, HOURS, DAYS]");
        assertThatThrownBy(() -> new DurationSpec.IntMinutesBound("2147483648m")).isInstanceOf(IllegalArgumentException.class)
                                                                                 .hasMessageContaining("Invalid duration: 2147483648m. " +
                                                                                                       "It shouldn't be more than 2147483646 in minutes");
        assertThatThrownBy(() -> new DurationSpec.IntMinutesBound("35791395h")).isInstanceOf(IllegalArgumentException.class)
                                                                               .hasMessageContaining("Invalid duration: 35791395h. " +
                                                                                                     "It shouldn't be more than 2147483646 in minutes");
        assertThatThrownBy(() -> new DurationSpec.IntMinutesBound("1491309d")).isInstanceOf(IllegalArgumentException.class)
                                                                              .hasMessageContaining("Invalid duration: 1491309d. " +
                                                                                                    "It shouldn't be more than 2147483646 in minutes");
        assertThatThrownBy(() -> new DurationSpec.IntMinutesBound(2147483648L)).isInstanceOf(IllegalArgumentException.class)
                                                                               .hasMessageContaining("Invalid duration: 2147483648 minutes. " +
                                                                                                     "It shouldn't be more than 2147483646 in minutes");
    }

    @SuppressWarnings("AssertBetweenInconvertibleTypes")
    @Test
    public void testEquals()
    {
        assertEquals(new DurationSpec.LongNanosecondsBound ("10s"), new DurationSpec.LongNanosecondsBound ("10s"));
        assertEquals(new DurationSpec.LongNanosecondsBound ("10s"), new DurationSpec.LongNanosecondsBound ("10000ms"));
        assertEquals(new DurationSpec.LongNanosecondsBound ("10000ms"), new DurationSpec.LongNanosecondsBound ("10s"));
        assertEquals(new DurationSpec.LongNanosecondsBound ("4h"), new DurationSpec.LongNanosecondsBound ("14400s"));
        assertEquals(DurationSpec.LongNanosecondsBound .IntSecondsBound.inSecondsString("14400"), new DurationSpec.LongNanosecondsBound ("14400s"));
        assertEquals(DurationSpec.LongNanosecondsBound .IntSecondsBound.inSecondsString("4h"), new DurationSpec.LongNanosecondsBound ("14400s"));
        assertEquals(DurationSpec.LongNanosecondsBound .IntSecondsBound.inSecondsString("14400s"), new DurationSpec.LongNanosecondsBound ("14400s"));
        assertNotEquals(new DurationSpec.LongNanosecondsBound ("0m"), new DurationSpec.LongNanosecondsBound ("10ms"));
        assertEquals(Long.MAX_VALUE-1, new DurationSpec.LongNanosecondsBound ("9223372036854775806ns").toNanoseconds());
    }

    @Test
    public void thereAndBack()
    {
        Gen<TimeUnit> unitGen = SourceDSL.arbitrary().enumValues(TimeUnit.class);
        Gen<Long> valueGen = SourceDSL.longs().between(0, Long.MAX_VALUE/24/60/60/1000L/1000L/1000L);
        qt().forAll(valueGen, unitGen).check((value, unit) -> {
            DurationSpec.LongNanosecondsBound  there = new DurationSpec.LongNanosecondsBound (value, unit);
            DurationSpec.LongNanosecondsBound  back = new DurationSpec.LongNanosecondsBound (there.toString());
            return there.equals(back);
        });
    }

    @Test
    public void testValidUnits()
    {
        assertEquals(10L, new DurationSpec.IntMillisecondsBound("10ms").toMilliseconds());
        assertEquals(10L, new DurationSpec.IntSecondsBound("10s").toSeconds());
        assertEquals(new DurationSpec.IntSecondsBound("10s"), DurationSpec.IntSecondsBound.inSecondsString("10"));
        assertEquals(new DurationSpec.IntSecondsBound("10s"), DurationSpec.IntSecondsBound.inSecondsString("10s"));

        assertEquals(10L, new DurationSpec.LongMicrosecondsBound("10us").toMicroseconds());
        assertEquals(10L, new DurationSpec.LongMillisecondsBound("10ms").toMilliseconds());
        assertEquals(10L, new DurationSpec.LongSecondsBound("10s").toSeconds());
    }

    @Test
    public void testInvalidUnits()
    {
        assertThatThrownBy(() -> new DurationSpec.IntMillisecondsBound("10ns")).isInstanceOf(IllegalArgumentException.class)
                                                                               .hasMessageContaining("Invalid duration: 10ns " +
                                                                                                     "Accepted units:[MILLISECONDS, SECONDS, MINUTES, HOURS, DAYS]");
        assertThatThrownBy(() -> new DurationSpec.IntMillisecondsBound(10, NANOSECONDS)).isInstanceOf(IllegalArgumentException.class)
                                                                               .hasMessageContaining("Invalid duration: 10 NANOSECONDS " +
                                                                                                     "Accepted units:[MILLISECONDS, SECONDS, MINUTES, HOURS, DAYS]");
        assertThatThrownBy(() -> new DurationSpec.IntMillisecondsBound("10us")).isInstanceOf(IllegalArgumentException.class)
                                                                               .hasMessageContaining("Invalid duration: 10us " +
                                                                                                     "Accepted units:[MILLISECONDS, SECONDS, MINUTES, HOURS, DAYS]");
        assertThatThrownBy(() -> new DurationSpec.IntMillisecondsBound(10, MICROSECONDS)).isInstanceOf(IllegalArgumentException.class)
                                                                               .hasMessageContaining("Invalid duration: 10 MICROSECONDS " +
                                                                                                     "Accepted units:[MILLISECONDS, SECONDS, MINUTES, HOURS, DAYS]");
        assertThatThrownBy(() -> new DurationSpec.IntMillisecondsBound("10µs")).isInstanceOf(IllegalArgumentException.class)
                                                                               .hasMessageContaining("Invalid duration: 10µs " +
                                                                                                     "Accepted units:[MILLISECONDS, SECONDS, MINUTES, HOURS, DAYS]");
        assertThatThrownBy(() -> new DurationSpec.IntMillisecondsBound(10, MICROSECONDS)).isInstanceOf(IllegalArgumentException.class)
                                                                               .hasMessageContaining("Invalid duration: 10 MICROSECONDS " +
                                                                                                     "Accepted units:[MILLISECONDS, SECONDS, MINUTES, HOURS, DAYS]");
        assertThatThrownBy(() -> new DurationSpec.IntMillisecondsBound("-10s")).isInstanceOf(IllegalArgumentException.class)
                                                                               .hasMessageContaining("Invalid duration: -10s " +
                                                                                                     "Accepted units:[MILLISECONDS, SECONDS, MINUTES, HOURS, DAYS]");

        assertThatThrownBy(() -> new DurationSpec.IntSecondsBound("10ms")).isInstanceOf(IllegalArgumentException.class)
                                                                          .hasMessageContaining("Invalid duration: 10ms Accepted units");
        assertThatThrownBy(() -> new DurationSpec.IntSecondsBound(10, MILLISECONDS)).isInstanceOf(IllegalArgumentException.class)
                                                                          .hasMessageContaining("Invalid duration: 10 MILLISECONDS Accepted units");
        assertThatThrownBy(() -> new DurationSpec.IntSecondsBound("10ns")).isInstanceOf(IllegalArgumentException.class)
                                                                          .hasMessageContaining("Invalid duration: 10ns Accepted units");
        assertThatThrownBy(() -> new DurationSpec.IntSecondsBound(10, NANOSECONDS)).isInstanceOf(IllegalArgumentException.class)
                                                                          .hasMessageContaining("Invalid duration: 10 NANOSECONDS Accepted units");
        assertThatThrownBy(() -> new DurationSpec.IntSecondsBound("10us")).isInstanceOf(IllegalArgumentException.class)
                                                                          .hasMessageContaining("Invalid duration: 10us Accepted units");
        assertThatThrownBy(() -> new DurationSpec.IntSecondsBound(10, MICROSECONDS)).isInstanceOf(IllegalArgumentException.class)
                                                                          .hasMessageContaining("Invalid duration: 10 MICROSECONDS Accepted units");
        assertThatThrownBy(() -> new DurationSpec.IntSecondsBound("10µs")).isInstanceOf(IllegalArgumentException.class)
                                                                          .hasMessageContaining("Invalid duration: 10µs Accepted units");
        assertThatThrownBy(() -> new DurationSpec.IntSecondsBound(10, MICROSECONDS)).isInstanceOf(IllegalArgumentException.class)
                                                                          .hasMessageContaining("Invalid duration: 10 MICROSECONDS Accepted units");
        assertThatThrownBy(() -> new DurationSpec.IntSecondsBound("-10s")).isInstanceOf(IllegalArgumentException.class)
                                                                          .hasMessageContaining("Invalid duration: -10s");

        assertThatThrownBy(() -> new DurationSpec.IntMinutesBound("10s")).isInstanceOf(IllegalArgumentException.class)
                                                                         .hasMessageContaining("Invalid duration: 10s Accepted units");
        assertThatThrownBy(() -> new DurationSpec.IntMinutesBound(10, SECONDS)).isInstanceOf(IllegalArgumentException.class)
                                                                         .hasMessageContaining("Invalid duration: 10 SECONDS Accepted units");
        assertThatThrownBy(() -> new DurationSpec.IntMinutesBound("10ms")).isInstanceOf(IllegalArgumentException.class)
                                                                          .hasMessageContaining("Invalid duration: 10ms Accepted units");
        assertThatThrownBy(() -> new DurationSpec.IntMinutesBound(10, MILLISECONDS)).isInstanceOf(IllegalArgumentException.class)
                                                                          .hasMessageContaining("Invalid duration: 10 MILLISECONDS Accepted units");
        assertThatThrownBy(() -> new DurationSpec.IntMinutesBound("10ns")).isInstanceOf(IllegalArgumentException.class)
                                                                          .hasMessageContaining("Invalid duration: 10ns Accepted units");
        assertThatThrownBy(() -> new DurationSpec.IntMinutesBound(10, NANOSECONDS)).isInstanceOf(IllegalArgumentException.class)
                                                                          .hasMessageContaining("Invalid duration: 10 NANOSECONDS Accepted units");
        assertThatThrownBy(() -> new DurationSpec.IntMinutesBound("10us")).isInstanceOf(IllegalArgumentException.class)
                                                                          .hasMessageContaining("Invalid duration: 10us Accepted units");
        assertThatThrownBy(() -> new DurationSpec.IntMinutesBound(10, MICROSECONDS)).isInstanceOf(IllegalArgumentException.class)
                                                                          .hasMessageContaining("Invalid duration: 10 MICROSECONDS Accepted units");
        assertThatThrownBy(() -> new DurationSpec.IntMinutesBound("10µs")).isInstanceOf(IllegalArgumentException.class)
                                                                          .hasMessageContaining("Invalid duration: 10µs Accepted units");
        assertThatThrownBy(() -> new DurationSpec.IntMinutesBound(10, MICROSECONDS)).isInstanceOf(IllegalArgumentException.class)
                                                                          .hasMessageContaining("Invalid duration: 10 MICROSECONDS Accepted units");
        assertThatThrownBy(() -> new DurationSpec.IntMinutesBound("-10s")).isInstanceOf(IllegalArgumentException.class)
                                                                          .hasMessageContaining("Invalid duration: -10s");

        assertThatThrownBy(() -> new DurationSpec.LongMicrosecondsBound("10ns")).isInstanceOf(IllegalArgumentException.class)
                                                                                .hasMessageContaining("Invalid duration: 10ns Accepted units");
        assertThatThrownBy(() -> new DurationSpec.LongMicrosecondsBound(10, NANOSECONDS)).isInstanceOf(IllegalArgumentException.class)
                                                                                         .hasMessageContaining("Invalid duration: 10 NANOSECONDS Accepted units");

        assertThatThrownBy(() -> new DurationSpec.LongMillisecondsBound("10ns")).isInstanceOf(IllegalArgumentException.class)
                                                                                .hasMessageContaining("Invalid duration: 10ns Accepted units");
        assertThatThrownBy(() -> new DurationSpec.LongMillisecondsBound(10, NANOSECONDS)).isInstanceOf(IllegalArgumentException.class)
                                                                                .hasMessageContaining("Invalid duration: 10 NANOSECONDS Accepted units");
        assertThatThrownBy(() -> new DurationSpec.LongMillisecondsBound("10us")).isInstanceOf(IllegalArgumentException.class)
                                                                                .hasMessageContaining("Invalid duration: 10us Accepted units");
        assertThatThrownBy(() -> new DurationSpec.LongMillisecondsBound(10, MICROSECONDS)).isInstanceOf(IllegalArgumentException.class)
                                                                                .hasMessageContaining("Invalid duration: 10 MICROSECONDS Accepted units");
        assertThatThrownBy(() -> new DurationSpec.LongMillisecondsBound("10µs")).isInstanceOf(IllegalArgumentException.class)
                                                                                .hasMessageContaining("Invalid duration: 10µs Accepted units");
        assertThatThrownBy(() -> new DurationSpec.LongMillisecondsBound(10, MICROSECONDS)).isInstanceOf(IllegalArgumentException.class)
                                                                                .hasMessageContaining("Invalid duration: 10 MICROSECONDS Accepted units");
        assertThatThrownBy(() -> new DurationSpec.LongMillisecondsBound("-10s")).isInstanceOf(IllegalArgumentException.class)
                                                                                .hasMessageContaining("Invalid duration: -10s");

        assertThatThrownBy(() -> new DurationSpec.LongSecondsBound("10ms")).isInstanceOf(IllegalArgumentException.class)
                                                                           .hasMessageContaining("Invalid duration: 10ms Accepted units");
        assertThatThrownBy(() -> new DurationSpec.LongSecondsBound(10, MILLISECONDS)).isInstanceOf(IllegalArgumentException.class)
                                                                           .hasMessageContaining("Invalid duration: 10 MILLISECONDS Accepted units");
        assertThatThrownBy(() -> new DurationSpec.LongSecondsBound("10ns")).isInstanceOf(IllegalArgumentException.class)
                                                                           .hasMessageContaining("Invalid duration: 10ns Accepted units");
        assertThatThrownBy(() -> new DurationSpec.LongSecondsBound(10, NANOSECONDS)).isInstanceOf(IllegalArgumentException.class)
                                                                           .hasMessageContaining("Invalid duration: 10 NANOSECONDS Accepted units");
        assertThatThrownBy(() -> new DurationSpec.LongSecondsBound(10, MICROSECONDS)).isInstanceOf(IllegalArgumentException.class)
                                                                           .hasMessageContaining("Invalid duration: 10 MICROSECONDS Accepted units");
        assertThatThrownBy(() -> new DurationSpec.LongSecondsBound("10µs")).isInstanceOf(IllegalArgumentException.class)
                                                                           .hasMessageContaining("Invalid duration: 10µs Accepted units");
        assertThatThrownBy(() -> new DurationSpec.LongSecondsBound(10, MICROSECONDS)).isInstanceOf(IllegalArgumentException.class)
                                                                           .hasMessageContaining("Invalid duration: 10 MICROSECONDS Accepted units");
        assertThatThrownBy(() -> new DurationSpec.LongSecondsBound("-10s")).isInstanceOf(IllegalArgumentException.class)
                                                                           .hasMessageContaining("Invalid duration: -10s");
    }
}
