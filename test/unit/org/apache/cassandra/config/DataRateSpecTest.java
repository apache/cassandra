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

import org.junit.Test;

import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.*;
import static org.quicktheories.QuickTheory.qt;

public class DataRateSpecTest
{
    @Test
    public void testConversions()
    {
        assertEquals(10, new DataRateSpec.LongBytesPerSecondBound("10B/s").toBytesPerSecond(), 0);
        assertEquals(10240, new DataRateSpec.LongBytesPerSecondBound("10KiB/s").toBytesPerSecond(), 0);
        assertEquals(0, new DataRateSpec.LongBytesPerSecondBound("10KiB/s").toMebibytesPerSecond(), 0.1);
        assertEquals(10240, new DataRateSpec.LongBytesPerSecondBound("10MiB/s").toKibibytesPerSecond(), 0);
        assertEquals(10485760, new DataRateSpec.LongBytesPerSecondBound("10MiB/s").toBytesPerSecond(), 0);
        assertEquals(10, new DataRateSpec.LongBytesPerSecondBound("10MiB/s").toMebibytesPerSecond(), 0);
        assertEquals(new DataRateSpec.LongBytesPerSecondBound("25000000B/s").toString(), DataRateSpec.LongBytesPerSecondBound.megabitsPerSecondInBytesPerSecond(200L).toString());
    }

    @Test
    public void testOverflowingDuringConversion()
    {
        assertThatThrownBy(() -> new DataRateSpec.LongBytesPerSecondBound(Long.MAX_VALUE + "B/s"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid data rate: " +
                              "9223372036854775807B/s. It shouldn't be " +
                              "more than 9223372036854775806 in bytes_per_second");
        assertThatThrownBy(() -> new DataRateSpec.LongBytesPerSecondBound("9007199254740992KiB/s"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid data rate: " +
                              "9007199254740992KiB/s. It shouldn't be " +
                              "more than 9223372036854775806 in bytes_per_second");
        assertThatThrownBy(() -> new DataRateSpec.LongBytesPerSecondBound("8796093022208MiB/s"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid data rate: " +
                              "8796093022208MiB/s. It shouldn't be " +
                              "more than 9223372036854775806 in bytes_per_second");

        assertEquals(Integer.MAX_VALUE, new DataRateSpec.LongBytesPerSecondBound(Integer.MAX_VALUE + "MiB/s").toMegabitsPerSecondAsInt());
        assertEquals(Integer.MAX_VALUE, new DataRateSpec.LongBytesPerSecondBound(2147483649L + "KiB/s").toKibibytesPerSecondAsInt());
        assertEquals(Integer.MAX_VALUE, new DataRateSpec.LongBytesPerSecondBound(2147483649L / 1024L + "MiB/s").toKibibytesPerSecondAsInt());
        assertEquals(Integer.MAX_VALUE, new DataRateSpec.LongBytesPerSecondBound(2147483649L + "MiB/s").toMebibytesPerSecondAsInt());

        assertThatThrownBy(() -> new DataRateSpec.LongBytesPerSecondBound(Long.MAX_VALUE + "MiB/s"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid data rate: 9223372036854775807MiB/s. " +
                              "It shouldn't be more than 9223372036854775806 in bytes_per_second");
        assertThatThrownBy(() -> new DataRateSpec.LongBytesPerSecondBound(Long.MAX_VALUE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid data rate: 9.223372036854776E18 bytes_per_second. " +
                              "It shouldn't be more than 9223372036854775806 in bytes_per_second");

        assertThatThrownBy(() -> new DataRateSpec.LongBytesPerSecondBound("9007199254740992KiB/s"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid data rate: 9007199254740992KiB/s. " +
                              "It shouldn't be more than 9223372036854775806 in bytes_per_second");
        assertThatThrownBy(() -> new DataRateSpec.LongBytesPerSecondBound("8796093022208MiB/s"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid data rate: 8796093022208MiB/s. " +
                                                                                                                                                    "It shouldn't be more than 9223372036854775806 in bytes_per_second");
        assertThatThrownBy(() -> DataRateSpec.LongBytesPerSecondBound.megabitsPerSecondInBytesPerSecond(Integer.MAX_VALUE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid data rate: 2147483647 megabits per second; " +
                                                                                                                                                  "stream_throughput_outbound and " +
                                                                                                                                                  "inter_dc_stream_throughput_outbound should " +
                                                                                                                                                  "be between 0 and 2147483646 in megabits per second");
    }

    @Test
    public void testFromSymbol()
    {
        assertEquals(DataRateSpec.DataRateUnit.fromSymbol("B/s"), DataRateSpec.DataRateUnit.BYTES_PER_SECOND);
        assertEquals(DataRateSpec.DataRateUnit.fromSymbol("KiB/s"), DataRateSpec.DataRateUnit.KIBIBYTES_PER_SECOND);
        assertEquals(DataRateSpec.DataRateUnit.fromSymbol("MiB/s"), DataRateSpec.DataRateUnit.MEBIBYTES_PER_SECOND);
        assertThatThrownBy(() -> DataRateSpec.DataRateUnit.fromSymbol("n"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported data rate unit: n");
    }

    @Test
    public void testInvalidInputs()
    {
        assertThatThrownBy(() -> new DataRateSpec.LongBytesPerSecondBound("10")).isInstanceOf(IllegalArgumentException.class)
                                                                                .hasMessageContaining("Invalid data rate: 10");
        assertThatThrownBy(() -> new DataRateSpec.LongBytesPerSecondBound("-10b/s")).isInstanceOf(IllegalArgumentException.class)
                                                                                    .hasMessageContaining("Invalid data rate: -10b/s");
        assertThatThrownBy(() -> new DataRateSpec.LongBytesPerSecondBound(-10, DataRateSpec.DataRateUnit.BYTES_PER_SECOND))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid data rate: value must be non-negative");
        assertThatThrownBy(() -> new DataRateSpec.LongBytesPerSecondBound("10xb/s")).isInstanceOf(IllegalArgumentException.class)
                                                                                    .hasMessageContaining("Invalid data rate: 10xb/s");
        assertThatThrownBy(() -> new DataRateSpec.LongBytesPerSecondBound("9223372036854775809B/s")
                                 .toBytesPerSecond()).isInstanceOf(NumberFormatException.class)
                                                     .hasMessageContaining("For input string: \"9223372036854775809\"");
        assertThatThrownBy(() -> new DataRateSpec.LongBytesPerSecondBound("9223372036854775809KiB/s")
                                 .toBytesPerSecond()).isInstanceOf(NumberFormatException.class)
                                                     .hasMessageContaining("For input string: \"9223372036854775809\"");
        assertThatThrownBy(() -> new DataRateSpec.LongBytesPerSecondBound("9223372036854775809MiB/s")
                                 .toBytesPerSecond()).isInstanceOf(NumberFormatException.class)
                                                     .hasMessageContaining("For input string: \"9223372036854775809\"");
    }

    @Test
    public void testInvalidForConversion()
    {
        //just test the cast to Int as currently we don't even have any long bound rates and there is a very low probability of ever having them
        assertEquals(Integer.MAX_VALUE, new DataRateSpec.LongBytesPerSecondBound("92233720368547758B/s").toBytesPerSecondAsInt());

        assertThatThrownBy(() -> new DataRateSpec.LongBytesPerSecondBound(Long.MAX_VALUE + "B/s"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid data rate: 9223372036854775807B/s. " +
                              "It shouldn't be more than 9223372036854775806 in bytes_per_second");
        assertThatThrownBy(() -> new DataRateSpec.LongBytesPerSecondBound(Long.MAX_VALUE + "MiB/s"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid data rate: 9223372036854775807MiB/s. " +
                              "It shouldn't be more than 9223372036854775806 in bytes_per_second");
        assertThatThrownBy(() -> new DataRateSpec.LongBytesPerSecondBound(Long.MAX_VALUE - 5 + "KiB/s"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid data rate: 9223372036854775802KiB/s. " +
                              "It shouldn't be more than 9223372036854775806 in bytes_per_second");
    }

    @Test
    public void testValidUnits()
    {
        assertEquals(new DataRateSpec.LongBytesPerSecondBound("25000000B/s"), DataRateSpec.LongBytesPerSecondBound.megabitsPerSecondInBytesPerSecond(200));
        assertEquals(new DataRateSpec.LongBytesPerSecondBound("24MiB/s"), new DataRateSpec.LongBytesPerSecondBound("25165824B/s"));
    }

    @Test
    public void testEquals()
    {
        assertEquals(new DataRateSpec.LongBytesPerSecondBound("10B/s"), new DataRateSpec.LongBytesPerSecondBound("10B/s"));
        assertEquals(new DataRateSpec.LongBytesPerSecondBound("10KiB/s"), new DataRateSpec.LongBytesPerSecondBound("10240B/s"));
        assertEquals(new DataRateSpec.LongBytesPerSecondBound("10240B/s"), new DataRateSpec.LongBytesPerSecondBound("10KiB/s"));
        assertNotEquals(new DataRateSpec.LongBytesPerSecondBound("0KiB/s"), new DataRateSpec.LongBytesPerSecondBound("10MiB/s"));
    }

    @Test
    public void thereAndBackLongBytesPerSecondBound()
    {
        Gen<DataRateSpec.DataRateUnit> unitGen = SourceDSL.arbitrary().enumValues(DataRateSpec.DataRateUnit.class);
        Gen<Long> valueGen = SourceDSL.longs().between(0, 8796093022207L); // the biggest value in MiB/s that won't lead to B/s overflow
        qt().forAll(valueGen, unitGen).check((value, unit) -> {
            DataRateSpec.LongBytesPerSecondBound there = new DataRateSpec.LongBytesPerSecondBound(value, unit);
            DataRateSpec.LongBytesPerSecondBound back = new DataRateSpec.LongBytesPerSecondBound(there.toString());
            return there.equals(back) && back.equals(there);
        });
    }

    @Test
    public void testToString()
    {
        DataRateSpec testProperty = new DataRateSpec.LongBytesPerSecondBound("5B/s");
        assertEquals("5B/s", testProperty.toString());
    }

    @Test
    public void eq()
    {
        qt().forAll(gen(), gen()).check((a, b) -> a.equals(b) == b.equals(a));
    }

    @Test
    public void eqAndHash()
    {
        qt().forAll(gen(), gen()).check((a, b) -> !a.equals(b) || a.hashCode() == b.hashCode());
    }

    private static Gen<DataRateSpec> gen()
    {
        Gen<DataRateSpec.DataRateUnit> unitGen = SourceDSL.arbitrary().enumValues(DataRateSpec.DataRateUnit.class);
        Gen<Long> valueGen = SourceDSL.longs().between(0, Long.MAX_VALUE/1024L/1024/1024);
        Gen<DataRateSpec> gen = rs -> new DataRateSpec.LongBytesPerSecondBound(valueGen.generate(rs), unitGen.generate(rs));
        return gen.describedAs(DataRateSpec::toString);
    }
}
