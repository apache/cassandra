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
        assertEquals(10, new DataRateSpec("10B/s").toBytesPerSecond());
        assertEquals(10240, new DataRateSpec("10KiB/s").toBytesPerSecond());
        assertEquals(0, new DataRateSpec("10KiB/s").toMebibytesPerSecond());
        assertEquals(10240, new DataRateSpec("10MiB/s").toKibibytesPerSecond());
        assertEquals(10485760, new DataRateSpec("10MiB/s").toBytesPerSecond());
        assertEquals(10485760, new DataRateSpec("10MiB/s").toBytesPerSecond());
        assertEquals(new DataRateSpec("24MiB/s"), DataRateSpec.megabitsPerSecondInMebibytesPerSecond(200L));
    }

    @Test
    public void testOverflowingDuringConversion()
    {
        assertEquals(Long.MAX_VALUE, new DataRateSpec("9223372036854775807B/s").toBytesPerSecond());
        assertEquals(Integer.MAX_VALUE, new DataRateSpec("9223372036854775807B/s").toBytesPerSecondAsInt());
        assertEquals(Long.MAX_VALUE, new DataRateSpec("9223372036854775807KiB/s").toBytesPerSecond());
        assertEquals(Integer.MAX_VALUE, new DataRateSpec("9223372036854775807KiB/s").toBytesPerSecondAsInt());
        assertEquals(Long.MAX_VALUE, new DataRateSpec("9223372036854775807MiB/s").toBytesPerSecond());
        assertEquals(Integer.MAX_VALUE, new DataRateSpec("9223372036854775807MiB/s").toBytesPerSecondAsInt());
        assertEquals(Long.MAX_VALUE, new DataRateSpec("9223372036854775807MiB/s").toBytesPerSecond());
        assertEquals(Integer.MAX_VALUE, new DataRateSpec("9223372036854775807MiB/s").toBytesPerSecondAsInt());

        assertEquals(Long.MAX_VALUE, new DataRateSpec("9223372036854775807MiB/s").toMegabitsPerSecond());
        assertEquals(Integer.MAX_VALUE, new DataRateSpec("9223372036854775807MiB/s").toMegabitsPerSecondAsInt());

        assertEquals(Long.MAX_VALUE, new DataRateSpec("9223372036854775807KiB/s").toKibibytesPerSecond());
        assertEquals(Integer.MAX_VALUE, new DataRateSpec("9223372036854775807KiB/s").toKibibytesPerSecondAsInt());
        assertEquals(Long.MAX_VALUE, new DataRateSpec("9223372036854775807MiB/s").toKibibytesPerSecond());
        assertEquals(Integer.MAX_VALUE, new DataRateSpec("9223372036854775807MiB/s").toKibibytesPerSecondAsInt());

        assertEquals(Long.MAX_VALUE, new DataRateSpec("9223372036854775807MiB/s").toMebibytesPerSecond());
        assertEquals(Integer.MAX_VALUE, new DataRateSpec("9223372036854775807MiB/s").toMebibytesPerSecondAsInt());
    }

    @Test
    public void testFromSymbol()
    {
        assertEquals(DataStorageSpec.DataStorageUnit.fromSymbol("B"), DataStorageSpec.DataStorageUnit.BYTES);
        assertEquals(DataStorageSpec.DataStorageUnit.fromSymbol("KiB"), DataStorageSpec.DataStorageUnit.KIBIBYTES);
        assertEquals(DataStorageSpec.DataStorageUnit.fromSymbol("MiB"), DataStorageSpec.DataStorageUnit.MEBIBYTES);
        assertEquals(DataStorageSpec.DataStorageUnit.fromSymbol("GiB"), DataStorageSpec.DataStorageUnit.GIBIBYTES);
        assertThatThrownBy(() -> DataStorageSpec.DataStorageUnit.fromSymbol("n"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported data storage unit: n");
    }

    @Test
    public void testInvalidInputs()
    {
        assertThatThrownBy(() -> new DataRateSpec("10")).isInstanceOf(IllegalArgumentException.class)
                                                        .hasMessageContaining("Invalid bit rate: 10");
        assertThatThrownBy(() -> new DataRateSpec("-10b/s")).isInstanceOf(IllegalArgumentException.class)
                                                            .hasMessageContaining("Invalid bit rate: -10b/s");
        assertThatThrownBy(() -> new DataRateSpec("10xb/s")).isInstanceOf(IllegalArgumentException.class)
                                                            .hasMessageContaining("Invalid bit rate: 10xb/s");
        assertThatThrownBy(() -> new DataRateSpec("9223372036854775809B/s")
                                 .toBytesPerSecond()).isInstanceOf(NumberFormatException.class)
                                            .hasMessageContaining("For input string: \"9223372036854775809\"");
    }

    @Test
    public void testEquals()
    {
        assertEquals(new DataRateSpec("10B/s"), new DataRateSpec("10B/s"));
        assertEquals(new DataRateSpec("10KiB/s"), new DataRateSpec("10240B/s"));
        assertEquals(new DataRateSpec("10240B/s"), new DataRateSpec("10KiB/s"));
        assertEquals(DataRateSpec.inMebibytesPerSecond(Long.MAX_VALUE), DataRateSpec.inMebibytesPerSecond(Long.MAX_VALUE));
        assertNotEquals(DataRateSpec.inMebibytesPerSecond(Long.MAX_VALUE), DataRateSpec.inBytesPerSecond(Long.MAX_VALUE));
        assertNotEquals(new DataRateSpec("0KiB/s"), new DataRateSpec("10MiB/s"));
    }

    @Test
    public void thereAndBack()
    {
        Gen<DataRateSpec.DataRateUnit> unitGen = SourceDSL.arbitrary().enumValues(DataRateSpec.DataRateUnit.class);
        Gen<Long> valueGen = SourceDSL.longs().between(0, Long.MAX_VALUE);
        qt().forAll(valueGen, unitGen).check((value, unit) -> {
            DataRateSpec there = new DataRateSpec(value, unit);
            DataRateSpec back = new DataRateSpec(there.toString());
            DataRateSpec BACK = new DataRateSpec(there.toString());
            return there.equals(back) && there.equals(BACK);
        });
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
        Gen<Long> valueGen = SourceDSL.longs().between(0, Long.MAX_VALUE);
        Gen<DataRateSpec> gen = rs -> new DataRateSpec(valueGen.generate(rs), unitGen.generate(rs));
        return gen.describedAs(DataRateSpec::toString);
    }
}
