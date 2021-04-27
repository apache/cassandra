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
package org.apache.cassandra.utils.units;

import org.junit.Test;

import static org.apache.cassandra.utils.units.SizeUnit.BYTES;
import static org.apache.cassandra.utils.units.SizeUnit.C0;
import static org.apache.cassandra.utils.units.SizeUnit.C1;
import static org.apache.cassandra.utils.units.SizeUnit.C2;
import static org.apache.cassandra.utils.units.SizeUnit.C3;
import static org.apache.cassandra.utils.units.SizeUnit.C4;
import static org.apache.cassandra.utils.units.SizeUnit.GIGABYTES;
import static org.apache.cassandra.utils.units.SizeUnit.KILOBYTES;
import static org.apache.cassandra.utils.units.SizeUnit.MEGABYTES;
import static org.apache.cassandra.utils.units.SizeUnit.TERABYTES;
import static org.assertj.core.api.Assertions.assertThat;

public class SizeUnitTest
{
    @Test
    public void testConvert()
    {
        // We know convert delegates to the other methods, so we don't go overboard on testing it with all units. We
        // just use that test for a few random conversions.
        assertThat(GIGABYTES.convert(100, BYTES)).isEqualTo(0);
        assertThat(GIGABYTES.convert(100 * C3, BYTES)).isEqualTo(100);
        assertThat(GIGABYTES.convert(100 * C4, BYTES)).isEqualTo(100 * C1);
        assertThat(BYTES.convert(100 * C4, GIGABYTES)).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    public void testToBytes()
    {
        testToBytes(BYTES, C0);
        testToBytes(KILOBYTES, C1);
        testToBytes(MEGABYTES, C2);
        testToBytes(GIGABYTES, C3);
        testToBytes(TERABYTES, C4);

        // Test overflow
        assertThat(TERABYTES.toBytes(Long.MAX_VALUE / 10)).isEqualTo(Long.MAX_VALUE);
    }

    private void testToBytes(SizeUnit unit, long constant)
    {
        assertThat(unit.toBytes(1)).isEqualTo(1 * constant);
        assertThat(unit.toBytes(1023)).isEqualTo(1023 * constant);
        assertThat(unit.toBytes(1024)).isEqualTo(1024 * constant);
        assertThat(unit.toBytes(2049)).isEqualTo(2049 * constant);
    }

    @Test
    public void testToKiloBytes()
    {
        assertThat(BYTES.toKiloBytes(1)).isEqualTo(0);
        assertThat(BYTES.toKiloBytes(1023)).isEqualTo(0);
        assertThat(BYTES.toKiloBytes(1024)).isEqualTo(1);
        assertThat(BYTES.toKiloBytes(2049)).isEqualTo(2);

        testToKiloBytes(KILOBYTES, C0);
        testToKiloBytes(MEGABYTES, C1);
        testToKiloBytes(GIGABYTES, C2);
        testToKiloBytes(TERABYTES, C3);
    }

    private void testToKiloBytes(SizeUnit unit, long constant)
    {
        assertThat(unit.toKiloBytes(1)).isEqualTo(1 * constant);
        assertThat(unit.toKiloBytes(1023)).isEqualTo(1023 * constant);
        assertThat(unit.toKiloBytes(1024)).isEqualTo(1024 * constant);
        assertThat(unit.toKiloBytes(2049)).isEqualTo(2049 * constant);
    }

    @Test
    public void testToMegaBytes() throws Exception
    {
        testToMegaBytes(BYTES, 0);

        assertThat(KILOBYTES.toMegaBytes(1)).isEqualTo(0);
        assertThat(KILOBYTES.toMegaBytes(1023)).isEqualTo(0);
        assertThat(KILOBYTES.toMegaBytes(1024)).isEqualTo(1);
        assertThat(KILOBYTES.toMegaBytes(2049)).isEqualTo(2);

        testToMegaBytes(MEGABYTES, C0);
        testToMegaBytes(GIGABYTES, C1);
        testToMegaBytes(TERABYTES, C2);
    }

    private void testToMegaBytes(SizeUnit unit, long constant)
    {
        assertThat(unit.toMegaBytes(1)).isEqualTo(1 * constant);
        assertThat(unit.toMegaBytes(1023)).isEqualTo(1023 * constant);
        assertThat(unit.toMegaBytes(1024)).isEqualTo(1024 * constant);
        assertThat(unit.toMegaBytes(2049)).isEqualTo(2049 * constant);
    }

    @Test
    public void testToGigaBytes()
    {
        testToGigaBytes(BYTES, 0);
        testToGigaBytes(KILOBYTES, 0);

        assertThat(MEGABYTES.toGigaBytes(1)).isEqualTo(0);
        assertThat(MEGABYTES.toGigaBytes(1023)).isEqualTo(0);
        assertThat(MEGABYTES.toGigaBytes(1024)).isEqualTo(1);
        assertThat(MEGABYTES.toGigaBytes(2049)).isEqualTo(2);

        testToGigaBytes(GIGABYTES, C0);
        testToGigaBytes(TERABYTES, C1);
    }

    private void testToGigaBytes(SizeUnit unit, long constant)
    {
        assertThat(unit.toGigaBytes(1)).isEqualTo(1 * constant);
        assertThat(unit.toGigaBytes(1023)).isEqualTo(1023 * constant);
        assertThat(unit.toGigaBytes(1024)).isEqualTo(1024 * constant);
        assertThat(unit.toGigaBytes(2049)).isEqualTo(2049 * constant);
    }

    @Test
    public void testToTeraBytes()
    {
        testToTeraBytes(BYTES, 0);
        testToTeraBytes(KILOBYTES, 0);
        testToTeraBytes(MEGABYTES, 0);

        assertThat(GIGABYTES.toTeraBytes(1)).isEqualTo(0);
        assertThat(GIGABYTES.toTeraBytes(1023)).isEqualTo(0);
        assertThat(GIGABYTES.toTeraBytes(1024)).isEqualTo(1);
        assertThat(GIGABYTES.toTeraBytes(2049)).isEqualTo(2);

        testToTeraBytes(TERABYTES, C0);
    }

    private void testToTeraBytes(SizeUnit unit, long constant)
    {
        assertThat(unit.toTeraBytes(1)).isEqualTo(1 * constant);
        assertThat(unit.toTeraBytes(1023)).isEqualTo(1023 * constant);
        assertThat(unit.toTeraBytes(1024)).isEqualTo(1024 * constant);
        assertThat(unit.toTeraBytes(2049)).isEqualTo(2049 * constant);
    }
}