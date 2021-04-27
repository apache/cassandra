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

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RateUnitTest
{
    @Test
    public void testConvert()
    {
        assertThat(RateUnit.B_S.convert(10L, RateUnit.KB_S)).isEqualTo(10 * 1024);
        assertThat(RateUnit.B_S.convert(10L, RateUnit.MB_S)).isEqualTo(10 * 1024 * 1024);
        assertThat(RateUnit.MB_S.convert(10L, RateUnit.GB_S)).isEqualTo(10 * 1024);
        assertThat(RateUnit.GB_S.convert(10L, RateUnit.TB_S)).isEqualTo(10 * 1024);
        assertThat(RateUnit.MB_S.convert(10L, RateUnit.B_S)).isEqualTo(0);

        RateUnit B_MS = RateUnit.of(SizeUnit.BYTES, TimeUnit.MILLISECONDS);
        // 10 kB/s == 10,240 B/s == 10 kB/ms
        assertThat(B_MS.convert(10L, RateUnit.KB_S)).isEqualTo(10L);

        RateUnit GB_D = RateUnit.of(SizeUnit.GIGABYTES, TimeUnit.DAYS);
        // 10 MB/s == 10 * 3600 MB/h == 36,000 MB/h == 864,000 MB/days == 843 GB/days
        assertThat(GB_D.convert(10L, RateUnit.MB_S)).isEqualTo(843L);

        RateUnit GB_MS = RateUnit.of(SizeUnit.GIGABYTES, TimeUnit.MILLISECONDS);
        // 10 MB/s == 0 GB/ms
        assertThat(GB_MS.convert(10L, RateUnit.MB_S)).isEqualTo(0L);

        RateUnit B_H = RateUnit.of(SizeUnit.BYTES, TimeUnit.HOURS);
        // 10 MB/s == 10 * 1024 * 1024 B/s = 10 * 1024 * 1024 * 3600 B/hours
        assertThat(B_H.convert(10L, RateUnit.MB_S)).isEqualTo(10L * 1024 * 1024 * 3600);
    }

    @Test
    public void testToHumanReadableString()
    {
        assertThat(RateUnit.B_S.toHumanReadableString(10)).isEqualTo("10B/s");
        assertThat(RateUnit.B_S.toHumanReadableString(1024L)).isEqualTo("1kB/s");
        assertThat(RateUnit.B_S.toHumanReadableString(1150L)).isEqualTo("1.1kB/s");
        assertThat(RateUnit.B_S.toHumanReadableString(4 * 1024 * 1024L)).isEqualTo("4MB/s");
        assertThat(RateUnit.GB_S.toHumanReadableString(2600 * 1024L)).isEqualTo("2,600TB/s");
    }

    @Test
    public void testCompare()
    {
        assertThat(RateUnit.MB_S.compareTo(RateUnit.MB_S)).isEqualTo(0);

        assertThat(RateUnit.MB_S.compareTo(RateUnit.GB_S)).isLessThan(0);
        assertThat(RateUnit.MB_S.compareTo(RateUnit.TB_S)).isLessThan(0);

        assertThat(RateUnit.MB_S.compareTo(RateUnit.KB_S)).isGreaterThan(0);
        assertThat(RateUnit.MB_S.compareTo(RateUnit.B_S)).isGreaterThan(0);

        RateUnit MB_MS = RateUnit.of(SizeUnit.MEGABYTES, TimeUnit.MILLISECONDS);
        RateUnit MB_NS = RateUnit.of(SizeUnit.MEGABYTES, TimeUnit.NANOSECONDS);

        assertThat(RateUnit.MB_S.compareTo(MB_MS)).isLessThan(0);
        assertThat(RateUnit.MB_S.compareTo(MB_NS)).isLessThan(0);

        RateUnit KB_MS = RateUnit.of(SizeUnit.KILOBYTES, TimeUnit.MILLISECONDS);
        RateUnit KB_NS = RateUnit.of(SizeUnit.KILOBYTES, TimeUnit.NANOSECONDS);

        // 1 MB/s = 1024 kB/s > 1000 kB/s = 1kB/ms
        assertThat(RateUnit.MB_S.compareTo(KB_MS)).isGreaterThan(0);
        // 1 MB/s = 1024 kB/s < 1000 * 1000 kB/s = 1kB/ns
        assertThat(RateUnit.MB_S.compareTo(KB_NS)).isLessThan(0);

        RateUnit MB_M = RateUnit.of(SizeUnit.MEGABYTES, TimeUnit.MINUTES);
        RateUnit GB_D = RateUnit.of(SizeUnit.GIGABYTES, TimeUnit.DAYS);
        // 1 MB/m = 1440 MB/d > 1024 MB/d = 1 GB/d
        assertThat(MB_M.compareTo(GB_D)).isGreaterThan(0);

        RateUnit GB_MS = RateUnit.of(SizeUnit.GIGABYTES, TimeUnit.MILLISECONDS);
        // 1 MB/s < 1 GB/ms
        assertThat(RateUnit.MB_S.compareTo(GB_MS)).isLessThan(0);
    }

    @Test
    public void testSmallestRepresentation()
    {
        // The smallest unit
        RateUnit B_D = RateUnit.of(SizeUnit.BYTES, TimeUnit.DAYS);

        // A few simple case that all resolve to the smallest unit
        assertThat(B_D.smallestRepresentableUnit(1)).isEqualTo(B_D);
        assertThat(B_D.smallestRepresentableUnit(Long.MAX_VALUE)).isEqualTo(B_D);
        assertThat(RateUnit.B_S.smallestRepresentableUnit(1)).isEqualTo(B_D);
        assertThat(RateUnit.KB_S.smallestRepresentableUnit(1)).isEqualTo(B_D);

        assertThat(RateUnit.MB_S.smallestRepresentableUnit(Long.MAX_VALUE - 10)).isEqualTo(RateUnit.MB_S);
        assertThat(RateUnit.MB_S.smallestRepresentableUnit((Long.MAX_VALUE / 1024) - 10)).isEqualTo(RateUnit.KB_S);

        // Slightly more subtle cases
        long v1 = (Long.MAX_VALUE - 1) / 1000;
        long v2 = (Long.MAX_VALUE - 1) / 1024;
        long v3 = (Long.MAX_VALUE - 1) / (1000 * 60);

        RateUnit MB_MS = RateUnit.of(SizeUnit.MEGABYTES, TimeUnit.MILLISECONDS);
        RateUnit KB_MS = RateUnit.of(SizeUnit.KILOBYTES, TimeUnit.MILLISECONDS);
        RateUnit MB_M = RateUnit.of(SizeUnit.MEGABYTES, TimeUnit.MINUTES);
        assertThat(MB_MS.smallestRepresentableUnit(v1)).isEqualTo(RateUnit.MB_S);
        assertThat(MB_MS.smallestRepresentableUnit(v2)).isEqualTo(KB_MS);
        assertThat(MB_MS.smallestRepresentableUnit(v3)).isEqualTo(MB_M);
    }
}