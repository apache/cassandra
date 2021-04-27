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

public class RateValueTest
{
    @Test
    public void testCompute()
    {
        assertThat(RateValue.compute(SizeValue.of(10, SizeUnit.MEGABYTES), TimeValue.of(2, TimeUnit.SECONDS)))
        .isEqualTo(RateValue.of(5, RateUnit.MB_S).convert(RateUnit.B_S));

        assertThat(RateValue.compute(SizeValue.of(10, SizeUnit.MEGABYTES), TimeValue.of(5, TimeUnit.SECONDS)))
        .isEqualTo(RateValue.of(2, RateUnit.MB_S));

        assertThat(RateValue.compute(SizeValue.of(10, SizeUnit.MEGABYTES), TimeValue.of(10, TimeUnit.SECONDS)))
        .isEqualTo(RateValue.of(1, RateUnit.MB_S));

        // Reminder that 1MB = 1204KB, so 0.5MB == 512KB
        assertThat(RateValue.compute(SizeValue.of(10, SizeUnit.MEGABYTES), TimeValue.of(20, TimeUnit.SECONDS)))
        .isEqualTo(RateValue.of(512, RateUnit.KB_S));
    }

    @Test
    public void testTimeFor()
    {
        RateValue rate = RateValue.of(1, RateUnit.MB_S);
        assertThat(rate.timeFor(SizeValue.of(50, SizeUnit.MEGABYTES))).isEqualTo(TimeValue.of(50, TimeUnit.SECONDS));
        assertThat(rate.timeFor(SizeValue.of(93, SizeUnit.MEGABYTES))).isEqualTo(TimeValue.of(93, TimeUnit.SECONDS));
    }
}