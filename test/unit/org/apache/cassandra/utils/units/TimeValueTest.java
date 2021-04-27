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

public class TimeValueTest
{
    @Test
    public void testToString()
    {
        assertThat(TimeValue.of(10L, TimeUnit.MILLISECONDS).toString()).isEqualTo("10ms");
        assertThat(TimeValue.of(1000L, TimeUnit.MILLISECONDS).toString()).isEqualTo("1s");
        assertThat(TimeValue.of(1200L, TimeUnit.MILLISECONDS).toString()).isEqualTo("1.2s");
        assertThat(TimeValue.of(42_324L, TimeUnit.MILLISECONDS).toString()).isEqualTo("42s");
        assertThat(TimeValue.of(60_000L, TimeUnit.MILLISECONDS).toString()).isEqualTo("1m");
        assertThat(TimeValue.of(250_000L, TimeUnit.MILLISECONDS).toString()).isEqualTo("4.2m");
        assertThat(TimeValue.of(3_600_000L, TimeUnit.MILLISECONDS).toString()).isEqualTo("1h");
        assertThat(TimeValue.of(24 * 10_200_000L, TimeUnit.MILLISECONDS).toString()).isEqualTo("2.8d");
    }
}