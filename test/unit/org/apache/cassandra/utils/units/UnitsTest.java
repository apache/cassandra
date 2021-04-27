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

import static org.assertj.core.api.Assertions.assertThat;

public class UnitsTest
{
    @Test
    public void testFormatValue()
    {
        assertThat(Units.formatValue(0L)).isEqualTo("0");
        // No comma
        assertThat(Units.formatValue(1L)).isEqualTo("1");
        assertThat(Units.formatValue(-1L)).isEqualTo("-1");
        assertThat(Units.formatValue(10L)).isEqualTo("10");
        assertThat(Units.formatValue(-10L)).isEqualTo("-10");
        assertThat(Units.formatValue(999L)).isEqualTo("999");
        assertThat(Units.formatValue(-999L)).isEqualTo("-999");

        // One comma
        assertThat(Units.formatValue(1_000L)).isEqualTo("1,000");
        assertThat(Units.formatValue(-1_000L)).isEqualTo("-1,000");
        assertThat(Units.formatValue(12_345L)).isEqualTo("12,345");
        assertThat(Units.formatValue(-12_345L)).isEqualTo("-12,345");
        assertThat(Units.formatValue(999_999L)).isEqualTo("999,999");
        assertThat(Units.formatValue(-999_999L)).isEqualTo("-999,999");

        // Two comma
        assertThat(Units.formatValue(1_000_000L)).isEqualTo("1,000,000");
        assertThat(Units.formatValue(-1_000_000L)).isEqualTo("-1,000,000");
        assertThat(Units.formatValue(999_999_999L)).isEqualTo("999,999,999");
        assertThat(Units.formatValue(-999_999_999L)).isEqualTo("-999,999,999");

        // Lots of comma
        assertThat(Units.formatValue(123_456_789_123_456_789L)).isEqualTo("123,456,789,123,456,789");
        assertThat(Units.formatValue(-123_456_789_123_456_789L)).isEqualTo("-123,456,789,123,456,789");
    }
}