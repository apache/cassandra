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

public class SizeValueTest
{
    @Test
    public void testToString()
    {
        assertThat(SizeUnit.BYTES.value(10).toString()).isEqualTo("10B");
        assertThat(SizeUnit.BYTES.value(1900).toString()).isEqualTo("1.9kB");
        assertThat(SizeUnit.BYTES.value(2000).toString()).isEqualTo("2kB");
        assertThat(SizeUnit.BYTES.value(2200).toString()).isEqualTo("2.1kB");
        assertThat(SizeUnit.BYTES.value(42_345L).toString()).isEqualTo("41kB");
        assertThat(SizeUnit.BYTES.value(100_334_345L).toString()).isEqualTo("95MB");
        assertThat(SizeUnit.BYTES.value(345_100_334_345L).toString()).isEqualTo("321GB");
        assertThat(SizeUnit.BYTES.value(2_345_100_334_345L).toString()).isEqualTo("2.1TB");
        assertThat(SizeUnit.BYTES.value(98_345_100_334_345L).toString()).isEqualTo("89TB");
    }
}