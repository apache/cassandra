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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.*;

public class DataStorageTest
{
    @Test
    public void testConversions()
    {
        assertEquals(10, new DataStorage("10B").toBytes());
        assertEquals(10240, new DataStorage("10KB").toBytes());
        assertEquals(0, new DataStorage("10KB").toMegabytes());
        assertEquals(10240, new DataStorage("10MB").toKilobytes());
        assertEquals(10485760, new DataStorage("10MB").toBytes());
    }

    @Test
    public void testInvalidInputs()
    {
        assertThatThrownBy(() -> new DataStorage("10")).isInstanceOf(IllegalArgumentException.class)
                                                    .hasMessageContaining("Invalid data storage size: 10");
        assertThatThrownBy(() -> new DataStorage("-10bps")).isInstanceOf(IllegalArgumentException.class)
                                                      .hasMessageContaining("Invalid data storage size: -10bps");
        assertThatThrownBy(() -> new DataStorage("10HG")).isInstanceOf(IllegalArgumentException.class)
                                                      .hasMessageContaining("Unsupported data storage unit: HG. Supported units are: B, KB, MB, GB");
    }

    @Test
    public void testEquals()
    {
        assertEquals(new DataStorage("10B"), new DataStorage("10B"));
        assertEquals(new DataStorage("10KB"), new DataStorage("10240B"));
        assertEquals(new DataStorage("10240B"), new DataStorage("10KB"));
        assertEquals(DataStorage.inMegabytes(Long.MAX_VALUE), DataStorage.inMegabytes(Long.MAX_VALUE));
        assertNotEquals(DataStorage.inMegabytes(Long.MAX_VALUE), DataStorage.inBytes(Long.MAX_VALUE));
        assertNotEquals(new DataStorage("0MB"), new DataStorage("10KB"));
    }

}
