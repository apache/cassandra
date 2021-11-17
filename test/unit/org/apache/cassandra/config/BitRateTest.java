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

public class BitRateTest
{
    @Test
    public void testConversions()
    {
        assertEquals(10, new BitRate("10bps").toBitsPerSecond());
        assertEquals(10000, new BitRate("10Kbps").toBitsPerSecond());
        assertEquals(0, new BitRate("10Kbps").toMegabitsPerSecond());
        assertEquals(10000, new BitRate("10Mbps").toKilobitsPerSecond());
        assertEquals(10000000, new BitRate("10Mbps").toBitsPerSecond());
    }

    @Test
    public void testInvalidInputs()
    {
        assertThatThrownBy(() -> new BitRate("10")).isInstanceOf(IllegalArgumentException.class)
                                                    .hasMessageContaining("Invalid bit rate: 10");
        assertThatThrownBy(() -> new BitRate("-10bps")).isInstanceOf(IllegalArgumentException.class)
                                                      .hasMessageContaining("Invalid bit rate: -10bps");
        assertThatThrownBy(() -> new BitRate("10xbps")).isInstanceOf(IllegalArgumentException.class)
                                                      .hasMessageContaining("Unsupported bit rate unit: xbps. Supported units are: bps, kbps, mbps");
    }

    @Test
    public void testEquals()
    {
        assertEquals(new BitRate("10bps"), new BitRate("10bps"));
        assertEquals(new BitRate("10kbps"), new BitRate("10000bps"));
        assertEquals(new BitRate("10000bps"), new BitRate("10kbps"));
        assertEquals(BitRate.inMegabitsPerSecond(Long.MAX_VALUE), BitRate.inMegabitsPerSecond(Long.MAX_VALUE));
        assertNotEquals(BitRate.inMegabitsPerSecond(Long.MAX_VALUE), BitRate.inBitsPerSeconds(Long.MAX_VALUE));
        assertNotEquals(new BitRate("0kbps"), new BitRate("10mbps"));
    }

}
