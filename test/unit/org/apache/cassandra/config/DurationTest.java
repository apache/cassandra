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

public class DurationTest
{
    @Test
    public void testConversions()
    {
        assertEquals(10, new Duration("10s").toSeconds());
        assertEquals(10000, new Duration("10s").toMilliseconds());
        assertEquals(0, new Duration("10s").toMinutes());
        assertEquals(10, new Duration("10m").toMinutes());
        assertEquals(600000, new Duration("10m").toMilliseconds());
        assertEquals(600, new Duration("10m").toSeconds());
    }

    @Test
    public void testInvalidInputs()
    {
        assertThatThrownBy(() -> new Duration("10")).isInstanceOf(IllegalArgumentException.class)
                                                    .hasMessageContaining("Invalid duration: 10");
        assertThatThrownBy(() -> new Duration("-10s")).isInstanceOf(IllegalArgumentException.class)
                                                      .hasMessageContaining("Invalid duration: -10s");
        assertThatThrownBy(() -> new Duration("10xd")).isInstanceOf(IllegalArgumentException.class)
                                                      .hasMessageContaining("Unsupported time unit: xd. Supported units are: ns, us, ms, s, m, h, d");
    }

    @Test
    public void testEquals()
    {
        assertEquals(new Duration("10s"), new Duration("10s"));
        assertEquals(new Duration("10s"), new Duration("10000ms"));
        assertEquals(new Duration("10000ms"), new Duration("10s"));
        assertEquals(Duration.inMinutes(Long.MAX_VALUE), Duration.inMinutes(Long.MAX_VALUE));
        assertNotEquals(Duration.inMinutes(Long.MAX_VALUE), Duration.inMilliseconds(Long.MAX_VALUE));
        assertNotEquals(new Duration("0m"), new Duration("10ms"));
    }

}
