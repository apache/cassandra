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

import org.apache.cassandra.exceptions.ConfigurationException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

public class SmallestDurationSecondsTest
{
    @Test
    public void testInvalidUnits()
    {
        assertThatThrownBy(() -> new SmallestDurationSeconds("10ms")).isInstanceOf(ConfigurationException.class)
                                                                     .hasMessageContaining("Invalid duration: 10ms");
        assertThatThrownBy(() -> new SmallestDurationSeconds("10ns")).isInstanceOf(ConfigurationException.class)
                                                                     .hasMessageContaining("Invalid duration: 10ns");
        assertThatThrownBy(() -> new SmallestDurationSeconds("10us")).isInstanceOf(ConfigurationException.class)
                                                                     .hasMessageContaining("Invalid duration: 10us");
        assertThatThrownBy(() -> new SmallestDurationSeconds("10µs")).isInstanceOf(ConfigurationException.class)
                                                                     .hasMessageContaining("Invalid duration: 10µs");
        assertThatThrownBy(() -> new SmallestDurationSeconds("-10s")).isInstanceOf(ConfigurationException.class)
                                                                     .hasMessageContaining("Invalid duration: -10s");
    }

    @Test
    public void testValidUnits()
    {
        assertEquals(10L, new SmallestDurationSeconds("10s").toSeconds());
    }
}
