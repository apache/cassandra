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

package org.apache.cassandra.stress.settings;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.stress.report.StressMetrics;

import static org.junit.Assert.assertEquals;

public class StressSettingsTest
{
    @Test
    public void isSerializable() throws Exception
    {
        Map<String, String[]> args = new HashMap<>();
        args.put("write", new String[] {});
        StressSettings settings = StressSettings.get(args);
        // Will throw if not all settings are Serializable
        new ObjectOutputStream(new ByteArrayOutputStream()).writeObject(settings);
    }

    @Test
    public void test16473()
    {
        Set<String> jmxNodes = StressMetrics.toJmxNodes(new HashSet<String>(Arrays.asList("127.0.0.1:9042", "127.0.0.1")));
        assertEquals(0, jmxNodes.stream().filter(n -> n.contains(":")).count());
    }
}
