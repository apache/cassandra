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

package org.apache.cassandra.utils;

import org.junit.Test;

import static java.time.Instant.parse;
import static org.apache.cassandra.utils.JsonUtils.writeAsJsonString;
import static org.junit.Assert.assertEquals;

public class JsonUtilsTest
{
    @Test
    public void testTimestampSerialisation()
    {
        assertEquals("\"2025-01-15T13:26:45.040Z\"", writeAsJsonString(parse("2025-01-15T13:26:45.04Z")));
        assertEquals("\"2025-01-15T13:26:45.100Z\"", writeAsJsonString(parse("2025-01-15T13:26:45.1Z")));
        assertEquals("\"2025-01-15T13:26:45.123Z\"", writeAsJsonString(parse("2025-01-15T13:26:45.123Z")));
    }
}
