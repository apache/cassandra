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
package org.apache.cassandra.cql3;

import org.junit.Test;

public class TimestampTest extends CQLTester
{
    @Test
    public void testNegativeTimestamps() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");

        execute("INSERT INTO %s (k, v) VALUES (?, ?) USING TIMESTAMP ?", 1, 1, -42L);
        assertRows(execute("SELECT writetime(v) FROM %s WHERE k = ?", 1),
            row(-42L)
        );

        assertInvalid("INSERT INTO %s (k, v) VALUES (?, ?) USING TIMESTAMP ?", 2, 2, Long.MIN_VALUE);
    }
}
