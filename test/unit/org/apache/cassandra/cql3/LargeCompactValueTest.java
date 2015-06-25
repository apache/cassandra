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

import java.nio.ByteBuffer;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.io.util.RandomAccessReader;

public class LargeCompactValueTest extends CQLTester
{
    @Before
    public void before()
    {
        createTable("CREATE TABLE %s (key TEXT, column TEXT, value BLOB, PRIMARY KEY (key, column)) WITH COMPACT STORAGE");
    }

    @Test
    public void testInsertAndQuery() throws Throwable
    {
        ByteBuffer largeBytes = ByteBuffer.wrap(new byte[100000]);
        execute("INSERT INTO %s (key, column, value) VALUES (?, ?, ?)", "test", "a", largeBytes);
        ByteBuffer smallBytes = ByteBuffer.wrap(new byte[10]);
        execute("INSERT INTO %s (key, column, value) VALUES (?, ?, ?)", "test", "c", smallBytes);

        flush();

        assertRows(execute("SELECT column FROM %s WHERE key = ? AND column IN (?, ?, ?)", "test", "c", "a", "b"),
                   row("a"),
                   row("c"));
    }
}
