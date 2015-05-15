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

import org.apache.cassandra.utils.ByteBufferUtil;

import static junit.framework.Assert.assertFalse;

public class CreateTableTest extends CQLTester
{
    @Test
    public void testCQL3PartitionKeyOnlyTable()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY);");
        assertFalse(currentTableMetadata().isThriftCompatible());
    }

    @Test
    public void testCreateTableWithSmallintColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b smallint, c smallint, primary key (a, b));");
        execute("INSERT INTO %s (a, b, c) VALUES ('1', 1, 2)");
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", "2", Short.MAX_VALUE, Short.MIN_VALUE);

        assertRows(execute("SELECT * FROM %s"),
                   row("1", (short) 1, (short) 2),
                   row("2", Short.MAX_VALUE, Short.MIN_VALUE));

        assertInvalidMessage("Expected 2 bytes for a smallint (4)",
                             "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", "3", 1, 2);
        assertInvalidMessage("Expected 2 bytes for a smallint (0)",
                             "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", "3", (short) 1, ByteBufferUtil.EMPTY_BYTE_BUFFER);
     }

    @Test
    public void testCreateTinyintColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b tinyint, c tinyint, primary key (a, b));");
        execute("INSERT INTO %s (a, b, c) VALUES ('1', 1, 2)");
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", "2", Byte.MAX_VALUE, Byte.MIN_VALUE);

        assertRows(execute("SELECT * FROM %s"),
                   row("1", (byte) 1, (byte) 2),
                   row("2", Byte.MAX_VALUE, Byte.MIN_VALUE));

        assertInvalidMessage("Expected 1 byte for a tinyint (4)",
                             "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", "3", 1, 2);

        assertInvalidMessage("Expected 1 byte for a tinyint (0)",
                             "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", "3", (byte) 1, ByteBufferUtil.EMPTY_BYTE_BUFFER);
     }
}
