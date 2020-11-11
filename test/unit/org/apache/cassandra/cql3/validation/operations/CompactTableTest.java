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

package org.apache.cassandra.cql3.validation.operations;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;

public class CompactTableTest extends CQLTester
{
    @Test
    public void dropCompactStorageTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, PRIMARY KEY (pk)) WITH COMPACT STORAGE;");
        execute("INSERT INTO %s (pk, ck) VALUES (1, 1)");
        alterTable("ALTER TABLE %s DROP COMPACT STORAGE");
        assertRows(execute( "SELECT * FROM %s"),
                   row(1, null, 1, null));

        createTable("CREATE TABLE %s (pk int, ck int, PRIMARY KEY (pk, ck)) WITH COMPACT STORAGE;");
        execute("INSERT INTO %s (pk, ck) VALUES (1, 1)");
        alterTable("ALTER TABLE %s DROP COMPACT STORAGE");
        assertRows(execute( "SELECT * FROM %s"),
                   row(1, 1, null));

        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH COMPACT STORAGE;");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 1)");
        alterTable("ALTER TABLE %s DROP COMPACT STORAGE");
        assertRows(execute( "SELECT * FROM %s"),
                   row(1, 1, 1));
    }

    @Test
    public void compactStorageSemanticsTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, PRIMARY KEY (pk, ck)) WITH COMPACT STORAGE");
        execute("INSERT INTO %s (pk, ck) VALUES (?, ?)", 1, 1);
        execute("DELETE FROM %s WHERE pk = ? AND ck = ?", 1, 1);
        assertEmpty(execute("SELECT * FROM %s WHERE pk = ?", 1));

        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY (pk, ck1, ck2)) WITH COMPACT STORAGE");
        execute("INSERT INTO %s (pk, ck1, v) VALUES (?, ?, ?)", 2, 2, 2);
        assertRows(execute("SELECT * FROM %s WHERE pk = ?",2),
                   row(2, 2, null, 2));
    }
}
