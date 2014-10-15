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

public class UserTypesTest extends CQLTester
{
    @Test
    public void testInvalidField() throws Throwable
    {
        String myType = createType("CREATE TYPE %s (f int)");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v frozen<" + myType + ">)");

        // 's' is not a field of myType
        assertInvalid("INSERT INTO %s (k, v) VALUES (?, {s : ?})", 0, 1);
    }

    @Test
    public void testCassandra8105() throws Throwable
    {
        String ut1 = createType("CREATE TYPE %s (a int, b int)");
        String ut2 = createType("CREATE TYPE %s (j frozen<" + KEYSPACE + "." + ut1 + ">, k int)");
        createTable("CREATE TABLE %s (x int PRIMARY KEY, y set<frozen<" + KEYSPACE + "." + ut2 + ">>)");
        execute("INSERT INTO %s (x, y) VALUES (1, { { k: 1 } })");

        String ut3 = createType("CREATE TYPE %s (a int, b int)");
        String ut4 = createType("CREATE TYPE %s (j frozen<" + KEYSPACE + "." + ut3 + ">, k int)");
        createTable("CREATE TABLE %s (x int PRIMARY KEY, y list<frozen<" + KEYSPACE + "." + ut4 + ">>)");
        execute("INSERT INTO %s (x, y) VALUES (1, [ { k: 1 } ])");

        String ut5 = createType("CREATE TYPE %s (a int, b int)");
        String ut6 = createType("CREATE TYPE %s (i int, j frozen<" + KEYSPACE + "." + ut5 + ">)");
        createTable("CREATE TABLE %s (x int PRIMARY KEY, y set<frozen<" + KEYSPACE + "." + ut6 + ">>)");
        execute("INSERT INTO %s (x, y) VALUES (1, { { i: 1 } })");
    }

    @Test
    public void testFor7684() throws Throwable
    {
        String myType = createType("CREATE TYPE %s (x double)");
        createTable("CREATE TABLE %s (k int, v frozen<" + myType + ">, b boolean static, PRIMARY KEY (k, v))");

        execute("INSERT INTO %s(k, v) VALUES (?, {x:?})", 1, -104.99251);
        execute("UPDATE %s SET b = ? WHERE k = ?", true, 1);

        assertRows(execute("SELECT v.x FROM %s WHERE k = ? AND v = {x:?}", 1, -104.99251),
            row(-104.99251)
        );

        flush();

        assertRows(execute("SELECT v.x FROM %s WHERE k = ? AND v = {x:?}", 1, -104.99251),
            row(-104.99251)
        );
    }

    @Test
    public void testNonFrozenUDT() throws Throwable
    {
        // Using a UDT without frozen shouldn't work
        String myType = createType("CREATE TYPE %s (f int)");
        assertInvalid("CREATE TABLE wrong (k int PRIMARY KEY, v " + myType + ")");
    }
}
