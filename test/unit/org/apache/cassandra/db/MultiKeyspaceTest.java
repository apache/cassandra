package org.apache.cassandra.db;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;

import org.junit.Test;


public class MultiKeyspaceTest extends CQLTester
{
    @Test
    public void testSameTableNames() throws Throwable
    {
        schemaChange("CREATE KEYSPACE multikstest1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
        schemaChange("CREATE TABLE multikstest1.standard1 (a int PRIMARY KEY, b int)");

        schemaChange("CREATE KEYSPACE multikstest2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
        schemaChange("CREATE TABLE multikstest2.standard1 (a int PRIMARY KEY, b int)");

        execute("INSERT INTO multikstest1.standard1 (a, b) VALUES (0, 0)");
        execute("INSERT INTO multikstest2.standard1 (a, b) VALUES (0, 0)");

        Util.flushKeyspace("multikstest1");
        Util.flushKeyspace("multikstest2");

        assertRows(execute("SELECT * FROM multikstest1.standard1"),
                   row(0, 0));
        assertRows(execute("SELECT * FROM multikstest2.standard1"),
                   row(0, 0));
    }
}
