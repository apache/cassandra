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
package org.apache.cassandra.db;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.service.CacheService;
import static org.junit.Assert.assertEquals;

public class RowCacheCQLTest extends CQLTester
{
    @Test
    public void test7636() throws Throwable
    {
        CacheService.instance.setRowCacheCapacityInMB(1);
        createTable("CREATE TABLE %s (p1 bigint, c1 int, v int, PRIMARY KEY (p1, c1)) WITH caching = { 'keys': 'NONE', 'rows_per_partition': 'ALL' }");
        execute("INSERT INTO %s (p1, c1, v) VALUES (?, ?, ?)", 123L, 10, 12);
        assertEmpty(execute("SELECT * FROM %s WHERE p1 = ? and c1 > ?", 123L, 1000));
        UntypedResultSet res = execute("SELECT * FROM %s WHERE p1 = ? and c1 > ?", 123L, 0);
        assertEquals(1, res.size());
        assertEmpty(execute("SELECT * FROM %s WHERE p1 = ? and c1 > ?", 123L, 1000));
    }

    /**
     * Test for CASSANDRA-13482
     */
    @Test
    public void testPartialCache() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, v1 int, v2 int, primary key (pk, ck1))" +
                    "WITH CACHING = { 'keys': 'ALL', 'rows_per_partition': '1' }");
        assertEmpty(execute("select * from %s where pk = 10000"));

        execute("DELETE FROM %s WHERE pk = 1 AND ck1 = 0");
        execute("DELETE FROM %s WHERE pk = 1 AND ck1 = 1");
        execute("DELETE FROM %s WHERE pk = 1 AND ck1 = 2");
        execute("INSERT INTO %s (pk, ck1, v1, v2) VALUES (1, 1, 1, 1)");
        execute("INSERT INTO %s (pk, ck1, v1, v2) VALUES (1, 2, 2, 2)");
        execute("INSERT INTO %s (pk, ck1, v1, v2) VALUES (1, 3, 3, 3)");
        execute("DELETE FROM %s WHERE pk = 1 AND ck1 = 2");
        execute("DELETE FROM %s WHERE pk = 1 AND ck1 = 3");
        execute("INSERT INTO %s (pk, ck1, v1, v2) VALUES (1, 4, 4, 4)");
        execute("INSERT INTO %s (pk, ck1, v1, v2) VALUES (1, 5, 5, 5)");

        assertRows(execute("select * from %s where pk = 1"),
                   row(1, 1, 1, 1),
                   row(1, 4, 4, 4),
                   row(1, 5, 5, 5));
        assertRows(execute("select * from %s where pk = 1 LIMIT 1"),
                   row(1, 1, 1, 1));

        assertRows(execute("select * from %s where pk = 1 and ck1 >=2"),
                   row(1, 4, 4, 4),
                   row(1, 5, 5, 5));
        assertRows(execute("select * from %s where pk = 1 and ck1 >=2 LIMIT 1"),
                   row(1, 4, 4, 4));

        assertRows(execute("select * from %s where pk = 1 and ck1 >=2"),
                   row(1, 4, 4, 4),
                   row(1, 5, 5, 5));
        assertRows(execute("select * from %s where pk = 1 and ck1 >=2 LIMIT 1"),
                   row(1, 4, 4, 4));
    }

    @Test
    public void testPartialCacheWithStatic() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, s int static, v1 int, primary key (pk, ck1))" +
                    "WITH CACHING = { 'keys': 'ALL', 'rows_per_partition': '1' }");
        assertEmpty(execute("select * from %s where pk = 10000"));

        execute("INSERT INTO %s (pk, s) VALUES (1, 1)");
        execute("INSERT INTO %s (pk, ck1, v1) VALUES (1, 2, 2)");
        execute("INSERT INTO %s (pk, ck1, v1) VALUES (1, 3, 3)");

        execute("DELETE FROM %s WHERE pk = 2 AND ck1 = 0");
        execute("DELETE FROM %s WHERE pk = 2 AND ck1 = 1");
        execute("DELETE FROM %s WHERE pk = 3 AND ck1 = 2");
        execute("INSERT INTO %s (pk, s) VALUES (2, 2)");
        execute("INSERT INTO %s (pk, ck1, v1) VALUES (2, 1, 1)");
        execute("INSERT INTO %s (pk, ck1, v1) VALUES (2, 2, 2)");
        execute("INSERT INTO %s (pk, ck1, v1) VALUES (2, 3, 3)");

        assertRows(execute("select * from %s WHERE pk = 1"),
                   row(1, 2, 1, 2),
                   row(1, 3, 1, 3));

        assertRows(execute("select * from %s WHERE pk = 2"),
                   row(2, 1, 2, 1),
                   row(2, 2, 2, 2),
                   row(2, 3, 2, 3));
    }
}
