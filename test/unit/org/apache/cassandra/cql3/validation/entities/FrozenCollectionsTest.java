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
package org.apache.cassandra.cql3.validation.entities;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;

public class FrozenCollectionsTest extends CQLTester
{
    @BeforeClass
    public static void setUpClass()     // overrides CQLTester.setUpClass()
    {
        ServerTestUtils.daemonInitialization();
        // Selecting partitioner for a table is not exposed on CREATE TABLE.
        StorageService.instance.setPartitionerUnsafe(ByteOrderedPartitioner.instance);

        prepareServer();
    }

    @Test
    public void testPartitionKeyUsageSet() throws Throwable
    {
        testPartitionKeyUsage("set<int>",
                              set(),
                              set(1, 2, 3),
                              set(4, 5, 6),
                              set(7, 8, 9));
    }

    @Test
    public void testPartitionKeyUsageList() throws Throwable
    {
        testPartitionKeyUsage("list<int>",
                              list(),
                              list(1, 2, 3),
                              list(4, 5, 6),
                              list(7, 8, 9));
    }

    @Test
    public void testPartitionKeyUsageMap() throws Throwable
    {
        testPartitionKeyUsage("map<int, int>",
                              map(),
                              map(1, 10, 2, 20, 3, 30),
                              map(4, 40, 5, 50, 6, 60),
                              map(7, 70, 8, 80, 9, 90));
    }

    private void testPartitionKeyUsage(String type, Object v1, Object v2, Object v3, Object v4) throws Throwable
    {
        createTable("CREATE TABLE %s (k frozen<" + type + "> PRIMARY KEY, v int)");

        execute("INSERT INTO %s (k, v) VALUES (?, ?)", v1, 1);
        execute("INSERT INTO %s (k, v) VALUES (?, ?)", v2, 1);
        execute("INSERT INTO %s (k, v) VALUES (?, ?)", v3, 0);
        execute("INSERT INTO %s (k, v) VALUES (?, ?)", v4, 0);

        // overwrite with an update
        execute("UPDATE %s SET v=? WHERE k=?", 0, v1);
        execute("UPDATE %s SET v=? WHERE k=?", 0, v2);

        assertRows(execute("SELECT * FROM %s"),
                   row(v1, 0),
                   row(v2, 0),
                   row(v3, 0),
                   row(v4, 0)
        );

        assertRows(execute("SELECT k FROM %s"),
                   row(v1),
                   row(v2),
                   row(v3),
                   row(v4)
        );

        assertRows(execute("SELECT * FROM %s LIMIT 2"),
                   row(v1, 0),
                   row(v2, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE k=?", v3),
                   row(v3, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE k=?", v1),
                   row(v1, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE k IN ?", list(v3, v1)),
                   row(v1, 0),
                   row(v3, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE token(k) >= token(?)", v3),
                   row(v3, 0),
                   row(v4, 0)
        );

        assertInvalid("INSERT INTO %s (k, v) VALUES (null, 0)");

        execute("DELETE FROM %s WHERE k=?", v1);
        execute("DELETE FROM %s WHERE k=?", v3);
        assertRows(execute("SELECT * FROM %s"),
                   row(v2, 0),
                   row(v4, 0)
        );
    }

    @Test
    public void testNestedPartitionKeyUsage() throws Throwable
    {
        createTable("CREATE TABLE %s (k frozen<map<set<int>, list<int>>> PRIMARY KEY, v int)");

        execute("INSERT INTO %s (k, v) VALUES (?, ?)", map(), 1);
        execute("INSERT INTO %s (k, v) VALUES (?, ?)", map(set(), list(1, 2, 3)), 0);
        execute("INSERT INTO %s (k, v) VALUES (?, ?)", map(set(1, 2, 3), list(1, 2, 3)), 1);
        execute("INSERT INTO %s (k, v) VALUES (?, ?)", map(set(4, 5, 6), list(1, 2, 3)), 0);
        execute("INSERT INTO %s (k, v) VALUES (?, ?)", map(set(7, 8, 9), list(1, 2, 3)), 0);

        // overwrite with an update
        execute("UPDATE %s SET v=? WHERE k=?", 0, map());
        execute("UPDATE %s SET v=? WHERE k=?", 0, map(set(1, 2, 3), list(1, 2, 3)));

        assertRows(execute("SELECT * FROM %s"),
            row(map(), 0),
            row(map(set(), list(1, 2, 3)), 0),
            row(map(set(1, 2, 3), list(1, 2, 3)), 0),
            row(map(set(4, 5, 6), list(1, 2, 3)), 0),
            row(map(set(7, 8, 9), list(1, 2, 3)), 0)
        );

        assertRows(execute("SELECT k FROM %s"),
            row(map()),
            row(map(set(), list(1, 2, 3))),
            row(map(set(1, 2, 3), list(1, 2, 3))),
            row(map(set(4, 5, 6), list(1, 2, 3))),
            row(map(set(7, 8, 9), list(1, 2, 3)))
        );

        assertRows(execute("SELECT * FROM %s LIMIT 3"),
            row(map(), 0),
            row(map(set(), list(1, 2, 3)), 0),
            row(map(set(1, 2, 3), list(1, 2, 3)), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE k=?", map(set(4, 5, 6), list(1, 2, 3))),
            row(map(set(4, 5, 6), list(1, 2, 3)), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE k=?", map()),
                row(map(), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE k=?", map(set(), list(1, 2, 3))),
                row(map(set(), list(1, 2, 3)), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE k IN ?", list(map(set(4, 5, 6), list(1, 2, 3)), map(), map(set(), list(1, 2, 3)))),
                   row(map(), 0),
                   row(map(set(), list(1, 2, 3)), 0),
                   row(map(set(4, 5, 6), list(1, 2, 3)), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE token(k) >= token(?)", map(set(4, 5, 6), list(1, 2, 3))),
            row(map(set(4, 5, 6), list(1, 2, 3)), 0),
            row(map(set(7, 8, 9), list(1, 2, 3)), 0)
        );

        execute("DELETE FROM %s WHERE k=?", map());
        execute("DELETE FROM %s WHERE k=?", map(set(), list(1, 2, 3)));
        execute("DELETE FROM %s WHERE k=?", map(set(4, 5, 6), list(1, 2, 3)));
        assertRows(execute("SELECT * FROM %s"),
            row(map(set(1, 2, 3), list(1, 2, 3)), 0),
            row(map(set(7, 8, 9), list(1, 2, 3)), 0)
        );

    }

    @Test
    public void testClusteringKeyUsageSet() throws Throwable
    {
        testClusteringKeyUsage("set<int>",
                               set(),
                               set(1, 2, 3),
                               set(4, 5, 6),
                               set(7, 8, 9));
    }

    @Test
    public void testClusteringKeyUsageList() throws Throwable
    {
        testClusteringKeyUsage("list<int>",
                               list(),
                               list(1, 2, 3),
                               list(4, 5, 6),
                               list(7, 8, 9));
    }

    @Test
    public void testClusteringKeyUsageMap() throws Throwable
    {
        testClusteringKeyUsage("map<int, int>",
                               map(),
                               map(1, 10, 2, 20, 3, 30),
                               map(4, 40, 5, 50, 6, 60),
                               map(7, 70, 8, 80, 9, 90));
    }

    private void testClusteringKeyUsage(String type, Object v1, Object v2, Object v3, Object v4) throws Throwable
    {
        createTable(String.format("CREATE TABLE %%s (a int, b frozen<%s>, c int, PRIMARY KEY (a, b))", type));

        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, v1, 1);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, v2, 1);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, v3, 0);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, v4, 0);

        // overwrite with an update
        execute("UPDATE %s SET c=? WHERE a=? AND b=?", 0, 0, v1);
        execute("UPDATE %s SET c=? WHERE a=? AND b=?", 0, 0, v2);

        assertRows(execute("SELECT * FROM %s"),
                   row(0, v1, 0),
                   row(0, v2, 0),
                   row(0, v3, 0),
                   row(0, v4, 0)
        );

        assertRows(execute("SELECT b FROM %s"),
                   row(v1),
                   row(v2),
                   row(v3),
                   row(v4)
        );

        assertRows(execute("SELECT * FROM %s LIMIT 2"),
                   row(0, v1, 0),
                   row(0, v2, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b=?", 0, v3),
                   row(0, v3, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b=?", 0, v1),
                   row(0, v1, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b IN ?", 0, list(v3, v1)),
                   row(0, v1, 0),
                   row(0, v3, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b > ?", 0, v3),
                   row(0, v4, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b >= ?", 0, v3),
                   row(0, v3, 0),
                   row(0, v4, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b < ?", 0, v3),
                   row(0, v1, 0),
                   row(0, v2, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b <= ?", 0, v3),
                   row(0, v1, 0),
                   row(0, v2, 0),
                   row(0, v3, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b > ? AND b <= ?", 0, v2, v3),
                   row(0, v3, 0)
        );

        execute("DELETE FROM %s WHERE a=? AND b=?", 0, v1);
        execute("DELETE FROM %s WHERE a=? AND b=?", 0, v3);
        assertRows(execute("SELECT * FROM %s"),
                   row(0, v2, 0),
                   row(0, v4, 0)
        );
    }

    @Test
    public void testClusteringKeyUsageWithReverseOrderSet() throws Throwable
    {
        testClusteringKeyUsageWithReverseOrder("set<int>",
                                               set(),
                                               set(1, 2, 3),
                                               set(4, 5, 6),
                                               set(7, 8, 9));
    }

    @Test
    public void testClusteringKeyUsageWithReverseOrderList() throws Throwable
    {
        testClusteringKeyUsageWithReverseOrder("list<int>",
                                               list(),
                                               list(1, 2, 3),
                                               list(4, 5, 6),
                                               list(7, 8, 9));
    }

    @Test
    public void testClusteringKeyUsageWithReverseOrderMap() throws Throwable
    {
        testClusteringKeyUsageWithReverseOrder("map<int, int>",
                                               map(),
                                               map(1, 10, 2, 20, 3, 30),
                                               map(4, 40, 5, 50, 6, 60),
                                               map(7, 70, 8, 80, 9, 90));
    }

    private void testClusteringKeyUsageWithReverseOrder(String type, Object v1, Object v2, Object v3, Object v4) throws Throwable
    {
        createTable(String.format("CREATE TABLE %%s (a int, b frozen<%s>, c int, PRIMARY KEY (a, b)) " +
                                  "WITH CLUSTERING ORDER BY (b DESC)", type));

        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, v1, 1);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, v2, 1);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, v3, 0);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, v4, 0);

        // overwrite with an update
        execute("UPDATE %s SET c=? WHERE a=? AND b=?", 0, 0, v1);
        execute("UPDATE %s SET c=? WHERE a=? AND b=?", 0, 0, v2);

        assertRows(execute("SELECT * FROM %s"),
                   row(0, v4, 0),
                   row(0, v3, 0),
                   row(0, v2, 0),
                   row(0, v1, 0)
        );

        assertRows(execute("SELECT b FROM %s"),
                   row(v4),
                   row(v3),
                   row(v2),
                   row(v1)
        );

        assertRows(execute("SELECT * FROM %s LIMIT 2"),
                   row(0, v4, 0),
                   row(0, v3, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b=?", 0, v3),
                   row(0, v3, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b=?", 0, v1),
                   row(0, v1, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b IN ?", 0, list(v3, v1)),
                   row(0, v3, 0),
                   row(0, v1, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b > ?", 0, v3),
                   row(0, v4, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b >= ?", 0, v3),
                   row(0, v4, 0),
                   row(0, v3, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b < ?", 0, v3),
                   row(0, v2, 0),
                   row(0, v1, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b <= ?", 0, v3),
                   row(0, v3, 0),
                   row(0, v2, 0),
                   row(0, v1, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b > ? AND b <= ?", 0, v2, v3),
                   row(0, v3, 0)
        );

        execute("DELETE FROM %s WHERE a=? AND b=?", 0, v1);
        execute("DELETE FROM %s WHERE a=? AND b=?", 0, v3);
        assertRows(execute("SELECT * FROM %s"),
                   row(0, v4, 0),
                   row(0, v2, 0)
        );
    }

    @Test
    public void testNestedClusteringKeyUsage() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b frozen<map<set<int>, list<int>>>, c frozen<set<int>>, d int, PRIMARY KEY (a, b, c))");

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, map(), set(), 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, map(set(), list(1, 2, 3)), set(), 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3), 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, map(set(7, 8, 9), list(1, 2, 3)), set(1, 2, 3), 0);

        assertRows(execute("SELECT * FROM %s"),
                   row(0, map(), set(), 0),
                   row(0, map(set(), list(1, 2, 3)), set(), 0),
                   row(0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(7, 8, 9), list(1, 2, 3)), set(1, 2, 3), 0)
        );

        assertRows(execute("SELECT b FROM %s"),
                   row(map()),
                   row(map(set(), list(1, 2, 3))),
                   row(map(set(1, 2, 3), list(1, 2, 3))),
                   row(map(set(4, 5, 6), list(1, 2, 3))),
                   row(map(set(7, 8, 9), list(1, 2, 3)))
        );

        assertRows(execute("SELECT c FROM %s"),
                   row(set()),
                   row(set()),
                   row(set(1, 2, 3)),
                   row(set(1, 2, 3)),
                   row(set(1, 2, 3))
        );

        assertRows(execute("SELECT * FROM %s LIMIT 3"),
                   row(0, map(), set(), 0),
                   row(0, map(set(), list(1, 2, 3)), set(), 0),
                   row(0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=0 ORDER BY b DESC LIMIT 4"),
                   row(0, map(set(7, 8, 9), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(), list(1, 2, 3)), set(), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b=?", 0, map()),
                   row(0, map(), set(), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b=?", 0, map(set(), list(1, 2, 3))),
                   row(0, map(set(), list(1, 2, 3)), set(), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b=?", 0, map(set(1, 2, 3), list(1, 2, 3))),
                   row(0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b=? AND c=?", 0, map(set(), list(1, 2, 3)), set()),
                   row(0, map(set(), list(1, 2, 3)), set(), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND (b, c) IN ?", 0, list(tuple(map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3)),
                                                                                 tuple(map(), set()))),
                   row(0, map(), set(), 0),
                   row(0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b > ?", 0, map(set(4, 5, 6), list(1, 2, 3))),
                   row(0, map(set(7, 8, 9), list(1, 2, 3)), set(1, 2, 3), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b >= ?", 0, map(set(4, 5, 6), list(1, 2, 3))),
                   row(0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(7, 8, 9), list(1, 2, 3)), set(1, 2, 3), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b < ?", 0, map(set(4, 5, 6), list(1, 2, 3))),
                   row(0, map(), set(), 0),
                   row(0, map(set(), list(1, 2, 3)), set(), 0),
                   row(0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b <= ?", 0, map(set(4, 5, 6), list(1, 2, 3))),
                   row(0, map(), set(), 0),
                   row(0, map(set(), list(1, 2, 3)), set(), 0),
                   row(0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b > ? AND b <= ?", 0, map(set(1, 2, 3), list(1, 2, 3)), map(set(4, 5, 6), list(1, 2, 3))),
                   row(0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3), 0)
        );

        execute("DELETE FROM %s WHERE a=? AND b=? AND c=?", 0, map(), set());
        assertEmpty(execute("SELECT * FROM %s WHERE a=? AND b=? AND c=?", 0, map(), set()));

        execute("DELETE FROM %s WHERE a=? AND b=? AND c=?", 0, map(set(), list(1, 2, 3)), set());
        assertEmpty(execute("SELECT * FROM %s WHERE a=? AND b=? AND c=?", 0, map(set(), list(1, 2, 3)), set()));

        execute("DELETE FROM %s WHERE a=? AND b=? AND c=?", 0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3));
        assertEmpty(execute("SELECT * FROM %s WHERE a=? AND b=? AND c=?", 0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3)));

        assertRows(execute("SELECT * FROM %s"),
                   row(0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(7, 8, 9), list(1, 2, 3)), set(1, 2, 3), 0)
        );
    }

    @Test
    public void testNestedClusteringKeyUsageWithReverseOrder() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b frozen<map<set<int>, list<int>>>, c frozen<set<int>>, d int, " +
                    "PRIMARY KEY (a, b, c)) WITH CLUSTERING ORDER BY (b DESC)");

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, map(), set(), 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, map(set(), list(1, 2, 3)), set(), 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3), 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, map(set(7, 8, 9), list(1, 2, 3)), set(1, 2, 3), 0);

        assertRows(execute("SELECT * FROM %s"),
                   row(0, map(set(7, 8, 9), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(), list(1, 2, 3)), set(), 0),
                   row(0, map(), set(), 0)
        );

        assertRows(execute("SELECT b FROM %s"),
                   row(map(set(7, 8, 9), list(1, 2, 3))),
                   row(map(set(4, 5, 6), list(1, 2, 3))),
                   row(map(set(1, 2, 3), list(1, 2, 3))),
                   row(map(set(), list(1, 2, 3))),
                   row(map())
        );

        assertRows(execute("SELECT c FROM %s"),
                   row(set(1, 2, 3)),
                   row(set(1, 2, 3)),
                   row(set(1, 2, 3)),
                   row(set()),
                   row(set())
        );

        assertRows(execute("SELECT * FROM %s LIMIT 3"),
                   row(0, map(set(7, 8, 9), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=0 ORDER BY b DESC LIMIT 4"),
                   row(0, map(set(7, 8, 9), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(), list(1, 2, 3)), set(), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b=?", 0, map()),
                   row(0, map(), set(), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b=?", 0, map(set(), list(1, 2, 3))),
                   row(0, map(set(), list(1, 2, 3)), set(), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b=?", 0, map(set(1, 2, 3), list(1, 2, 3))),
                   row(0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b=? AND c=?", 0, map(set(), list(1, 2, 3)), set()),
                   row(0, map(set(), list(1, 2, 3)), set(), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND (b, c) IN ?", 0, list(tuple(map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3)),
                                                                                 tuple(map(), set()))),
                   row(0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(), set(), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b > ?", 0, map(set(4, 5, 6), list(1, 2, 3))),
                   row(0, map(set(7, 8, 9), list(1, 2, 3)), set(1, 2, 3), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b >= ?", 0, map(set(4, 5, 6), list(1, 2, 3))),
                   row(0, map(set(7, 8, 9), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b < ?", 0, map(set(4, 5, 6), list(1, 2, 3))),
                   row(0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(), list(1, 2, 3)), set(), 0),
                   row(0, map(), set(), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b <= ?", 0, map(set(4, 5, 6), list(1, 2, 3))),
                   row(0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(), list(1, 2, 3)), set(), 0),
                   row(0, map(), set(), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b > ? AND b <= ?", 0, map(set(1, 2, 3), list(1, 2, 3)), map(set(4, 5, 6), list(1, 2, 3))),
                   row(0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3), 0)
        );

        execute("DELETE FROM %s WHERE a=? AND b=? AND c=?", 0, map(), set());
        assertEmpty(execute("SELECT * FROM %s WHERE a=? AND b=? AND c=?", 0, map(), set()));

        execute("DELETE FROM %s WHERE a=? AND b=? AND c=?", 0, map(set(), list(1, 2, 3)), set());
        assertEmpty(execute("SELECT * FROM %s WHERE a=? AND b=? AND c=?", 0, map(set(), list(1, 2, 3)), set()));

        execute("DELETE FROM %s WHERE a=? AND b=? AND c=?", 0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3));
        assertEmpty(execute("SELECT * FROM %s WHERE a=? AND b=? AND c=?", 0, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3)));

        assertRows(execute("SELECT * FROM %s"),
                   row(0, map(set(7, 8, 9), list(1, 2, 3)), set(1, 2, 3), 0),
                   row(0, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3), 0)
        );
    }

    @Test
    public void testNormalColumnUsage() throws Throwable
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b frozen<map<set<int>, list<int>>>, c frozen<set<int>>)");

        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, map(), set());
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, map(set(), list(99999, 999999, 99999)), set());
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 2, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3));
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 3, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3));
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 4, map(set(7, 8, 9), list(1, 2, 3)), set(1, 2, 3));

        // overwrite with update
        execute("UPDATE %s SET b=? WHERE a=?", map(set(), list(1, 2, 3)), 1);

        assertRows(execute("SELECT * FROM %s"),
                   row(0, map(), set()),
                   row(1, map(set(), list(1, 2, 3)), set()),
                   row(2, map(set(1, 2, 3), list(1, 2, 3)), set(1, 2, 3)),
                   row(3, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3)),
                   row(4, map(set(7, 8, 9), list(1, 2, 3)), set(1, 2, 3))
        );

        assertRows(execute("SELECT b FROM %s"),
                   row(map()),
                   row(map(set(), list(1, 2, 3))),
                   row(map(set(1, 2, 3), list(1, 2, 3))),
                   row(map(set(4, 5, 6), list(1, 2, 3))),
                   row(map(set(7, 8, 9), list(1, 2, 3)))
        );

        assertRows(execute("SELECT c FROM %s"),
                   row(set()),
                   row(set()),
                   row(set(1, 2, 3)),
                   row(set(1, 2, 3)),
                   row(set(1, 2, 3))
        );

        assertRows(execute("SELECT * FROM %s WHERE a=?", 3),
                   row(3, map(set(4, 5, 6), list(1, 2, 3)), set(1, 2, 3))
        );

        execute("UPDATE %s SET b=? WHERE a=?", null, 1);
        assertRows(execute("SELECT * FROM %s WHERE a=?", 1),
                   row(1, null, set())
        );

        execute("UPDATE %s SET b=? WHERE a=?", map(), 1);
        assertRows(execute("SELECT * FROM %s WHERE a=?", 1),
                   row(1, map(), set())
        );

        execute("UPDATE %s SET c=? WHERE a=?", null, 2);
        assertRows(execute("SELECT * FROM %s WHERE a=?", 2),
                   row(2, map(set(1, 2, 3), list(1, 2, 3)), null)
        );

        execute("UPDATE %s SET c=? WHERE a=?", set(), 2);
        assertRows(execute("SELECT * FROM %s WHERE a=?", 2),
                   row(2, map(set(1, 2, 3), list(1, 2, 3)), set())
        );

        execute("DELETE b FROM %s WHERE a=?", 3);
        assertRows(execute("SELECT * FROM %s WHERE a=?", 3),
                   row(3, null, set(1, 2, 3))
        );

        execute("DELETE c FROM %s WHERE a=?", 4);
        assertRows(execute("SELECT * FROM %s WHERE a=?", 4),
                   row(4, map(set(7, 8, 9), list(1, 2, 3)), null)
        );
    }

    @Test
    public void testStaticColumnUsage() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c frozen<map<set<int>, list<int>>> static, d int, PRIMARY KEY (a, b))");
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, map(), 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, map(), 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, map(set(), list(1, 2, 3)), 0);
        execute("INSERT INTO %s (a, b, d) VALUES (?, ?, ?)", 1, 1, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, 0, map(set(1, 2, 3), list(1, 2, 3)), 0);

        assertRows(execute("SELECT * FROM %s"),
            row(0, 0, map(), 0),
            row(0, 1, map(), 0),
            row(1, 0, map(set(), list(1, 2, 3)), 0),
            row(1, 1, map(set(), list(1, 2, 3)), 0),
            row(2, 0, map(set(1, 2, 3), list(1, 2, 3)), 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b=?", 0, 1),
            row(0, 1, map(), 0)
        );

        execute("DELETE c FROM %s WHERE a=?", 0);
        assertRows(execute("SELECT * FROM %s"),
                row(0, 0, null, 0),
                row(0, 1, null, 0),
                row(1, 0, map(set(), list(1, 2, 3)), 0),
                row(1, 1, map(set(), list(1, 2, 3)), 0),
                row(2, 0, map(set(1, 2, 3), list(1, 2, 3)), 0)
        );

        execute("DELETE FROM %s WHERE a=?", 0);
        assertRows(execute("SELECT * FROM %s"),
                row(1, 0, map(set(), list(1, 2, 3)), 0),
                row(1, 1, map(set(), list(1, 2, 3)), 0),
                row(2, 0, map(set(1, 2, 3), list(1, 2, 3)), 0)
        );

        execute("UPDATE %s SET c=? WHERE a=?", map(set(1, 2, 3), list(1, 2, 3)), 1);
        assertRows(execute("SELECT * FROM %s"),
                row(1, 0, map(set(1, 2, 3), list(1, 2, 3)), 0),
                row(1, 1, map(set(1, 2, 3), list(1, 2, 3)), 0),
                row(2, 0, map(set(1, 2, 3), list(1, 2, 3)), 0)
        );
    }

    private void assertInvalidCreateWithMessage(String createTableStatement, String errorMessage) throws Throwable
    {
         try
        {
            createTableMayThrow(createTableStatement);
            Assert.fail("Expected CREATE TABLE statement to error: " + createTableStatement);
        }
        catch (InvalidRequestException | ConfigurationException | SyntaxException ex)
        {
            Assert.assertTrue("Expected error message to contain '" + errorMessage + "', but got '" + ex.getMessage() + "'",
                    ex.getMessage().contains(errorMessage));
        }
    }

    @Test
    public void testInvalidOperations() throws Throwable
    {
        // lists
        createTable("CREATE TABLE %s (k int PRIMARY KEY, l frozen<list<int>>)");
        assertInvalid("UPDATE %s SET l[?]=? WHERE k=?", 0, 0, 0);
        assertInvalid("UPDATE %s SET l = ? + l WHERE k=?", list(0), 0);
        assertInvalid("UPDATE %s SET l = l + ? WHERE k=?", list(4), 0);
        assertInvalid("UPDATE %s SET l = l - ? WHERE k=?", list(3), 0);
        assertInvalid("DELETE l[?] FROM %s WHERE k=?", 0, 0);

        // sets
        createTable("CREATE TABLE %s (k int PRIMARY KEY, s frozen<set<int>>)");
        assertInvalid("UPDATE %s SET s = s + ? WHERE k=?", set(0), 0);
        assertInvalid("UPDATE %s SET s = s - ? WHERE k=?", set(3), 0);

        // maps
        createTable("CREATE TABLE %s (k int PRIMARY KEY, m frozen<map<int, int>>)");
        assertInvalid("UPDATE %s SET m[?]=? WHERE k=?", 0, 0, 0);
        assertInvalid("UPDATE %s SET m = m + ? WHERE k=?", map(4, 4), 0);
        assertInvalid("DELETE m[?] FROM %s WHERE k=?", 0, 0);

        assertInvalidCreateWithMessage("CREATE TABLE %s (k int PRIMARY KEY, t set<set<int>>)",
                "Non-frozen collections are not allowed inside collections");

        assertInvalidCreateWithMessage("CREATE TABLE %s (k int PRIMARY KEY, t frozen<set<counter>>)",
                                       "Counters are not allowed inside collections");

        assertInvalidCreateWithMessage("CREATE TABLE %s (k int PRIMARY KEY, t frozen<text>)",
                "frozen<> is only allowed on collections, tuples, and user-defined types");
    }

    private void assertInvalidIndexCreationWithMessage(String statement, String errorMessage) throws Throwable
    {
        try
        {
            createIndexMayThrow(statement);
            Assert.fail("Expected index creation to fail: " + statement);
        }
        catch (InvalidRequestException ex)
        {
            Assert.assertTrue("Expected error message to contain '" + errorMessage + "', but got '" + ex.getMessage() + "'",
                              ex.getMessage().contains(errorMessage));
        }
    }

    @Test
    public void testSecondaryIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (a frozen<map<int, text>> PRIMARY KEY, b frozen<map<int, text>>)");

        // for now, we don't support indexing values or keys of collections in the primary key
        assertInvalidIndexCreationWithMessage("CREATE INDEX ON %s (full(a))", "Cannot create secondary index on the only partition key column");
        assertInvalidIndexCreationWithMessage("CREATE INDEX ON %s (keys(a))", "Cannot create secondary index on the only partition key column");
        assertInvalidIndexCreationWithMessage("CREATE INDEX ON %s (keys(b))", "Cannot create keys() index on frozen column b. " +
                                                                              "Frozen collections are immutable and must be fully indexed");

        createTable("CREATE TABLE %s (a int, b frozen<list<int>>, c frozen<set<int>>, d frozen<map<int, text>>, PRIMARY KEY (a, b))");

        createIndex("CREATE INDEX ON %s (full(b))");
        createIndex("CREATE INDEX ON %s (full(c))");
        createIndex("CREATE INDEX ON %s (full(d))");

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, list(1, 2, 3), set(1, 2, 3), map(1, "a"));
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, list(4, 5, 6), set(1, 2, 3), map(1, "a"));
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, list(1, 2, 3), set(4, 5, 6), map(2, "b"));
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, list(4, 5, 6), set(4, 5, 6), map(2, "b"));

        // CONTAINS KEY doesn't work on non-maps
        assertInvalidMessage("Cannot use CONTAINS KEY on non-map column",
                             "SELECT * FROM %s WHERE b CONTAINS KEY ?", 1);

        assertInvalidMessage("Cannot use CONTAINS KEY on non-map column",
                             "SELECT * FROM %s WHERE b CONTAINS KEY ? ALLOW FILTERING", 1);

        assertInvalidMessage("Cannot use CONTAINS KEY on non-map column",
                             "SELECT * FROM %s WHERE c CONTAINS KEY ?", 1);

        // normal indexes on frozen collections don't support CONTAINS or CONTAINS KEY
        assertInvalidMessage("Clustering columns can only be restricted with CONTAINS with a secondary index or filtering",
                             "SELECT * FROM %s WHERE b CONTAINS ?", 1);

        assertRows(execute("SELECT * FROM %s WHERE b CONTAINS ? ALLOW FILTERING", 1),
                   row(0, list(1, 2, 3), set(1, 2, 3), map(1, "a")),
                   row(1, list(1, 2, 3), set(4, 5, 6), map(2, "b")));

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE d CONTAINS KEY ?", 1);

        assertRows(execute("SELECT * FROM %s WHERE b CONTAINS ? AND d CONTAINS KEY ? ALLOW FILTERING", 1, 1),
                   row(0, list(1, 2, 3), set(1, 2, 3), map(1, "a")));

        // index lookup on b
        assertRows(execute("SELECT * FROM %s WHERE b=?", list(1, 2, 3)),
            row(0, list(1, 2, 3), set(1, 2, 3), map(1, "a")),
            row(1, list(1, 2, 3), set(4, 5, 6), map(2, "b"))
        );

        assertEmpty(execute("SELECT * FROM %s WHERE b=?", list(-1)));

        assertInvalidMessage("ALLOW FILTERING", "SELECT * FROM %s WHERE b=? AND c=?", list(1, 2, 3), set(4, 5, 6));
        assertRows(execute("SELECT * FROM %s WHERE b=? AND c=? ALLOW FILTERING", list(1, 2, 3), set(4, 5, 6)),
            row(1, list(1, 2, 3), set(4, 5, 6), map(2, "b"))
        );

        assertInvalidMessage("ALLOW FILTERING", "SELECT * FROM %s WHERE b=? AND c CONTAINS ?", list(1, 2, 3), 5);
        assertRows(execute("SELECT * FROM %s WHERE b=? AND c CONTAINS ? ALLOW FILTERING", list(1, 2, 3), 5),
            row(1, list(1, 2, 3), set(4, 5, 6), map(2, "b"))
        );

        assertInvalidMessage("ALLOW FILTERING", "SELECT * FROM %s WHERE b=? AND d=?", list(1, 2, 3), map(1, "a"));
        assertRows(execute("SELECT * FROM %s WHERE b=? AND d=? ALLOW FILTERING", list(1, 2, 3), map(1, "a")),
            row(0, list(1, 2, 3), set(1, 2, 3), map(1, "a"))
        );

        assertInvalidMessage("ALLOW FILTERING", "SELECT * FROM %s WHERE b=? AND d CONTAINS ?", list(1, 2, 3), "a");
        assertRows(execute("SELECT * FROM %s WHERE b=? AND d CONTAINS ? ALLOW FILTERING", list(1, 2, 3), "a"),
            row(0, list(1, 2, 3), set(1, 2, 3), map(1, "a"))
        );

        assertInvalidMessage("ALLOW FILTERING", "SELECT * FROM %s WHERE b=? AND d CONTAINS KEY ?", list(1, 2, 3), 1);
        assertRows(execute("SELECT * FROM %s WHERE b=? AND d CONTAINS KEY ? ALLOW FILTERING", list(1, 2, 3), 1),
            row(0, list(1, 2, 3), set(1, 2, 3), map(1, "a"))
        );

        // index lookup on c
        assertRows(execute("SELECT * FROM %s WHERE c=?", set(1, 2, 3)),
            row(0, list(1, 2, 3), set(1, 2, 3), map(1, "a")),
            row(0, list(4, 5, 6), set(1, 2, 3), map(1, "a"))
        );

        // ordering of c should not matter
        assertRows(execute("SELECT * FROM %s WHERE c=?", set(2, 1, 3)),
                row(0, list(1, 2, 3), set(1, 2, 3), map(1, "a")),
                row(0, list(4, 5, 6), set(1, 2, 3), map(1, "a"))
        );

        assertEmpty(execute("SELECT * FROM %s WHERE c=?", set(-1)));

        assertInvalidMessage("ALLOW FILTERING", "SELECT * FROM %s WHERE c=? AND b=?", set(1, 2, 3), list(1, 2, 3));
        assertRows(execute("SELECT * FROM %s WHERE c=? AND b=? ALLOW FILTERING", set(1, 2, 3), list(1, 2, 3)),
            row(0, list(1, 2, 3), set(1, 2, 3), map(1, "a"))
        );

        assertInvalidMessage("ALLOW FILTERING", "SELECT * FROM %s WHERE c=? AND b CONTAINS ?", set(1, 2, 3), 1);
        assertRows(execute("SELECT * FROM %s WHERE c=? AND b CONTAINS ? ALLOW FILTERING", set(1, 2, 3), 1),
            row(0, list(1, 2, 3), set(1, 2, 3), map(1, "a"))
        );

        assertInvalidMessage("ALLOW FILTERING", "SELECT * FROM %s WHERE c=? AND d = ?", set(1, 2, 3), map(1, "a"));
        assertRows(execute("SELECT * FROM %s WHERE c=? AND d = ? ALLOW FILTERING", set(1, 2, 3), map(1, "a")),
            row(0, list(1, 2, 3), set(1, 2, 3), map(1, "a")),
            row(0, list(4, 5, 6), set(1, 2, 3), map(1, "a"))
        );

        assertInvalidMessage("ALLOW FILTERING", "SELECT * FROM %s WHERE c=? AND d CONTAINS ?", set(1, 2, 3), "a");
        assertRows(execute("SELECT * FROM %s WHERE c=? AND d CONTAINS ? ALLOW FILTERING", set(1, 2, 3), "a"),
            row(0, list(1, 2, 3), set(1, 2, 3), map(1, "a")),
            row(0, list(4, 5, 6), set(1, 2, 3), map(1, "a"))
        );

        assertInvalidMessage("ALLOW FILTERING", "SELECT * FROM %s WHERE c=? AND d CONTAINS KEY ?", set(1, 2, 3), 1);
        assertRows(execute("SELECT * FROM %s WHERE c=? AND d CONTAINS KEY ? ALLOW FILTERING", set(1, 2, 3), 1),
            row(0, list(1, 2, 3), set(1, 2, 3), map(1, "a")),
            row(0, list(4, 5, 6), set(1, 2, 3), map(1, "a"))
        );

        // index lookup on d
        assertRows(execute("SELECT * FROM %s WHERE d=?", map(1, "a")),
            row(0, list(1, 2, 3), set(1, 2, 3), map(1, "a")),
            row(0, list(4, 5, 6), set(1, 2, 3), map(1, "a"))
        );

        assertRows(execute("SELECT * FROM %s WHERE d=?", map(2, "b")),
            row(1, list(1, 2, 3), set(4, 5, 6), map(2, "b")),
            row(1, list(4, 5, 6), set(4, 5, 6), map(2, "b"))
        );

        assertEmpty(execute("SELECT * FROM %s WHERE d=?", map(3, "c")));

        assertInvalidMessage("ALLOW FILTERING", "SELECT * FROM %s WHERE d=? AND c=?", map(1, "a"), set(1, 2, 3));
        assertRows(execute("SELECT * FROM %s WHERE d=? AND b=? ALLOW FILTERING", map(1, "a"), list(1, 2, 3)),
            row(0, list(1, 2, 3), set(1, 2, 3), map(1, "a"))
        );

        assertInvalidMessage("ALLOW FILTERING", "SELECT * FROM %s WHERE d=? AND b CONTAINS ?", map(1, "a"), 3);
        assertRows(execute("SELECT * FROM %s WHERE d=? AND b CONTAINS ? ALLOW FILTERING", map(1, "a"), 3),
            row(0, list(1, 2, 3), set(1, 2, 3), map(1, "a"))
        );

        assertInvalidMessage("ALLOW FILTERING", "SELECT * FROM %s WHERE d=? AND b=? AND c=?", map(1, "a"), list(1, 2, 3), set(1, 2, 3));
        assertRows(execute("SELECT * FROM %s WHERE d=? AND b=? AND c=? ALLOW FILTERING", map(1, "a"), list(1, 2, 3), set(1, 2, 3)),
            row(0, list(1, 2, 3), set(1, 2, 3), map(1, "a"))
        );

        assertRows(execute("SELECT * FROM %s WHERE d=? AND b CONTAINS ? AND c CONTAINS ? ALLOW FILTERING", map(1, "a"), 2, 2),
            row(0, list(1, 2, 3), set(1, 2, 3), map(1, "a"))
        );

        assertRows(execute("SELECT * FROM %s WHERE d CONTAINS KEY ? ALLOW FILTERING", 1),
            row(0, list(1, 2, 3), set(1, 2, 3), map(1, "a")),
            row(0, list(4, 5, 6), set(1, 2, 3), map(1, "a"))
        );

        execute("DELETE d FROM %s WHERE a=? AND b=?", 0, list(1, 2, 3));
        assertRows(execute("SELECT * FROM %s WHERE d=?", map(1, "a")),
            row(0, list(4, 5, 6), set(1, 2, 3), map(1, "a"))
        );
    }

    /** Test for CASSANDRA-8302 */
    @Test
    public void testClusteringColumnFiltering() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b frozen<map<int, int>>, c int, d int, PRIMARY KEY (a, b, c))");
        createIndex("CREATE INDEX c_index ON %s (c)");
        createIndex("CREATE INDEX d_index ON %s (d)");

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, map(0, 0, 1, 1), 0, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, map(1, 1, 2, 2), 0, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, map(0, 0, 1, 1), 0, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, map(1, 1, 2, 2), 0, 0);

        assertRows(execute("SELECT * FROM %s WHERE d=? AND b CONTAINS ? ALLOW FILTERING", 0, 0),
                row(0, map(0, 0, 1, 1), 0, 0),
                row(1, map(0, 0, 1, 1), 0, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE d=? AND b CONTAINS KEY ? ALLOW FILTERING", 0, 0),
                row(0, map(0, 0, 1, 1), 0, 0),
                row(1, map(0, 0, 1, 1), 0, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND d=? AND b CONTAINS ? ALLOW FILTERING", 0, 0, 0),
                row(0, map(0, 0, 1, 1), 0, 0)
        );
        assertRows(execute("SELECT * FROM %s WHERE a=? AND d=? AND b CONTAINS KEY ? ALLOW FILTERING", 0, 0, 0),
                row(0, map(0, 0, 1, 1), 0, 0)
        );

        dropIndex("DROP INDEX %s.d_index");

        assertRows(execute("SELECT * FROM %s WHERE c=? AND b CONTAINS ? ALLOW FILTERING", 0, 0),
                row(0, map(0, 0, 1, 1), 0, 0),
                row(1, map(0, 0, 1, 1), 0, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE c=? AND b CONTAINS KEY ? ALLOW FILTERING", 0, 0),
                row(0, map(0, 0, 1, 1), 0, 0),
                row(1, map(0, 0, 1, 1), 0, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND c=? AND b CONTAINS ? ALLOW FILTERING", 0, 0, 0),
                row(0, map(0, 0, 1, 1), 0, 0)
        );
        assertRows(execute("SELECT * FROM %s WHERE a=? AND c=? AND b CONTAINS KEY ? ALLOW FILTERING", 0, 0, 0),
                row(0, map(0, 0, 1, 1), 0, 0)
        );
    }

    @Test
    public void testFrozenListInMap() throws Throwable
    {
        createTable("CREATE TABLE %s (k int primary key, m map<frozen<list<int>>, int>)");

        execute("INSERT INTO %s (k, m) VALUES (1, {[1, 2, 3] : 1})");
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, map(list(1, 2, 3), 1)));

        execute("UPDATE %s SET m[[1, 2, 3]]=2 WHERE k=1");
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, map(list(1, 2, 3), 2)));

        execute("UPDATE %s SET m = m + ? WHERE k=1", map(list(4, 5, 6), 3));
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1,
                    map(list(1, 2, 3), 2,
                        list(4, 5, 6), 3)));

        execute("DELETE m[[1, 2, 3]] FROM %s WHERE k = 1");
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, map(list(4, 5, 6), 3)));
    }

    @Test
    public void testFrozenListInSet() throws Throwable
    {
        createTable("CREATE TABLE %s (k int primary key, s set<frozen<list<int>>>)");

        execute("INSERT INTO %s (k, s) VALUES (1, {[1, 2, 3]})");
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, set(list(1, 2, 3)))
        );

        execute("UPDATE %s SET s = s + ? WHERE k=1", set(list(4, 5, 6)));
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, set(list(1, 2, 3), list(4, 5, 6)))
        );

        execute("UPDATE %s SET s = s - ? WHERE k=1", set(list(4, 5, 6)));
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, set(list(1, 2, 3)))
        );

        execute("DELETE s[[1, 2, 3]] FROM %s WHERE k = 1");
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, null)
        );
    }

    @Test
    public void testFrozenListInList() throws Throwable
    {
        createTable("CREATE TABLE %s (k int primary key, l list<frozen<list<int>>>)");

        execute("INSERT INTO %s (k, l) VALUES (1, [[1, 2, 3]])");
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, list(list(1, 2, 3)))
        );

        execute("UPDATE %s SET l[?]=? WHERE k=1", 0, list(4, 5, 6));
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, list(list(4, 5, 6)))
        );

        execute("UPDATE %s SET l = ? + l WHERE k=1", list(list(1, 2, 3)));
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, list(list(1, 2, 3), list(4, 5, 6)))
        );

        execute("UPDATE %s SET l = l + ? WHERE k=1", list(list(7, 8, 9)));
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, list(list(1, 2, 3), list(4, 5, 6), list(7, 8, 9)))
        );

        execute("UPDATE %s SET l = l - ? WHERE k=1", list(list(4, 5, 6)));
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, list(list(1, 2, 3), list(7, 8, 9)))
        );

        execute("DELETE l[0] FROM %s WHERE k = 1");
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, list(list(7, 8, 9)))
        );
    }

    @Test
    public void testFrozenMapInMap() throws Throwable
    {
        createTable("CREATE TABLE %s (k int primary key, m map<frozen<map<int, int>>, int>)");

        execute("INSERT INTO %s (k, m) VALUES (1, {{1 : 1, 2 : 2} : 1})");
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, map(map(1, 1, 2, 2), 1)));

        execute("UPDATE %s SET m[?]=2 WHERE k=1", map(1, 1, 2, 2));
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, map(map(1, 1, 2, 2), 2)));

        execute("UPDATE %s SET m = m + ? WHERE k=1", map(map(3, 3, 4, 4), 3));
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1,
                    map(map(1, 1, 2, 2), 2,
                        map(3, 3, 4, 4), 3)));

        execute("DELETE m[?] FROM %s WHERE k = 1", map(1, 1, 2, 2));
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, map(map(3, 3, 4, 4), 3)));
    }

    @Test
    public void testFrozenMapInSet() throws Throwable
    {
        createTable("CREATE TABLE %s (k int primary key, s set<frozen<map<int, int>>>)");

        execute("INSERT INTO %s (k, s) VALUES (1, {{1 : 1, 2 : 2}})");

        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, set(map(1, 1, 2, 2)))
        );

        execute("UPDATE %s SET s = s + ? WHERE k=1", set(map(3, 3, 4, 4)));
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, set(map(1, 1, 2, 2), map(3, 3, 4, 4)))
        );

        execute("UPDATE %s SET s = s - ? WHERE k=1", set(map(3, 3, 4, 4)));
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, set(map(1, 1, 2, 2)))
        );

        execute("DELETE s[?] FROM %s WHERE k = 1", map(1, 1, 2, 2));
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, null)
        );
    }

    @Test
    public void testFrozenMapInList() throws Throwable
    {
        createTable("CREATE TABLE %s (k int primary key, l list<frozen<map<int, int>>>)");

        execute("INSERT INTO %s (k, l) VALUES (1, [{1 : 1, 2 : 2}])");
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, list(map(1, 1, 2, 2)))
        );

        execute("UPDATE %s SET l[?]=? WHERE k=1", 0, map(3, 3, 4, 4));
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, list(map(3, 3, 4, 4)))
        );

        execute("UPDATE %s SET l = ? + l WHERE k=1", list(map(1, 1, 2, 2)));
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, list(map(1, 1, 2, 2), map(3, 3, 4, 4)))
        );

        execute("UPDATE %s SET l = l + ? WHERE k=1", list(map(5, 5, 6, 6)));
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, list(map(1, 1, 2, 2), map(3, 3, 4, 4), map(5, 5, 6, 6)))
        );

        execute("UPDATE %s SET l = l - ? WHERE k=1", list(map(3, 3, 4, 4)));
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, list(map(1, 1, 2, 2), map(5, 5, 6, 6)))
        );

        execute("DELETE l[0] FROM %s WHERE k = 1");
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, list(map(5, 5, 6, 6)))
        );
    }

    @Test
    public void testFrozenSetInMap() throws Throwable
    {
        createTable("CREATE TABLE %s (k int primary key, m map<frozen<set<int>>, int>)");

        execute("INSERT INTO %s (k, m) VALUES (1, {{1, 2, 3} : 1})");
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, map(set(1, 2, 3), 1)));

        execute("UPDATE %s SET m[?]=2 WHERE k=1", set(1, 2, 3));
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, map(set(1, 2, 3), 2)));

        execute("UPDATE %s SET m = m + ? WHERE k=1", map(set(4, 5, 6), 3));
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1,
                    map(set(1, 2, 3), 2,
                        set(4, 5, 6), 3)));

        execute("DELETE m[?] FROM %s WHERE k = 1", set(1, 2, 3));
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, map(set(4, 5, 6), 3)));
    }

    @Test
    public void testFrozenSetInSet() throws Throwable
    {
        createTable("CREATE TABLE %s (k int primary key, s set<frozen<set<int>>>)");

        execute("INSERT INTO %s (k, s) VALUES (1, {{1, 2, 3}})");

        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, set(set(1, 2, 3)))
        );

        execute("UPDATE %s SET s = s + ? WHERE k=1", set(set(4, 5, 6)));
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, set(set(1, 2, 3), set(4, 5, 6)))
        );

        execute("UPDATE %s SET s = s - ? WHERE k=1", set(set(4, 5, 6)));
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, set(set(1, 2, 3)))
        );

        execute("DELETE s[?] FROM %s WHERE k = 1", set(1, 2, 3));
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, null)
        );
    }

    @Test
    public void testFrozenSetInList() throws Throwable
    {
        createTable("CREATE TABLE %s (k int primary key, l list<frozen<set<int>>>)");

        execute("INSERT INTO %s (k, l) VALUES (1, [{1, 2, 3}])");
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, list(set(1, 2, 3)))
        );

        execute("UPDATE %s SET l[?]=? WHERE k=1", 0, set(4, 5, 6));
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, list(set(4, 5, 6)))
        );

        execute("UPDATE %s SET l = ? + l WHERE k=1", list(set(1, 2, 3)));
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, list(set(1, 2, 3), set(4, 5, 6)))
        );

        execute("UPDATE %s SET l = l + ? WHERE k=1", list(set(7, 8, 9)));
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, list(set(1, 2, 3), set(4, 5, 6), set(7, 8, 9)))
        );

        execute("UPDATE %s SET l = l - ? WHERE k=1", list(set(4, 5, 6)));
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, list(set(1, 2, 3), set(7, 8, 9)))
        );

        execute("DELETE l[0] FROM %s WHERE k = 1");
        assertRows(execute("SELECT * FROM %s WHERE k = 1"),
                row(1, list(set(7, 8, 9)))
        );
    }

    @Test
    public void testUserDefinedTypes() throws Throwable
    {
        String myType = createType("CREATE TYPE %s (a set<int>, b tuple<list<int>>)");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v frozen<" + myType + ">)");
        execute("INSERT INTO %s (k, v) VALUES (?, {a: ?, b: ?})", 0, set(1, 2, 3), tuple(list(1, 2, 3)));
        assertRows(execute("SELECT v.a, v.b FROM %s WHERE k=?", 0),
            row(set(1, 2, 3), tuple(list(1, 2, 3)))
        );
    }

    /**
     * Test parsing of literal lists when the column type is reversed (CASSANDRA-15814)
     */
    @Test
    public void testLiteralReversedList() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c frozen<list<int>>, PRIMARY KEY (k, c)) WITH CLUSTERING ORDER BY (c DESC)");
        execute("INSERT INTO %s (k, c) VALUES (0, [1, 2])");
        assertRows(execute("SELECT c FROM %s WHERE k=0 AND c=[1, 2]"), row(list(1, 2)));
    }

    /**
     * Test parsing of literal sets when the column type is reversed (CASSANDRA-15814)
     */
    @Test
    public void testLiteralReversedSet() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c frozen<set<int>>, PRIMARY KEY (k, c)) WITH CLUSTERING ORDER BY (c DESC)");
        execute("INSERT INTO %s (k, c) VALUES (0, {1, 2})");
        assertRows(execute("SELECT c FROM %s WHERE k=0 AND c={1, 2}"), row(set(1, 2)));
    }

    /**
     * Test parsing of literal maps when the column type is reversed (CASSANDRA-15814)
     */
    @Test
    public void testLiteralReversedMap() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c frozen<map<int,int>>, PRIMARY KEY (k, c)) WITH CLUSTERING ORDER BY (c DESC)");
        execute("INSERT INTO %s (k, c) VALUES (0, {1:2, 3:4})");
        assertRows(execute("SELECT c FROM %s WHERE k=0 AND c={1:2, 3:4}"), row(map(1, 2, 3, 4)));
    }

    private static String clean(String classname)
    {
        return StringUtils.remove(classname, "org.apache.cassandra.db.marshal.");
    }

    @Test
    public void testToString()
    {
        // set<frozen<list<int>>>
        SetType t = SetType.getInstance(ListType.getInstance(Int32Type.instance, false), true);
        assertEquals("SetType(FrozenType(ListType(Int32Type)))", clean(t.toString()));
        assertEquals("SetType(ListType(Int32Type))", clean(t.toString(true)));

        // frozen<set<list<int>>>
        t = SetType.getInstance(ListType.getInstance(Int32Type.instance, false), false);
        assertEquals("FrozenType(SetType(ListType(Int32Type)))", clean(t.toString()));
        assertEquals("SetType(ListType(Int32Type))", clean(t.toString(true)));

        // map<frozen<list<int>>, int>
        MapType m = MapType.getInstance(ListType.getInstance(Int32Type.instance, false), Int32Type.instance, true);
        assertEquals("MapType(FrozenType(ListType(Int32Type)),Int32Type)", clean(m.toString()));
        assertEquals("MapType(ListType(Int32Type),Int32Type)", clean(m.toString(true)));

        // frozen<map<list<int>, int>>
        m = MapType.getInstance(ListType.getInstance(Int32Type.instance, false), Int32Type.instance, false);
        assertEquals("FrozenType(MapType(ListType(Int32Type),Int32Type))", clean(m.toString()));
        assertEquals("MapType(ListType(Int32Type),Int32Type)", clean(m.toString(true)));

        // tuple<set<int>>
        List<AbstractType<?>> types = new ArrayList<>();
        types.add(SetType.getInstance(Int32Type.instance, true));
        TupleType tuple = new TupleType(types);
        assertEquals("TupleType(SetType(Int32Type))", clean(tuple.toString()));
    }

    @Test
    public void testListWithElementsBiggerThan64K() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, l frozen<list<text>>)");

        byte[] bytes = new byte[FBUtilities.MAX_UNSIGNED_SHORT + 10];
        Arrays.fill(bytes, (byte) 1);
        String largeText = new String(bytes);

        execute("INSERT INTO %s(k, l) VALUES (0, ?)", list(largeText, "v2"));
        flush();

        assertRows(execute("SELECT l FROM %s WHERE k = 0"), row(list(largeText, "v2")));

        // Full overwrite
        execute("UPDATE %s SET l = ? WHERE k = 0", list("v1", largeText));
        flush();

        assertRows(execute("SELECT l FROM %s WHERE k = 0"), row(list("v1", largeText)));

        execute("DELETE l FROM %s WHERE k = 0");

        assertRows(execute("SELECT l FROM %s WHERE k = 0"), row((Object) null));

        execute("INSERT INTO %s(k, l) VALUES (0, ['" + largeText + "', 'v2'])");
        flush();

        assertRows(execute("SELECT l FROM %s WHERE k = 0"), row(list(largeText, "v2")));
    }

    @Test
    public void testMapsWithElementsBiggerThan64K() throws Throwable
    {
        byte[] bytes = new byte[FBUtilities.MAX_UNSIGNED_SHORT + 10];
        Arrays.fill(bytes, (byte) 1);
        String largeText = new String(bytes);

        bytes = new byte[FBUtilities.MAX_UNSIGNED_SHORT + 10];
        Arrays.fill(bytes, (byte) 2);
        String largeText2 = new String(bytes);

        createTable("CREATE TABLE %s (k int PRIMARY KEY, m frozen<map<text, text>>)");

        execute("INSERT INTO %s(k, m) VALUES (0, ?)", map(largeText, "v1", "k2", largeText));
        flush();

        assertRows(execute("SELECT m FROM %s WHERE k = 0"),
            row(map(largeText, "v1", "k2", largeText)));

        // Full overwrite
        execute("UPDATE %s SET m = ? WHERE k = 0", map("k5", largeText, largeText2, "v6"));
        flush();

        assertRows(execute("SELECT m FROM %s WHERE k = 0"),
                   row(map("k5", largeText, largeText2, "v6")));

        execute("DELETE m FROM %s WHERE k = 0");

        assertRows(execute("SELECT m FROM %s WHERE k = 0"), row((Object) null));

        execute("INSERT INTO %s(k, m) VALUES (0, {'" + largeText + "' : 'v1', 'k2' : '" + largeText + "'})");
        flush();

        assertRows(execute("SELECT m FROM %s WHERE k = 0"),
                   row(map(largeText, "v1", "k2", largeText)));
    }

    @Test
    public void testSetsWithElementsBiggerThan64K() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, s frozen<set<text>>)");

        byte[] bytes = new byte[FBUtilities.MAX_UNSIGNED_SHORT + 10];
        Arrays.fill(bytes, (byte) 1);
        String largeText = new String(bytes);

        execute("INSERT INTO %s(k, s) VALUES (0, ?)", set(largeText, "v1", "v2"));
        flush();

        assertRows(execute("SELECT s FROM %s WHERE k = 0"), row(set(largeText, "v1", "v2")));

        // Full overwrite
        execute("UPDATE %s SET s = ? WHERE k = 0", set(largeText, "v3"));
        flush();

        assertRows(execute("SELECT s FROM %s WHERE k = 0"), row(set(largeText, "v3")));

        execute("DELETE s FROM %s WHERE k = 0");

        assertRows(execute("SELECT s FROM %s WHERE k = 0"), row((Object) null));

        execute("INSERT INTO %s(k, s) VALUES (0, {'" + largeText + "', 'v1', 'v2'})");
        flush();

        assertRows(execute("SELECT s FROM %s WHERE k = 0"), row(set(largeText, "v1", "v2")));
    }
}
