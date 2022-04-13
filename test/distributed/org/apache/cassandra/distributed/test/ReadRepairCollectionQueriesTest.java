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

package org.apache.cassandra.distributed.test;

import org.junit.Test;

import static org.apache.cassandra.distributed.shared.AssertUtils.row;

/**
 * {@link ReadRepairQueryTester} for queries on collections.
 */
public class ReadRepairCollectionQueriesTest extends ReadRepairQueryTester
{
    /**
     * Test unrestricted queries with frozen tuples.
     */
    @Test
    public void testTuple()
    {
        tester("")
        .createTable("CREATE TABLE %s (k int PRIMARY KEY, a tuple<int,int>, b tuple<int,int>)")
        .mutate("INSERT INTO %s (k, a, b) VALUES (0, (1, 2), (3, 4))")
        .queryColumns("a", 1, 1,
                      rows(row(tuple(1, 2))),
                      rows(row(0, tuple(1, 2), tuple(3, 4))),
                      rows(row(0, tuple(1, 2), null)))
        .deleteColumn("DELETE a FROM %s WHERE k=0", "b", 0, 1,
                      rows(row(tuple(3, 4))),
                      rows(row(0, null, tuple(3, 4))),
                      rows(row(0, tuple(1, 2), tuple(3, 4))))
        .deleteRows("DELETE FROM %s WHERE k=0", 1,
                    rows(),
                    rows(row(0, null, tuple(3, 4))))
        .tearDown();
    }

    /**
     * Test unrestricted queries with frozen sets.
     */
    @Test
    public void testFrozenSet()
    {
        tester("")
        .createTable("CREATE TABLE %s (k int PRIMARY KEY, a frozen<set<int>>, b frozen<set<int>>)")
        .mutate("INSERT INTO %s (k, a, b) VALUES (0, {1, 2}, {3, 4})")
        .queryColumns("a[1]", 1, 1,
                      rows(row(1)),
                      rows(row(0, set(1, 2), set(3, 4))),
                      rows(row(0, set(1, 2), null)))
        .deleteColumn("DELETE a FROM %s WHERE k=0", "b[4]", 0, 1,
                      rows(row(4)),
                      rows(row(0, null, set(3, 4))),
                      rows(row(0, set(1, 2), set(3, 4))))
        .deleteRows("DELETE FROM %s WHERE k=0", 1,
                    rows(),
                    rows(row(0, null, set(3, 4))))
        .tearDown();
    }

    /**
     * Test unrestricted queries with frozen lists.
     */
    @Test
    public void testFrozenList()
    {
        tester("")
        .createTable("CREATE TABLE %s (k int PRIMARY KEY, a frozen<list<int>>, b frozen<list<int>>)")
        .mutate("INSERT INTO %s (k, a, b) VALUES (0, [1, 2], [3, 4])")
        .queryColumns("a", 1, 1,
                      rows(row(list(1, 2))),
                      rows(row(0, list(1, 2), list(3, 4))),
                      rows(row(0, list(1, 2), null)))
        .deleteColumn("DELETE a FROM %s WHERE k=0", "b", 0, 1,
                      rows(row(list(3, 4))),
                      rows(row(0, null, list(3, 4))),
                      rows(row(0, list(1, 2), list(3, 4))))
        .deleteRows("DELETE FROM %s WHERE k=0", 1,
                    rows(),
                    rows(row(0, null, list(3, 4))))
        .tearDown();
    }

    /**
     * Test unrestricted queries with frozen maps.
     */
    @Test
    public void testFrozenMap()
    {
        tester("")
        .createTable("CREATE TABLE %s (k int PRIMARY KEY, a frozen<map<int,int>>, b frozen<map<int,int>>)")
        .mutate("INSERT INTO %s (k, a, b) VALUES (0, {1:10, 2:20}, {3:30, 4:40})")
        .queryColumns("a[2]", 1, 1,
                      rows(row(20)),
                      rows(row(0, map(1, 10, 2, 20), map(3, 30, 4, 40))),
                      rows(row(0, map(1, 10, 2, 20), null)))
        .deleteColumn("DELETE a FROM %s WHERE k=0", "b[4]", 0, 1,
                      rows(row(40)),
                      rows(row(0, null, map(3, 30, 4, 40))),
                      rows(row(0, map(1, 10, 2, 20), map(3, 30, 4, 40))))
        .deleteRows("DELETE FROM %s WHERE k=0", 1,
                    rows(),
                    rows(row(0, null, map(3, 30, 4, 40))))
        .tearDown();
    }

    /**
     * Test unrestricted queries with frozen user-defined types.
     */
    @Test
    public void testFrozentuple()
    {
        tester("")
        .createTable("CREATE TABLE %s (k int PRIMARY KEY, a frozen<udt>, b frozen<udt>)")
        .mutate("INSERT INTO %s (k, a, b) VALUES (0, {x:1, y:2}, {x:3, y:4})")
        .queryColumns("a.x", 1, 1,
                      rows(row(1)),
                      rows(row(0, tuple(1, 2), tuple(3, 4))),
                      rows(row(0, tuple(1, 2), null)))
        .deleteColumn("DELETE a FROM %s WHERE k=0", "b.y", 0, 1,
                      rows(row(4)),
                      rows(row(0, null, tuple(3, 4))),
                      rows(row(0, tuple(1, 2), tuple(3, 4))))
        .deleteRows("DELETE FROM %s WHERE k=0", 1,
                    rows(),
                    rows(row(0, null, tuple(3, 4))))
        .tearDown();
    }

    /**
     * Test unrestricted queries with non-frozen sets.
     */
    @Test
    public void testNonFrozenSet()
    {
        tester("")
        .createTable("CREATE TABLE %s (k int PRIMARY KEY, a set<int>, b set<int>, c int)")
        .mutate("INSERT INTO %s (k, a, b, c) VALUES (0, {1, 2}, {3, 4}, 10)")
        .queryColumns("a[1]", 1, 1,
                      rows(row(1)),
                      rows(row(0, set(1, 2), set(3, 4), 10)),
                      rows(row(0, set(1), null, null)))
        .deleteColumn("UPDATE %s SET a=a-{2} WHERE k=0", "b[4]", 0, 1,
                      rows(row(4)),
                      rows(row(0, set(1), set(3, 4), 10)),
                      rows(row(0, set(1, 2), set(3, 4), 10)))
        .deleteRows("DELETE FROM %s WHERE k=0", 1,
                    rows(),
                    rows(row(0, set(1), set(3, 4), 10)))
        .tearDown();
    }

    /**
     * Test unrestricted queries with non-frozen lists.
     */
    @Test
    public void testNonFrozenList()
    {
        tester("")
        .createTable("CREATE TABLE %s (k int PRIMARY KEY, a list<int>, b list<int>, c int)")
        .mutate("INSERT INTO %s (k, a, b, c) VALUES (0, [1, 2], [3, 4], 10)")
        .queryColumns("a", 1, 1,
                      rows(row(list(1, 2))),
                      rows(row(0, list(1, 2), list(3, 4), 10)),
                      rows(row(0, list(1, 2), null, null)))
        .deleteColumn("DELETE a[1] FROM %s WHERE k=0", "b", 0, 1,
                      rows(row(list(3, 4))),
                      rows(row(0, list(1), list(3, 4), 10)),
                      rows(row(0, list(1, 2), list(3, 4), 10)))
        .deleteRows("DELETE FROM %s WHERE k=0", 1,
                    rows(),
                    rows(row(0, list(1), list(3, 4), 10)))
        .tearDown();
    }

    /**
     * Test unrestricted queries with non-frozen maps.
     */
    @Test
    public void testNonFrozenMap()
    {
        tester("")
        .createTable("CREATE TABLE %s (k int PRIMARY KEY, a map<int,int>, b map<int,int>, c int)")
        .mutate("INSERT INTO %s (k, a, b, c) VALUES (0, {1:10, 2:20}, {3:30, 4:40}, 10)")
        .queryColumns("a[2]", 1, 1,
                      rows(row(20)),
                      rows(row(0, map(1, 10, 2, 20), map(3, 30, 4, 40), 10)),
                      rows(row(0, map(2, 20), null, null)))
        .deleteColumn("DELETE a[1] FROM %s WHERE k=0", "b[4]", 0, 1,
                      rows(row(40)),
                      rows(row(0, map(2, 20), map(3, 30, 4, 40), 10)),
                      rows(row(0, map(1, 10, 2, 20), map(3, 30, 4, 40), 10)))
        .deleteRows("DELETE FROM %s WHERE k=0", 1,
                    rows(),
                    rows(row(0, map(2, 20), map(3, 30, 4, 40), 10)))
        .tearDown();
    }

    /**
     * Test unrestricted queries with non-frozen user-defined types.
     */
    @Test
    public void testNonFrozentuple()
    {
        tester("")
        .createTable("CREATE TABLE %s (k int PRIMARY KEY, a udt, b udt, c int)")
        .mutate("INSERT INTO %s (k, a, b, c) VALUES (0, {x:1, y:2}, {x:3, y:4}, 10)")
        .queryColumns("a.x", 1, 1,
                      rows(row(1)),
                      rows(row(0, tuple(1, 2), tuple(3, 4), 10)),
                      rows(row(0, tuple(1, 2), null, null)))
        .deleteColumn("DELETE a.x FROM %s WHERE k=0", "b.y", 0, 1,
                      rows(row(4)),
                      rows(row(0, tuple(null, 2), tuple(3, 4), 10)),
                      rows(row(0, tuple(1, 2), tuple(3, 4), 10)))
        .deleteRows("DELETE FROM %s WHERE k=0", 1,
                    rows(),
                    rows(row(0, tuple(null, 2), tuple(3, 4), 10)))
        .tearDown();
    }
}
