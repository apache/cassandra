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

package org.apache.cassandra.db.compaction.differential;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;

/** Checks reversed clustering order and range-bound kinds through the differential harness. */
public class ReversedClusteringDifferentialCompactionTest extends DifferentialCompactionTester
{
    /** Rows only, reversed order. */
    @Test
    public void reversedClusteringRows() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH CLUSTERING ORDER BY (ck DESC)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int ck = 0; ck < 40; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1, ck, "a" + ck);
        flush();
        for (int ck = 20; ck < 60; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1, ck, "b" + ck);
        flush();

        assertCursorMatchesIterator(cfs);
    }

    /** Range deletions with every inclusive/exclusive bound combination under reversed order. */
    @Test
    public void reversedClusteringWithBoundKinds() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH CLUSTERING ORDER BY (ck DESC)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int ck = 0; ck < 80; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1, ck, "a" + ck);
        flush();

        execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck <= ?", 1, 10, 15);
        execute("DELETE FROM %s WHERE pk = ? AND ck > ? AND ck < ?", 1, 20, 25);
        execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck < ?", 1, 30, 35);
        execute("DELETE FROM %s WHERE pk = ? AND ck > ? AND ck <= ?", 1, 40, 45);
        // Open-ended on each side.
        execute("DELETE FROM %s WHERE pk = ? AND ck < ?", 1, 3);
        execute("DELETE FROM %s WHERE pk = ? AND ck > ?", 1, 76);
        flush();

        assertCursorMatchesIterator(cfs);
    }

    /** A static row alongside reversed clustering. */
    @Test
    public void reversedClusteringWithStaticRow() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, s text static, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH CLUSTERING ORDER BY (ck DESC)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        execute("INSERT INTO %s (pk, s) VALUES (?, ?)", 1, "static-one");
        for (int ck = 0; ck < 30; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1, ck, "a" + ck);
        flush();

        execute("INSERT INTO %s (pk, s) VALUES (?, ?)", 1, "static-two");
        execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck < ?", 1, 5, 12);
        flush();

        assertCursorMatchesIterator(cfs);
    }

    /** Two reversed clustering columns. */
    @Test
    public void twoReversedClusteringColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, c1 int, c2 text, v text, PRIMARY KEY (pk, c1, c2)) " +
                    "WITH CLUSTERING ORDER BY (c1 DESC, c2 ASC)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int c1 = 0; c1 < 10; c1++)
            for (int c2 = 0; c2 < 5; c2++)
                execute("INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", 1, c1, "c" + c2, "v" + c1 + c2);
        flush();

        execute("DELETE FROM %s WHERE pk = ? AND c1 = ? AND c2 >= ?", 1, 4, "c1");
        execute("DELETE FROM %s WHERE pk = ? AND c1 > ? AND c1 < ?", 1, 6, 9);
        flush();

        assertCursorMatchesIterator(cfs);
    }
}
