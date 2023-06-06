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

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for CQL's {@code WRITETIME}, {@code MAXWRITETIME} and {@code TTL} selection functions.
 */
public class WritetimeOrTTLTest extends CQLTester
{
    private static final long TIMESTAMP_1 = 1;
    private static final long TIMESTAMP_2 = 2;
    private static final Long NO_TIMESTAMP = null;

    private static final int TTL_1 = 10000;
    private static final int TTL_2 = 20000;
    private static final Integer NO_TTL = null;

    @Test
    public void testSimple() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int,  PRIMARY KEY(pk, ck))");

        // Primary key columns should be rejected
        assertInvalidPrimaryKeySelection("pk");
        assertInvalidPrimaryKeySelection("ck");

        // No rows
        assertEmpty(execute("SELECT WRITETIME(v) FROM %s"));
        assertEmpty(execute("SELECT TTL(v) FROM %s"));

        // Insert row without TTL
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 2, 3) USING TIMESTAMP ?", TIMESTAMP_1);
        assertWritetimeAndTTL("v", TIMESTAMP_1, NO_TTL);

        // Update the row with TTL and a new timestamp
        execute("UPDATE %s USING TIMESTAMP ? AND TTL ? SET v=8 WHERE pk=1 AND ck=2", TIMESTAMP_2, TTL_1);
        assertWritetimeAndTTL("v", TIMESTAMP_2, TTL_1);

        // Combine with other columns
        assertRows("SELECT pk, WRITETIME(v) FROM %s", row(1, TIMESTAMP_2));
        assertRows("SELECT WRITETIME(v), pk FROM %s", row(TIMESTAMP_2, 1));
        assertRows("SELECT pk, WRITETIME(v), v, ck FROM %s", row(1, TIMESTAMP_2, 8, 2));
    }

    @Test
    public void testList() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, l list<int>)");

        // Null column
        execute("INSERT INTO %s (k) VALUES (1) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("l", NO_TIMESTAMP, NO_TTL);

        // Create empty
        execute("INSERT INTO %s (k, l) VALUES (1, []) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("l", NO_TIMESTAMP, NO_TTL);

        // Create with a single element without TTL
        execute("INSERT INTO %s (k, l) VALUES (1, [1]) USING TIMESTAMP ?", TIMESTAMP_1);
        assertWritetimeAndTTL("l", timestamps(TIMESTAMP_1), ttls(NO_TTL));

        // Add a new element to the list with a new timestamp and a TTL
        execute("UPDATE %s USING TIMESTAMP ? AND TTL ? SET l=l+[2] WHERE k=1", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("l", timestamps(TIMESTAMP_1, TIMESTAMP_2), ttls(NO_TTL, TTL_2));

        assertInvalidListElementSelection("l[0]", "l");
        assertInvalidListSliceSelection("l[..0]", "l");
        assertInvalidListSliceSelection("l[0..]", "l");
        assertInvalidListSliceSelection("l[1..1]", "l");
        assertInvalidListSliceSelection("l[1..2]", "l");

        // Read multiple rows to verify selector reset
        execute("TRUNCATE TABLE %s");
        execute("INSERT INTO %s (k, l) VALUES (1, [1, 2, 3]) USING TIMESTAMP ?", TIMESTAMP_1);
        execute("INSERT INTO %s (k, l) VALUES (2, [1, 2]) USING TIMESTAMP ?", TIMESTAMP_2);
        execute("INSERT INTO %s (k, l) VALUES (3, [1]) USING TIMESTAMP ?", TIMESTAMP_1);
        execute("INSERT INTO %s (k, l) VALUES (4, []) USING TIMESTAMP ?", TIMESTAMP_2);
        execute("INSERT INTO %s (k, l) VALUES (5, null) USING TIMESTAMP ?", TIMESTAMP_2);
        assertRows("SELECT k, WRITETIME(l) FROM %s",
                   row(5, NO_TIMESTAMP),
                   row(1, timestamps(TIMESTAMP_1, TIMESTAMP_1, TIMESTAMP_1)),
                   row(2, timestamps(TIMESTAMP_2, TIMESTAMP_2)),
                   row(4, NO_TIMESTAMP),
                   row(3, timestamps(TIMESTAMP_1)));
    }

    @Test
    public void testFrozenList() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v frozen<list<int>>)");

        // Null column
        execute("INSERT INTO %s (k) VALUES (1) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("v", NO_TIMESTAMP, NO_TTL);

        // Create empty
        execute("INSERT INTO %s (k, v) VALUES (1, []) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("v", TIMESTAMP_1, TTL_1);

        // truncate, since previous columns would win on reconcilliation because of their TTL (CASSANDRA-14592)
        execute("TRUNCATE TABLE %s");

        // Update with a single element without TTL
        execute("INSERT INTO %s (k, v) VALUES (1, [1]) USING TIMESTAMP ?", TIMESTAMP_1);
        assertWritetimeAndTTL("v", TIMESTAMP_1, NO_TTL);

        // Add a new element to the list with a new timestamp and a TTL
        execute("INSERT INTO %s (k, v) VALUES (1, [1, 2, 3]) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("v", TIMESTAMP_2, TTL_2);

        assertInvalidListElementSelection("v[1]", "v");
        assertInvalidListSliceSelection("v[..0]", "v");
        assertInvalidListSliceSelection("v[0..]", "v");
        assertInvalidListSliceSelection("v[1..1]", "v");
        assertInvalidListSliceSelection("v[1..2]", "v");

        // Read multiple rows to verify selector reset
        execute("TRUNCATE TABLE %s");
        execute("INSERT INTO %s (k, v) VALUES (1, [1, 2, 3]) USING TIMESTAMP ?", TIMESTAMP_1);
        execute("INSERT INTO %s (k, v) VALUES (2, [1, 2]) USING TIMESTAMP ?", TIMESTAMP_2);
        execute("INSERT INTO %s (k, v) VALUES (3, [1]) USING TIMESTAMP ?", TIMESTAMP_1);
        execute("INSERT INTO %s (k, v) VALUES (4, []) USING TIMESTAMP ?", TIMESTAMP_2);
        execute("INSERT INTO %s (k, v) VALUES (5, null) USING TIMESTAMP ?", TIMESTAMP_2);
        assertRows("SELECT k, WRITETIME(v) FROM %s",
                   row(5, NO_TIMESTAMP),
                   row(1, TIMESTAMP_1),
                   row(2, TIMESTAMP_2),
                   row(4, TIMESTAMP_2),
                   row(3, TIMESTAMP_1));
    }

    @Test
    public void testSet() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, s set<int>)");

        // Null column
        execute("INSERT INTO %s (k) VALUES (1) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("s", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("s[0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("s[..0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("s[0..]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("s[0..0]", NO_TIMESTAMP, NO_TTL);

        // Create empty
        execute("INSERT INTO %s (k, s) VALUES (1, {}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("s", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("s[0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("s[..0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("s[0..]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("s[0..0]", NO_TIMESTAMP, NO_TTL);

        // Update with a single element without TTL
        execute("INSERT INTO %s (k, s) VALUES (1, {1}) USING TIMESTAMP ?", TIMESTAMP_1);
        assertWritetimeAndTTL("s", timestamps(TIMESTAMP_1), ttls(NO_TTL));
        assertWritetimeAndTTL("s[0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("s[1]", TIMESTAMP_1, NO_TTL);
        assertWritetimeAndTTL("s[2]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("s[..0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("s[..1]", timestamps(TIMESTAMP_1), ttls(NO_TTL));
        assertWritetimeAndTTL("s[..2]", timestamps(TIMESTAMP_1), ttls(NO_TTL));
        assertWritetimeAndTTL("s[0..]", timestamps(TIMESTAMP_1), ttls(NO_TTL));
        assertWritetimeAndTTL("s[1..]", timestamps(TIMESTAMP_1), ttls(NO_TTL));
        assertWritetimeAndTTL("s[2..]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("s[0..0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("s[0..1]", timestamps(TIMESTAMP_1), ttls(NO_TTL));
        assertWritetimeAndTTL("s[1..1]", timestamps(TIMESTAMP_1), ttls(NO_TTL));
        assertWritetimeAndTTL("s[1..2]", timestamps(TIMESTAMP_1), ttls(NO_TTL));
        assertWritetimeAndTTL("s[2..2]", NO_TIMESTAMP, NO_TTL);

        // Add a new element to the set with a new timestamp and a TTL
        execute("UPDATE %s USING TIMESTAMP ? AND TTL ? SET s=s+{2} WHERE k=1", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("s", timestamps(TIMESTAMP_1, TIMESTAMP_2), ttls(NO_TTL, TTL_2));
        assertWritetimeAndTTL("s[0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("s[1]", TIMESTAMP_1, NO_TTL);
        assertWritetimeAndTTL("s[2]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("s[3]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("s[..0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("s[..1]", timestamps(TIMESTAMP_1), ttls(NO_TTL));
        assertWritetimeAndTTL("s[..2]", timestamps(TIMESTAMP_1, TIMESTAMP_2), ttls(NO_TTL, TTL_2));
        assertWritetimeAndTTL("s[..3]", timestamps(TIMESTAMP_1, TIMESTAMP_2), ttls(NO_TTL, TTL_2));
        assertWritetimeAndTTL("s[0..]", timestamps(TIMESTAMP_1, TIMESTAMP_2), ttls(NO_TTL, TTL_2));
        assertWritetimeAndTTL("s[1..]", timestamps(TIMESTAMP_1, TIMESTAMP_2), ttls(NO_TTL, TTL_2));
        assertWritetimeAndTTL("s[2..]", timestamps(TIMESTAMP_2), ttls(TTL_2));
        assertWritetimeAndTTL("s[3..]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("s[0..0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("s[0..1]", timestamps(TIMESTAMP_1), ttls(NO_TTL));
        assertWritetimeAndTTL("s[0..2]", timestamps(TIMESTAMP_1, TIMESTAMP_2), ttls(NO_TTL, TTL_2));
        assertWritetimeAndTTL("s[0..3]", timestamps(TIMESTAMP_1, TIMESTAMP_2), ttls(NO_TTL, TTL_2));
        assertWritetimeAndTTL("s[1..1]", timestamps(TIMESTAMP_1), ttls(NO_TTL));
        assertWritetimeAndTTL("s[1..2]", timestamps(TIMESTAMP_1, TIMESTAMP_2), ttls(NO_TTL, TTL_2));
        assertWritetimeAndTTL("s[1..3]", timestamps(TIMESTAMP_1, TIMESTAMP_2), ttls(NO_TTL, TTL_2));
        assertWritetimeAndTTL("s[2..2]", timestamps(TIMESTAMP_2), ttls(TTL_2));
        assertWritetimeAndTTL("s[2..3]", timestamps(TIMESTAMP_2), ttls(TTL_2));
        assertWritetimeAndTTL("s[3..3]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("s[3..4]", NO_TIMESTAMP, NO_TTL);

        // Combine timestamp selection with other selections and orders
        assertRows("SELECT k, WRITETIME(s[1]) FROM %s", row(1, TIMESTAMP_1));
        assertRows("SELECT WRITETIME(s[1]), k FROM %s", row(TIMESTAMP_1, 1));
        assertRows("SELECT WRITETIME(s[1]), WRITETIME(s[2]) FROM %s", row(TIMESTAMP_1, TIMESTAMP_2));
        assertRows("SELECT WRITETIME(s[2]), WRITETIME(s[1]) FROM %s", row(TIMESTAMP_2, TIMESTAMP_1));

        // Read multiple rows to verify selector reset
        execute("TRUNCATE TABLE %s");
        execute("INSERT INTO %s (k, s) VALUES (1, {1, 2, 3}) USING TIMESTAMP ?", TIMESTAMP_1);
        execute("INSERT INTO %s (k, s) VALUES (2, {1, 2}) USING TIMESTAMP ?", TIMESTAMP_2);
        execute("INSERT INTO %s (k, s) VALUES (3, {1}) USING TIMESTAMP ?", TIMESTAMP_1);
        execute("INSERT INTO %s (k, s) VALUES (4, {}) USING TIMESTAMP ?", TIMESTAMP_2);
        execute("INSERT INTO %s (k, s) VALUES (5, null) USING TIMESTAMP ?", TIMESTAMP_2);
        assertRows("SELECT k, WRITETIME(s) FROM %s",
                   row(5, NO_TIMESTAMP),
                   row(1, timestamps(TIMESTAMP_1, TIMESTAMP_1, TIMESTAMP_1)),
                   row(2, timestamps(TIMESTAMP_2, TIMESTAMP_2)),
                   row(4, NO_TIMESTAMP),
                   row(3, timestamps(TIMESTAMP_1)));
        assertRows("SELECT k, WRITETIME(s[1]) FROM %s",
                   row(5, NO_TIMESTAMP),
                   row(1, TIMESTAMP_1),
                   row(2, TIMESTAMP_2),
                   row(4, NO_TIMESTAMP),
                   row(3, TIMESTAMP_1));
    }

    @Test
    public void testFrozenSet() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, s frozen<set<int>>)");

        // Null column
        execute("INSERT INTO %s (k) VALUES (1) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("s", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("s[0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("s[..0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("s[0..]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("s[0..0]", NO_TIMESTAMP, NO_TTL);

        // Create empty
        execute("INSERT INTO %s (k, s) VALUES (1, {}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("s", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("s[..0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("s[0..]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("s[0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("s[0..0]", NO_TIMESTAMP, NO_TTL);

        // truncate, since previous columns would win on reconcilliation because of their TTL (CASSANDRA-14592)
        execute("TRUNCATE TABLE %s");

        // Update with a single element without TTL
        execute("INSERT INTO %s (k, s) VALUES (1, {1}) USING TIMESTAMP ?", TIMESTAMP_1);
        assertWritetimeAndTTL("s", TIMESTAMP_1, NO_TTL);
        assertWritetimeAndTTL("s[0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("s[1]", TIMESTAMP_1, NO_TTL);
        assertWritetimeAndTTL("s[2]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("s[..0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("s[..1]", TIMESTAMP_1, NO_TTL);
        assertWritetimeAndTTL("s[..2]", TIMESTAMP_1, NO_TTL);
        assertWritetimeAndTTL("s[0..]", TIMESTAMP_1, NO_TTL);
        assertWritetimeAndTTL("s[1..]", TIMESTAMP_1, NO_TTL);
        assertWritetimeAndTTL("s[2..]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("s[0..0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("s[0..1]", TIMESTAMP_1, NO_TTL);
        assertWritetimeAndTTL("s[0..2]", TIMESTAMP_1, NO_TTL);
        assertWritetimeAndTTL("s[1..1]", TIMESTAMP_1, NO_TTL);
        assertWritetimeAndTTL("s[1..2]", TIMESTAMP_1, NO_TTL);
        assertWritetimeAndTTL("s[2..2]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("s[2..3]", NO_TIMESTAMP, NO_TTL);

        // Add a new element to the set with a new timestamp and a TTL
        execute("INSERT INTO %s (k, s) VALUES (1, {1, 2}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("s", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("s[0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("s[1]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("s[2]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("s[3]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("s[..0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("s[..1]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("s[..2]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("s[..3]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("s[0..]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("s[1..]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("s[2..]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("s[3..]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("s[0..0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("s[0..1]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("s[0..2]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("s[0..3]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("s[1..1]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("s[1..2]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("s[1..3]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("s[2..2]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("s[2..3]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("s[3..3]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("s[3..4]", NO_TIMESTAMP, NO_TTL);

        // Read multiple rows to verify selector reset
        execute("TRUNCATE TABLE %s");
        execute("INSERT INTO %s (k, s) VALUES (1, {1, 2, 3}) USING TIMESTAMP ?", TIMESTAMP_1);
        execute("INSERT INTO %s (k, s) VALUES (2, {1, 2}) USING TIMESTAMP ?", TIMESTAMP_2);
        execute("INSERT INTO %s (k, s) VALUES (3, {1}) USING TIMESTAMP ?", TIMESTAMP_1);
        execute("INSERT INTO %s (k, s) VALUES (4, {}) USING TIMESTAMP ?", TIMESTAMP_2);
        execute("INSERT INTO %s (k, s) VALUES (5, null) USING TIMESTAMP ?", TIMESTAMP_2);
        assertRows("SELECT k, WRITETIME(s) FROM %s",
                   row(5, NO_TIMESTAMP),
                   row(1, TIMESTAMP_1),
                   row(2, TIMESTAMP_2),
                   row(4, TIMESTAMP_2),
                   row(3, TIMESTAMP_1));
        assertRows("SELECT k, WRITETIME(s[1]) FROM %s",
                   row(5, NO_TIMESTAMP),
                   row(1, TIMESTAMP_1),
                   row(2, TIMESTAMP_2),
                   row(4, NO_TIMESTAMP),
                   row(3, TIMESTAMP_1));
    }

    @Test
    public void testMap() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, m map<int, int>)");

        // Null column
        execute("INSERT INTO %s (k) VALUES (1) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("m", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("m[0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("m[..0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("m[0..]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("m[0..0]", NO_TIMESTAMP, NO_TTL);

        // Create empty
        execute("INSERT INTO %s (k, m) VALUES (1, {}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("m", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("m[0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("m[..0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("m[0..]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("m[0..0]", NO_TIMESTAMP, NO_TTL);

        // Update with a single element without TTL
        execute("INSERT INTO %s (k, m) VALUES (1, {1:10}) USING TIMESTAMP ?", TIMESTAMP_1);
        assertWritetimeAndTTL("m", timestamps(TIMESTAMP_1), ttls(NO_TTL));
        assertWritetimeAndTTL("m[1]", TIMESTAMP_1, NO_TTL);
        assertWritetimeAndTTL("m[2]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("m[..0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("m[..1]", timestamps(TIMESTAMP_1), ttls(NO_TTL));
        assertWritetimeAndTTL("m[..2]", timestamps(TIMESTAMP_1), ttls(NO_TTL));
        assertWritetimeAndTTL("m[0..]", timestamps(TIMESTAMP_1), ttls(NO_TTL));
        assertWritetimeAndTTL("m[1..]", timestamps(TIMESTAMP_1), ttls(NO_TTL));
        assertWritetimeAndTTL("m[2..]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("m[0..0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("m[0..1]", timestamps(TIMESTAMP_1), ttls(NO_TTL));
        assertWritetimeAndTTL("m[0..2]", timestamps(TIMESTAMP_1), ttls(NO_TTL));
        assertWritetimeAndTTL("m[1..1]", timestamps(TIMESTAMP_1), ttls(NO_TTL));
        assertWritetimeAndTTL("m[1..2]", timestamps(TIMESTAMP_1), ttls(NO_TTL));
        assertWritetimeAndTTL("m[2..2]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("m[2..3]", NO_TIMESTAMP, NO_TTL);

        // Add a new element to the map with a new timestamp and a TTL
        execute("UPDATE %s USING TIMESTAMP ? AND TTL ? SET m=m+{2:20} WHERE k=1", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("m", timestamps(TIMESTAMP_1, TIMESTAMP_2), ttls(NO_TTL, TTL_2));
        assertWritetimeAndTTL("m[0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("m[1]", TIMESTAMP_1, NO_TTL);
        assertWritetimeAndTTL("m[2]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("m[3]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("m[..0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("m[..1]", timestamps(TIMESTAMP_1), ttls(NO_TTL));
        assertWritetimeAndTTL("m[..2]", timestamps(TIMESTAMP_1, TIMESTAMP_2), ttls(NO_TTL, TTL_2));
        assertWritetimeAndTTL("m[..3]", timestamps(TIMESTAMP_1, TIMESTAMP_2), ttls(NO_TTL, TTL_2));
        assertWritetimeAndTTL("m[0..]", timestamps(TIMESTAMP_1, TIMESTAMP_2), ttls(NO_TTL, TTL_2));
        assertWritetimeAndTTL("m[1..]", timestamps(TIMESTAMP_1, TIMESTAMP_2), ttls(NO_TTL, TTL_2));
        assertWritetimeAndTTL("m[2..]", timestamps(TIMESTAMP_2), ttls(TTL_2));
        assertWritetimeAndTTL("m[3..]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("m[0..0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("m[0..1]", timestamps(TIMESTAMP_1), ttls(NO_TTL));
        assertWritetimeAndTTL("m[0..2]", timestamps(TIMESTAMP_1, TIMESTAMP_2), ttls(NO_TTL, TTL_2));
        assertWritetimeAndTTL("m[0..3]", timestamps(TIMESTAMP_1, TIMESTAMP_2), ttls(NO_TTL, TTL_2));
        assertWritetimeAndTTL("m[1..1]", timestamps(TIMESTAMP_1), ttls(NO_TTL));
        assertWritetimeAndTTL("m[1..2]", timestamps(TIMESTAMP_1, TIMESTAMP_2), ttls(NO_TTL, TTL_2));
        assertWritetimeAndTTL("m[1..3]", timestamps(TIMESTAMP_1, TIMESTAMP_2), ttls(NO_TTL, TTL_2));
        assertWritetimeAndTTL("m[2..2]", timestamps(TIMESTAMP_2), ttls(TTL_2));
        assertWritetimeAndTTL("m[2..3]", timestamps(TIMESTAMP_2), ttls(TTL_2));
        assertWritetimeAndTTL("m[3..3]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("m[3..4]", NO_TIMESTAMP, NO_TTL);

        // Combine timestamp selection with other selections and orders
        assertRows("SELECT k, WRITETIME(m[1]) FROM %s", row(1, TIMESTAMP_1));
        assertRows("SELECT WRITETIME(m[1]), k FROM %s", row(TIMESTAMP_1, 1));
        assertRows("SELECT WRITETIME(m[1]), WRITETIME(m[2]) FROM %s", row(TIMESTAMP_1, TIMESTAMP_2));
        assertRows("SELECT WRITETIME(m[2]), WRITETIME(m[1]) FROM %s", row(TIMESTAMP_2, TIMESTAMP_1));

        // Read multiple rows to verify selector reset
        execute("TRUNCATE TABLE %s");
        execute("INSERT INTO %s (k, m) VALUES (1, {1:10, 2:20, 3:30}) USING TIMESTAMP ?", TIMESTAMP_1);
        execute("INSERT INTO %s (k, m) VALUES (2, {1:10, 2:20}) USING TIMESTAMP ?", TIMESTAMP_2);
        execute("INSERT INTO %s (k, m) VALUES (3, {1:10}) USING TIMESTAMP ?", TIMESTAMP_1);
        execute("INSERT INTO %s (k, m) VALUES (4, {}) USING TIMESTAMP ?", TIMESTAMP_2);
        execute("INSERT INTO %s (k, m) VALUES (5, null) USING TIMESTAMP ?", TIMESTAMP_2);
        assertRows("SELECT k, WRITETIME(m) FROM %s",
                   row(5, NO_TIMESTAMP),
                   row(1, timestamps(TIMESTAMP_1, TIMESTAMP_1, TIMESTAMP_1)),
                   row(2, timestamps(TIMESTAMP_2, TIMESTAMP_2)),
                   row(4, NO_TIMESTAMP),
                   row(3, timestamps(TIMESTAMP_1)));
        assertRows("SELECT k, WRITETIME(m[1]) FROM %s",
                   row(5, NO_TIMESTAMP),
                   row(1, TIMESTAMP_1),
                   row(2, TIMESTAMP_2),
                   row(4, NO_TIMESTAMP),
                   row(3, TIMESTAMP_1));
    }

    @Test
    public void testFrozenMap() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, m frozen<map<int,int>>)");

        // Null column
        execute("INSERT INTO %s (k) VALUES (1) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("m", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("m[0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("m[..0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("m[0..]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("m[0..0]", NO_TIMESTAMP, NO_TTL);

        // Create empty
        execute("INSERT INTO %s (k, m) VALUES (1, {}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("m", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("m[0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("m[..0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("m[0..]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("m[0..0]", NO_TIMESTAMP, NO_TTL);

        // truncate, since previous columns would win on reconcilliation because of their TTL (CASSANDRA-14592)
        execute("TRUNCATE TABLE %s");

        // Create with a single element without TTL
        execute("INSERT INTO %s (k, m) VALUES (1, {1:10}) USING TIMESTAMP ?", TIMESTAMP_1);
        assertWritetimeAndTTL("m", TIMESTAMP_1, NO_TTL);
        assertWritetimeAndTTL("m[0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("m[1]", TIMESTAMP_1, NO_TTL);
        assertWritetimeAndTTL("m[2]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("m[..0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("m[..1]", TIMESTAMP_1, NO_TTL);
        assertWritetimeAndTTL("m[..2]", TIMESTAMP_1, NO_TTL);
        assertWritetimeAndTTL("m[0..]", TIMESTAMP_1, NO_TTL);
        assertWritetimeAndTTL("m[1..]", TIMESTAMP_1, NO_TTL);
        assertWritetimeAndTTL("m[2..]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("m[0..0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("m[0..1]", TIMESTAMP_1, NO_TTL);
        assertWritetimeAndTTL("m[0..2]", TIMESTAMP_1, NO_TTL);
        assertWritetimeAndTTL("m[1..1]", TIMESTAMP_1, NO_TTL);
        assertWritetimeAndTTL("m[1..2]", TIMESTAMP_1, NO_TTL);
        assertWritetimeAndTTL("m[2..2]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("m[2..3]", NO_TIMESTAMP, NO_TTL);

        // Add a new element to the map with a new timestamp and a TTL
        execute("INSERT INTO %s (k, m) VALUES (1, {1:10, 2:20}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("m", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("m[0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("m[1]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("m[2]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("m[3]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("m[..0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("m[..1]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("m[..2]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("m[..3]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("m[0..]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("m[1..]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("m[2..]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("m[3..]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("m[0..0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("m[0..1]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("m[0..2]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("m[0..3]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("m[1..1]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("m[1..2]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("m[1..3]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("m[2..2]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("m[2..3]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("m[3..3]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("m[3..4]", NO_TIMESTAMP, NO_TTL);

        // Read multiple rows to verify selector reset
        execute("TRUNCATE TABLE %s");
        execute("INSERT INTO %s (k, m) VALUES (1, {1:10, 2:20, 3:30}) USING TIMESTAMP ?", TIMESTAMP_1);
        execute("INSERT INTO %s (k, m) VALUES (2, {1:10, 2:20}) USING TIMESTAMP ?", TIMESTAMP_2);
        execute("INSERT INTO %s (k, m) VALUES (3, {1:10}) USING TIMESTAMP ?", TIMESTAMP_1);
        execute("INSERT INTO %s (k, m) VALUES (4, {}) USING TIMESTAMP ?", TIMESTAMP_2);
        execute("INSERT INTO %s (k, m) VALUES (5, null) USING TIMESTAMP ?", TIMESTAMP_2);
        assertRows("SELECT k, WRITETIME(m) FROM %s",
                   row(5, NO_TIMESTAMP),
                   row(1, TIMESTAMP_1),
                   row(2, TIMESTAMP_2),
                   row(4, TIMESTAMP_2),
                   row(3, TIMESTAMP_1));
        assertRows("SELECT k, WRITETIME(m[1]) FROM %s",
                   row(5, NO_TIMESTAMP),
                   row(1, TIMESTAMP_1),
                   row(2, TIMESTAMP_2),
                   row(4, NO_TIMESTAMP),
                   row(3, TIMESTAMP_1));
    }

    @Test
    public void testNestedCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v map<int,frozen<set<int>>>)");

        // Null column
        execute("INSERT INTO %s (k) VALUES (1) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("v", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[0][0]", NO_TIMESTAMP, NO_TTL);

        execute("INSERT INTO %s (k, v) VALUES (1, {1:{1,2}}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        execute("UPDATE %s USING TIMESTAMP ? AND TTL ? SET v=v+{2:{1, 2}} WHERE k=1", TIMESTAMP_2, TTL_2);

        assertWritetimeAndTTL("v", timestamps(TIMESTAMP_1, TIMESTAMP_2), ttls(TTL_1, TTL_2));

        assertWritetimeAndTTL("v[0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[1]", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("v[2]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("v[3]", NO_TIMESTAMP, NO_TTL);

        assertWritetimeAndTTL("v[0..0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[0..1]", timestamps(TIMESTAMP_1), ttls(TTL_1));
        assertWritetimeAndTTL("v[0..2]", timestamps(TIMESTAMP_1, TIMESTAMP_2), ttls(TTL_1, TTL_2));
        assertWritetimeAndTTL("v[0..3]", timestamps(TIMESTAMP_1, TIMESTAMP_2), ttls(TTL_1, TTL_2));
        assertWritetimeAndTTL("v[1..3]", timestamps(TIMESTAMP_1, TIMESTAMP_2), ttls(TTL_1, TTL_2));
        assertWritetimeAndTTL("v[2..3]", timestamps(TIMESTAMP_2), ttls(TTL_2));
        assertWritetimeAndTTL("v[3..3]", NO_TIMESTAMP, NO_TTL);

        assertWritetimeAndTTL("v[0][0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[0][1]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[0][2]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[0][3]", NO_TIMESTAMP, NO_TTL);

        assertWritetimeAndTTL("v[1][0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[1][1]", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("v[1][2]", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("v[1][3]", NO_TIMESTAMP, NO_TTL);

        assertWritetimeAndTTL("v[2][0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[2][1]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("v[2][2]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("v[2][3]", NO_TIMESTAMP, NO_TTL);

        assertWritetimeAndTTL("v[3][0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[3][1]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[3][2]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[3][3]", NO_TIMESTAMP, NO_TTL);

        assertWritetimeAndTTL("v[0][0..0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[0][0..1]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[0][1..2]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[0][2..3]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[0][3..4]", NO_TIMESTAMP, NO_TTL);

        assertWritetimeAndTTL("v[1][0..0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[1][0..1]", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("v[1][1..2]", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("v[1][2..3]", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("v[1][3..4]", NO_TIMESTAMP, NO_TTL);

        assertWritetimeAndTTL("v[2][0..0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[2][0..1]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("v[2][1..2]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("v[2][2..3]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("v[2][3..4]", NO_TIMESTAMP, NO_TTL);

        assertWritetimeAndTTL("v[3][0..0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[3][0..1]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[3][1..2]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[3][2..3]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[3][3..4]", NO_TIMESTAMP, NO_TTL);

        assertWritetimeAndTTL("v[0..1][0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[0..1][1]", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("v[0..1][2]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[0..2][0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[0..2][1]", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("v[0..2][2]", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("v[0..2][3]", NO_TIMESTAMP, NO_TTL);
    }

    @Test
    public void testFrozenNestedCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v frozen<map<int,frozen<set<int>>>>)");
        execute("INSERT INTO %s (k, v) VALUES (1, {1:{1,2}, 2:{1,2}}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);

        assertWritetimeAndTTL("v", TIMESTAMP_1, TTL_1);

        assertWritetimeAndTTL("v[0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[1]", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("v[2]", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("v[3]", NO_TIMESTAMP, NO_TTL);

        assertWritetimeAndTTL("v[0][0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[0][1]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[0][2]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[0][3]", NO_TIMESTAMP, NO_TTL);

        assertWritetimeAndTTL("v[1][0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[1][1]", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("v[1][2]", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("v[1][3]", NO_TIMESTAMP, NO_TTL);

        assertWritetimeAndTTL("v[2][0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[2][1]", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("v[2][2]", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("v[2][3]", NO_TIMESTAMP, NO_TTL);

        assertWritetimeAndTTL("v[3][0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[3][1]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[3][2]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[3][3]", NO_TIMESTAMP, NO_TTL);

        assertWritetimeAndTTL("v[0][0..0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[0][0..1]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[0][1..2]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[0][2..3]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[0][3..4]", NO_TIMESTAMP, NO_TTL);

        assertWritetimeAndTTL("v[1][0..0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[1][0..1]", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("v[1][1..2]", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("v[1][2..3]", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("v[1][3..4]", NO_TIMESTAMP, NO_TTL);

        assertWritetimeAndTTL("v[2][0..0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[2][0..1]", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("v[2][1..2]", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("v[2][2..3]", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("v[2][3..4]", NO_TIMESTAMP, NO_TTL);

        assertWritetimeAndTTL("v[3][0..0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[3][0..1]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[3][1..2]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[3][2..3]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[3][3..4]", NO_TIMESTAMP, NO_TTL);

        assertWritetimeAndTTL("v[0..0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[0..1]", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("v[1..2]", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("v[2..3]", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("v[3..4]", NO_TIMESTAMP, NO_TTL);

        assertWritetimeAndTTL("v[0..0][0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[0..0][1]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[0..1][0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[0..1][1]", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("v[0..1][2]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[1..2][0]", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("v[1..2][1]", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("v[1..2][2]", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("v[1..2][3]", NO_TIMESTAMP, NO_TTL);
    }

    @Test
    public void testUDT() throws Throwable
    {
        String type = createType("CREATE TYPE %s (f1 int, f2 int)");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, t " + type + ')');

        // Null column
        execute("INSERT INTO %s (k) VALUES (0) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f1", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2", NO_TIMESTAMP, NO_TTL);

        // Both fields are empty
        execute("INSERT INTO %s (k, t) VALUES (0, {f1:null, f2:null}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t", "k=0", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f1", "k=0", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2", "k=0", NO_TIMESTAMP, NO_TTL);
        assertRows("SELECT k, WRITETIME(t.f1), WRITETIME(t.f2) FROM %s WHERE k=0", row(0, NO_TIMESTAMP, NO_TIMESTAMP));

        // Only the first field is set
        execute("INSERT INTO %s (k, t) VALUES (1, {f1:1, f2:null}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t", "k=1", timestamps(TIMESTAMP_1, NO_TIMESTAMP), ttls(TTL_1, NO_TTL));
        assertWritetimeAndTTL("t.f1", "k=1", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f2", "k=1", NO_TIMESTAMP, NO_TTL);
        assertRows("SELECT k, WRITETIME(t.f1), WRITETIME(t.f2) FROM %s WHERE k=1", row(1, TIMESTAMP_1, NO_TIMESTAMP));
        assertRows("SELECT k, WRITETIME(t.f2), WRITETIME(t.f1) FROM %s WHERE k=1", row(1, NO_TIMESTAMP, TIMESTAMP_1));

        // Only the second field is set
        execute("INSERT INTO %s (k, t) VALUES (2, {f1:null, f2:2}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t", "k=2", timestamps(NO_TIMESTAMP, TIMESTAMP_1), ttls(NO_TTL, TTL_1));
        assertWritetimeAndTTL("t.f1", "k=2", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2", "k=2", TIMESTAMP_1, TTL_1);
        assertRows("SELECT k, WRITETIME(t.f1), WRITETIME(t.f2) FROM %s WHERE k=2", row(2, NO_TIMESTAMP, TIMESTAMP_1));
        assertRows("SELECT k, WRITETIME(t.f2), WRITETIME(t.f1) FROM %s WHERE k=2", row(2, TIMESTAMP_1, NO_TIMESTAMP));

        // Both fields are set
        execute("INSERT INTO %s (k, t) VALUES (3, {f1:1, f2:2}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t", "k=3", timestamps(TIMESTAMP_1, TIMESTAMP_1), ttls(TTL_1, TTL_1));
        assertWritetimeAndTTL("t.f1", "k=3", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f2", "k=3", TIMESTAMP_1, TTL_1);
        assertRows("SELECT k, WRITETIME(t.f1), WRITETIME(t.f2) FROM %s WHERE k=3", row(3, TIMESTAMP_1, TIMESTAMP_1));

        // Having only the first field set, update the second field
        execute("UPDATE %s USING TIMESTAMP ? AND TTL ? SET t.f2=2 WHERE k=1", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("t", "k=1", timestamps(TIMESTAMP_1, TIMESTAMP_2), ttls(TTL_1, TTL_2));
        assertWritetimeAndTTL("t.f1", "k=1", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f2", "k=1", TIMESTAMP_2, TTL_2);
        assertRows("SELECT k, WRITETIME(t.f1), WRITETIME(t.f2) FROM %s WHERE k=1", row(1, TIMESTAMP_1, TIMESTAMP_2));
        assertRows("SELECT k, WRITETIME(t.f2), WRITETIME(t.f1) FROM %s WHERE k=1", row(1, TIMESTAMP_2, TIMESTAMP_1));

        // Having only the second field set, update the second field
        execute("UPDATE %s USING TIMESTAMP ? AND TTL ? SET t.f1=1 WHERE k=2", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("t", "k=2", timestamps(TIMESTAMP_2, TIMESTAMP_1), ttls(TTL_2, TTL_1));
        assertWritetimeAndTTL("t.f1", "k=2", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("t.f2", "k=2", TIMESTAMP_1, TTL_1);
        assertRows("SELECT k, WRITETIME(t.f1), WRITETIME(t.f2) FROM %s WHERE k=2", row(2, TIMESTAMP_2, TIMESTAMP_1));
        assertRows("SELECT k, WRITETIME(t.f2), WRITETIME(t.f1) FROM %s WHERE k=2", row(2, TIMESTAMP_1, TIMESTAMP_2));
    }

    @Test
    public void testFrozenUDT() throws Throwable
    {
        String type = createType("CREATE TYPE %s (f1 int, f2 int)");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, t frozen<" + type + ">)");

        // Null column
        execute("INSERT INTO %s (k) VALUES (0) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f1", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2", NO_TIMESTAMP, NO_TTL);

        // Both fields are empty
        execute("INSERT INTO %s (k, t) VALUES (0, {f1:null, f2:null}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t", "k=0", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f1", "k=0", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2", "k=0", NO_TIMESTAMP, NO_TTL);
        assertRows("SELECT k, WRITETIME(t.f1), WRITETIME(t.f2) FROM %s WHERE k=0", row(0, NO_TIMESTAMP, NO_TIMESTAMP));

        // Only the first field is set
        execute("INSERT INTO %s (k, t) VALUES (1, {f1:1, f2:null}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t", "k=1", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f1", "k=1", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f2", "k=1", NO_TIMESTAMP, NO_TTL);
        assertRows("SELECT k, WRITETIME(t.f1), WRITETIME(t.f2) FROM %s WHERE k=1", row(1, TIMESTAMP_1, NO_TIMESTAMP));
        assertRows("SELECT k, WRITETIME(t.f2), WRITETIME(t.f1) FROM %s WHERE k=1", row(1, NO_TIMESTAMP, TIMESTAMP_1));

        // Only the second field is set
        execute("INSERT INTO %s (k, t) VALUES (2, {f1:null, f2:2}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t", "k=2", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f1", "k=2", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2", "k=2", TIMESTAMP_1, TTL_1);
        assertRows("SELECT k, WRITETIME(t.f1), WRITETIME(t.f2) FROM %s WHERE k=2", row(2, NO_TIMESTAMP, TIMESTAMP_1));
        assertRows("SELECT k, WRITETIME(t.f2), WRITETIME(t.f1) FROM %s WHERE k=2", row(2, TIMESTAMP_1, NO_TIMESTAMP));

        // Both fields are set
        execute("INSERT INTO %s (k, t) VALUES (3, {f1:1, f2:2}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t", "k=3", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f1", "k=3", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f2", "k=3", TIMESTAMP_1, TTL_1);
        assertRows("SELECT k, WRITETIME(t.f1), WRITETIME(t.f2) FROM %s WHERE k=3", row(3, TIMESTAMP_1, TIMESTAMP_1));
    }

    @Test
    public void testNestedUDTs() throws Throwable
    {
        String nestedType = createType("CREATE TYPE %s (f1 int, f2 int)");
        String type = createType(format("CREATE TYPE %%s (f1 frozen<%s>, f2 frozen<%<s>)", nestedType));
        createTable("CREATE TABLE %s (k int PRIMARY KEY, t " + type + ')');

        // Both fields are empty
        execute("INSERT INTO %s (k, t) VALUES (1, {f1:null, f2:null}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t", "k=1", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f1", "k=1", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f1.f1", "k=1", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f1.f2", "k=1", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2", "k=1", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2.f1", "k=1", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2.f2", "k=1", NO_TIMESTAMP, NO_TTL);

        // Only the first field is set, no nested field is set
        execute("INSERT INTO %s (k, t) VALUES (2, {f1:{f1:null,f2:null}, f2:null}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t", "k=2", timestamps(TIMESTAMP_1, NO_TIMESTAMP), ttls(TTL_1, NO_TTL));
        assertWritetimeAndTTL("t.f1", "k=2", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f1.f1", "k=2", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f1.f2", "k=2", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2", "k=2", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2.f1", "k=2", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2.f2", "k=2", NO_TIMESTAMP, NO_TTL);

        // Only the first field is set, only the first nested field is set
        execute("INSERT INTO %s (k, t) VALUES (3, {f1:{f1:1,f2:null}, f2:null}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t", "k=2", timestamps(TIMESTAMP_1, NO_TIMESTAMP), ttls(TTL_1, NO_TTL));
        assertWritetimeAndTTL("t.f1", "k=3", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f1.f1", "k=3", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f1.f2", "k=3", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2", "k=3", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2.f1", "k=3", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2.f2", "k=3", NO_TIMESTAMP, NO_TTL);

        // Only the first field is set, only the second nested field is set
        execute("INSERT INTO %s (k, t) VALUES (4, {f1:{f1:null,f2:2}, f2:null}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t", "k=4", timestamps(TIMESTAMP_1, NO_TIMESTAMP), ttls(TTL_1, NO_TTL));
        assertWritetimeAndTTL("t.f1", "k=4", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f1.f1", "k=4", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f1.f2", "k=4", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f2", "k=4", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2.f1", "k=4", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2.f2", "k=4", NO_TIMESTAMP, NO_TTL);

        // Only the first field is set, both nested field are set
        execute("INSERT INTO %s (k, t) VALUES (5, {f1:{f1:1,f2:2}, f2:null}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t", "k=5", timestamps(TIMESTAMP_1, NO_TIMESTAMP), ttls(TTL_1, NO_TTL));
        assertWritetimeAndTTL("t.f1", "k=5", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f1.f1", "k=5", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f1.f2", "k=5", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f2", "k=5", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2.f1", "k=5", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2.f2", "k=5", NO_TIMESTAMP, NO_TTL);

        // Only the second field is set, no nested field is set
        execute("INSERT INTO %s (k, t) VALUES (6, {f1:null, f2:{f1:null,f2:null}}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t", "k=6", timestamps(NO_TIMESTAMP, TIMESTAMP_1), ttls(NO_TTL, TTL_1));
        assertWritetimeAndTTL("t.f1", "k=6", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f1.f1", "k=6", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f1.f2", "k=6", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2", "k=6", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f2.f1", "k=6", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2.f2", "k=6", NO_TIMESTAMP, NO_TTL);

        // Only the second field is set, only the first nested field is set
        execute("INSERT INTO %s (k, t) VALUES (7, {f1:null, f2:{f1:1,f2:null}}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t", "k=7", timestamps(NO_TIMESTAMP, TIMESTAMP_1), ttls(NO_TTL, TTL_1));
        assertWritetimeAndTTL("t.f1", "k=7", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f1.f1", "k=7", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f1.f2", "k=7", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2", "k=7", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f2.f1", "k=7", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f2.f2", "k=7", NO_TIMESTAMP, NO_TTL);

        // Only the second field is set, only the second nested field is set
        execute("INSERT INTO %s (k, t) VALUES (8, {f1:null, f2:{f1:null,f2:2}}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t", "k=8", timestamps(NO_TIMESTAMP, TIMESTAMP_1), ttls(NO_TTL, TTL_1));
        assertWritetimeAndTTL("t.f1", "k=8", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f1.f1", "k=8", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f1.f2", "k=8", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2", "k=8", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f2.f1", "k=8", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2.f2", "k=8", TIMESTAMP_1, TTL_1);

        // Only the second field is set, both nested field are set
        execute("INSERT INTO %s (k, t) VALUES (9, {f1:null, f2:{f1:1,f2:2}}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t", "k=9", timestamps(NO_TIMESTAMP, TIMESTAMP_1), ttls(NO_TTL, TTL_1));
        assertWritetimeAndTTL("t.f1", "k=9", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f1.f1", "k=9", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f1.f2", "k=9", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2", "k=9", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f2.f1", "k=9", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f2.f2", "k=9", TIMESTAMP_1, TTL_1);

        // Both fields are set, alternate fields are set
        execute("INSERT INTO %s (k, t) VALUES (10, {f1:{f1:1}}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        execute("UPDATE %s USING TIMESTAMP ? AND TTL ? SET t.f2={f2:2} WHERE k=10", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("t", "k=10", timestamps(TIMESTAMP_1, TIMESTAMP_2), ttls(TTL_1, TTL_2));
        assertWritetimeAndTTL("t.f1", "k=10", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f1.f1", "k=10", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f1.f2", "k=10", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2", "k=10", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("t.f2.f1", "k=10", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2.f2", "k=10", TIMESTAMP_2, TTL_2);

        // Both fields are set, alternate fields are set
        execute("INSERT INTO %s (k, t) VALUES (11, {f1:{f2:2}}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        execute("UPDATE %s USING TIMESTAMP ? AND TTL ? SET t.f2={f1:2} WHERE k=11", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("t", "k=11", timestamps(TIMESTAMP_1, TIMESTAMP_2), ttls(TTL_1, TTL_2));
        assertWritetimeAndTTL("t.f1", "k=11", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f1.f1", "k=11", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f1.f2", "k=11", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f2", "k=11", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("t.f2.f1", "k=11", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("t.f2.f2", "k=11", NO_TIMESTAMP, NO_TTL);

        // Both fields are set, all fields are set
        execute("INSERT INTO %s (k, t) VALUES (12, {f1:{f1:1,f2:2}}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        execute("UPDATE %s USING TIMESTAMP ? AND TTL ? SET t.f2={f1:1,f2:2} WHERE k=12", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("t", "k=12", timestamps(TIMESTAMP_1, TIMESTAMP_2), ttls(TTL_1, TTL_2));
        assertWritetimeAndTTL("t.f1", "k=12", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f1.f1", "k=12", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f1.f2", "k=12", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f2", "k=12", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("t.f2.f1", "k=12", TIMESTAMP_2, TTL_2);
        assertWritetimeAndTTL("t.f2.f2", "k=12", TIMESTAMP_2, TTL_2);
    }

    @Test
    public void testFrozenNestedUDTs() throws Throwable
    {
        String nestedType = createType("CREATE TYPE %s (f1 int, f2 int)");
        String type = createType(format("CREATE TYPE %%s (f1 frozen<%s>, f2 frozen<%<s>)", nestedType));
        createTable("CREATE TABLE %s (k int PRIMARY KEY, t frozen<" + type + ">)");

        // Both fields are empty
        execute("INSERT INTO %s (k, t) VALUES (1, {f1:null, f2:null}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t", "k=1", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f1", "k=1", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f1.f1", "k=1", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f1.f2", "k=1", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2", "k=1", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2.f1", "k=1", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2.f2", "k=1", NO_TIMESTAMP, NO_TTL);

        // Only the first field is set, no nested field is set
        execute("INSERT INTO %s (k, t) VALUES (2, {f1:{f1:null,f2:null}, f2:null}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t", "k=2", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f1", "k=2", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f1.f1", "k=2", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f1.f2", "k=2", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2", "k=2", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2.f1", "k=2", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2.f2", "k=2", NO_TIMESTAMP, NO_TTL);

        // Only the first field is set, only the first nested field is set
        execute("INSERT INTO %s (k, t) VALUES (3, {f1:{f1:1,f2:null}, f2:null}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t", "k=3", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f1", "k=3", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f1.f1", "k=3", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f1.f2", "k=3", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2", "k=3", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2.f1", "k=3", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2.f2", "k=3", NO_TIMESTAMP, NO_TTL);

        // Only the first field is set, only the second nested field is set
        execute("INSERT INTO %s (k, t) VALUES (4, {f1:{f1:null,f2:2}, f2:null}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t", "k=4", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f1", "k=4", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f1.f1", "k=4", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f1.f2", "k=4", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f2", "k=4", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2.f1", "k=4", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2.f2", "k=4", NO_TIMESTAMP, NO_TTL);

        // Only the first field is set, both nested field are set
        execute("INSERT INTO %s (k, t) VALUES (5, {f1:{f1:1,f2:2}, f2:null}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t", "k=5", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f1", "k=5", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f1.f1", "k=5", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f1.f2", "k=5", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f2", "k=5", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2.f1", "k=5", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2.f2", "k=5", NO_TIMESTAMP, NO_TTL);

        // Only the second field is set, no nested field is set
        execute("INSERT INTO %s (k, t) VALUES (6, {f1:null, f2:{f1:null,f2:null}}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t", "k=6", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f1", "k=6", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f1.f1", "k=6", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f1.f2", "k=6", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2", "k=6", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f2.f1", "k=6", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2.f2", "k=6", NO_TIMESTAMP, NO_TTL);

        // Only the second field is set, only the first nested field is set
        execute("INSERT INTO %s (k, t) VALUES (7, {f1:null, f2:{f1:1,f2:null}}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t", "k=7", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f1", "k=7", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f1.f1", "k=7", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f1.f2", "k=7", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2", "k=7", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f2.f1", "k=7", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f2.f2", "k=7", NO_TIMESTAMP, NO_TTL);

        // Only the second field is set, only the second nested field is set
        execute("INSERT INTO %s (k, t) VALUES (8, {f1:null, f2:{f1:null,f2:2}}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t", "k=8", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f1", "k=8", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f1.f1", "k=8", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f1.f2", "k=8", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2", "k=8", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f2.f1", "k=8", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2.f2", "k=8", TIMESTAMP_1, TTL_1);

        // Only the second field is set, both nested field are set
        execute("INSERT INTO %s (k, t) VALUES (9, {f1:null, f2:{f1:1,f2:2}}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t", "k=9", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f1", "k=9", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f1.f1", "k=9", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f1.f2", "k=9", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2", "k=9", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f2.f1", "k=9", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f2.f2", "k=9", TIMESTAMP_1, TTL_1);

        // Both fields are set, alternate fields are set
        execute("INSERT INTO %s (k, t) VALUES (10, {f1:{f1:1}, f2:{f2:2}}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t", "k=10", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f1", "k=10", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f1.f1", "k=10", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f1.f2", "k=10", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2", "k=10", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f2.f1", "k=10", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f2.f2", "k=10", TIMESTAMP_1, TTL_1);
        // Both fields are set, alternate fields are set
        execute("INSERT INTO %s (k, t) VALUES (11, {f1:{f2:2}, f2:{f1:1}}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t", "k=11", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f1", "k=11", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f1.f1", "k=11", NO_TIMESTAMP, NO_TTL);
        assertWritetimeAndTTL("t.f1.f2", "k=11", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f2", "k=11", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f2.f1", "k=11", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f2.f2", "k=11", NO_TIMESTAMP, NO_TTL);

        // Both fields are set, all fields are set
        execute("INSERT INTO %s (k, t) VALUES (12, {f1:{f1:1,f2:2},f2:{f1:1,f2:2}}) USING TIMESTAMP ? AND TTL ?", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t", "k=12", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f1", "k=12", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f1.f1", "k=12", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f1.f2", "k=12", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f2", "k=12", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f2.f1", "k=12", TIMESTAMP_1, TTL_1);
        assertWritetimeAndTTL("t.f2.f2", "k=12", TIMESTAMP_1, TTL_1);
    }

    @Test
    public void testFunctions() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, s set<int>, fs frozen<set<int>>)");
        execute("INSERT INTO %s (k, v, s, fs) VALUES (0, 0, {1, 2, 3}, {1, 2, 3}) USING TIMESTAMP 1 AND TTL 1000");
        execute("INSERT INTO %s (k, v, s, fs) VALUES (1, 1, {10, 20, 30}, {10, 20, 30}) USING TIMESTAMP 10 AND TTL 1000");
        execute("UPDATE %s USING TIMESTAMP 2 AND TTL 2000 SET s = s + {2, 3} WHERE k = 0");
        execute("UPDATE %s USING TIMESTAMP 20 AND TTL 2000 SET s = s + {20, 30} WHERE k = 1");

        // Regular column
        assertRows("SELECT min(v) FROM %s", row(0));
        assertRows("SELECT max(v) FROM %s", row(1));
        assertRows("SELECT writetime(v) FROM %s", row(10L), row(1L));
        assertRows("SELECT min(writetime(v)) FROM %s", row(1L));
        assertRows("SELECT max(writetime(v)) FROM %s", row(10L));
        assertRows("SELECT min(maxwritetime(v)) FROM %s", row(1L));
        assertRows("SELECT max(maxwritetime(v)) FROM %s", row(10L));

        // Frozen collection
        assertRows("SELECT min(fs) FROM %s", row(set(1, 2, 3)));
        assertRows("SELECT max(fs) FROM %s", row(set(10, 20, 30)));
        assertRows("SELECT writetime(fs) FROM %s", row(10L), row(1L));
        assertRows("SELECT min(writetime(fs)) FROM %s", row(1L));
        assertRows("SELECT max(writetime(fs)) FROM %s", row(10L));
        assertRows("SELECT min(maxwritetime(fs)) FROM %s", row(1L));
        assertRows("SELECT max(maxwritetime(fs)) FROM %s", row(10L));

        // Multi-cell collection
        assertRows("SELECT min(s) FROM %s", row(set(1, 2, 3)));
        assertRows("SELECT max(s) FROM %s", row(set(10, 20, 30)));
        assertRows("SELECT writetime(s) FROM %s", row(list(10L, 20L, 20L)), row(list(1L, 2L, 2L)));
        assertRows("SELECT min(writetime(s)) FROM %s", row(list(1L, 2L, 2L)));
        assertRows("SELECT max(writetime(s)) FROM %s", row(list(10L, 20L, 20L)));
        assertRows("SELECT min(maxwritetime(s)) FROM %s", row(2L));
        assertRows("SELECT max(maxwritetime(s)) FROM %s", row(20L));
    }

    private static List<Integer> ttls(Integer... a)
    {
        return Arrays.asList(a);
    }

    private static List<Long> timestamps(Long... a)
    {
        return Arrays.asList(a);
    }

    private void assertRows(String query, Object[]... rows) throws Throwable
    {
        assertRows(execute(query), rows);
    }

    private void assertWritetimeAndTTL(String column, Long timestamp, Integer ttl) throws Throwable
    {
        assertWritetimeAndTTL(column, null, timestamp, ttl);
    }

    private void assertWritetimeAndTTL(String column, List<Long> timestamps, List<Integer> ttls) throws Throwable
    {
        assertWritetimeAndTTL(column, null, timestamps, ttls);
    }

    private void assertWritetimeAndTTL(String column, String where, Long timestamp, Integer ttl)
    throws Throwable
    {
        where = where == null ? "" : " WHERE " + where;

        // Verify write time
        assertRows(format("SELECT WRITETIME(%s) FROM %%s %s", column, where), row(timestamp));

        // Verify max write time
        assertRows(format("SELECT MAXWRITETIME(%s) FROM %%s %s", column, where), row(timestamp));

        // Verify write time and max write time together
        assertRows(format("SELECT WRITETIME(%s), MAXWRITETIME(%s) FROM %%s %s", column, column, where),
                   row(timestamp, timestamp));

        // Verify ttl
        UntypedResultSet rs = execute(format("SELECT TTL(%s) FROM %%s %s", column, where));
        assertRowCount(rs, 1);
        UntypedResultSet.Row row = rs.one();
        String ttlColumn = format("ttl(%s)", column);
        if (ttl == null)
        {
            assertFalse(row.has(ttlColumn));
        }
        else
        {
            assertTTL(ttl, row.getInt(ttlColumn));
        }
    }

    private void assertWritetimeAndTTL(String column, String where, List<Long> timestamps, List<Integer> ttls)
    throws Throwable
    {
        where = where == null ? "" : " WHERE " + where;

        // Verify write time
        assertRows(format("SELECT WRITETIME(%s) FROM %%s %s", column, where), row(timestamps));

        // Verify max write time
        Long maxTimestamp = timestamps.stream().filter(Objects::nonNull).max(Long::compare).orElse(null);
        assertRows(format("SELECT MAXWRITETIME(%s) FROM %%s %s", column, where), row(maxTimestamp));

        // Verify write time and max write time together
        assertRows(format("SELECT WRITETIME(%s), MAXWRITETIME(%s) FROM %%s %s", column, column, where),
                   row(timestamps, maxTimestamp));

        // Verify ttl
        UntypedResultSet rs = execute(format("SELECT TTL(%s) FROM %%s %s", column, where));
        assertRowCount(rs, 1);
        UntypedResultSet.Row row = rs.one();
        String ttlColumn = format("ttl(%s)", column);
        if (ttls == null)
        {
            assertFalse(row.has(ttlColumn));
        }
        else
        {
            List<Integer> actualTTLs = row.getList(ttlColumn, Int32Type.instance);
            assertEquals(ttls.size(), actualTTLs.size());

            for (int i = 0; i < actualTTLs.size(); i++)
            {
                Integer expectedTTL = ttls.get(i);
                Integer actualTTL = actualTTLs.get(i);
                assertTTL(expectedTTL, actualTTL);
            }
        }
    }

    /**
     * Since the returned TTL is the remaining seconds since last update, it could be lower than the
     * specified TTL depending on the test execution time, se we allow up to one-minute difference
     */
    private void assertTTL(Integer expected, Integer actual)
    {
        if (expected == null)
        {
            assertNull(actual);
        }
        else
        {
            assertNotNull(actual);
            assertTrue(actual > expected - 60);
            assertTrue(actual <= expected);
        }
    }

    private void assertInvalidPrimaryKeySelection(String column) throws Throwable
    {
        assertInvalidThrowMessage("Cannot use selection function writetime on PRIMARY KEY part " + column,
                                  InvalidRequestException.class,
                                  format("SELECT WRITETIME(%s) FROM %%s", column));
        assertInvalidThrowMessage("Cannot use selection function maxwritetime on PRIMARY KEY part " + column,
                                  InvalidRequestException.class,
                                  format("SELECT MAXWRITETIME(%s) FROM %%s", column));
        assertInvalidThrowMessage("Cannot use selection function ttl on PRIMARY KEY part " + column,
                                  InvalidRequestException.class,
                                  format("SELECT TTL(%s) FROM %%s", column));
    }

    private void assertInvalidListElementSelection(String column, String list) throws Throwable
    {
        String message = format("Element selection is only allowed on sets and maps, but %s is a list", list);
        assertInvalidThrowMessage(message,
                                  InvalidRequestException.class,
                                  format("SELECT WRITETIME(%s) FROM %%s", column));
        assertInvalidThrowMessage(message,
                                  InvalidRequestException.class,
                                  format("SELECT MAXWRITETIME(%s) FROM %%s", column));
        assertInvalidThrowMessage(message,
                                  InvalidRequestException.class,
                                  format("SELECT TTL(%s) FROM %%s", column));
    }

    private void assertInvalidListSliceSelection(String column, String list) throws Throwable
    {
        String message = format("Slice selection is only allowed on sets and maps, but %s is a list", list);
        assertInvalidThrowMessage(message,
                                  InvalidRequestException.class,
                                  format("SELECT WRITETIME(%s) FROM %%s", column));
        assertInvalidThrowMessage(message,
                                  InvalidRequestException.class,
                                  format("SELECT MAXWRITETIME(%s) FROM %%s", column));
        assertInvalidThrowMessage(message,
                                  InvalidRequestException.class,
                                  format("SELECT TTL(%s) FROM %%s", column));
    }
}
