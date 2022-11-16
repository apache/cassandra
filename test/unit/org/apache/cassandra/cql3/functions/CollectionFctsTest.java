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

package org.apache.cassandra.cql3.functions;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

import org.junit.Test;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.marshal.NumberType;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * Tests for the functions defined on {@link CollectionFcts}.
 */
public class CollectionFctsTest extends CQLTester
{
    private static final BigInteger bigint1 = new BigInteger("12345678901234567890");
    private static final BigInteger bigint2 = new BigInteger("23456789012345678901");
    private static final BigDecimal bigdecimal1 = new BigDecimal("1234567890.1234567890");
    private static final BigDecimal bigdecimal2 = new BigDecimal("2345678901.2345678901");

    @Test
    public void testNotNumericCollection() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v uuid, l list<text>, s set<boolean>, fl frozen<list<text>>, fs frozen<set<boolean>>)");

        // sum
        assertInvalidThrowMessage("Function system.collection_sum requires a numeric set/list argument, " +
                                  "but found argument v of type uuid",
                                  InvalidRequestException.class,
                                  "SELECT collection_sum(v) FROM %s");
        assertInvalidThrowMessage("Function system.collection_sum requires a numeric set/list argument, " +
                                  "but found argument l of type list<text>",
                                  InvalidRequestException.class,
                                  "SELECT collection_sum(l) FROM %s");
        assertInvalidThrowMessage("Function system.collection_sum requires a numeric set/list argument, " +
                                  "but found argument s of type set<boolean>",
                                  InvalidRequestException.class,
                                  "SELECT collection_sum(s) FROM %s");
        assertInvalidThrowMessage("Function system.collection_sum requires a numeric set/list argument, " +
                                  "but found argument fl of type frozen<list<text>>",
                                  InvalidRequestException.class,
                                  "SELECT collection_sum(fl) FROM %s");
        assertInvalidThrowMessage("Function system.collection_sum requires a numeric set/list argument, " +
                                  "but found argument fs of type frozen<set<boolean>>",
                                  InvalidRequestException.class,
                                  "SELECT collection_sum(fs) FROM %s");

        // avg
        assertInvalidThrowMessage("Function system.collection_avg requires a numeric set/list argument, " +
                                  "but found argument v of type uuid",
                                  InvalidRequestException.class,
                                  "SELECT collection_avg(v) FROM %s");
        assertInvalidThrowMessage("Function system.collection_avg requires a numeric set/list argument, " +
                                  "but found argument l of type list<text>",
                                  InvalidRequestException.class,
                                  "SELECT collection_avg(l) FROM %s");
        assertInvalidThrowMessage("Function system.collection_avg requires a numeric set/list argument, " +
                                  "but found argument s of type set<boolean>",
                                  InvalidRequestException.class,
                                  "SELECT collection_avg(s) FROM %s");
        assertInvalidThrowMessage("Function system.collection_avg requires a numeric set/list argument, " +
                                  "but found argument fl of type frozen<list<text>>",
                                  InvalidRequestException.class,
                                  "SELECT collection_avg(fl) FROM %s");
        assertInvalidThrowMessage("Function system.collection_avg requires a numeric set/list argument, " +
                                  "but found argument fs of type frozen<set<boolean>>",
                                  InvalidRequestException.class,
                                  "SELECT collection_avg(fs) FROM %s");
    }

    @Test
    public void testTinyInt() throws Throwable
    {
        createTable(CQL3Type.Native.TINYINT);

        // empty collections
        assertRows(execute("SELECT map_keys(m), map_values(m), map_keys(fm), map_values(fm) FROM %s"),
                   row(null, null, set(), list()));
        assertRows(execute("SELECT collection_count(l), collection_count(s), collection_count(m), " +
                           "collection_count(fl), collection_count(fs), collection_count(fm) FROM %s"),
                   row(null, null, null, 0, 0, 0));
        assertRows(execute("SELECT collection_min(l), collection_min(s), collection_min(fl), collection_min(fs) FROM %s"),
                   row(null, null, null, null));
        assertRows(execute("SELECT collection_max(l), collection_max(s), collection_max(fl), collection_max(fs) FROM %s"),
                   row(null, null, null, null));
        assertRows(execute("SELECT collection_sum(l), collection_sum(s), collection_sum(fl), collection_sum(fs) FROM %s"),
                   row(null, null, (byte) 0, (byte) 0));
        assertRows(execute("SELECT collection_avg(l), collection_avg(s), collection_avg(fl), collection_avg(fs) FROM %s"),
                   row(null, null, (byte) 0, (byte) 0));

        // not empty collections
        execute("INSERT INTO %s (k, l, fl, s, fs, m, fm)  VALUES (1, ?, ?, ?, ?, ?, ?)",
                list((byte) 1, (byte) 2), set((byte) 1, (byte) 2),
                list((byte) 1, (byte) 2), set((byte) 1, (byte) 2),
                map((byte) 1, (byte) 2, (byte) 3, (byte) 4),
                map((byte) 1, (byte) 2, (byte) 3, (byte) 4));

        assertRows(execute("SELECT map_keys(m), map_keys(fm) FROM %s"),
                   row(set((byte) 1, (byte) 3), set((byte) 1, (byte) 3)));
        assertRows(execute("SELECT map_values(m), map_values(fm) FROM %s"),
                   row(list((byte) 2, (byte) 4), list((byte) 2, (byte) 4)));
        assertRows(execute("SELECT collection_count(l), collection_count(s), collection_count(m) FROM %s"),
                   row(2, 2, 2));
        assertRows(execute("SELECT collection_count(fl), collection_count(fs), collection_count(fm) FROM %s"),
                   row(2, 2, 2));
        assertRows(execute("SELECT collection_min(l), collection_min(s), collection_min(fl), collection_min(fs) FROM %s"),
                   row((byte) 1, (byte) 1, (byte) 1, (byte) 1));
        assertRows(execute("SELECT collection_max(l), collection_max(s), collection_max(fl), collection_max(fs) FROM %s"),
                   row((byte) 2, (byte) 2, (byte) 2, (byte) 2));
        assertRows(execute("SELECT collection_sum(l), collection_sum(s), collection_sum(fl), collection_sum(fs) FROM %s"),
                   row((byte) 3, (byte) 3, (byte) 3, (byte) 3));
        assertRows(execute("SELECT collection_avg(l), collection_avg(s), collection_avg(fl), collection_avg(fs) FROM %s"),
                   row((byte) 1, (byte) 1, (byte) 1, (byte) 1));
    }

    @Test
    public void testSmallInt() throws Throwable
    {
        createTable(CQL3Type.Native.SMALLINT);

        // empty collections
        assertRows(execute("SELECT map_keys(m), map_values(m), map_keys(fm), map_values(fm) FROM %s"),
                   row(null, null, set(), list()));
        assertRows(execute("SELECT collection_count(l), collection_count(s), collection_count(m), " +
                           "collection_count(fl), collection_count(fs), collection_count(fm) FROM %s"),
                   row(null, null, null, 0, 0, 0));
        assertRows(execute("SELECT collection_min(l), collection_min(s), collection_min(fl), collection_min(fs) FROM %s"),
                   row(null, null, null, null));
        assertRows(execute("SELECT collection_max(l), collection_max(s), collection_max(fl), collection_max(fs) FROM %s"),
                   row(null, null, null, null));
        assertRows(execute("SELECT collection_sum(l), collection_sum(s), collection_sum(fl), collection_sum(fs) FROM %s"),
                   row(null, null, (short) 0, (short) 0));
        assertRows(execute("SELECT collection_avg(l), collection_avg(s), collection_avg(fl), collection_avg(fs) FROM %s"),
                   row(null, null, (short) 0, (short) 0));

        // not empty collections
        execute("INSERT INTO %s (k, l, fl, s, fs, m, fm)  VALUES (1, ?, ?, ?, ?, ?, ?)",
                list((short) 1, (short) 2), set((short) 1, (short) 2),
                list((short) 1, (short) 2), set((short) 1, (short) 2),
                map((short) 1, (short) 2, (short) 3, (short) 4),
                map((short) 1, (short) 2, (short) 3, (short) 4));

        assertRows(execute("SELECT map_keys(m), map_keys(fm) FROM %s"),
                   row(set((short) 1, (short) 3), set((short) 1, (short) 3)));
        assertRows(execute("SELECT map_values(m), map_values(fm) FROM %s"),
                   row(list((short) 2, (short) 4), list((short) 2, (short) 4)));
        assertRows(execute("SELECT collection_count(l), collection_count(s), collection_count(m) FROM %s"),
                   row(2, 2, 2));
        assertRows(execute("SELECT collection_count(fl), collection_count(fs), collection_count(fm) FROM %s"),
                   row(2, 2, 2));
        assertRows(execute("SELECT collection_min(l), collection_min(s), collection_min(fl), collection_min(fs) FROM %s"),
                   row((short) 1, (short) 1, (short) 1, (short) 1));
        assertRows(execute("SELECT collection_max(l), collection_max(s), collection_max(fl), collection_max(fs) FROM %s"),
                   row((short) 2, (short) 2, (short) 2, (short) 2));
        assertRows(execute("SELECT collection_sum(l), collection_sum(s), collection_sum(fl), collection_sum(fs) FROM %s"),
                   row((short) 3, (short) 3, (short) 3, (short) 3));
        assertRows(execute("SELECT collection_avg(l), collection_avg(s), collection_avg(fl), collection_avg(fs) FROM %s"),
                   row((short) 1, (short) 1, (short) 1, (short) 1));
    }

    @Test
    public void testInt() throws Throwable
    {
        createTable(CQL3Type.Native.INT);

        // empty collections
        assertRows(execute("SELECT map_keys(m), map_values(m), map_keys(fm), map_values(fm) FROM %s"),
                   row(null, null, set(), list()));
        assertRows(execute("SELECT collection_count(l), collection_count(s), collection_count(m), " +
                           "collection_count(fl), collection_count(fs), collection_count(fm) FROM %s"),
                   row(null, null, null, 0, 0, 0));
        assertRows(execute("SELECT collection_min(l), collection_min(s), collection_min(fl), collection_min(fs) FROM %s"),
                   row(null, null, null, null));
        assertRows(execute("SELECT collection_max(l), collection_max(s), collection_max(fl), collection_max(fs) FROM %s"),
                   row(null, null, null, null));
        assertRows(execute("SELECT collection_sum(l), collection_sum(s), collection_sum(fl), collection_sum(fs) FROM %s"),
                   row(null, null, 0, 0));
        assertRows(execute("SELECT collection_avg(l), collection_avg(s), collection_avg(fl), collection_avg(fs) FROM %s"),
                   row(null, null, 0, 0));

        // not empty collections
        execute("INSERT INTO %s (k, l, fl, s, fs, m, fm)  VALUES (1, ?, ?, ?, ?, ?, ?)",
                list(1, 2), set(1, 2), list(1, 2), set(1, 2), map(1, 2, 3, 4), map(1, 2, 3, 4));

        assertRows(execute("SELECT map_keys(m), map_keys(fm) FROM %s"),
                   row(set(1, 3), set(1, 3)));
        assertRows(execute("SELECT map_values(m), map_values(fm) FROM %s"),
                   row(list(2, 4), list(2, 4)));
        assertRows(execute("SELECT collection_count(l), collection_count(s), collection_count(m) FROM %s"),
                   row(2, 2, 2));
        assertRows(execute("SELECT collection_count(fl), collection_count(fs), collection_count(fm) FROM %s"),
                   row(2, 2, 2));
        assertRows(execute("SELECT collection_min(l), collection_min(s), collection_min(fl), collection_min(fs) FROM %s"),
                   row(1, 1, 1, 1));
        assertRows(execute("SELECT collection_max(l), collection_max(s), collection_max(fl), collection_max(fs) FROM %s"),
                   row(2, 2, 2, 2));
        assertRows(execute("SELECT collection_sum(l), collection_sum(s), collection_sum(fl), collection_sum(fs) FROM %s"),
                   row(3, 3, 3, 3));
        assertRows(execute("SELECT collection_avg(l), collection_avg(s), collection_avg(fl), collection_avg(fs) FROM %s"),
                   row(1, 1, 1, 1));
    }

    @Test
    public void testBigInt() throws Throwable
    {
        createTable(CQL3Type.Native.BIGINT);

        // empty collections
        assertRows(execute("SELECT map_keys(m), map_values(m), map_keys(fm), map_values(fm) FROM %s"),
                   row(null, null, set(), list()));
        assertRows(execute("SELECT collection_count(l), collection_count(s), collection_count(m), " +
                           "collection_count(fl), collection_count(fs), collection_count(fm) FROM %s"),
                   row(null, null, null, 0, 0, 0));
        assertRows(execute("SELECT collection_min(l), collection_min(s), collection_min(fl), collection_min(fs) FROM %s"),
                   row(null, null, null, null));
        assertRows(execute("SELECT collection_max(l), collection_max(s), collection_max(fl), collection_max(fs) FROM %s"),
                   row(null, null, null, null));
        assertRows(execute("SELECT collection_sum(l), collection_sum(s), collection_sum(fl), collection_sum(fs) FROM %s"),
                   row(null, null, 0L, 0L));
        assertRows(execute("SELECT collection_avg(l), collection_avg(s), collection_avg(fl), collection_avg(fs) FROM %s"),
                   row(null, null, 0L, 0L));

        // not empty collections
        execute("INSERT INTO %s (k, l, fl, s, fs, m, fm)  VALUES (1, ?, ?, ?, ?, ?, ?)",
                list(1L, 2L), set(1L, 2L), list(1L, 2L), set(1L, 2L), map(1L, 2L, 3L, 4L), map(1L, 2L, 3L, 4L));

        assertRows(execute("SELECT map_keys(m), map_keys(fm) FROM %s"),
                   row(set(1L, 3L), set(1L, 3L)));
        assertRows(execute("SELECT map_values(m), map_values(fm) FROM %s"),
                   row(list(2L, 4L), list(2L, 4L)));
        assertRows(execute("SELECT collection_count(l), collection_count(s), collection_count(m) FROM %s"),
                   row(2, 2, 2));
        assertRows(execute("SELECT collection_count(fl), collection_count(fs), collection_count(fm) FROM %s"),
                   row(2, 2, 2));
        assertRows(execute("SELECT collection_min(l), collection_min(s), collection_min(fl), collection_min(fs) FROM %s"),
                   row(1L, 1L, 1L, 1L));
        assertRows(execute("SELECT collection_max(l), collection_max(s), collection_max(fl), collection_max(fs) FROM %s"),
                   row(2L, 2L, 2L, 2L));
        assertRows(execute("SELECT collection_sum(l), collection_sum(s), collection_sum(fl), collection_sum(fs) FROM %s"),
                   row(3L, 3L, 3L, 3L));
        assertRows(execute("SELECT collection_avg(l), collection_avg(s), collection_avg(fl), collection_avg(fs) FROM %s"),
                   row(1L, 1L, 1L, 1L));
    }

    @Test
    public void testFloat() throws Throwable
    {
        createTable(CQL3Type.Native.FLOAT);

        // empty collections
        assertRows(execute("SELECT map_keys(m), map_values(m), map_keys(fm), map_values(fm) FROM %s"),
                   row(null, null, set(), list()));
        assertRows(execute("SELECT collection_count(l), collection_count(s), collection_count(m), " +
                           "collection_count(fl), collection_count(fs), collection_count(fm) FROM %s"),
                   row(null, null, null, 0, 0, 0));
        assertRows(execute("SELECT collection_min(l), collection_min(s), collection_min(fl), collection_min(fs) FROM %s"),
                   row(null, null, null, null));
        assertRows(execute("SELECT collection_max(l), collection_max(s), collection_max(fl), collection_max(fs) FROM %s"),
                   row(null, null, null, null));
        assertRows(execute("SELECT collection_sum(l), collection_sum(s), collection_sum(fl), collection_sum(fs) FROM %s"),
                   row(null, null, 0f, 0f));
        assertRows(execute("SELECT collection_avg(l), collection_avg(s), collection_avg(fl), collection_avg(fs) FROM %s"),
                   row(null, null, 0f, 0f));

        // not empty collections
        execute("INSERT INTO %s (k, l, fl, s, fs, m, fm)  VALUES (1, ?, ?, ?, ?, ?, ?)",
                list(1.23f, 2.34f), list(1.23f, 2.34f),
                set(1.23f, 2.34f), set(1.23f, 2.34f),
                map(1.23f, 2.34f, 3.45f, 4.56f), map(1.23f, 2.34f, 3.45f, 4.56f));

        assertRows(execute("SELECT map_keys(m), map_keys(fm) FROM %s"),
                   row(set(1.23f, 3.45f), set(1.23f, 3.45f)));
        assertRows(execute("SELECT map_values(m), map_values(fm) FROM %s"),
                   row(list(2.34f, 4.56f), list(2.34f, 4.56f)));
        assertRows(execute("SELECT collection_count(l), collection_count(s), collection_count(m) FROM %s"),
                   row(2, 2, 2));
        assertRows(execute("SELECT collection_count(fl), collection_count(fs), collection_count(fm) FROM %s"),
                   row(2, 2, 2));
        assertRows(execute("SELECT collection_min(l), collection_min(s), collection_min(fl), collection_min(fs) FROM %s"),
                   row(1.23f, 1.23f, 1.23f, 1.23f));
        assertRows(execute("SELECT collection_max(l), collection_max(s), collection_max(fl), collection_max(fs) FROM %s"),
                   row(2.34f, 2.34f, 2.34f, 2.34f));

        float sum = 1.23f + 2.34f;
        assertRows(execute("SELECT collection_sum(l), collection_sum(s), collection_sum(fl), collection_sum(fs) FROM %s"),
                   row(sum, sum, sum, sum));

        float avg = (1.23f + 2.34f) / 2;
        assertRows(execute("SELECT collection_avg(l), collection_avg(s), collection_avg(fl), collection_avg(fs) FROM %s"),
                   row(avg, avg, avg, avg));
    }

    @Test
    public void testDouble() throws Throwable
    {
        createTable(CQL3Type.Native.DOUBLE);

        // empty collections
        assertRows(execute("SELECT map_keys(m), map_values(m), map_keys(fm), map_values(fm) FROM %s"),
                   row(null, null, set(), list()));
        assertRows(execute("SELECT collection_count(l), collection_count(s), collection_count(m), " +
                           "collection_count(fl), collection_count(fs), collection_count(fm) FROM %s"),
                   row(null, null, null, 0, 0, 0));
        assertRows(execute("SELECT collection_min(l), collection_min(s), collection_min(fl), collection_min(fs) FROM %s"),
                   row(null, null, null, null));
        assertRows(execute("SELECT collection_max(l), collection_max(s), collection_max(fl), collection_max(fs) FROM %s"),
                   row(null, null, null, null));
        assertRows(execute("SELECT collection_sum(l), collection_sum(s), collection_sum(fl), collection_sum(fs) FROM %s"),
                   row(null, null, 0d, 0d));
        assertRows(execute("SELECT collection_avg(l), collection_avg(s), collection_avg(fl), collection_avg(fs) FROM %s"),
                   row(null, null, 0d, 0d));

        // not empty collections
        execute("INSERT INTO %s (k, l, fl, s, fs, m, fm)  VALUES (1, ?, ?, ?, ?, ?, ?)",
                list(1.23d, 2.34d), list(1.23d, 2.34d),
                set(1.23d, 2.34d), set(1.23d, 2.34d),
                map(1.23d, 2.34d, 3.45d, 4.56d), map(1.23d, 2.34d, 3.45d, 4.56d));

        assertRows(execute("SELECT map_keys(m), map_keys(fm) FROM %s"),
                   row(set(1.23d, 3.45d), set(1.23d, 3.45d)));
        assertRows(execute("SELECT map_values(m), map_values(fm) FROM %s"),
                   row(list(2.34d, 4.56d), list(2.34d, 4.56d)));
        assertRows(execute("SELECT collection_count(l), collection_count(s), collection_count(m) FROM %s"),
                   row(2, 2, 2));
        assertRows(execute("SELECT collection_count(fl), collection_count(fs), collection_count(fm) FROM %s"),
                   row(2, 2, 2));
        assertRows(execute("SELECT collection_min(l), collection_min(s), collection_min(fl), collection_min(fs) FROM %s"),
                   row(1.23d, 1.23d, 1.23d, 1.23d));
        assertRows(execute("SELECT collection_max(l), collection_max(s), collection_max(fl), collection_max(fs) FROM %s"),
                   row(2.34d, 2.34d, 2.34d, 2.34d));
        assertRows(execute("SELECT collection_sum(l), collection_sum(s), collection_sum(fl), collection_sum(fs) FROM %s"),
                   row(3.57d, 3.57d, 3.57d, 3.57d));
        assertRows(execute("SELECT collection_avg(l), collection_avg(s), collection_avg(fl), collection_avg(fs) FROM %s"),
                   row(1.785d, 1.785d, 1.785d, 1.785d));
    }

    @Test
    public void testVarInt() throws Throwable
    {
        createTable(CQL3Type.Native.VARINT);

        // empty collections
        assertRows(execute("SELECT map_keys(m), map_values(m), map_keys(fm), map_values(fm) FROM %s"),
                   row(null, null, set(), list()));
        assertRows(execute("SELECT collection_count(l), collection_count(s), collection_count(m), " +
                           "collection_count(fl), collection_count(fs), collection_count(fm) FROM %s"),
                   row(null, null, null, 0, 0, 0));
        assertRows(execute("SELECT collection_min(l), collection_min(s), collection_min(fl), collection_min(fs) FROM %s"),
                   row(null, null, null, null));
        assertRows(execute("SELECT collection_max(l), collection_max(s), collection_max(fl), collection_max(fs) FROM %s"),
                   row(null, null, null, null));
        assertRows(execute("SELECT collection_sum(l), collection_sum(s), collection_sum(fl), collection_sum(fs) FROM %s"),
                   row(null, null, BigInteger.ZERO, BigInteger.ZERO));
        assertRows(execute("SELECT collection_avg(l), collection_avg(s), collection_avg(fl), collection_avg(fs) FROM %s"),
                   row(null, null, BigInteger.ZERO, BigInteger.ZERO));

        // not empty collections
        execute("INSERT INTO %s (k, l, fl, s, fs, m, fm)  VALUES (1, ?, ?, ?, ?, ?, ?)",
                list(bigint1, bigint2), list(bigint1, bigint2),
                set(bigint1, bigint2), set(bigint1, bigint2),
                map(bigint1, bigint2, bigint2, bigint1), map(bigint1, bigint2, bigint2, bigint1));

        assertRows(execute("SELECT map_keys(m), map_keys(fm) FROM %s"),
                   row(set(bigint1, bigint2), set(bigint1, bigint2)));
        assertRows(execute("SELECT map_values(m), map_values(fm) FROM %s"),
                   row(list(bigint2, bigint1), list(bigint2, bigint1)));
        assertRows(execute("SELECT collection_count(l), collection_count(s), collection_count(m) FROM %s"),
                   row(2, 2, 2));
        assertRows(execute("SELECT collection_count(fl), collection_count(fs), collection_count(fm) FROM %s"),
                   row(2, 2, 2));
        assertRows(execute("SELECT collection_min(l), collection_min(s), collection_min(fl), collection_min(fs) FROM %s"),
                   row(bigint1, bigint1, bigint1, bigint1));
        assertRows(execute("SELECT collection_max(l), collection_max(s), collection_max(fl), collection_max(fs) FROM %s"),
                   row(bigint2, bigint2, bigint2, bigint2));

        BigInteger sum = bigint1.add(bigint2);
        assertRows(execute("SELECT collection_sum(l), collection_sum(s), collection_sum(fl), collection_sum(fs) FROM %s"),
                   row(sum, sum, sum, sum));

        BigInteger avg = bigint1.add(bigint2).divide(BigInteger.valueOf(2));
        assertRows(execute("SELECT collection_avg(l), collection_avg(s), collection_avg(fl), collection_avg(fs) FROM %s"),
                   row(avg, avg, avg, avg));
    }

    @Test
    public void testDecimal() throws Throwable
    {
        createTable(CQL3Type.Native.DECIMAL);

        // empty collections
        assertRows(execute("SELECT map_keys(m), map_values(m), map_keys(fm), map_values(fm) FROM %s"),
                   row(null, null, set(), list()));
        assertRows(execute("SELECT collection_count(l), collection_count(s), collection_count(m), " +
                           "collection_count(fl), collection_count(fs), collection_count(fm) FROM %s"),
                   row(null, null, null, 0, 0, 0));
        assertRows(execute("SELECT collection_min(l), collection_min(s), collection_min(fl), collection_min(fs) FROM %s"),
                   row(null, null, null, null));
        assertRows(execute("SELECT collection_max(l), collection_max(s), collection_max(fl), collection_max(fs) FROM %s"),
                   row(null, null, null, null));
        assertRows(execute("SELECT collection_sum(l), collection_sum(s), collection_sum(fl), collection_sum(fs) FROM %s"),
                   row(null, null, BigDecimal.ZERO, BigDecimal.ZERO));
        assertRows(execute("SELECT collection_avg(l), collection_avg(s), collection_avg(fl), collection_avg(fs) FROM %s"),
                   row(null, null, BigDecimal.ZERO, BigDecimal.ZERO));

        // not empty collections
        execute("INSERT INTO %s (k, l, fl, s, fs, m, fm)  VALUES (1, ?, ?, ?, ?, ?, ?)",
                list(bigdecimal1, bigdecimal2), list(bigdecimal1, bigdecimal2),
                set(bigdecimal1, bigdecimal2), set(bigdecimal1, bigdecimal2),
                map(bigdecimal1, bigdecimal2, bigdecimal2, bigdecimal1),
                map(bigdecimal1, bigdecimal2, bigdecimal2, bigdecimal1));

        assertRows(execute("SELECT map_keys(m), map_keys(fm) FROM %s"),
                   row(set(bigdecimal1, bigdecimal2), set(bigdecimal1, bigdecimal2)));
        assertRows(execute("SELECT map_values(m), map_values(fm) FROM %s"),
                   row(list(bigdecimal2, bigdecimal1), list(bigdecimal2, bigdecimal1)));
        assertRows(execute("SELECT collection_count(l), collection_count(s), collection_count(m) FROM %s"),
                   row(2, 2, 2));
        assertRows(execute("SELECT collection_count(fl), collection_count(fs), collection_count(fm) FROM %s"),
                   row(2, 2, 2));
        assertRows(execute("SELECT collection_min(l), collection_min(s), collection_min(fl), collection_min(fs) FROM %s"),
                   row(bigdecimal1, bigdecimal1, bigdecimal1, bigdecimal1));
        assertRows(execute("SELECT collection_max(l), collection_max(s), collection_max(fl), collection_max(fs) FROM %s"),
                   row(bigdecimal2, bigdecimal2, bigdecimal2, bigdecimal2));

        BigDecimal sum = bigdecimal1.add(bigdecimal2);
        assertRows(execute("SELECT collection_sum(l), collection_sum(s), collection_sum(fl), collection_sum(fs) FROM %s"),
                   row(sum, sum, sum, sum));

        BigDecimal avg = bigdecimal1.add(bigdecimal2).divide(BigDecimal.valueOf(2), RoundingMode.HALF_EVEN);
        assertRows(execute("SELECT collection_avg(l), collection_avg(s), collection_avg(fl), collection_avg(fs) FROM %s"),
                   row(avg, avg, avg, avg));
    }

    @Test
    public void testAscii() throws Throwable
    {
        createTable(CQL3Type.Native.ASCII);

        // empty collections
        assertRows(execute("SELECT map_keys(m), map_values(m), map_keys(fm), map_values(fm) FROM %s"),
                   row(null, null, set(), list()));
        assertRows(execute("SELECT collection_count(l), collection_count(s), collection_count(m), " +
                           "collection_count(fl), collection_count(fs), collection_count(fm) FROM %s"),
                   row(null, null, null, 0, 0, 0));

        // not empty collections
        execute("INSERT INTO %s (k, l, fl, s, fs, m, fm)  VALUES (1, ?, ?, ?, ?, ?, ?)",
                list("abc", "bcd"), set("abc", "bcd"),
                list("abc", "bcd"), set("abc", "bcd"),
                map("abc", "bcd", "cde", "def"), map("abc", "bcd", "cde", "def"));

        assertRows(execute("SELECT map_keys(m), map_keys(fm) FROM %s"),
                   row(set("abc", "cde"), set("abc", "cde")));
        assertRows(execute("SELECT map_values(m), map_values(fm) FROM %s"),
                   row(list("bcd", "def"), list("bcd", "def")));
        assertRows(execute("SELECT collection_count(l), collection_count(s), collection_count(m) FROM %s"),
                   row(2, 2, 2));
        assertRows(execute("SELECT collection_count(fl), collection_count(fs), collection_count(fm) FROM %s"),
                   row(2, 2, 2));
        assertRows(execute("SELECT collection_min(l), collection_min(s), collection_min(fl), collection_min(fs) FROM %s"),
                   row("abc", "abc", "abc", "abc"));
        assertRows(execute("SELECT collection_max(l), collection_max(s), collection_max(fl), collection_max(fs) FROM %s"),
                   row("bcd", "bcd", "bcd", "bcd"));
    }

    @Test
    public void testText() throws Throwable
    {
        createTable(CQL3Type.Native.TEXT);

        // empty collections
        assertRows(execute("SELECT map_keys(m), map_values(m), map_keys(fm), map_values(fm) FROM %s"),
                   row(null, null, set(), list()));
        assertRows(execute("SELECT collection_count(l), collection_count(s), collection_count(m), " +
                           "collection_count(fl), collection_count(fs), collection_count(fm) FROM %s"),
                   row(null, null, null, 0, 0, 0));

        // not empty collections
        execute("INSERT INTO %s (k, l, fl, s, fs, m, fm)  VALUES (1, ?, ?, ?, ?, ?, ?)",
                list("ábc", "bcd"), set("ábc", "bcd"),
                list("ábc", "bcd"), set("ábc", "bcd"),
                map("ábc", "bcd", "cdé", "déf"), map("ábc", "bcd", "cdé", "déf"));

        assertRows(execute("SELECT map_keys(m), map_keys(fm) FROM %s"),
                   row(set("ábc", "cdé"), set("ábc", "cdé")));
        assertRows(execute("SELECT map_values(m), map_values(fm) FROM %s"),
                   row(list("déf", "bcd"), list("déf", "bcd")));
        assertRows(execute("SELECT collection_count(l), collection_count(s), collection_count(m) FROM %s"),
                   row(2, 2, 2));
        assertRows(execute("SELECT collection_count(fl), collection_count(fs), collection_count(fm) FROM %s"),
                   row(2, 2, 2));
        assertRows(execute("SELECT collection_min(l), collection_min(s), collection_min(fl), collection_min(fs) FROM %s"),
                   row("bcd", "bcd", "bcd", "bcd"));
        assertRows(execute("SELECT collection_max(l), collection_max(s), collection_max(fl), collection_max(fs) FROM %s"),
                   row("ábc", "ábc", "ábc", "ábc"));
    }

    @Test
    public void testBoolean() throws Throwable
    {
        createTable(CQL3Type.Native.BOOLEAN);

        // empty collections
        assertRows(execute("SELECT map_keys(m), map_values(m), map_keys(fm), map_values(fm) FROM %s"),
                   row(null, null, set(), list()));
        assertRows(execute("SELECT collection_count(l), collection_count(s), collection_count(m), " +
                           "collection_count(fl), collection_count(fs), collection_count(fm) FROM %s"),
                   row(null, null, null, 0, 0, 0));

        // not empty collections
        execute("INSERT INTO %s (k, l, fl, s, fs, m, fm)  VALUES (1, ?, ?, ?, ?, ?, ?)",
                list(true, false), set(true, false),
                list(true, false), set(true, false),
                map(true, false, false, true), map(true, false, false, true));

        assertRows(execute("SELECT map_keys(m), map_keys(fm) FROM %s"),
                   row(set(true, false), set(true, false)));
        assertRows(execute("SELECT map_values(m), map_values(fm) FROM %s"),
                   row(list(true, false), list(true, false)));
        assertRows(execute("SELECT collection_count(l), collection_count(s), collection_count(m) FROM %s"),
                   row(2, 2, 2));
        assertRows(execute("SELECT collection_count(fl), collection_count(fs), collection_count(fm) FROM %s"),
                   row(2, 2, 2));
        assertRows(execute("SELECT collection_min(l), collection_min(s), collection_min(fl), collection_min(fs) FROM %s"),
                   row(false, false, false, false));
        assertRows(execute("SELECT collection_max(l), collection_max(s), collection_max(fl), collection_max(fs) FROM %s"),
                   row(true, true, true, true));
    }

    private void createTable(CQL3Type.Native type) throws Throwable
    {
        createTable(String.format("CREATE TABLE %%s (" +
                                  " k int PRIMARY KEY, " +
                                  " l list<%s>, " +
                                  " s set<%<s>, " +
                                  " m map<%<s, %<s>, " +
                                  " fl frozen<list<%<s>>, " +
                                  " fs frozen<set<%<s>>, " +
                                  " fm frozen<map<%<s, %<s>>)", type));

        // test functions with an empty table
        assertEmpty(execute("SELECT map_keys(m), map_keys(fm), map_values(m), map_values(fm) FROM %s"));
        assertEmpty(execute("SELECT collection_count(l), collection_count(s), collection_count(m) FROM %s"));
        assertEmpty(execute("SELECT collection_count(fl), collection_count(fs), collection_count(fm) FROM %s"));
        assertEmpty(execute("SELECT collection_min(l), collection_min(s), collection_min(fl), collection_min(fs) FROM %s"));
        assertEmpty(execute("SELECT collection_max(l), collection_max(s), collection_max(fl), collection_max(fs) FROM %s"));

        String errorMsg = "requires a numeric set/list argument";
        if (type.getType() instanceof NumberType)
        {
            assertEmpty(execute("SELECT collection_sum(l), collection_sum(s), collection_sum(fl), collection_sum(fs) FROM %s"));
            assertEmpty(execute("SELECT collection_avg(l), collection_avg(s), collection_avg(fl), collection_avg(fs) FROM %s"));
        }
        else
        {
            assertInvalidThrowMessage(errorMsg, InvalidRequestException.class, "SELECT collection_sum(l) FROM %s");
            assertInvalidThrowMessage(errorMsg, InvalidRequestException.class, "SELECT collection_avg(l) FROM %s");
            assertInvalidThrowMessage(errorMsg, InvalidRequestException.class, "SELECT collection_sum(s) FROM %s");
            assertInvalidThrowMessage(errorMsg, InvalidRequestException.class, "SELECT collection_avg(s) FROM %s");
            assertInvalidThrowMessage(errorMsg, InvalidRequestException.class, "SELECT collection_sum(fl) FROM %s");
            assertInvalidThrowMessage(errorMsg, InvalidRequestException.class, "SELECT collection_avg(fl) FROM %s");
            assertInvalidThrowMessage(errorMsg, InvalidRequestException.class, "SELECT collection_sum(fs) FROM %s");
            assertInvalidThrowMessage(errorMsg, InvalidRequestException.class, "SELECT collection_avg(fs) FROM %s");
        }
        assertInvalidThrowMessage(errorMsg, InvalidRequestException.class, "SELECT collection_sum(m) FROM %s");
        assertInvalidThrowMessage(errorMsg, InvalidRequestException.class, "SELECT collection_avg(m) FROM %s");
        assertInvalidThrowMessage(errorMsg, InvalidRequestException.class, "SELECT collection_sum(fm) FROM %s");
        assertInvalidThrowMessage(errorMsg, InvalidRequestException.class, "SELECT collection_avg(fm) FROM %s");

        // prepare empty collections
        execute("INSERT INTO %s (k, l, fl, s, fs, m, fm) VALUES (1, ?, ?, ?, ?, ?, ?)",
                list(), list(), set(), set(), map(), map());
    }
}
