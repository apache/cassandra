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

package org.apache.cassandra.db.guardrails;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;

import static java.lang.String.format;
import static java.nio.ByteBuffer.allocate;
import static org.apache.cassandra.config.DataStorageSpec.DataStorageUnit.BYTES;

/**
 * Tests the guardrail for the size of column values, {@link Guardrails#columnValueSize}.
 */
public class GuardrailColumnValueSizeTest extends ThresholdTester
{
    private static final int WARN_THRESHOLD = 1024; // bytes
    private static final int FAIL_THRESHOLD = WARN_THRESHOLD * 4; // bytes

    public GuardrailColumnValueSizeTest()
    {
        super(WARN_THRESHOLD + "B",
              FAIL_THRESHOLD + "B",
              Guardrails.columnValueSize,
              Guardrails::setColumnValueSizeThreshold,
              Guardrails::getColumnValueSizeWarnThreshold,
              Guardrails::getColumnValueSizeFailThreshold,
              bytes -> new DataStorageSpec.LongBytesBound(bytes, BYTES).toString(),
              size -> new DataStorageSpec.LongBytesBound(size).toBytes());
    }

    @Test
    public void testSimplePartitionKey() throws Throwable
    {
        createTable("CREATE TABLE %s (k text PRIMARY KEY, v int)");

        // the size of primary key columns is not guarded because they already have a fixed limit of 65535B

        testNoThreshold("INSERT INTO %s (k, v) VALUES (?, 0)");
        testNoThreshold("UPDATE %s SET v = 1 WHERE k = ?");
        testNoThreshold("DELETE v FROM %s WHERE k = ?");
        testNoThreshold("DELETE FROM %s WHERE k = ?");
    }

    @Test
    public void testCompositePartitionKey() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 text, k2 text, v int, PRIMARY KEY((k1, k2)))");

        // the size of primary key columns is not guarded because they already have a fixed limit of 65535B

        testNoThreshold2("INSERT INTO %s (k1, k2, v) VALUES (?, ?, 0)");
        testNoThreshold2("UPDATE %s SET v = 1 WHERE k1 = ? AND k2 = ?");
        testNoThreshold2("DELETE v FROM %s WHERE k1 = ? AND k2 = ?");
        testNoThreshold2("DELETE FROM %s WHERE k1 = ? AND k2 = ?");
    }

    @Test
    public void testSimpleClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c text, v int, PRIMARY KEY(k, c))");

        // the size of primary key columns is not guarded because they already have a fixed limit of 65535B

        testNoThreshold("INSERT INTO %s (k, c, v) VALUES (0, ?, 0)");
        testNoThreshold("UPDATE %s SET v = 1 WHERE k = 0 AND c = ?");
        testNoThreshold("DELETE v FROM %s WHERE k = 0 AND c = ?");
        testNoThreshold("DELETE FROM %s WHERE k = 0 AND c = ?");
    }

    @Test
    public void testCompositeClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c1 text, c2 text, v int, PRIMARY KEY(k, c1, c2))");

        // the size of primary key columns is not guarded because they already have a fixed limit of 65535B

        testNoThreshold("DELETE FROM %s WHERE k = 0 AND c1 = ?");
        testNoThreshold("DELETE FROM %s WHERE k = 0 AND c1 > ?");
        testNoThreshold("DELETE FROM %s WHERE k = 0 AND c1 < ?");
        testNoThreshold("DELETE FROM %s WHERE k = 0 AND c1 >= ?");
        testNoThreshold("DELETE FROM %s WHERE k = 0 AND c1 <= ?");

        testNoThreshold2("INSERT INTO %s (k, c1, c2, v) VALUES (0, ?, ?, 0)");
        testNoThreshold2("UPDATE %s SET v = 1 WHERE k = 0 AND c1 = ? AND c2 = ?");
        testNoThreshold2("DELETE v FROM %s WHERE k = 0 AND c1 = ? AND c2 = ?");
        testNoThreshold2("DELETE FROM %s WHERE k = 0 AND c1 = ? AND c2 = ?");
        testNoThreshold2("DELETE FROM %s WHERE k = 0 AND c1 = ? AND c2 > ?");
        testNoThreshold2("DELETE FROM %s WHERE k = 0 AND c1 = ? AND c2 < ?");
        testNoThreshold2("DELETE FROM %s WHERE k = 0 AND c1 = ? AND c2 >= ?");
        testNoThreshold2("DELETE FROM %s WHERE k = 0 AND c1 = ? AND c2 <= ?");
    }

    @Test
    public void testRegularColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");

        testThreshold("v", "INSERT INTO %s (k, v) VALUES (0, ?)");
        testThreshold("v", "UPDATE %s SET v = ? WHERE k = 0");
    }

    @Test
    public void testStaticColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, s text STATIC, r int, PRIMARY KEY(k, c))");

        testThreshold("s", "INSERT INTO %s (k, s) VALUES (0, ?)");
        testThreshold("s", "INSERT INTO %s (k, c, s, r) VALUES (0, 0, ?, 0)");
        testThreshold("s", "UPDATE %s SET s = ? WHERE k = 0");
        testThreshold("s", "UPDATE %s SET s = ?, r = 0 WHERE k = 0 AND c = 0");
    }

    @Test
    public void testTuple() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, t tuple<text, text>)");

        testThreshold2("t", "INSERT INTO %s (k, t) VALUES (0, (?, ?))", 8);
        testThreshold2("t", "UPDATE %s SET t = (?, ?) WHERE k = 0", 8);
    }

    @Test
    public void testUDT() throws Throwable
    {
        String udt = createType("CREATE TYPE %s (a text, b text)");
        createTable(format("CREATE TABLE %%s (k int PRIMARY KEY, u %s)", udt));

        testThreshold("u", "INSERT INTO %s (k, u) VALUES (0, {a: ?})");
        testThreshold("u", "INSERT INTO %s (k, u) VALUES (0, {b: ?})");
        testThreshold("u", "UPDATE %s SET u = {a: ?} WHERE k = 0");
        testThreshold("u", "UPDATE %s SET u = {b: ?} WHERE k = 0");
        testThreshold("u", "UPDATE %s SET u.a = ? WHERE k = 0");
        testThreshold("u", "UPDATE %s SET u.b = ? WHERE k = 0");
        testThreshold2("u", "INSERT INTO %s (k, u) VALUES (0, {a: ?, b: ?})");
        testThreshold2("u", "UPDATE %s SET u.a = ?, u.b = ? WHERE k = 0");
        testThreshold2("u", "UPDATE %s SET u = {a: ?, b: ?} WHERE k = 0");
    }

    @Test
    public void testFrozenUDT() throws Throwable
    {
        String udt = createType("CREATE TYPE %s (a text, b text)");
        createTable(format("CREATE TABLE %%s (k int PRIMARY KEY, v frozen<%s>)", udt));

        testThreshold("v", "INSERT INTO %s (k, v) VALUES (0, {a: ?})", 8);
        testThreshold("v", "INSERT INTO %s (k, v) VALUES (0, {b: ?})", 8);
        testThreshold("v", "UPDATE %s SET v = {a: ?} WHERE k = 0", 8);
        testThreshold("v", "UPDATE %s SET v = {b: ?} WHERE k = 0", 8);
        testThreshold2("v", "INSERT INTO %s (k, v) VALUES (0, {a: ?, b: ?})", 8);
        testThreshold2("v", "UPDATE %s SET v = {a: ?, b: ?} WHERE k = 0", 8);
    }

    @Test
    public void testNestedUDT() throws Throwable
    {
        String inner = createType("CREATE TYPE %s (c text, d text)");
        String outer = createType(format("CREATE TYPE %%s (a text, b frozen<%s>)", inner));
        createTable(format("CREATE TABLE %%s (k int PRIMARY KEY, v %s)", outer));

        for (String query : Arrays.asList("INSERT INTO %s (k, v) VALUES (0, {a: ?, b: {c: ?, d: ?}})",
                                          "UPDATE %s SET v = {a: ?, b: {c: ?, d: ?}} WHERE k = 0"))
        {
            assertValid(query, allocate(0), allocate(0), allocate(0));
            assertValid(query, allocate(WARN_THRESHOLD - 8), allocate(0), allocate(0));
            assertValid(query, allocate(0), allocate(WARN_THRESHOLD - 8), allocate(0));
            assertValid(query, allocate(0), allocate(0), allocate(WARN_THRESHOLD - 8));

            assertWarns("v", query, allocate(WARN_THRESHOLD + 1), allocate(0), allocate(0));
            assertWarns("v", query, allocate(0), allocate(WARN_THRESHOLD - 7), allocate(0));
            assertWarns("v", query, allocate(0), allocate(0), allocate(WARN_THRESHOLD - 7));

            assertFails("v", query, allocate(FAIL_THRESHOLD + 1), allocate(0), allocate(0));
            assertFails("v", query, allocate(0), allocate(FAIL_THRESHOLD - 7), allocate(0));
            assertFails("v", query, allocate(0), allocate(0), allocate(FAIL_THRESHOLD - 7));
        }
    }

    @Test
    public void testList() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, l list<text>)");

        for (String query : Arrays.asList("INSERT INTO %s (k, l) VALUES (0, ?)",
                                          "UPDATE %s SET l = ? WHERE k = 0",
                                          "UPDATE %s SET l = l + ? WHERE k = 0"))
        {
            testCollection("l", query, this::list);
        }

        testThreshold("l", "UPDATE %s SET l[0] = ? WHERE k = 0");

        String query = "UPDATE %s SET l = l - ? WHERE k = 0";
        assertValid(query, this::list, allocate(1));
        assertValid(query, this::list, allocate(FAIL_THRESHOLD));
        assertValid(query, this::list, allocate(FAIL_THRESHOLD + 1)); // Doesn't write anything because we couldn't write
    }

    @Test
    public void testFrozenList() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, fl frozen<list<text>>)");

        // the serialized size of a frozen list is the size of its serialized elements, plus a 32-bit integer prefix for
        // the number of elements, and another 32-bit integer for the size of each element

        for (String query : Arrays.asList("INSERT INTO %s (k, fl) VALUES (0, ?)",
                                          "UPDATE %s SET fl = ? WHERE k = 0"))
        {
            testFrozenCollection("fl", query, this::list);
        }
    }

    @Test
    public void testSet() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, s set<text>)");

        for (String query : Arrays.asList("INSERT INTO %s (k, s) VALUES (0, ?)",
                                          "UPDATE %s SET s = ? WHERE k = 0",
                                          "UPDATE %s SET s = s + ? WHERE k = 0",
                                          "UPDATE %s SET s = s - ? WHERE k = 0"))
        {
            testCollection("s", query, this::set);
        }
    }

    @Test
    public void testSetWithClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c1 int, c2 int, s set<text>, PRIMARY KEY(k, c1, c2))");

        for (String query : Arrays.asList("INSERT INTO %s (k, c1, c2, s) VALUES (0, 0, 0, ?)",
                                          "UPDATE %s SET s = ? WHERE k = 0 AND c1 = 0 AND c2 = 0",
                                          "UPDATE %s SET s = s + ? WHERE k = 0 AND c1 = 0 AND c2 = 0",
                                          "UPDATE %s SET s = s - ? WHERE k = 0 AND c1 = 0 AND c2 = 0"))
        {
            testCollection("s", query, this::set);
        }
    }

    @Test
    public void testFrozenSet() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, fs frozen<set<text>>)");

        // the serialized size of a frozen set is the size of its serialized elements, plus a 32-bit integer prefix for
        // the number of elements, and another 32-bit integer for the size of each element

        for (String query : Arrays.asList("INSERT INTO %s (k, fs) VALUES (0, ?)",
                                          "UPDATE %s SET fs = ? WHERE k = 0"))
        {
            testFrozenCollection("fs", query, this::set);
        }
    }

    @Test
    public void testMap() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, m map<text, text>)");

        for (String query : Arrays.asList("INSERT INTO %s (k, m) VALUES (0, ?)",
                                          "UPDATE %s SET m = ? WHERE k = 0",
                                          "UPDATE %s SET m = m + ? WHERE k = 0"))
        {
            testMap("m", query);
        }

        testThreshold2("m", "UPDATE %s SET m[?] = ? WHERE k = 0");
        testCollection("m", "UPDATE %s SET m = m - ? WHERE k = 0", this::set);
    }

    @Test
    public void testMapWithClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c1 int, c2 int, mc map<text, text>, PRIMARY KEY(k, c1, c2))");
        testMap("mc", "INSERT INTO %s (k, c1, c2, mc) VALUES (0, 0, 0, ?)");
    }

    @Test
    public void testFrozenMap() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY,fm frozen<map<text, text>>)");

        // the serialized size of a frozen map is the size of the serialized values plus a 32-bit integer prefix for the
        // number of key-value pairs, and another 32-bit integer for the size of each value

        for (String query : Arrays.asList("INSERT INTO %s (k, fm) VALUES (0, ?)",
                                          "UPDATE %s SET fm = ? WHERE k = 0"))
        {
            assertValid(query, this::map, allocate(1), allocate(1));
            assertValid(query, this::map, allocate(WARN_THRESHOLD - 13), allocate(1));
            assertValid(query, this::map, allocate(1), allocate(WARN_THRESHOLD - 13));

            assertWarns("fm", query, this::map, allocate(WARN_THRESHOLD - 12), allocate(1));
            assertWarns("fm", query, this::map, allocate(1), allocate(WARN_THRESHOLD - 12));

            assertFails("fm", query, this::map, allocate(FAIL_THRESHOLD - 12), allocate(1));
            assertFails("fm", query, this::map, allocate(1), allocate(FAIL_THRESHOLD - 12));
        }
    }

    @Test
    public void testBatch() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, c text, r text, s text STATIC, PRIMARY KEY(k, c))");

        // partition key
        testNoThreshold("BEGIN BATCH INSERT INTO %s (k, c, r) VALUES (?, '0', '0'); APPLY BATCH;");
        testNoThreshold("BEGIN BATCH UPDATE %s SET r = '0' WHERE k = ? AND c = '0'; APPLY BATCH;");
        testNoThreshold("BEGIN BATCH DELETE r FROM %s WHERE k = ? AND c = '0'; APPLY BATCH;");
        testNoThreshold("BEGIN BATCH DELETE FROM %s WHERE k = ?; APPLY BATCH;");

        // static column
        testThreshold("s", "BEGIN BATCH INSERT INTO %s (k, s) VALUES ('0', ?); APPLY BATCH;");
        testThreshold("s", "BEGIN BATCH INSERT INTO %s (k, s, c, r) VALUES ('0', ?, '0', '0'); APPLY BATCH;");
        testThreshold("s", "BEGIN BATCH UPDATE %s SET s = ? WHERE k = '0'; APPLY BATCH;");
        testThreshold("s", "BEGIN BATCH UPDATE %s SET s = ?, r = '0' WHERE k = '0' AND c = '0'; APPLY BATCH;");

        // clustering key
        testNoThreshold("BEGIN BATCH INSERT INTO %s (k, c, r) VALUES ('0', ?, '0'); APPLY BATCH;");
        testNoThreshold("BEGIN BATCH UPDATE %s SET r = '0' WHERE k = '0' AND c = ?; APPLY BATCH;");
        testNoThreshold("BEGIN BATCH DELETE r FROM %s WHERE k = '0' AND c = ?; APPLY BATCH;");
        testNoThreshold("BEGIN BATCH DELETE FROM %s WHERE k = '0' AND c = ?; APPLY BATCH;");

        // regular column
        testThreshold("r", "BEGIN BATCH INSERT INTO %s (k, c, r) VALUES ('0', '0', ?); APPLY BATCH;");
        testThreshold("r", "BEGIN BATCH UPDATE %s SET r = ? WHERE k = '0' AND c = '0'; APPLY BATCH;");
    }

    @Test
    public void testCASWithIfNotExistsCondition() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, c text, v text, s text STATIC, PRIMARY KEY(k, c))");

        // partition key
        testNoThreshold("INSERT INTO %s (k, c, v) VALUES (?, '0', '0') IF NOT EXISTS");

        // clustering key
        testNoThreshold("INSERT INTO %s (k, c, v) VALUES ('0', ?, '0') IF NOT EXISTS");

        // static column
        assertValid("INSERT INTO %s (k, s) VALUES ('1', ?) IF NOT EXISTS", allocate(1));
        assertValid("INSERT INTO %s (k, s) VALUES ('2', ?) IF NOT EXISTS", allocate(WARN_THRESHOLD));
        assertValid("INSERT INTO %s (k, s) VALUES ('2', ?) IF NOT EXISTS", allocate(WARN_THRESHOLD + 1)); // not applied
        assertWarns("s", "INSERT INTO %s (k, s) VALUES ('3', ?) IF NOT EXISTS", allocate(WARN_THRESHOLD + 1));

        // regular column
        assertValid("INSERT INTO %s (k, c, v) VALUES ('4', '0', ?) IF NOT EXISTS", allocate(1));
        assertValid("INSERT INTO %s (k, c, v) VALUES ('5', '0', ?) IF NOT EXISTS", allocate(WARN_THRESHOLD));
        assertValid("INSERT INTO %s (k, c, v) VALUES ('5', '0', ?) IF NOT EXISTS", allocate(WARN_THRESHOLD + 1)); // not applied
        assertWarns("v", "INSERT INTO %s (k, c, v) VALUES ('6', '0', ?) IF NOT EXISTS", allocate(WARN_THRESHOLD + 1));
    }

    @Test
    public void testCASWithIfExistsCondition() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, c text, v text, s text STATIC, PRIMARY KEY(k, c))");

        // partition key, the CAS updates with values beyond the threshold are not applied so they don't come to fail
        testNoThreshold("UPDATE %s SET v = '0' WHERE k = ? AND c = '0' IF EXISTS");

        // clustering key, the CAS updates with values beyond the threshold are not applied so they don't come to fail
        testNoThreshold("UPDATE %s SET v = '0' WHERE k = '0' AND c = ? IF EXISTS");

        // static column, only the applied CAS updates can fire the guardrail
        assertValid("INSERT INTO %s (k, s) VALUES ('0', '0')");
        testThreshold("s", "UPDATE %s SET s = ? WHERE k = '0' IF EXISTS");
        assertValid("DELETE FROM %s WHERE k = '0'");
        testNoThreshold("UPDATE %s SET s = ? WHERE k = '0' IF EXISTS");

        // regular column, only the applied CAS updates can fire the guardrail
        assertValid("INSERT INTO %s (k, c) VALUES ('0', '0')");
        testThreshold("v", "UPDATE %s SET v = ? WHERE k = '0' AND c = '0' IF EXISTS");
        assertValid("DELETE FROM %s WHERE k = '0' AND c = '0'");
        testNoThreshold("UPDATE %s SET v = ? WHERE k = '0' AND c = '0' IF EXISTS");
    }

    @Test
    public void testCASWithColumnsCondition() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");

        // updates are always accepted for values lesser than the threshold, independently of whether they are applied
        assertValid("DELETE FROM %s WHERE k = 0");
        assertValid("UPDATE %s SET v = ? WHERE k = 0 IF v = '0'", allocate(1));
        assertValid("UPDATE %s SET v = '0' WHERE k = 0");
        assertValid("UPDATE %s SET v = ? WHERE k = 0 IF v = '0'", allocate(1));

        // updates are always accepted for values equals to the threshold, independently of whether they are applied
        assertValid("DELETE FROM %s WHERE k = 0");
        assertValid("UPDATE %s SET v = ? WHERE k = 0 IF v = '0'", allocate(WARN_THRESHOLD));
        assertValid("UPDATE %s SET v = '0' WHERE k = 0");
        assertValid("UPDATE %s SET v = ? WHERE k = 0 IF v = '0'", allocate(WARN_THRESHOLD));

        // updates beyond the threshold fail only if the update is applied
        assertValid("DELETE FROM %s WHERE k = 0");
        assertValid("UPDATE %s SET v = ? WHERE k = 0 IF v = '0'", allocate(WARN_THRESHOLD + 1));
        assertValid("UPDATE %s SET v = '0' WHERE k = 0");
        assertWarns("v", "UPDATE %s SET v = ? WHERE k = 0 IF v = '0'", allocate(WARN_THRESHOLD + 1));
    }

    @Test
    public void testSelect() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, c text, r text, s text STATIC, PRIMARY KEY(k, c))");

        // the guardail is only checked for writes; reads are excluded

        testNoThreshold("SELECT * FROM %s WHERE k = ?");
        testNoThreshold("SELECT * FROM %s WHERE k = '0' AND c = ?");
        testNoThreshold("SELECT * FROM %s WHERE c = ? ALLOW FILTERING");
        testNoThreshold("SELECT * FROM %s WHERE s = ? ALLOW FILTERING");
        testNoThreshold("SELECT * FROM %s WHERE r = ? ALLOW FILTERING");
    }

    /**
     * Tests that the max column size guardrail threshold is not applied for the specified 1-placeholder CQL query.
     *
     * @param query a CQL modification statement with exactly one placeholder
     */
    private void testNoThreshold(String query) throws Throwable
    {
        assertValid(query, allocate(1));

        assertValid(query, allocate(WARN_THRESHOLD));
        assertValid(query, allocate(WARN_THRESHOLD + 1));

        assertValid(query, allocate(FAIL_THRESHOLD));
        assertValid(query, allocate(FAIL_THRESHOLD + 1));
    }

    /**
     * Tests that the max column size guardrail threshold is not applied for the specified 2-placeholder CQL query.
     *
     * @param query a CQL modification statement with exactly two placeholders
     */
    private void testNoThreshold2(String query) throws Throwable
    {
        assertValid(query, allocate(1), allocate(1));

        assertValid(query, allocate(WARN_THRESHOLD), allocate(1));
        assertValid(query, allocate(1), allocate(WARN_THRESHOLD));
        assertValid(query, allocate((WARN_THRESHOLD)), allocate((WARN_THRESHOLD)));
        assertValid(query, allocate(WARN_THRESHOLD + 1), allocate(1));
        assertValid(query, allocate(1), allocate(WARN_THRESHOLD + 1));

        assertValid(query, allocate(FAIL_THRESHOLD), allocate(1));
        assertValid(query, allocate(1), allocate(FAIL_THRESHOLD));
        assertValid(query, allocate((FAIL_THRESHOLD)), allocate((FAIL_THRESHOLD)));
        assertValid(query, allocate(FAIL_THRESHOLD + 1), allocate(1));
        assertValid(query, allocate(1), allocate(FAIL_THRESHOLD + 1));
    }

    /**
     * Tests that the max column size guardrail threshold is applied for the specified 1-placeholder CQL query.
     *
     * @param column the name of the column referenced by the query placeholder
     * @param query  a CQL query with exactly one placeholder
     */
    private void testThreshold(String column, String query) throws Throwable
    {
        testThreshold(column, query, 0);
    }

    /**
     * Tests that the max column size guardrail threshold is applied for the specified 1-placeholder CQL query.
     *
     * @param column             the name of the column referenced by the query placeholder
     * @param query              a CQL query with exactly one placeholder
     * @param serializationBytes the extra bytes added to the placeholder value by its wrapping column type serializer
     */
    private void testThreshold(String column, String query, int serializationBytes) throws Throwable
    {
        int warn = WARN_THRESHOLD - serializationBytes;
        int fail = FAIL_THRESHOLD - serializationBytes;

        assertValid(query, allocate(0));
        assertValid(query, allocate(warn));
        assertWarns(column, query, allocate(warn + 1));
        assertFails(column, query, allocate(fail + 1));
    }

    /**
     * Tests that the max column size guardrail threshold is applied for the specified 2-placeholder CQL query.
     *
     * @param column the name of the column referenced by the placeholders
     * @param query  a CQL query with exactly two placeholders
     */
    private void testThreshold2(String column, String query) throws Throwable
    {
        testThreshold2(column, query, 0);
    }

    /**
     * Tests that the max column size guardrail threshold is applied for the specified 2-placeholder query.
     *
     * @param column             the name of the column referenced by the placeholders
     * @param query              a CQL query with exactly two placeholders
     * @param serializationBytes the extra bytes added to the size of the placeholder value by their wrapping serializer
     */
    private void testThreshold2(String column, String query, int serializationBytes) throws Throwable
    {
        int warn = WARN_THRESHOLD - serializationBytes;
        int fail = FAIL_THRESHOLD - serializationBytes;

        assertValid(query, allocate(0), allocate(0));
        assertValid(query, allocate(warn), allocate(0));
        assertValid(query, allocate(0), allocate(warn));
        assertValid(query, allocate(warn / 2), allocate(warn / 2));

        assertWarns(column, query, allocate(warn + 1), allocate(0));
        assertWarns(column, query, allocate(0), allocate(warn + 1));

        assertFails(column, query, allocate(fail + 1), allocate(0));
        assertFails(column, query, allocate(0), allocate(fail + 1));
    }

    private void testCollection(String column, String query, Function<ByteBuffer[], ByteBuffer> collectionBuilder) throws Throwable
    {
        assertValid(query, collectionBuilder, allocate(1));
        assertValid(query, collectionBuilder, allocate(1), allocate(1));
        assertValid(query, collectionBuilder, allocate(WARN_THRESHOLD));
        assertValid(query, collectionBuilder, allocate(WARN_THRESHOLD), allocate(1));
        assertValid(query, collectionBuilder, allocate(1), allocate(WARN_THRESHOLD));
        assertValid(query, collectionBuilder, allocate(WARN_THRESHOLD), allocate(WARN_THRESHOLD));

        assertWarns(column, query, collectionBuilder, allocate(WARN_THRESHOLD + 1));
        assertWarns(column, query, collectionBuilder, allocate(WARN_THRESHOLD + 1), allocate(1));
        assertWarns(column, query, collectionBuilder, allocate(1), allocate(WARN_THRESHOLD + 1));

        assertFails(column, query, collectionBuilder, allocate(FAIL_THRESHOLD + 1));
        assertFails(column, query, collectionBuilder, allocate(FAIL_THRESHOLD + 1), allocate(1));
        assertFails(column, query, collectionBuilder, allocate(1), allocate(FAIL_THRESHOLD + 1));
    }

    private void testFrozenCollection(String column, String query, Function<ByteBuffer[], ByteBuffer> collectionBuilder) throws Throwable
    {
        assertValid(query, collectionBuilder, allocate(1));
        assertValid(query, collectionBuilder, allocate(WARN_THRESHOLD - 8));
        assertValid(query, collectionBuilder, allocate((WARN_THRESHOLD - 12) / 2), allocate((WARN_THRESHOLD - 12) / 2));

        assertWarns(column, query, collectionBuilder, allocate(WARN_THRESHOLD - 7));
        assertWarns(column, query, collectionBuilder, allocate(WARN_THRESHOLD - 12), allocate(1));

        assertFails(column, query, collectionBuilder, allocate(FAIL_THRESHOLD - 7));
        assertFails(column, query, collectionBuilder, allocate(FAIL_THRESHOLD - 12), allocate(1));
    }

    private void testMap(String column, String query) throws Throwable
    {
        assertValid(query, this::map, allocate(1), allocate(1));
        assertValid(query, this::map, allocate(WARN_THRESHOLD), allocate(1));
        assertValid(query, this::map, allocate(1), allocate(WARN_THRESHOLD));
        assertValid(query, this::map, allocate(WARN_THRESHOLD), allocate(WARN_THRESHOLD));

        assertWarns(column, query, this::map, allocate(1), allocate(WARN_THRESHOLD + 1));
        assertWarns(column, query, this::map, allocate(WARN_THRESHOLD + 1), allocate(1));

        assertFails(column, query, this::map, allocate(FAIL_THRESHOLD + 1), allocate(1));
        assertFails(column, query, this::map, allocate(1), allocate(FAIL_THRESHOLD + 1));
        assertFails(column, query, this::map, allocate(FAIL_THRESHOLD + 1), allocate(FAIL_THRESHOLD + 1));
    }

    private void assertValid(String query, ByteBuffer... values) throws Throwable
    {
        assertValid(() -> execute(query, values));
    }

    private void assertValid(String query, Function<ByteBuffer[], ByteBuffer> collectionBuilder, ByteBuffer... values) throws Throwable
    {
        assertValid(() -> execute(query, collectionBuilder.apply(values)));
    }

    private void assertWarns(String column, String query, Function<ByteBuffer[], ByteBuffer> collectionBuilder, ByteBuffer... values) throws Throwable
    {
        assertWarns(column, query, collectionBuilder.apply(values));
    }

    private void assertWarns(String column, String query, ByteBuffer... values) throws Throwable
    {
        String errorMessage = format("Value of column %s has size %s, this exceeds the warning threshold of %s.",
                                     column, WARN_THRESHOLD + 1, WARN_THRESHOLD);
        assertWarns(() -> execute(query, values), errorMessage);
    }

    private void assertFails(String column, String query, Function<ByteBuffer[], ByteBuffer> collectionBuilder, ByteBuffer... values) throws Throwable
    {
        assertFails(column, query, collectionBuilder.apply(values));
    }

    private void assertFails(String column, String query, ByteBuffer... values) throws Throwable
    {
        String errorMessage = format("Value of column %s has size %s, this exceeds the failure threshold of %s.",
                                     column, FAIL_THRESHOLD + 1, FAIL_THRESHOLD);
        assertFails(() -> execute(query, values), errorMessage);
    }

    private void execute(String query, ByteBuffer... values)
    {
        execute(userClientState, query, Arrays.asList(values));
    }

    private ByteBuffer set(ByteBuffer... values)
    {
        return SetType.getInstance(BytesType.instance, true).decompose(ImmutableSet.copyOf(values));
    }

    private ByteBuffer list(ByteBuffer... values)
    {
        return ListType.getInstance(BytesType.instance, true).decompose(ImmutableList.copyOf(values));
    }

    private ByteBuffer map(ByteBuffer... values)
    {
        assert values.length % 2 == 0;

        int size = values.length / 2;
        Map<ByteBuffer, ByteBuffer> m = new LinkedHashMap<>(size);
        for (int i = 0; i < size; i++)
            m.put(values[2 * i], values[(2 * i) + 1]);

        return MapType.getInstance(BytesType.instance, BytesType.instance, true).decompose(m);
    }
}
