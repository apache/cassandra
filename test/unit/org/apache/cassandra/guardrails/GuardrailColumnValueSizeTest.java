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

package org.apache.cassandra.guardrails;


import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.utils.units.SizeUnit;
import org.apache.cassandra.utils.units.Units;

import static java.lang.String.format;
import static java.nio.ByteBuffer.allocate;

/**
 * Tests the guardrail for max column value size.
 */
public class GuardrailColumnValueSizeTest extends GuardrailTester
{
    private static final int THRESHOLD_IN_KB = 1;
    private static final int THRESHOLD_IN_BYTES = THRESHOLD_IN_KB * 1024;

    private long defaultColumnValueSizeThreshold;

    @Before
    public void before()
    {
        defaultColumnValueSizeThreshold = config().column_value_size_failure_threshold_in_kb;
        config().column_value_size_failure_threshold_in_kb = (long) THRESHOLD_IN_KB;
    }

    @After
    public void after()
    {
        config().column_value_size_failure_threshold_in_kb = defaultColumnValueSizeThreshold;
    }

    @Test
    public void testConfigValidation()
    {
        testValidationOfStrictlyPositiveProperty((c, v) -> c.column_value_size_failure_threshold_in_kb = v,
                                                 "column_value_size_failure_threshold_in_kb");
    }

    @Test
    public void testSimplePartitionKey() throws Throwable
    {
        createTable("CREATE TABLE %s (k text PRIMARY KEY, v int)");

        testNoThreshold("INSERT INTO %s (k, v) VALUES (?, 0)");
        testNoThreshold("UPDATE %s SET v = 1 WHERE k = ?");
        testNoThreshold("DELETE v FROM %s WHERE k = ?");
        testNoThreshold("DELETE FROM %s WHERE k = ?");
    }

    @Test
    public void testComplexPartitionKey() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 text, k2 text, v int, PRIMARY KEY((k1, k2)))");

        testNoThreshold2("INSERT INTO %s (k1, k2, v) VALUES (?, ?, 0)");
        testNoThreshold2("UPDATE %s SET v = 1 WHERE k1 = ? AND k2 = ?");
        testNoThreshold2("DELETE v FROM %s WHERE k1 = ? AND k2 = ?");
        testNoThreshold2("DELETE FROM %s WHERE k1 = ? AND k2 = ?");
    }

    @Test
    public void testSimpleClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c text, v int, PRIMARY KEY(k, c))");

        testNoThreshold("INSERT INTO %s (k, c, v) VALUES (0, ?, 0)");
        testNoThreshold("UPDATE %s SET v = 1 WHERE k = 0 AND c = ?");
        testNoThreshold("DELETE v FROM %s WHERE k = 0 AND c = ?");
        testNoThreshold("DELETE FROM %s WHERE k = 0 AND c = ?");
    }

    @Test
    public void testComplexClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c1 text, c2 text, v int, PRIMARY KEY(k, c1, c2))");

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
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v tuple<text, text>)");

        testThreshold2("v", "INSERT INTO %s (k, v) VALUES (0, (?, ?))", 8);
        testThreshold2("v", "UPDATE %s SET v = (?, ?) WHERE k = 0", 8);
    }

    @Test
    public void testUDT() throws Throwable
    {
        String udt = createType("CREATE TYPE %s (a text, b text)");
        createTable(format("CREATE TABLE %%s (k int PRIMARY KEY, v %s)", udt));

        testThreshold("v", "INSERT INTO %s (k, v) VALUES (0, {a: ?})");
        testThreshold("v", "INSERT INTO %s (k, v) VALUES (0, {b: ?})");
        testThreshold("v", "UPDATE %s SET v = {a: ?} WHERE k = 0");
        testThreshold("v", "UPDATE %s SET v = {b: ?} WHERE k = 0");
        testThreshold("v", "UPDATE %s SET v.a = ? WHERE k = 0");
        testThreshold("v", "UPDATE %s SET v.b = ? WHERE k = 0");
        testThreshold2("v", "INSERT INTO %s (k, v) VALUES (0, {a: ?, b: ?})");
        testThreshold2("v", "UPDATE %s SET v.a = ?, v.b = ? WHERE k = 0");
        testThreshold2("v", "UPDATE %s SET v = {a: ?, b: ?} WHERE k = 0");
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
            assertValid(query, allocate(THRESHOLD_IN_BYTES), allocate(0), allocate(0));
            assertValid(query, allocate(0), allocate(THRESHOLD_IN_BYTES - 8), allocate(0));
            assertValid(query, allocate(0), allocate(0), allocate(THRESHOLD_IN_BYTES - 8));
            assertGuardrailFailed("v", query, allocate(THRESHOLD_IN_BYTES + 1), allocate(0), allocate(0));
            assertGuardrailFailed("v", query, allocate(0), allocate(THRESHOLD_IN_BYTES - 7), allocate(0));
            assertGuardrailFailed("v", query, allocate(0), allocate(0), allocate(THRESHOLD_IN_BYTES - 7));
        }
    }

    @Test
    public void testList() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v list<text>)");

        for (String query : Arrays.asList("INSERT INTO %s (k, v) VALUES (0, ?)",
                                          "UPDATE %s SET v = ? WHERE k = 0",
                                          "UPDATE %s SET v = v + ? WHERE k = 0"))
        {
            assertValid(query, list(allocate(1)));
            assertValid(query, list(allocate(THRESHOLD_IN_BYTES)));
            assertValid(query, list(allocate(THRESHOLD_IN_BYTES), allocate(THRESHOLD_IN_BYTES)));
            assertGuardrailFailed("v", query, list(allocate(THRESHOLD_IN_BYTES + 1)));
        }

        testThreshold("v", "UPDATE %s SET v[0] = ? WHERE k = 0");

        String query = "UPDATE %s SET v = v - ? WHERE k = 0";
        assertValid(query, list(allocate(1)));
        assertValid(query, list(allocate(THRESHOLD_IN_BYTES)));
        assertValid(query, list(allocate(THRESHOLD_IN_BYTES + 1))); // Doesn't write anything because we couldn't write
    }

    @Test
    public void testFrozenList() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v frozen<list<text>>)");

        // the serialized size of a frozen list is the size of its serialized elements, plus a 32-bit integer prefix for
        // the number of elements, and another 32-bit integer for the size of each element

        for (String query : Arrays.asList("INSERT INTO %s (k, v) VALUES (0, ?)",
                                          "UPDATE %s SET v = ? WHERE k = 0"))
        {
            assertValid(query, list(allocate(1)));
            assertValid(query, list(allocate(THRESHOLD_IN_BYTES - 8)));
            assertValid(query, list(allocate((THRESHOLD_IN_BYTES - 12) / 2), allocate((THRESHOLD_IN_BYTES - 12) / 2)));
            assertGuardrailFailed("v", query, list(allocate(THRESHOLD_IN_BYTES - 7)));
            assertGuardrailFailed("v", query, list(allocate(THRESHOLD_IN_BYTES - 12), allocate(1)));
        }
    }

    @Test
    public void testSet() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v set<text>)");

        for (String query : Arrays.asList("INSERT INTO %s (k, v) VALUES (0, ?)",
                                          "UPDATE %s SET v = ? WHERE k = 0",
                                          "UPDATE %s SET v = v + ? WHERE k = 0",
                                          "UPDATE %s SET v = v - ? WHERE k = 0"))
        {
            assertValid(query, set(allocate(0)));
            assertValid(query, set(allocate(THRESHOLD_IN_BYTES)));
            assertValid(query, set(allocate(THRESHOLD_IN_BYTES), allocate(THRESHOLD_IN_BYTES)));
            assertGuardrailFailed("v", query, set(allocate(THRESHOLD_IN_BYTES + 1)));
        }
    }

    @Test
    public void testSetWithClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c1 int, c2 int, v set<text>, PRIMARY KEY(k, c1, c2))");

        for (String query : Arrays.asList("INSERT INTO %s (k, c1, c2, v) VALUES (0, 0, 0, ?)",
                                          "UPDATE %s SET v = ? WHERE k = 0 AND c1 = 0 AND c2 = 0",
                                          "UPDATE %s SET v = v + ? WHERE k = 0 AND c1 = 0 AND c2 = 0",
                                          "UPDATE %s SET v = v - ? WHERE k = 0 AND c1 = 0 AND c2 = 0"))
        {
            assertValid(query, set(allocate(0)));
            assertValid(query, set(allocate(THRESHOLD_IN_BYTES)));
            assertValid(query, set(allocate(THRESHOLD_IN_BYTES), allocate(THRESHOLD_IN_BYTES)));
            assertGuardrailFailed("v", query, set(allocate(THRESHOLD_IN_BYTES + 1)));
        }
    }

    @Test
    public void testFrozenSet() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v frozen<set<text>>)");

        // the serialized size of a frozen set is the size of its serialized elements, plus a 32-bit integer prefix for
        // the number of elements, and another 32-bit integer for the size of each element

        for (String query : Arrays.asList("INSERT INTO %s (k, v) VALUES (0, ?)",
                                          "UPDATE %s SET v = ? WHERE k = 0"))
        {
            assertValid(query, set(allocate(1)));
            assertValid(query, set(allocate(THRESHOLD_IN_BYTES - 8)));
            assertValid(query, set(allocate((THRESHOLD_IN_BYTES - 12) / 2), allocate((THRESHOLD_IN_BYTES - 12) / 2)));
            assertGuardrailFailed("v", query, set(allocate(THRESHOLD_IN_BYTES - 7)));
            assertGuardrailFailed("v", query, set(allocate(THRESHOLD_IN_BYTES - 12), allocate(1)));
        }
    }

    @Test
    public void testMap() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v map<text, text>)");

        for (String query : Arrays.asList("INSERT INTO %s (k, v) VALUES (0, ?)",
                                          "UPDATE %s SET v = ? WHERE k = 0",
                                          "UPDATE %s SET v = v + ? WHERE k = 0"))
        {
            assertValid(query, map(allocate(0), allocate(0)));
            assertValid(query, map(allocate(THRESHOLD_IN_BYTES), allocate(0)));
            assertValid(query, map(allocate(0), allocate(THRESHOLD_IN_BYTES)));
            assertValid(query, map(allocate(THRESHOLD_IN_BYTES), allocate(THRESHOLD_IN_BYTES)));
            assertGuardrailFailed("v", query, map(allocate(THRESHOLD_IN_BYTES + 1), allocate(1)));
            assertGuardrailFailed("v", query, map(allocate(1), allocate(THRESHOLD_IN_BYTES + 1)));
            assertGuardrailFailed("v", query, map(allocate(THRESHOLD_IN_BYTES + 1), allocate(THRESHOLD_IN_BYTES + 1)));
        }

        testThreshold2("v", "UPDATE %s SET v[?] = ? WHERE k = 0");

        String query = "UPDATE %s SET v = v - ? WHERE k = 0";
        assertValid(query, set(allocate(0)));
        assertValid(query, set(allocate(THRESHOLD_IN_BYTES)));
        assertGuardrailFailed("v", query, set(allocate(THRESHOLD_IN_BYTES + 1)));
    }

    @Test
    public void testMapWithClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c1 int, c2 int, v map<text, text>, PRIMARY KEY(k, c1, c2))");

        String query = "INSERT INTO %s (k, c1, c2, v) VALUES (0, 0, 0, ?)";
        assertValid(query, map(allocate(1), allocate(1)));
        assertValid(query, map(allocate(THRESHOLD_IN_BYTES), allocate(1)));
        assertValid(query, map(allocate(1), allocate(THRESHOLD_IN_BYTES)));
        assertGuardrailFailed("v", query, map(allocate(THRESHOLD_IN_BYTES + 1), allocate(1)));
        assertGuardrailFailed("v", query, map(allocate(1), allocate(THRESHOLD_IN_BYTES + 1)));
    }

    @Test
    public void testFrozenMap() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v frozen<map<text, text>>)");

        // the serialized size of a frozen map is the size of the serialized values plus a 32-bit integer prefix for the
        // number of key-value pairs, and another 32-bit integer for the size of each value

        for (String query : Arrays.asList("INSERT INTO %s (k, v) VALUES (0, ?)",
                                          "UPDATE %s SET v = ? WHERE k = 0"))
        {
            assertValid(query, map(allocate(1), allocate(1)));
            assertValid(query, map(allocate(THRESHOLD_IN_BYTES - 13), allocate(1)));
            assertValid(query, map(allocate(1), allocate(THRESHOLD_IN_BYTES - 13)));
            assertGuardrailFailed("v", query, map(allocate(THRESHOLD_IN_BYTES - 12), allocate(1)));
            assertGuardrailFailed("v", query, map(allocate(1), allocate(THRESHOLD_IN_BYTES - 12)));
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
        assertValid("INSERT INTO %s (k, s) VALUES ('2', ?) IF NOT EXISTS", allocate(THRESHOLD_IN_BYTES));
        assertValid("INSERT INTO %s (k, s) VALUES ('2', ?) IF NOT EXISTS", allocate(THRESHOLD_IN_BYTES + 1)); // not applied
        assertGuardrailFailed("s", "INSERT INTO %s (k, s) VALUES ('3', ?) IF NOT EXISTS", allocate(THRESHOLD_IN_BYTES + 1));

        // regular column
        assertValid("INSERT INTO %s (k, c, v) VALUES ('4', '0', ?) IF NOT EXISTS", allocate(1));
        assertValid("INSERT INTO %s (k, c, v) VALUES ('5', '0', ?) IF NOT EXISTS", allocate(THRESHOLD_IN_BYTES));
        assertValid("INSERT INTO %s (k, c, v) VALUES ('5', '0', ?) IF NOT EXISTS", allocate(THRESHOLD_IN_BYTES + 1)); // not applied
        assertGuardrailFailed("v", "INSERT INTO %s (k, c, v) VALUES ('6', '0', ?) IF NOT EXISTS", allocate(THRESHOLD_IN_BYTES + 1));
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
        assertValid("UPDATE %s SET v = ? WHERE k = 0 IF v = '0'", allocate(THRESHOLD_IN_BYTES));
        assertValid("UPDATE %s SET v = '0' WHERE k = 0");
        assertValid("UPDATE %s SET v = ? WHERE k = 0 IF v = '0'", allocate(THRESHOLD_IN_BYTES));

        // updates beyond the threshold fail only if the update is applied
        assertValid("DELETE FROM %s WHERE k = 0");
        assertValid("UPDATE %s SET v = ? WHERE k = 0 IF v = '0'", allocate(THRESHOLD_IN_BYTES + 1));
        assertValid("UPDATE %s SET v = '0' WHERE k = 0");
        assertGuardrailFailed("v", "UPDATE %s SET v = ? WHERE k = 0 IF v = '0'", allocate(THRESHOLD_IN_BYTES + 1));
    }

    @Test
    public void testSelect() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, c text, r text, s text STATIC, PRIMARY KEY(k, c))");

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
        assertValid(query, allocate(THRESHOLD_IN_BYTES));
        assertValid(query, allocate(THRESHOLD_IN_BYTES + 1));
    }

    /**
     * Tests that the max column size guardrail threshold is not applied for the specified 2-placeholder CQL query.
     *
     * @param query a CQL modification statement with exactly two placeholders
     */
    private void testNoThreshold2(String query) throws Throwable
    {
        assertValid(query, allocate(1), allocate(1));
        assertValid(query, allocate(THRESHOLD_IN_BYTES), allocate(1));
        assertValid(query, allocate(1), allocate(THRESHOLD_IN_BYTES));
        assertValid(query, allocate((THRESHOLD_IN_BYTES)), allocate((THRESHOLD_IN_BYTES)));
        assertValid(query, allocate(THRESHOLD_IN_BYTES + 1), allocate(1));
        assertValid(query, allocate(1), allocate(THRESHOLD_IN_BYTES + 1));
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
        int threshold = THRESHOLD_IN_BYTES - serializationBytes;
        assertValid(query, allocate(1));
        assertValid(query, allocate(threshold));
        assertGuardrailFailed(column, query, allocate(threshold + 1));
    }

    /**
     * Tests that the max column size guardrail threshold is applied for the specified 2-placeholder CQL query.
     *
     * @param column the name of the column referenced by the placeholders
     * @param query  a CQL query with exactly two placeholders
     */
    private void testThreshold2(String column, String query) throws Throwable
    {
        assertValid(query, allocate(1), allocate(1));
        assertValid(query, allocate(THRESHOLD_IN_BYTES), allocate(1));
        assertValid(query, allocate(1), allocate(THRESHOLD_IN_BYTES));
        assertValid(query, allocate((THRESHOLD_IN_BYTES)), allocate((THRESHOLD_IN_BYTES)));
        assertGuardrailFailed(column, query, allocate(THRESHOLD_IN_BYTES + 1), allocate(1));
        assertGuardrailFailed(column, query, allocate(1), allocate(THRESHOLD_IN_BYTES + 1));
    }

    /**
     * Tests that the max column size guardrail threshold is applied for the specified 2-placeholder query.
     *
     * @param column             the name of the column referenced by the placeholders
     * @param query              a CQL query with exactly two placeholders
     * @param serializationBytes the extra bytes added to the sum of the placeholder value by their wrapping serializer
     */
    private void testThreshold2(String column, String query, int serializationBytes) throws Throwable
    {
        assertValid(query, allocate(1), allocate(1));
        assertValid(query, allocate(THRESHOLD_IN_BYTES - serializationBytes - 1), allocate(1));
        assertValid(query, allocate(1), allocate(THRESHOLD_IN_BYTES - serializationBytes - 1));
        assertValid(query, allocate((THRESHOLD_IN_BYTES / 2) - serializationBytes), allocate((THRESHOLD_IN_BYTES / 2)));
        assertGuardrailFailed(column, query, allocate(THRESHOLD_IN_BYTES - serializationBytes), allocate(1));
        assertGuardrailFailed(column, query, allocate(1), allocate(THRESHOLD_IN_BYTES - serializationBytes));
    }

    private void assertGuardrailFailed(String column, String query, Object... values) throws Throwable
    {
        String errorMessage = format("Value of %s of size %s is greater than the maximum allowed (%s)",
                                     column,
                                     Units.toString(THRESHOLD_IN_BYTES + 1, SizeUnit.BYTES),
                                     Units.toString(THRESHOLD_IN_BYTES, SizeUnit.BYTES));
        assertFails(errorMessage, query, values);
    }
}