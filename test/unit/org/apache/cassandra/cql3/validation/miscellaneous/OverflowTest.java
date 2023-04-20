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
package org.apache.cassandra.cql3.validation.miscellaneous;

import java.math.BigInteger;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Any tests that do not fit in any other category,
 * migrated from python dtests, CASSANDRA-9160
 **/
public class OverflowTest extends CQLTester
{
    /**
     * Test support for nulls
     * migrated from cql_tests.py:TestCQL.null_support_test()
     */
    @Test
    public void testNullSupport() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v1 int, v2 set<text>, PRIMARY KEY (k, c))");

        execute("INSERT INTO %s (k, c, v1, v2) VALUES (0, 0, null, {'1', '2'})");
        execute("INSERT INTO %s (k, c, v1) VALUES (0, 1, 1)");

        assertRows(execute("SELECT * FROM %s"),
                   row(0, 0, null, set("1", "2")),
                   row(0, 1, 1, null));

        execute("INSERT INTO %s (k, c, v1) VALUES (0, 1, null)");
        execute("INSERT INTO %s (k, c, v2) VALUES (0, 0, null)");

        assertRows(execute("SELECT * FROM %s"),
                   row(0, 0, null, null),
                   row(0, 1, null, null));

        assertInvalid("INSERT INTO %s (k, c, v2) VALUES (0, 2, {1, null})");
        assertInvalid("SELECT * FROM %s WHERE k = null");
        assertInvalid("INSERT INTO %s (k, c, v2) VALUES (0, 0, { 'foo', 'bar', null })");
    }

    /**
     * Test reserved keywords
     * migrated from cql_tests.py:TestCQL.reserved_keyword_test()
     */
    @Test
    public void testReservedKeywords() throws Throwable
    {
        createTable("CREATE TABLE %s (key text PRIMARY KEY, count counter)");

        String tableName = createTableName();
        assertInvalidThrow(SyntaxException.class, String.format("CREATE TABLE %s.%s (select text PRIMARY KEY, x int)", keyspace(), tableName));
    }

    /**
     * Test identifiers
     * migrated from cql_tests.py:TestCQL.identifier_test()
     */
    @Test
    public void testIdentifiers() throws Throwable
    {
        createTable("CREATE TABLE %s (key_23 int PRIMARY KEY, CoLuMn int)");

        execute("INSERT INTO %s (Key_23, Column) VALUES (0, 0)");
        execute("INSERT INTO %s (KEY_23, COLUMN) VALUES (0, 0)");

        assertInvalid("INSERT INTO %s (key_23, column, column) VALUES (0, 0, 0)");
        assertInvalid("INSERT INTO %s (key_23, column, COLUMN) VALUES (0, 0, 0)");
        assertInvalid("INSERT INTO %s (key_23, key_23, column) VALUES (0, 0, 0)");
        assertInvalid("INSERT INTO %s (key_23, KEY_23, column) VALUES (0, 0, 0)");

        String tableName = createTableName();
        assertInvalidThrow(SyntaxException.class, String.format("CREATE TABLE %s.%s (select int PRIMARY KEY, column int)", keyspace(), tableName));
    }

    /**
     * Test table options
     * migrated from cql_tests.py:TestCQL.table_options_test()
     */
    @Test
    public void testTableOptions() throws Throwable
    {
        createTable("CREATE TABLE %s ( k int PRIMARY KEY, c int ) WITH "
                    + "comment = 'My comment' "
                    + "AND gc_grace_seconds = 4 "
                    + "AND bloom_filter_fp_chance = 0.01 "
                    + "AND compaction = { 'class' : 'LeveledCompactionStrategy', 'sstable_size_in_mb' : 10, 'fanout_size' : 5 } "
                    + "AND compression = { 'enabled': false } "
                    + "AND caching = { 'keys': 'ALL', 'rows_per_partition': 'ALL' }");

        execute("ALTER TABLE %s WITH "
                + "comment = 'other comment' "
                + "AND gc_grace_seconds = 100 "
                + "AND bloom_filter_fp_chance = 0.1 "
                + "AND compaction = { 'class': 'SizeTieredCompactionStrategy', 'min_sstable_size' : 42 } "
                + "AND compression = { 'class' : 'SnappyCompressor' } "
                + "AND caching = { 'rows_per_partition': 'ALL' }");
    }

    /**
     * Migrated from cql_tests.py:TestCQL.unescaped_string_test()
     */
    @Test
    public void testUnescapedString() throws Throwable
    {
        createTable("CREATE TABLE %s ( k text PRIMARY KEY, c text, )");

        //The \ in this query string is not forwarded to cassandra.
        //The ' is being escaped in python, but only ' is forwarded
        //over the wire instead of \'.
        assertInvalidThrow(SyntaxException.class, "INSERT INTO %s (k, c) VALUES ('foo', 'CQL is cassandra\'s best friend')");
    }

    /**
     * Migrated from cql_tests.py:TestCQL.boolean_test()
     */
    @Test
    public void testBoolean() throws Throwable
    {
        createTable("CREATE TABLE %s (k boolean PRIMARY KEY, b boolean)");

        execute("INSERT INTO %s (k, b) VALUES (true, false)");
        assertRows(execute("SELECT * FROM %s WHERE k = true"),
                   row(true, false));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.float_with_exponent_test()
     */
    @Test
    public void testFloatWithExponent() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, d double, f float)");

        execute("INSERT INTO %s (k, d, f) VALUES (0, 3E+10, 3.4E3)");
        execute("INSERT INTO %s (k, d, f) VALUES (1, 3.E10, -23.44E-3)");
        execute("INSERT INTO %s (k, d, f) VALUES (2, 3, -2)");
    }

    /**
     * Migrated from cql_tests.py:TestCQL.conversion_functions_test()
     */
    @Test
    public void testConversionFunctions() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, i varint, b blob)");

        execute("INSERT INTO %s (k, i, b) VALUES (0, blob_as_varint(bigint_as_blob(3)), text_as_blob('foobar'))");
        assertRows(execute("SELECT i, blob_as_text(b) FROM %s WHERE k = 0"),
                   row(BigInteger.valueOf(3), "foobar"));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.empty_blob_test()
     */
    @Test
    public void testEmptyBlob() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, b blob)");
        execute("INSERT INTO %s (k, b) VALUES (0, 0x)");

        assertRows(execute("SELECT * FROM %s"),
                   row(0, ByteBufferUtil.bytes("")));
    }

    private Object[][] fill() throws Throwable
    {
        for (int i = 0; i < 2; i++)
            for (int j = 0; j < 2; j++)
                execute("INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", i, j, i + j);

        return getRows(execute("SELECT * FROM %s"));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.empty_in_test()
     */
    @Test
    public void testEmpty() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, v int, PRIMARY KEY (k1, k2))");

        // Inserts a few rows to make sure we don 't actually query something
        Object[][] rows = fill();

        // Test empty IN() in SELECT
        assertEmpty(execute("SELECT v FROM %s WHERE k1 IN ()"));
        assertEmpty(execute("SELECT v FROM %s WHERE k1 = 0 AND k2 IN ()"));

        // Test empty IN() in DELETE
        execute("DELETE FROM %s WHERE k1 IN ()");
        assertArrayEquals(rows, getRows(execute("SELECT * FROM %s")));

        // Test empty IN() in UPDATE
        execute("UPDATE %s SET v = 3 WHERE k1 IN () AND k2 = 2");
        assertArrayEquals(rows, getRows(execute("SELECT * FROM %s")));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.function_with_null_test()
     */
    @Test
    public void testFunctionWithNull() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, t timeuuid,)");

        execute("INSERT INTO %s (k) VALUES (0)");
        Object[][] rows = getRows(execute("SELECT to_timestamp(t) FROM %s WHERE k=0"));
        assertNull(rows[0][0]);
    }

    /**
     * Migrated from cql_tests.py:TestCQL.column_name_validation_test()
     */
    @Test
    public void testColumnNameValidation() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, c int, v timeuuid, PRIMARY KEY (k, c))");

        assertInvalid("INSERT INTO %s (k, c) VALUES ('', 0)");

        // Insert a value that don't fit 'int'
        assertInvalid("INSERT INTO %s (k, c) VALUES (0, 10000000000)");

        // Insert a non-version 1 uuid
        assertInvalid("INSERT INTO %s (k, c, v) VALUES (0, 0, 550e8400-e29b-41d4-a716-446655440000)");
    }

    /**
     * Migrated from cql_tests.py:TestCQL.nan_infinity_test()
     */
    @Test
    public void testNanInfinityValues() throws Throwable
    {
        createTable("CREATE TABLE %s (f float PRIMARY KEY)");

        execute("INSERT INTO %s (f) VALUES (NaN)");
        execute("INSERT INTO %s (f) VALUES (-NaN)");
        execute("INSERT INTO %s (f) VALUES (Infinity)");
        execute("INSERT INTO %s (f) VALUES (-Infinity)");

        Object[][] selected = getRows(execute("SELECT * FROM %s"));

        // selected should be[[nan],[inf],[-inf]],
        // but assert element - wise because NaN!=NaN
        assertEquals(3, selected.length);
        assertEquals(1, selected[0].length);
        assertTrue(Float.isNaN((Float) selected[0][0]));

        assertTrue(Float.isInfinite((Float) selected[1][0])); //inf
        assertTrue(((Float) selected[1][0]) > 0);

        assertTrue(Float.isInfinite((Float) selected[2][0])); //-inf
        assertTrue(((Float) selected[2][0]) < 0);
    }

    /**
     * Migrated from cql_tests.py:TestCQL.blobAs_functions_test()
     */
    @Test
    public void testBlobAsFunction() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");

        //  A blob that is not 4 bytes should be rejected
        assertInvalid("INSERT INTO %s (k, v) VALUES (0, blob_as_int(0x01))");

        execute("INSERT INTO %s (k, v) VALUES (0, blob_as_int(0x00000001))");
        assertRows(execute("select v from %s where k=0"), row(1));
    }
}
