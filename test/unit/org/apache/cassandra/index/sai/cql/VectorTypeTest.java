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

package org.apache.cassandra.index.sai.cql;

import com.google.common.collect.Lists;
import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class VectorTypeTest extends SAITester
{
    @Test
    public void endToEndTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val float vector[3], PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (2, 'C', [3.0, 4.0, 5.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (3, 'D', [4.0, 5.0, 6.0])");

        UntypedResultSet result = execute("SELECT * FROM %s WHERE val ann of [2.5, 3.5, 4.5] LIMIT 3");
        assertThat(result).hasSize(3);
        System.out.println(makeRowStrings(result));

        flush();
        result = execute("SELECT * FROM %s WHERE val ann of [2.5, 3.5, 4.5] LIMIT 3");
        assertThat(result).hasSize(3);
        System.out.println(makeRowStrings(result));

        execute("INSERT INTO %s (pk, str_val, val) VALUES (4, 'E', [5.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (5, 'F', [6.0, 3.0, 4.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (6, 'G', [7.0, 4.0, 5.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (7, 'H', [8.0, 5.0, 6.0])");

        flush();
        compact();

        result = execute("SELECT * FROM %s WHERE val ann of [2.5, 3.5, 4.5] LIMIT 5");
        assertThat(result).hasSize(5);
        System.out.println(makeRowStrings(result));
    }

    @Test
    public void testCombinedPredicates() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, b boolean, v float vector[3], PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(b) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (pk, b, v) VALUES (0, true, [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, b, v) VALUES (1, true, [2.0, 3.0, 4.0])");
        execute("INSERT INTO %s (pk, b, v) VALUES (2, false, [3.0, 4.0, 5.0])");

        // the vector given is closest to row 2, but we exclude that row because b=false
        var result = execute("SELECT * FROM %s WHERE b=true AND v ANN OF [3.1, 4.1, 5.1] LIMIT 2");
        // TODO assert specific row keys
        assertThat(result).hasSize(2);

        flush();
        compact();

        result = execute("SELECT * FROM %s WHERE b=true AND v ANN OF [3.1, 4.1, 5.1] LIMIT 2");
        assertThat(result).hasSize(2);
    }

    @Test
    public void testSameVectorMultipleRows() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val float vector[3], PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (2, 'A', [1.0, 2.0, 3.0])");

        var result = execute("SELECT * FROM %s WHERE val ann of [2.5, 3.5, 4.5] LIMIT 3");
        assertThat(result).hasSize(3);

        flush();
        compact();

        result = execute("SELECT * FROM %s WHERE val ann of [2.5, 3.5, 4.5] LIMIT 3");
        assertThat(result).hasSize(3);
        System.out.println(makeRowStrings(result));
    }

    @Test
    public void testQueryEmptyTable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val float vector[3], PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        var result = execute("SELECT * FROM %s WHERE val ANN OF [2.5, 3.5, 4.5] LIMIT 1");
        assertThat(result).hasSize(0);
    }

    @Test
    public void testQueryMoreRowsThanInserted() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val float vector[3], PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");

        var result = execute("SELECT * FROM %s WHERE val ANN OF [2.5, 3.5, 4.5] LIMIT 2");
        assertThat(result).hasSize(1);
    }

    @Test
    public void cannotInsertWrongNumberOfDimensions()
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val float vector[3], PRIMARY KEY(pk))");

        assertThatThrownBy(() -> execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0])"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Invalid number of dimensions 2 in list literal for val of type float vector[3]");
    }

    @Test
    public void cannotQueryWrongNumberOfDimensions() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val float vector[3], PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (2, 'C', [3.0, 4.0, 5.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (3, 'D', [4.0, 5.0, 6.0])");

        assertThatThrownBy(() -> execute("SELECT * FROM %s WHERE val ann of [2.5, 3.5] LIMIT 5"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Invalid number of dimensions 2 in list literal for val of type float vector[3]");
    }

    @Test
    public void changingOptionsTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val float vector[3], PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = " +
                    "{'maximum_node_connections' : 10, 'construction_beam_width' : 200, 'similarity_function' : 'euclidean' }");
        waitForIndexQueryable();

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (2, 'C', [3.0, 4.0, 5.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (3, 'D', [4.0, 5.0, 6.0])");

        UntypedResultSet result = execute("SELECT * FROM %s WHERE val ann of [2.5, 3.5, 4.5] LIMIT 3");
        assertThat(result).hasSize(3);
        System.out.println(makeRowStrings(result));

        flush();
        result = execute("SELECT * FROM %s WHERE val ann of [2.5, 3.5, 4.5] LIMIT 3");
        assertThat(result).hasSize(3);
        System.out.println(makeRowStrings(result));

        execute("INSERT INTO %s (pk, str_val, val) VALUES (4, 'E', [5.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (5, 'F', [6.0, 3.0, 4.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (6, 'G', [7.0, 4.0, 5.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (7, 'H', [8.0, 5.0, 6.0])");

        flush();
        compact();

        result = execute("SELECT * FROM %s WHERE val ann of [2.5, 3.5, 4.5] LIMIT 5");
        assertThat(result).hasSize(5);
        System.out.println(makeRowStrings(result));
    }

    @Test
    public void bindVariablesTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val float vector[3], PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', ?)", new float[] {1.0f, 2.0f ,3.0f});
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', ?)", new float[] {2.0f ,3.0f, 4.0f});
        execute("INSERT INTO %s (pk, str_val, val) VALUES (2, 'C', ?)", new float[] {3.0f, 4.0f, 5.0f});
        execute("INSERT INTO %s (pk, str_val, val) VALUES (3, 'D', ?)", new float[] {4.0f, 5.0f, 6.0f});

        UntypedResultSet result = execute("SELECT * FROM %s WHERE val ann of ? LIMIT 3", new float[] {2.5f, 3.5f, 4.5f});
        assertThat(result).hasSize(3);
        System.out.println(makeRowStrings(result));
    }

    @Test
    public void batchLoadingSearcherTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val float vector[3], PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(str_val) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', ?)", Lists.newArrayList(1.0, 2.0 ,3.0));
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', ?)", Lists.newArrayList(2.0 ,3.0, 4.0));
        execute("INSERT INTO %s (pk, str_val, val) VALUES (2, 'C', ?)", Lists.newArrayList(3.0, 4.0, 5.0));
        execute("INSERT INTO %s (pk, str_val, val) VALUES (3, 'B', ?)", Lists.newArrayList(4.0, 5.0, 6.0));
        execute("INSERT INTO %s (pk, str_val, val) VALUES (4, 'E', ?)", Lists.newArrayList(5.0, 6.0, 7.0));

        UntypedResultSet result = execute("SELECT * FROM %s WHERE str_val = 'B' AND val ann of [2.5, 3.5, 4.5] LIMIT 2");
        assertThat(result).hasSize(2);
        System.out.println(makeRowStrings(result));

        flush();
        result = execute("SELECT * FROM %s WHERE str_val = 'B' AND val ann of [2.5, 3.5, 4.5] LIMIT 2");
        assertThat(result).hasSize(2);
        System.out.println(makeRowStrings(result));
    }
}
