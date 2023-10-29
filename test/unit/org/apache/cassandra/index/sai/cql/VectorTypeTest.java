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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.service.ClientWarn;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class VectorTypeTest extends VectorTester
{
    private static final IPartitioner partitioner = Murmur3Partitioner.instance;

    @Test
    public void endToEndTest()
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (2, 'C', [3.0, 4.0, 5.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (3, 'D', [4.0, 5.0, 6.0])");

        UntypedResultSet result = execute("SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 3");
        assertThat(result).hasSize(3);

        flush();
        result = execute("SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 3");
        assertThat(result).hasSize(3);

        execute("INSERT INTO %s (pk, str_val, val) VALUES (4, 'E', [5.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (5, 'F', [6.0, 3.0, 4.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (6, 'G', [7.0, 4.0, 5.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (7, 'H', [8.0, 5.0, 6.0])");

        flush();
        compact();

        result = execute("SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 5");
        assertThat(result).hasSize(5);

        // some data that only lives in memtable
        execute("INSERT INTO %s (pk, str_val, val) VALUES (8, 'I', [9.0, 5.0, 6.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (9, 'J', [10.0, 6.0, 7.0])");
        result = execute("SELECT * FROM %s ORDER BY val ann of [9.5, 5.5, 6.5] LIMIT 5");
        assertContainsInt(result, "pk", 8);
        assertContainsInt(result, "pk", 9);

        // data from sstables
        result = execute("SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 2");
        assertContainsInt(result, "pk", 1);
        assertContainsInt(result, "pk", 2);
    }

    @Test
    public void warningIsIssuedOnIndexCreation()
    {
        ClientWarn.instance.captureWarnings();
        createTable("CREATE TABLE %s (pk int, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        List<String> warnings = ClientWarn.instance.getWarnings();

        assertTrue(warnings.size() > 0);
        assertEquals(StorageAttachedIndex.VECTOR_USAGE_WARNING, warnings.get(0));
    }

    @Test
    public void createIndexAfterInsertTest()
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (2, 'C', [3.0, 4.0, 5.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (3, 'D', [4.0, 5.0, 6.0])");

        flush();
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        UntypedResultSet result = execute("SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 3");
        assertThat(result).hasSize(3);
    }

    public static void assertContainsInt(UntypedResultSet result, String columnName, int columnValue)
    {
        for (UntypedResultSet.Row row : result)
        {
            if (row.has(columnName))
            {
                int value = row.getInt(columnName);
                if (value == columnValue)
                {
                    return;
                }
            }
        }
        throw new AssertionError("Result set does not contain a row with " + columnName + " = " + columnValue);
    }

    @Test
    public void testTwoPredicates()
    {
        createTable("CREATE TABLE %s (pk int, b boolean, v vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(b) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, b, v) VALUES (0, true, [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, b, v) VALUES (1, true, [2.0, 3.0, 4.0])");
        execute("INSERT INTO %s (pk, b, v) VALUES (2, false, [3.0, 4.0, 5.0])");

        // the vector given is closest to row 2, but we exclude that row because b=false
        var result = execute("SELECT * FROM %s WHERE b=true ORDER BY v ANN OF [3.1, 4.1, 5.1] LIMIT 2");
        // VSTODO assert specific row keys
        assertThat(result).hasSize(2);

        flush();
        compact();

        result = execute("SELECT * FROM %s WHERE b=true ORDER BY v ANN OF [3.1, 4.1, 5.1] LIMIT 2");
        assertThat(result).hasSize(2);
    }

    @Test
    public void testTwoPredicatesManyRows()
    {
        createTable("CREATE TABLE %s (pk int, b boolean, v vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(b) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        for (int i = 0; i < 100; i++)
            execute("INSERT INTO %s (pk, b, v) VALUES (?, true, ?)",
                    i, vector((float) i, (float) (i + 1), (float) (i + 2)));

        var result = execute("SELECT * FROM %s WHERE b=true ORDER BY v ANN OF [3.1, 4.1, 5.1] LIMIT 2");
        assertThat(result).hasSize(2);

        flush();
        compact();

        result = execute("SELECT * FROM %s WHERE b=true ORDER BY v ANN OF [3.1, 4.1, 5.1] LIMIT 2");
        assertThat(result).hasSize(2);
    }

    @Test
    public void testThreePredicates()
    {
        createTable("CREATE TABLE %s (pk int, b boolean, v vector<float, 3>, str text, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(b) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(str) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, b, v, str) VALUES (0, true, [1.0, 2.0, 3.0], 'A')");
        execute("INSERT INTO %s (pk, b, v, str) VALUES (1, true, [2.0, 3.0, 4.0], 'B')");
        execute("INSERT INTO %s (pk, b, v, str) VALUES (2, false, [3.0, 4.0, 5.0], 'C')");

        // the vector given is closest to row 2, but we exclude that row because b=false and str!='B'
        var result = execute("SELECT * FROM %s WHERE b=true AND str='B' ORDER BY v ANN OF [3.1, 4.1, 5.1] LIMIT 2");
        // VSTODO assert specific row keys
        assertThat(result).hasSize(1);

        flush();
        compact();

        result = execute("SELECT * FROM %s WHERE b=true AND str='B' ORDER BY v ANN OF [3.1, 4.1, 5.1] LIMIT 2");
        assertThat(result).hasSize(1);
    }

    @Test
    public void testSameVectorMultipleRows()
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (2, 'A', [1.0, 2.0, 3.0])");

        var result = execute("SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 3");
        assertThat(result).hasSize(3);

        flush();
        compact();

        result = execute("SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 3");
        assertThat(result).hasSize(3);
    }

    @Test
    public void testQueryEmptyTable()
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        var result = execute("SELECT * FROM %s ORDER BY val ANN OF [2.5, 3.5, 4.5] LIMIT 1");
        assertThat(result).hasSize(0);
    }

    @Test
    public void testQueryTableWithNulls()
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', null)");
        var result = execute("SELECT * FROM %s ORDER BY val ANN OF [2.5, 3.5, 4.5] LIMIT 1");
        assertThat(result).hasSize(0);

        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [4.0, 5.0, 6.0])");
        result = execute("SELECT pk FROM %s ORDER BY val ANN OF [2.5, 3.5, 4.5] LIMIT 1");
        assertRows(result, row(1));
    }

    @Test
    public void testLimitLessThanInsertedRowCount()
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        // Insert more rows than the query limit
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [4.0, 5.0, 6.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (2, 'C', [7.0, 8.0, 9.0])");

        // Query with limit less than inserted row count
        var result = execute("SELECT * FROM %s ORDER BY val ANN OF [2.5, 3.5, 4.5] LIMIT 2");
        assertThat(result).hasSize(2);
    }

    @Test
    public void testQueryMoreRowsThanInserted()
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");

        var result = execute("SELECT * FROM %s ORDER BY val ANN OF [2.5, 3.5, 4.5] LIMIT 2");
        assertThat(result).hasSize(1);
    }

    @Test
    public void changingOptionsTest()
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        if (CassandraRelevantProperties.SAI_VECTOR_ALLOW_CUSTOM_PARAMETERS.getBoolean())
        {
            createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = " +
                        "{'maximum_node_connections' : 10, 'construction_beam_width' : 200, 'similarity_function' : 'euclidean' }");
        }
        else
        {
            assertThatThrownBy(() -> createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = " +
                                                 "{'maximum_node_connections' : 10, 'construction_beam_width' : 200, 'similarity_function' : 'euclidean' }"))
            .isInstanceOf(InvalidRequestException.class);
            return;
        }

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (2, 'C', [3.0, 4.0, 5.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (3, 'D', [4.0, 5.0, 6.0])");

        UntypedResultSet result = execute("SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 3");
        assertThat(result).hasSize(3);

        flush();
        result = execute("SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 3");
        assertThat(result).hasSize(3);

        execute("INSERT INTO %s (pk, str_val, val) VALUES (4, 'E', [5.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (5, 'F', [6.0, 3.0, 4.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (6, 'G', [7.0, 4.0, 5.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (7, 'H', [8.0, 5.0, 6.0])");

        flush();
        compact();

        result = execute("SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 5");
        assertThat(result).hasSize(5);
    }

    @Test
    public void bindVariablesTest()
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', ?)", vector(1.0f, 2.0f ,3.0f));
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', ?)", vector(2.0f ,3.0f, 4.0f));
        execute("INSERT INTO %s (pk, str_val, val) VALUES (2, 'C', ?)", vector(3.0f, 4.0f, 5.0f));
        execute("INSERT INTO %s (pk, str_val, val) VALUES (3, 'D', ?)", vector(4.0f, 5.0f, 6.0f));

        UntypedResultSet result = execute("SELECT * FROM %s ORDER BY val ann of ? LIMIT 3", vector(2.5f, 3.5f, 4.5f));
        assertThat(result).hasSize(3);
    }

    @Test
    public void intersectedSearcherTest()
    {
        // check that we correctly get back the two rows with str_val=B even when those are not
        // the closest rows to the query vector
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(str_val) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', ?)", vector(1.0f, 2.0f ,3.0f));
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', ?)", vector(2.0f ,3.0f, 4.0f));
        execute("INSERT INTO %s (pk, str_val, val) VALUES (2, 'C', ?)", vector(3.0f, 4.0f, 5.0f));
        execute("INSERT INTO %s (pk, str_val, val) VALUES (3, 'B', ?)", vector(4.0f, 5.0f, 6.0f));
        execute("INSERT INTO %s (pk, str_val, val) VALUES (4, 'E', ?)", vector(5.0f, 6.0f, 7.0f));

        UntypedResultSet result = execute("SELECT * FROM %s WHERE str_val = 'B' ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 2");
        assertThat(result).hasSize(2);

        flush();
        result = execute("SELECT * FROM %s WHERE str_val = 'B' ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 2");
        assertThat(result).hasSize(2);
    }

    @Test
    public void nullVectorTest()
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(str_val) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', ?)", vector(1.0f, 2.0f ,3.0f));
        execute("INSERT INTO %s (pk, str_val) VALUES (1, 'B')"); // no vector
        execute("INSERT INTO %s (pk, str_val, val) VALUES (2, 'C', ?)", vector(3.0f, 4.0f, 5.0f));
        execute("INSERT INTO %s (pk, str_val) VALUES (3, 'D')"); // no vector
        execute("INSERT INTO %s (pk, str_val, val) VALUES (4, 'E', ?)", vector(5.0f, 6.0f, 7.0f));

        UntypedResultSet result = execute("SELECT * FROM %s WHERE str_val = 'B' ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 2");
        assertThat(result).hasSize(0);

        result = execute("SELECT * FROM %s WHERE str_val = 'A' ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 2");
        assertThat(result).hasSize(1);

        flush();

        result = execute("SELECT * FROM %s WHERE str_val = 'B' ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 2");
        assertThat(result).hasSize(0);

        result = execute("SELECT * FROM %s WHERE str_val = 'A' ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 2");
        assertThat(result).hasSize(1);
    }

    @Test
    public void lwtTest()
    {
        createTable("CREATE TABLE %s (p int, c int, v text, vec vector<float, 2>, PRIMARY KEY(p, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (p, c, v) VALUES (?, ?, ?)", 0, 0, "test");
        execute("INSERT INTO %s (p, c, v) VALUES (?, ?, ?)", 0, 1, "00112233445566");

        execute("UPDATE %s SET v='00112233', vec=[0.9, 0.7] WHERE p = 0 AND c = 0 IF v = 'test'");

        UntypedResultSet result = execute("SELECT * FROM %s ORDER BY vec ANN OF [0.1, 0.9] LIMIT 100");

        assertThat(result).hasSize(1);
    }

    @Test
    public void twoVectorFieldsTest()
    {
        createTable("CREATE TABLE %s (pk int, v2 vector<float, 2>, v3 vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(v2) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(v3) USING 'StorageAttachedIndex'");
    }

    @Test
    public void primaryKeySearchTest()
    {
        createTable("CREATE TABLE %s (pk int, val vector<float, 3>, i int, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        var N = 5;
        for (int i = 0; i < N; i++)
            execute("INSERT INTO %s (pk, val) VALUES (?, ?)", i, vector(1.0f + i, 2.0f + i, 3.0f + i));

        for (int i = 0; i < N; i++)
        {
            UntypedResultSet result = execute("SELECT pk FROM %s WHERE pk = ? ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 2", i);
            assertThat(result).hasSize(1);
            assertRows(result, row(i));
        }

        flush();
        for (int i = 0; i < N; i++)
        {
            UntypedResultSet result = execute("SELECT pk FROM %s WHERE pk = ? ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 2", i);
            assertThat(result).hasSize(1);
            assertRows(result, row(i));
        }
    }

    @Test
    public void partitionKeySearchTest()
    {
        createTable("CREATE TABLE %s (partition int, row int, val vector<float, 2>, PRIMARY KEY(partition, row))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");

        var nPartitions = 5;
        var rowsPerPartition = 10;
        Map<Integer, List<float[]>> vectorsByPartition = new HashMap<>();

        for (int i = 1; i <= nPartitions; i++)
        {
            for (int j = 1; j <= rowsPerPartition; j++)
            {
                logger.debug("Inserting partition {} row {}: [{}, {}]", i, j, i, j);
                execute("INSERT INTO %s (partition, row, val) VALUES (?, ?, ?)", i, j, vector((float) i, (float) j));
                float[] vector = {(float) i, (float) j};
                vectorsByPartition.computeIfAbsent(i, k -> new ArrayList<>()).add(vector);
            }
        }

        var queryVector = vector(new float[] { 1.5f, 1.5f });
        for (int i = 1; i <= nPartitions; i++)
        {
            UntypedResultSet result = execute("SELECT partition, row FROM %s WHERE partition = ? ORDER BY val ann of ? LIMIT 2", i, queryVector);
            assertThat(result).hasSize(2);
            assertRowsIgnoringOrder(result,
                                    row(i, 1),
                                    row(i, 2));
        }

        flush();
        for (int i = 1; i <= nPartitions; i++)
        {
            UntypedResultSet result = execute("SELECT partition, row FROM %s WHERE partition = ? ORDER BY val ann of ? LIMIT 2", i, queryVector);
            assertThat(result).hasSize(2);
            assertRowsIgnoringOrder(result,
                                    row(i, 1),
                                    row(i, 2));
        }
    }

    @Test
    public void clusteringKeyIndexTest()
    {
        createTable("CREATE TABLE %s (pk int, ck vector<float, 2>, PRIMARY KEY(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(ck) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, ck) VALUES (1, [1.0, 2.0])");

        assertRows(execute("SELECT * FROM %s ORDER BY ck ANN OF [1.0, 2.0] LIMIT 1"), row(1, vector(1.0F, 2.0F)));
    }

    @Test
    public void rangeSearchTest() throws Throwable
    {
        createTable("CREATE TABLE %s (partition int, val vector<float, 2>, PRIMARY KEY(partition))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");

        var nPartitions = 100;
        Map<Integer, float[]> vectorsByKey = new HashMap<>();

        for (int i = 1; i <= nPartitions; i++)
        {
            float[] vector = {(float) i, (float) i};
            execute("INSERT INTO %s (partition, val) VALUES (?, ?)", i, vector(vector));
            vectorsByKey.put(i, vector);
        }

        var queryVector = vector(new float[] { 1.5f, 1.5f });
        CheckedFunction tester = () -> {
            for (int i = 1; i <= nPartitions; i++)
            {
                UntypedResultSet result = execute("SELECT partition FROM %s WHERE token(partition) > token(?) ORDER BY val ann of ? LIMIT 1000", i, queryVector);
                assertThat(keys(result)).containsExactlyInAnyOrderElementsOf(keysWithLowerBound(vectorsByKey.keySet(), i, false));

                result = execute("SELECT partition FROM %s WHERE token(partition) >= token(?) ORDER BY val ann of ? LIMIT 1000", i, queryVector);
                assertThat(keys(result)).containsExactlyInAnyOrderElementsOf(keysWithLowerBound(vectorsByKey.keySet(), i, true));

                result = execute("SELECT partition FROM %s WHERE token(partition) < token(?) ORDER BY val ann of ? LIMIT 1000", i, queryVector);
                assertThat(keys(result)).containsExactlyInAnyOrderElementsOf(keysWithUpperBound(vectorsByKey.keySet(), i, false));

                result = execute("SELECT partition FROM %s WHERE token(partition) <= token(?) ORDER BY val ann of ? LIMIT 1000", i, queryVector);
                assertThat(keys(result)).containsExactlyInAnyOrderElementsOf(keysWithUpperBound(vectorsByKey.keySet(), i, true));

                for (int j = 1; j <= nPartitions; j++)
                {
                    result = execute("SELECT partition FROM %s WHERE token(partition) >= token(?) AND token(partition) <= token(?) ORDER BY val ann of ? LIMIT 1000", i, j, queryVector);
                    assertThat(keys(result)).containsExactlyInAnyOrderElementsOf(keysInBounds(vectorsByKey.keySet(), i, true, j, true));

                    result = execute("SELECT partition FROM %s WHERE token(partition) > token(?) AND token(partition) <= token(?) ORDER BY val ann of ? LIMIT 1000", i, j, queryVector);
                    assertThat(keys(result)).containsExactlyInAnyOrderElementsOf(keysInBounds(vectorsByKey.keySet(), i, false, j, true));

                    result = execute("SELECT partition FROM %s WHERE token(partition) >= token(?) AND token(partition) < token(?) ORDER BY val ann of ? LIMIT 1000", i, j, queryVector);
                    assertThat(keys(result)).containsExactlyInAnyOrderElementsOf(keysInBounds(vectorsByKey.keySet(), i, true, j, false));

                    result = execute("SELECT partition FROM %s WHERE token(partition) > token(?) AND token(partition) < token(?) ORDER BY val ann of ? LIMIT 1000", i, j, queryVector);
                    assertThat(keys(result)).containsExactlyInAnyOrderElementsOf(keysInBounds(vectorsByKey.keySet(), i, false, j, false));
                }
            }
        };

        tester.apply();

        flush();

        tester.apply();
    }

    private Collection<Integer> keys(UntypedResultSet result)
    {
        List<Integer> keys = new ArrayList<>(result.size());
        for (UntypedResultSet.Row row : result)
            keys.add(row.getInt("partition"));
        return keys;
    }

    private Collection<Integer> keysWithLowerBound(Collection<Integer> keys, int leftKey, boolean leftInclusive)
    {
        return keysInTokenRange(keys, partitioner.getToken(Int32Type.instance.decompose(leftKey)), leftInclusive,
                                partitioner.getMaximumToken().getToken(), true);
    }

    private Collection<Integer> keysWithUpperBound(Collection<Integer> keys, int rightKey, boolean rightInclusive)
    {
        return keysInTokenRange(keys, partitioner.getMinimumToken().getToken(), true,
                                partitioner.getToken(Int32Type.instance.decompose(rightKey)), rightInclusive);
    }

    private Collection<Integer> keysInBounds(Collection<Integer> keys, int leftKey, boolean leftInclusive, int rightKey, boolean rightInclusive)
    {
        return keysInTokenRange(keys, partitioner.getToken(Int32Type.instance.decompose(leftKey)), leftInclusive,
                                partitioner.getToken(Int32Type.instance.decompose(rightKey)), rightInclusive);
    }

    private Collection<Integer> keysInTokenRange(Collection<Integer> keys, Token leftToken, boolean leftInclusive, Token rightToken, boolean rightInclusive)
    {
        long left = leftToken.getLongValue();
        long right = rightToken.getLongValue();
        return keys.stream()
               .filter(k -> {
                   long t = partitioner.getToken(Int32Type.instance.decompose(k)).getLongValue();
                   return (left < t || left == t && leftInclusive) && (t < right || t == right && rightInclusive);
               }).collect(Collectors.toSet());
    }

    @Test
    public void selectFloatVectorFunctions()
    {
        createTable(KEYSPACE, "CREATE TABLE %s (pk int primary key, value vector<float, 2>)");

        // basic functionality
        Vector<Float> q = vector(1f, 2f);
        execute("INSERT INTO %s (pk, value) VALUES (0, ?)", vector(1f, 2f));
        execute("SELECT similarity_cosine(value, value) FROM %s WHERE pk=0");

        // type inference checks
        var result = execute("SELECT similarity_cosine(value, ?) FROM %s WHERe pk=0", q);
        assertRows(result, row(1f));
        result = execute("SELECT similarity_euclidean(value, ?) FROM %s WHERe pk=0", q);
        assertRows(result, row(1f));
        execute("SELECT similarity_cosine(?, value) FROM %s WHERE pk=0", q);
        assertThatThrownBy(() -> execute("SELECT similarity_cosine(?, ?) FROM %s WHERE pk=0", q, q))
        .hasMessageContaining("Cannot infer type of argument ?");

        // with explicit typing
        execute("SELECT similarity_cosine((vector<float, 2>) ?, ?) FROM %s WHERE pk=0", q, q);
        execute("SELECT similarity_cosine(?, (vector<float, 2>) ?) FROM %s WHERE pk=0", q, q);
        execute("SELECT similarity_cosine((vector<float, 2>) ?, (vector<float, 2>) ?) FROM %s WHERE pk=0", q, q);
    }

    @Test
    public void selectSimilarityWithAnn()
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (2, 'C', [3.0, 4.0, 5.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (3, 'D', [4.0, 5.0, 6.0])");

        Vector<Float> q = vector(1.5f, 2.5f, 3.5f);
        var result = execute("SELECT str_val, similarity_cosine(val, ?) FROM %s ORDER BY val ANN OF ? LIMIT 2",
                q, q);

        assertRowsIgnoringOrder(result,
                row("A", 0.9987074f),
                row("B", 0.9993764f));
    }

    @Test
    public void testTwoPredicatesWithUnnecessaryAllowFiltering()
    {
        createTable("CREATE TABLE %s (pk int, b int, v vector<float, 3>, PRIMARY KEY(pk, b))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(b) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, b, v) VALUES (0, 0, [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, b, v) VALUES (1, 2, [2.0, 3.0, 4.0])");
        execute("INSERT INTO %s (pk, b, v) VALUES (2, 4, [3.0, 4.0, 5.0])");
        execute("INSERT INTO %s (pk, b, v) VALUES (3, 6, [4.0, 5.0, 6.0])");

        // Choose a vector closer to b = 0 to ensure that b's restriction is applied.
        assertRows(execute("SELECT pk FROM %s WHERE b > 2 ORDER BY v ANN OF [1,2,3] LIMIT 2 ALLOW FILTERING;"),
                   row(2), row(3));
    }

    @Test
    public void testMultipleVectorsInMemoryWithPredicate()
    {
        createTable(KEYSPACE, "CREATE TABLE %s (pk int, val text, vec vector<float, 2>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        // When we search the memtable, we filter out PKs outside the memtable's bounrdaries.
        // Persist two rows and push to sstable that will be outside of bounds.
        execute("INSERT INTO %s (pk, val, vec) VALUES (1, 'match me', [1, 1])");
        execute("INSERT INTO %s (pk, val, vec) VALUES (5, 'match me', [1, 1])");
        flush();
        execute("INSERT INTO %s (pk, val, vec) VALUES (2, 'match me', [1, 1])");
        execute("INSERT INTO %s (pk, val, vec) VALUES (3, 'match me', [1, 1])");
        execute("INSERT INTO %s (pk, val, vec) VALUES (4, 'match me', [1, 1])");
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE val = 'match me' ORDER BY vec ANN OF [1,1] LIMIT 5"),
                                row(1), row(2), row(3), row(4), row(5));
    }
}
