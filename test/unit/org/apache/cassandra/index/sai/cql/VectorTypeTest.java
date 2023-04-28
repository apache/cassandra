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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class VectorTypeTest extends SAITester
{
    private int dimensionCount;

    @Before
    public void setup() throws Throwable
    {
        dimensionCount = getRandom().nextIntBetween(1, 2048);
    }

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

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', ?)", Lists.newArrayList(1.0, 2.0 ,3.0));
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', ?)", Lists.newArrayList(2.0 ,3.0, 4.0));
        execute("INSERT INTO %s (pk, str_val, val) VALUES (2, 'C', ?)", Lists.newArrayList(3.0, 4.0, 5.0));
        execute("INSERT INTO %s (pk, str_val, val) VALUES (3, 'D', ?)", Lists.newArrayList(4.0, 5.0, 6.0));

        UntypedResultSet result = execute("SELECT * FROM %s WHERE val ann of [2.5, 3.5, 4.5] LIMIT 3");
        assertThat(result).hasSize(3);
        System.out.println(makeRowStrings(result));
    }

    @Test
    public void randomizedTest() throws Throwable
    {
        createTable(String.format("CREATE TABLE %%s (pk int, str_val text, val float vector[%d], PRIMARY KEY(pk))", dimensionCount));
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        int vectorCount = getRandom().nextIntBetween(100, 1000);
        List<float[]> vectors = IntStream.range(0, vectorCount).mapToObj(s -> randomVector()).collect(Collectors.toList());

        int pk = 0;
        for (float[] vector : vectors)
            execute("INSERT INTO %s (pk, str_val, val) VALUES (?, 'A', " + vectorString(vector) + " )", pk++);

        // query memtable index
        int limit = Math.min(getRandom().nextIntBetween(5, 100), vectorCount);
        UntypedResultSet result = execute("SELECT * FROM %s WHERE val ann of " + randomVectorAsString() + "  LIMIT " + limit);
        verifyResultVectors(result, limit, vectors);

        flush();

        // query on-disk index
        limit = Math.min(getRandom().nextIntBetween(5, 100), vectors.size());
        result = execute("SELECT * FROM %s WHERE val ann of " + randomVectorAsString() + " LIMIT " + limit);
        verifyResultVectors(result, limit, vectors);

        // populate some more vectors
        int additionalVectorCount = getRandom().nextIntBetween(100, 1000);
        List<float[]> additionalVectors = IntStream.range(0, additionalVectorCount).mapToObj(s -> randomVector()).collect(Collectors.toList());
        vectors.addAll(additionalVectors);

        // query both memtable index and on-disk index
        limit = Math.min(getRandom().nextIntBetween(5, 100), vectors.size());
        result = execute("SELECT * FROM %s WHERE val ann of " + randomVectorAsString() + "  LIMIT " + limit);
        verifyResultVectors(result, limit, vectors);

        flush();
        compact();

        // query compacted on-disk index
        limit = Math.min(getRandom().nextIntBetween(5, 100), vectors.size());
        result = execute("SELECT * FROM %s WHERE val ann of " + randomVectorAsString() + "  LIMIT " + limit);
        verifyResultVectors(result, limit, vectors);
    }

    private void verifyResultVectors(UntypedResultSet result, int limit, List<float[]> insertedVectors)
    {
        assertThat(result).hasSize(limit);

        VectorType vectorType = VectorType.getInstance(dimensionCount);

        // verify results are part of inserted vectors
        for (UntypedResultSet.Row row : result)
        {
            ByteBuffer buffer = row.getBytes("val");
            assertThat(insertedVectors).contains(vectorType.compose(buffer));
        }
    }

    private String vectorString(float[] vector)
    {
        return Arrays.toString(vector);
    }

    private String randomVectorAsString()
    {
        float[] rawVector = new float[dimensionCount];
        for (int i = 0; i < dimensionCount; i++)
        {
            rawVector[i] = getRandom().nextFloat();
        }
        return vectorString(rawVector);
    }

    private float[] randomVector()
    {
        float[] rawVector = new float[dimensionCount];
        for (int i = 0; i < dimensionCount; i++)
        {
            rawVector[i] = getRandom().nextFloat();
        }
        return rawVector;
    }

    private ByteBuffer randomVectorBuffer()
    {
        float[] rawVector = new float[dimensionCount];
        for (int i = 0; i < dimensionCount; i++)
        {
            rawVector[i] = getRandom().nextFloat();
        }
        return VectorType.getInstance(dimensionCount).getSerializer().serialize(rawVector);
    }
}
