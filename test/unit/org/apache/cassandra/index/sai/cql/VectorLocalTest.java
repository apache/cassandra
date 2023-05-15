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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.SAITester;

import static org.assertj.core.api.Assertions.assertThat;

public class VectorLocalTest extends SAITester
{
    private int dimensionCount;

    @Before
    public void setup() throws Throwable
    {
        dimensionCount = getRandom().nextIntBetween(1, 2048);
    }

    @Test
    public void randomizedTest() throws Throwable
    {
        createTable(String.format("CREATE TABLE %%s (pk int, str_val text, val float vector[%d], PRIMARY KEY(pk))", dimensionCount));
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        int vectorCount = getRandom().nextIntBetween(500, 1000);
        List<float[]> vectors = IntStream.range(0, vectorCount).mapToObj(s -> randomVector()).collect(Collectors.toList());

        int pk = 0;
        for (float[] vector : vectors)
            execute("INSERT INTO %s (pk, str_val, val) VALUES (?, 'A', " + vectorString(vector) + " )", pk++);

        // query memtable index
        int limit = Math.min(getRandom().nextIntBetween(30, 50), vectors.size());
        search(randomVector(), limit);

        flush();

        // query on-disk index
        search(randomVector(), limit);

        // populate some more vectors
        int additionalVectorCount = getRandom().nextIntBetween(500, 1000);
        List<float[]> additionalVectors = IntStream.range(0, additionalVectorCount).mapToObj(s -> randomVector()).collect(Collectors.toList());
        for (float[] vector : additionalVectors)
            execute("INSERT INTO %s (pk, str_val, val) VALUES (?, 'A', " + vectorString(vector) + " )", pk++);

        vectors.addAll(additionalVectors);

        // query both memtable index and on-disk index
        search(randomVector(), limit);

        flush();

        // query multiple on-disk indexes
        search(randomVector(), limit);

        compact();

        // query compacted on-disk index
        search(randomVector(), limit);
    }

    @Test
    public void partitionRestrictedTest() throws Throwable
    {
        createTable(String.format("CREATE TABLE %%s (pk int, str_val text, val float vector[%d], PRIMARY KEY(pk))", dimensionCount));
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        int vectorCount = getRandom().nextIntBetween(500, 1000);
        List<float[]> vectors = IntStream.range(0, vectorCount).mapToObj(s -> randomVector()).collect(Collectors.toList());

        int pk = 0;
        for (float[] vector : vectors)
            execute("INSERT INTO %s (pk, str_val, val) VALUES (?, 'A', " + vectorString(vector) + " )", pk++);

        // query memtable index
        int key = getRandom().nextIntBetween(1, vectorCount);
        float[] queryVector = randomVector();
        searchWithKey(queryVector, key);

        // TODO: on-disk index doesn't support Bits filtering yet
//        flush();
//
//        // query on-disk index:
//        searchWithKey(queryVector, key);
//
//        // populate some more vectors
//        int additionalVectorCount = getRandom().nextIntBetween(500, 1000);
//        List<float[]> additionalVectors = IntStream.range(0, additionalVectorCount).mapToObj(s -> randomVector()).collect(Collectors.toList());
//        for (float[] vector : additionalVectors)
//            execute("INSERT INTO %s (pk, str_val, val) VALUES (?, 'A', " + vectorString(vector) + " )", pk++);
//
//        vectors.addAll(additionalVectors);
//
//        // query both memtable index and on-disk index
//        searchWithKey(queryVector, key);
//
//        flush();
//        compact();
//
//        // query compacted on-disk index
//        searchWithKey(queryVector, key);
    }

    private void search(float[] queryVector, int limit) throws Throwable
    {
        UntypedResultSet result = execute("SELECT * FROM %s WHERE val ann of " + Arrays.toString(queryVector) + " LIMIT " + limit);
        assertThat(result).hasSize(limit);
    }

    private void searchWithKey(float[] queryVector, int key) throws Throwable
    {
        UntypedResultSet result = execute("SELECT * FROM %s WHERE pk = " + key + " AND val ann of " + Arrays.toString(queryVector) + " LIMIT 1");
        assertThat(result).hasSize(1);
    }

    private String vectorString(float[] vector)
    {
        return Arrays.toString(vector);
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
}
