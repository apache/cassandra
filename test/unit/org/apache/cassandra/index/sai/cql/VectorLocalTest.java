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
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;

import static org.assertj.core.api.Assertions.assertThat;

public class VectorLocalTest extends SAITester
{
    private static final VectorSimilarityFunction function = IndexWriterConfig.DEFAULT_SIMILARITY_FUNCTION;

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
    public void multiSSTablesTest() throws Throwable
    {
        createTable(String.format("CREATE TABLE %%s (pk int, str_val text, val float vector[%d], PRIMARY KEY(pk))", dimensionCount));
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();
        disableCompaction(keyspace());

        int sstableCount = getRandom().nextIntBetween(3, 6);
        int vectorCountPerSSTable = getRandom().nextIntBetween(200, 400);
        int pk = 0;

        // create multiple sstables
        List<float[]> allVectors = new ArrayList<>(sstableCount * vectorCountPerSSTable);
        for (int i = 0; i < sstableCount; i++)
        {
            List<float[]> vectors = IntStream.range(0, vectorCountPerSSTable).mapToObj(s -> randomVector()).collect(Collectors.toList());
            for (float[] vector : vectors)
                execute("INSERT INTO %s (pk, str_val, val) VALUES (?, 'A', " + vectorString(vector) + " )", pk++);

            allVectors.addAll(vectors);
            flush();
        }
        assertThat(getCurrentColumnFamilyStore(keyspace()).getLiveSSTables()).hasSize(sstableCount);

        // query multiple on-disk indexes
        int limit = Math.min(getRandom().nextIntBetween(30, 50), vectorCountPerSSTable);
        float[] queryVector = randomVector();
        UntypedResultSet resultSet = search(queryVector, limit);

        // expect recall to be at least 0.8
        List<float[]> resultVectors = getVectorsFromResult(resultSet);
        double recall = getRecall(allVectors, queryVector, resultVectors);
        assertThat(recall).isGreaterThanOrEqualTo(0.8);
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

    private UntypedResultSet search(float[] queryVector, int limit) throws Throwable
    {
        UntypedResultSet result = execute("SELECT * FROM %s WHERE val ann of " + Arrays.toString(queryVector) + " LIMIT " + limit);
        assertThat(result).hasSize(limit);
        return result;
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

    private double getRecall(List<float[]> vectors, float[] query, List<float[]> result)
    {
        List<float[]> sortedVectors = new ArrayList<>(vectors);
        sortedVectors.sort((a, b) -> Double.compare(function.compare(b, query), function.compare(a, query)));

        assertThat(sortedVectors).containsAll(result);

        List<float[]> nearestNeighbors = sortedVectors.subList(0, result.size());

        int matches = 0;
        for (float[] in : nearestNeighbors)
        {
            for (float[] out : result)
            {
                if (Arrays.compare(in, out) ==0)
                {
                    matches++;
                    break;
                }
            }
        }

        return matches * 1.0 / result.size();
    }

    private List<float[]> getVectorsFromResult(UntypedResultSet result)
    {
        List<float[]> vectors = new ArrayList<>();
        VectorType vectorType = VectorType.getInstance(dimensionCount);

        // verify results are part of inserted vectors
        for (UntypedResultSet.Row row: result)
        {
            vectors.add(vectorType.compose(row.getBytes("val")));
        }

        return vectors;
    }
}
