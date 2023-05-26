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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.v1.SegmentBuilder;
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
        createTable(String.format("CREATE TABLE %%s (pk int, str_val text, val vector<float, %d>, PRIMARY KEY(pk))", dimensionCount));
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
        createTable(String.format("CREATE TABLE %%s (pk int, str_val text, val vector<float, %d>, PRIMARY KEY(pk))", dimensionCount));
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
        createTable(String.format("CREATE TABLE %%s (pk int, str_val text, val vector<float, %d>, PRIMARY KEY(pk))", dimensionCount));
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        int vectorCount = getRandom().nextIntBetween(500, 1000);
        List<float[]> vectors = IntStream.range(0, vectorCount).mapToObj(s -> randomVector()).collect(Collectors.toList());

        int pk = 0;
        for (float[] vector : vectors)
            execute("INSERT INTO %s (pk, str_val, val) VALUES (?, 'A', " + vectorString(vector) + " )", pk++);

        // query memtable index

        for (int executionCount = 0; executionCount < 50; executionCount++)
        {
            int key = getRandom().nextIntBetween(1, vectorCount);
            float[] queryVector = randomVector();
            searchWithKey(queryVector, key, 1);
        }

        flush();

        // query on-disk index with existing key:
        for (int executionCount = 0; executionCount < 50; executionCount++)
        {
            int key = getRandom().nextIntBetween(1, vectorCount);
            float[] queryVector = randomVector();
            searchWithKey(queryVector, key, 1);
        }

        // query on-disk index with non-existing key:
        for (int executionCount = 0; executionCount < 50; executionCount++)
        {
            int nonExistingKey = getRandom().nextIntBetween(1, vectorCount) + vectorCount;
            float[] queryVector = randomVector();
            searchWithNonExistingKey(queryVector, nonExistingKey);
        }
    }

    @Test
    public void partitionRestrictedWidePartitionTest() throws Throwable
    {
        createTable(String.format("CREATE TABLE %%s (pk int, ck int, val vector<float, %d>, PRIMARY KEY(pk, ck))", dimensionCount));
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        int partitions = 10;
        int vectorCountPerPartition = getRandom().nextIntBetween(50, 100);
        int vectorCount = partitions * vectorCountPerPartition;
        List<float[]> vectors = IntStream.range(0, vectorCount).mapToObj(s -> randomVector()).collect(Collectors.toList());

        int i = 0;
        for (int pk = 1; pk <= partitions; pk++)
        {
            for (int ck = 1; ck <= vectorCountPerPartition; ck++)
            {
                float[] vector = vectors.get(i++);
                execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, " + vectorString(vector) + " )", pk, ck);
            }
        }

        // query memtable index
        for (int executionCount = 0; executionCount < 50; executionCount++)
        {
            int key = getRandom().nextIntBetween(1, partitions);
            float[] queryVector = randomVector();
            searchWithKey(queryVector, key, vectorCountPerPartition);
        }

        flush();

        // query on-disk index with existing key:
        for (int executionCount = 0; executionCount < 50; executionCount++)
        {
            int key = getRandom().nextIntBetween(1, partitions);
            float[] queryVector = randomVector();
            searchWithKey(queryVector, key, vectorCountPerPartition);
        }

        // query on-disk index with non-existing key:
        for (int executionCount = 0; executionCount < 50; executionCount++)
        {
            int nonExistingKey = getRandom().nextIntBetween(1, partitions) + partitions;
            float[] queryVector = randomVector();
            searchWithNonExistingKey(queryVector, nonExistingKey);
        }
    }

    @Test
    public void rangeRestrictedTest() throws Throwable
    {
        createTable(String.format("CREATE TABLE %%s (pk int, str_val text, val vector<float, %d>, PRIMARY KEY(pk))", dimensionCount));
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        int vectorCount = getRandom().nextIntBetween(500, 1000);
        List<float[]> vectors = IntStream.range(0, vectorCount).mapToObj(s -> randomVector()).collect(Collectors.toList());

        int pk = 0;
        Multimap<Long, float[]> vectorsByToken = ArrayListMultimap.create();
        for (float[] vector : vectors)
        {
            vectorsByToken.put(Murmur3Partitioner.instance.getToken(Int32Type.instance.decompose(pk)).getLongValue(), vector);
            execute("INSERT INTO %s (pk, str_val, val) VALUES (?, 'A', " + vectorString(vector) + " )", pk++);
        }

        // query memtable index
        for (int executionCount = 0; executionCount < 50; executionCount++)
        {
            int key1 = getRandom().nextIntBetween(1, vectorCount * 2);
            long token1 = Murmur3Partitioner.instance.getToken(Int32Type.instance.decompose(key1)).getLongValue();
            int key2 = getRandom().nextIntBetween(1, vectorCount * 2);
            long token2 = Murmur3Partitioner.instance.getToken(Int32Type.instance.decompose(key2)).getLongValue();

            long minToken = Math.min(token1, token2);
            long maxToken = Math.max(token1, token2);
            List<float[]> expected = vectorsByToken.entries().stream()
                                                  .filter(e -> e.getKey() >= minToken && e.getKey() <= maxToken)
                                                  .map(Map.Entry::getValue)
                                                  .collect(Collectors.toList());

            float[] queryVector = randomVector();
            List<float[]> resultVectors = searchWithRange(queryVector, minToken, maxToken, expected.size());
            double recall = getRecall(resultVectors, queryVector, expected);
            assertThat(recall).isGreaterThanOrEqualTo(0.8);
        }

        flush();

        // query on-disk index with existing key:
        for (int executionCount = 0; executionCount < 50; executionCount++)
        {
            int key1 = getRandom().nextIntBetween(1, vectorCount * 2);
            long token1 = Murmur3Partitioner.instance.getToken(Int32Type.instance.decompose(key1)).getLongValue();
            int key2 = getRandom().nextIntBetween(1, vectorCount * 2);
            long token2 = Murmur3Partitioner.instance.getToken(Int32Type.instance.decompose(key2)).getLongValue();

            long minToken = Math.min(token1, token2);
            long maxToken = Math.max(token1, token2);
            List<float[]> expected = vectorsByToken.entries().stream()
                                                   .filter(e -> e.getKey() >= minToken && e.getKey() <= maxToken)
                                                   .map(Map.Entry::getValue)
                                                   .collect(Collectors.toList());

            float[] queryVector = randomVector();
            List<float[]> resultVectors = searchWithRange(queryVector, minToken, maxToken, expected.size());
            double recall = getRecall(resultVectors, queryVector, expected);
            assertThat(recall).isGreaterThanOrEqualTo(0.8);
        }
    }

    @Test
    public void multipleNonAnnSegmentsTest() throws Throwable
    {
        createTable(String.format("CREATE TABLE %%s (pk int, str_val text, val vector<float, %d>, PRIMARY KEY(pk))", dimensionCount));
        disableCompaction(KEYSPACE);

        int sstableCount = getRandom().nextIntBetween(3, 6);
        int vectorCountPerSSTable = getRandom().nextIntBetween(200, 400);
        int vectorCount = sstableCount * vectorCountPerSSTable;
        int pk = 0;

        // create multiple sstables
        Multimap<String, float[]> vectorsByStringValue = ArrayListMultimap.create();
        for (int i = 0; i < sstableCount; i++)
        {
            List<float[]> vectors = IntStream.range(0, vectorCountPerSSTable).mapToObj(s -> randomVector()).collect(Collectors.toList());
            for (float[] vector : vectors)
            {
                String stringValue = String.valueOf(pk % 10); // 10 different string values
                execute("INSERT INTO %s (pk, str_val, val) VALUES (?, '" + stringValue + "', " + vectorString(vector) + " )", pk++);
                vectorsByStringValue.put(stringValue, vector);
            }
            flush();
        }
        assertThat(getCurrentColumnFamilyStore(keyspace()).getLiveSSTables()).hasSize(sstableCount);

        // create indexes on existing sstable to flush multiple segments
        SegmentBuilder.updateLastValidSegmentRowId(50); // 50 rows per segment
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(str_val) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        // query multiple on-disk indexes
        for (String stringValue : vectorsByStringValue.keySet())
        {
            int limit = Math.min(getRandom().nextIntBetween(30, 50), vectorCountPerSSTable);
            float[] queryVector = randomVector();
            UntypedResultSet resultSet = search(stringValue, queryVector, limit);

            // expect recall to be at least 0.8
            List<float[]> resultVectors = getVectorsFromResult(resultSet);
            double recall = getRecall(vectorsByStringValue.get(stringValue), queryVector, resultVectors);
            assertThat(recall).isGreaterThanOrEqualTo(0.8);
        }
    }

    private UntypedResultSet search(String stringValue, float[] queryVector, int limit) throws Throwable
    {
        UntypedResultSet result = execute("SELECT * FROM %s WHERE str_val = '" + stringValue + "' AND val ann of " + Arrays.toString(queryVector) + " LIMIT " + limit);
        assertThat(result).hasSize(limit);
        return result;
    }

    private UntypedResultSet search(float[] queryVector, int limit) throws Throwable
    {
        UntypedResultSet result = execute("SELECT * FROM %s WHERE val ann of " + Arrays.toString(queryVector) + " LIMIT " + limit);
        assertThat(result).hasSize(limit);
        return result;
    }

    private List<float[]> searchWithRange(float[] queryVector, long minToken, long maxToken, int expectedSize) throws Throwable
    {
        UntypedResultSet result = execute("SELECT * FROM %s WHERE token(pk) <= " + maxToken + " AND token(pk) >= " + minToken + " AND val ann of " + Arrays.toString(queryVector) + " LIMIT 1000");
        assertThat(result).hasSize(expectedSize);
        return getVectorsFromResult(result);
    }

    private void searchWithNonExistingKey(float[] queryVector, int key) throws Throwable
    {
        searchWithKey(queryVector, key, 0);
    }

    private void searchWithKey(float[] queryVector, int key, int size) throws Throwable
    {
        UntypedResultSet result = execute("SELECT * FROM %s WHERE pk = " + key + " AND val ann of " + Arrays.toString(queryVector) + " LIMIT 1000");

        assertThat(result).hasSize(size);
        result.stream().forEach(row -> assertThat(row.getInt("pk")).isEqualTo(key));
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

    private double getRecall(Collection<float[]> vectors, float[] query, List<float[]> result)
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
        VectorType vectorType = VectorType.getInstance(FloatType.instance, dimensionCount);

        // verify results are part of inserted vectors
        for (UntypedResultSet.Row row: result)
        {
            vectors.add(vectorType.composeAsFloat(row.getBytes("val")));
        }

        return vectors;
    }
}
