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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.disk.v1.vector.hnsw.ConcurrentHnswGraphBuilder;
import org.apache.cassandra.index.sai.disk.v1.vector.hnsw.ConcurrentVectorValues;
import org.apache.cassandra.index.sai.disk.v1.vector.hnsw.HnswGraphSearcher;
import org.apache.cassandra.index.sai.disk.v1.vector.hnsw.NeighborQueue;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentBuilder;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.Bits;
import org.assertj.core.data.Percentage;
import org.junit.BeforeClass;
import org.junit.Test;
import smile.nlp.embedding.GloVe;
import smile.nlp.embedding.Word2Vec;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.in;

public class VectorLocalTest extends VectorTester
{
    private static Word2Vec word2vec;

    @BeforeClass
    public static void loadModel() throws Throwable
    {
        word2vec = GloVe.of(Path.of(VectorLocalTest.class.getClassLoader().getResource("glove.3K.50d.txt").getPath()));
    }

    @Test
    public void randomizedTest() throws Throwable
    {
        createTable(String.format("CREATE TABLE %%s (pk int, str_val text, val vector<float, %d>, PRIMARY KEY(pk))", word2vec.dimension()));
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        int vectorCount = getRandom().nextIntBetween(500, 1000);
        List<float[]> vectors = IntStream.range(0, vectorCount).mapToObj(s -> randomVector()).collect(Collectors.toList());

        int pk = 0;
        for (float[] vector : vectors)
            execute("INSERT INTO %s (pk, str_val, val) VALUES (?, 'A', " + vectorString(vector) + " )", pk++);

        // query memtable index
        int limit = Math.min(getRandom().nextIntBetween(30, 50), vectors.size());
        float[] queryVector = randomVector();
        UntypedResultSet resultSet = search(queryVector, limit);
        assertDescendingScore(queryVector, getVectorsFromResult(resultSet));

        flush();

        // query on-disk index
        queryVector = randomVector();
        resultSet = search(queryVector, limit);
        assertDescendingScore(queryVector, getVectorsFromResult(resultSet));

        // populate some more vectors
        int additionalVectorCount = getRandom().nextIntBetween(500, 1000);
        List<float[]> additionalVectors = IntStream.range(0, additionalVectorCount).mapToObj(s -> randomVector()).collect(Collectors.toList());
        for (float[] vector : additionalVectors)
            execute("INSERT INTO %s (pk, str_val, val) VALUES (?, 'A', " + vectorString(vector) + " )", pk++);

        vectors.addAll(additionalVectors);

        // query both memtable index and on-disk index
        queryVector = randomVector();
        resultSet = search(queryVector, limit);
        assertDescendingScore(queryVector, getVectorsFromResult(resultSet));

        flush();

        // query multiple on-disk indexes
        queryVector = randomVector();
        resultSet = search(queryVector, limit);
        assertDescendingScore(queryVector, getVectorsFromResult(resultSet));

        compact();

        // query compacted on-disk index
        queryVector = randomVector();
        resultSet = search(queryVector, limit);
        assertDescendingScore(queryVector, getVectorsFromResult(resultSet));
    }

    @Test
    public void multiSSTablesTest() throws Throwable
    {
        createTable(String.format("CREATE TABLE %%s (pk int, str_val text, val vector<float, %d>, PRIMARY KEY(pk))", word2vec.dimension()));
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        disableCompaction(keyspace());

        int sstableCount = getRandom().nextIntBetween(3, 6);
        int vectorCountPerSSTable = getRandom().nextIntBetween(200, 400);
        int pk = 0;

        int vectorCount = 0;
        // create multiple sstables
        List<float[]> allVectors = new ArrayList<>(sstableCount * vectorCountPerSSTable);
        for (int sstable = 0; sstable < sstableCount; sstable++)
        {
            for (int row = 0; row < vectorCountPerSSTable; row++)
            {
                String word = word2vec.words[vectorCount++];
                float[] vector = word2vec.get(word);
                execute("INSERT INTO %s (pk, str_val, val) VALUES (?, ?, " + vectorString(vector) + " )", pk++, word);
                allVectors.add(vector);
            }
            flush();
        }
        assertThat(getCurrentColumnFamilyStore(keyspace()).getLiveSSTables()).hasSize(sstableCount);

        // query multiple on-disk indexes
        int limit = Math.min(getRandom().nextIntBetween(30, 50), vectorCountPerSSTable);
        float[] queryVector = allVectors.get(getRandom().nextIntBetween(0, allVectors.size() - 1));
        UntypedResultSet resultSet = search(queryVector, limit);

        // expect recall to be at least 0.8
        List<float[]> resultVectors = getVectorsFromResult(resultSet);
        assertDescendingScore(queryVector, resultVectors);

        double recall = indexedRecall(allVectors, queryVector, resultVectors, limit);
        assertThat(recall).isGreaterThanOrEqualTo(0.8);
    }

    @Test
    public void partitionRestrictedTest() throws Throwable
    {
        createTable(String.format("CREATE TABLE %%s (pk int, str_val text, val vector<float, %d>, PRIMARY KEY(pk))", word2vec.dimension()));
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        int vectorCount = getRandom().nextIntBetween(500, 1000);

        for (int pk = 0; pk < vectorCount; pk++)
            execute("INSERT INTO %s (pk, str_val, val) VALUES (?, 'A', " + vectorString(word2vec.get(word2vec.words[pk])) + " )", pk);

        // query memtable index

        for (int executionCount = 0; executionCount < 50; executionCount++)
        {
            int key = getRandom().nextIntBetween(1, vectorCount - 1);
            float[] queryVector = word2vec.get(word2vec.words[getRandom().nextIntBetween(0, vectorCount - 1)]);
            searchWithKey(queryVector, key, 1);
        }

        flush();

        // query on-disk index with existing key:
        for (int executionCount = 0; executionCount < 50; executionCount++)
        {
            int key = getRandom().nextIntBetween(1, vectorCount - 1);
            float[] queryVector = word2vec.get(word2vec.words[getRandom().nextIntBetween(0, vectorCount - 1)]);
            searchWithKey(queryVector, key, 1);
        }

        // query on-disk index with non-existing key:
        for (int executionCount = 0; executionCount < 50; executionCount++)
        {
            int nonExistingKey = getRandom().nextIntBetween(1, vectorCount) + vectorCount;
            float[] queryVector = word2vec.get(word2vec.words[getRandom().nextIntBetween(0, vectorCount - 1)]);
            searchWithNonExistingKey(queryVector, nonExistingKey);
        }
    }

    @Test
    public void partitionRestrictedWidePartitionTest() throws Throwable
    {
        createTable(String.format("CREATE TABLE %%s (pk int, ck int, val vector<float, %d>, PRIMARY KEY(pk, ck))", word2vec.dimension()));
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        int partitions = getRandom().nextIntBetween(20, 40);
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
        createTable(String.format("CREATE TABLE %%s (pk int, str_val text, val vector<float, %d>, PRIMARY KEY(pk))", word2vec.dimension()));
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        int vectorCount = getRandom().nextIntBetween(500, 1000);

        int pk = 0;
        Multimap<Long, float[]> vectorsByToken = ArrayListMultimap.create();
        for (int index = 0; index < vectorCount; index++)
        {
            String word = word2vec.words[index];
            float[] vector = word2vec.get(word);
            vectorsByToken.put(Murmur3Partitioner.instance.getToken(Int32Type.instance.decompose(pk)).getLongValue(), vector);
            execute("INSERT INTO %s (pk, str_val, val) VALUES (?, ?, " + vectorString(vector) + " )", pk++, word);
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

            float[] queryVector = word2vec.get(word2vec.words[getRandom().nextIntBetween(0, vectorCount - 1)]);

            List<float[]> resultVectors = searchWithRange(queryVector, minToken, maxToken, expected.size());
            assertDescendingScore(queryVector, resultVectors);

            if (expected.isEmpty())
                assertThat(resultVectors).isEmpty();
            else
            {
                double recall = recallMatch(expected, resultVectors, expected.size());
                assertThat(recall).isGreaterThanOrEqualTo(0.8);
            }
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

            float[] queryVector = word2vec.get(word2vec.words[getRandom().nextIntBetween(0, vectorCount - 1)]);

            List<float[]> resultVectors = searchWithRange(queryVector, minToken, maxToken, expected.size());
            assertDescendingScore(queryVector, resultVectors);

            if (expected.isEmpty())
                assertThat(resultVectors).isEmpty();
            else
            {
                double recall = recallMatch(expected, resultVectors, expected.size());
                assertThat(recall).isGreaterThanOrEqualTo(0.8);
            }
        }
    }

    // test retrieval of multiple rows that have the same vector value
    @Test
    public void multipleSegmentsMultiplePostingsTest() throws Throwable
    {
        createTable(String.format("CREATE TABLE %%s (pk int, val vector<float, %d>, PRIMARY KEY(pk))", word2vec.dimension()));
        disableCompaction(KEYSPACE);

        int sstableCount = getRandom().nextIntBetween(3, 6);
        int vectorCountPerSSTable = getRandom().nextIntBetween(100, 200);
        int pk = 0;

        // create multiple sstables
        int vectorCount = 0;
        var population = new ArrayList<float[]>();
        for (int i = 0; i < sstableCount; i++)
        {
            for (int row = 0; row < vectorCountPerSSTable; row++)
            {
                float[] v = word2vec.get(word2vec.words[vectorCount++]);
                for (int j = 0; j < getRandom().nextIntBetween(1, 4); j++) {
                    execute("INSERT INTO %s (pk, val) VALUES (?, ?)", pk++, vector(v));
                    population.add(v);
                }
            }
            flush();
        }
        assertThat(getCurrentColumnFamilyStore(keyspace()).getLiveSSTables()).hasSize(sstableCount);

        // create index on existing sstable to produce multiple segments
        SegmentBuilder.updateLastValidSegmentRowId(50); // 50 rows per segment
        String index = createIndexAsync("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForIndexQueryable(index);
        // query multiple on-disk indexes
        var testCount = 200;
        var start = getRandom().nextIntBetween(vectorCount, word2vec.words.length - testCount);
        for (int i = start; i < start + testCount; i++)
        {
            var q = word2vec.get(word2vec.words[i]);
            int limit = Math.min(getRandom().nextIntBetween(10, 50), vectorCountPerSSTable);
            UntypedResultSet result = execute("SELECT * FROM %s ORDER BY val ann of ? LIMIT ?", vector(q), limit);
            assertThat(result).hasSize(limit);

            List<float[]> resultVectors = getVectorsFromResult(result);
            assertDescendingScore(q, resultVectors);

            double recall = bruteForceRecall(q, resultVectors, population, limit);
            assertThat(recall).isGreaterThanOrEqualTo(0.8);
        }
    }

    private double bruteForceRecall(float[] q, List<float[]> resultVectors, List<float[]> population, int limit)
    {
        List<float[]> expected = population
                                 .stream()
                                 .sorted(Comparator.comparingDouble(v -> -VectorSimilarityFunction.COSINE.compare(q, v)))
                                 .limit(limit)
                                 .collect(Collectors.toList());
        return recallMatch(expected, resultVectors, limit);
    }

    // search across multiple segments, combined with a non-ann predicate
    @Test
    public void multipleNonAnnSegmentsTest() throws Throwable
    {
        createTable(String.format("CREATE TABLE %%s (pk int, str_val text, val vector<float, %d>, PRIMARY KEY(pk))", word2vec.dimension()));
        disableCompaction(KEYSPACE);

        int sstableCount = getRandom().nextIntBetween(3, 6);
        int vectorCountPerSSTable = getRandom().nextIntBetween(200, 400);
        int pk = 0;

        // create multiple sstables
        Multimap<String, float[]> vectorsByStringValue = ArrayListMultimap.create();
        int vectorCount = 0;
        for (int i = 0; i < sstableCount; i++)
        {
            for (int row = 0; row < vectorCountPerSSTable; row++)
            {
                String stringValue = String.valueOf(pk % 10); // 10 different string values
                float[] vector = word2vec.get(word2vec.words[vectorCount++]);
                execute("INSERT INTO %s (pk, str_val, val) VALUES (?, ?, " + vectorString(vector) + " )", pk++, stringValue);
                vectorsByStringValue.put(stringValue, vector);
            }
            flush();
        }
        assertThat(getCurrentColumnFamilyStore(keyspace()).getLiveSSTables()).hasSize(sstableCount);

        // create indexes on existing sstable to produce multiple segments
        SegmentBuilder.updateLastValidSegmentRowId(50); // 50 rows per segment
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(str_val) USING 'StorageAttachedIndex'");

        // query multiple on-disk indexes
        for (String stringValue : vectorsByStringValue.keySet())
        {
            int limit = Math.min(getRandom().nextIntBetween(30, 50), vectorCountPerSSTable);
            float[] queryVector = vectorsByStringValue.get(stringValue).stream().findAny().get();
            UntypedResultSet resultSet = search(stringValue, queryVector, limit);

            // expect recall to be at least 0.8
            List<float[]> resultVectors = getVectorsFromResult(resultSet);
            assertDescendingScore(queryVector, resultVectors);

            double recall = indexedRecall(vectorsByStringValue.get(stringValue), queryVector, resultVectors, limit);
            assertThat(recall).isGreaterThanOrEqualTo(0.8);
        }
    }

    private UntypedResultSet search(String stringValue, float[] queryVector, int limit) throws Throwable
    {
        UntypedResultSet result = execute("SELECT * FROM %s WHERE str_val = '" + stringValue + "' ORDER BY val ann of " + Arrays.toString(queryVector) + " LIMIT " + limit);
        assertThat(result).hasSize(limit);
        return result;
    }

    private UntypedResultSet search(float[] queryVector, int limit) throws Throwable
    {
        UntypedResultSet result = execute("SELECT * FROM %s ORDER BY val ann of " + Arrays.toString(queryVector) + " LIMIT " + limit);
        assertThat(result.size()).isCloseTo(limit, Percentage.withPercentage(5));
        return result;
    }

    private List<float[]> searchWithRange(float[] queryVector, long minToken, long maxToken, int expectedSize) throws Throwable
    {
        UntypedResultSet result = execute("SELECT * FROM %s WHERE token(pk) <= " + maxToken + " AND token(pk) >= " + minToken + " ORDER BY val ann of " + Arrays.toString(queryVector) + " LIMIT 1000");
        assertThat(result.size()).isCloseTo(expectedSize, Percentage.withPercentage(5));
        return getVectorsFromResult(result);
    }

    private void searchWithNonExistingKey(float[] queryVector, int key) throws Throwable
    {
        searchWithKey(queryVector, key, 0);
    }

    private void searchWithKey(float[] queryVector, int key, int expectedSize) throws Throwable
    {
        UntypedResultSet result = execute("SELECT * FROM %s WHERE pk = " + key + " ORDER BY val ann of " + Arrays.toString(queryVector) + " LIMIT 1000");

        // VSTODO maybe we should have different methods for these cases
        if (expectedSize < 10)
            assertThat(result).hasSize(expectedSize);
        else
            assertThat(result.size()).isCloseTo(expectedSize, Percentage.withPercentage(5));
        result.stream().forEach(row -> assertThat(row.getInt("pk")).isEqualTo(key));
    }

    private String vectorString(float[] vector)
    {
        return Arrays.toString(vector);
    }

    private float[] randomVector()
    {
        float[] rawVector = new float[word2vec.dimension()];
        for (int i = 0; i < word2vec.dimension(); i++)
        {
            rawVector[i] = getRandom().nextFloat();
        }
        return rawVector;
    }

    private void assertDescendingScore(float[] queryVector, List<float[]> resultVectors)
    {
        float prevScore = -1;
        for (float[] current : resultVectors)
        {
            float score = VectorSimilarityFunction.COSINE.compare(current, queryVector);
            if (prevScore >= 0)
                assertThat(score).isLessThanOrEqualTo(prevScore);

            prevScore = score;
        }
    }

    private double indexedRecall(Collection<float[]> vectors, float[] query, List<float[]> result, int topK) throws IOException
    {
        ConcurrentVectorValues vectorValues = new ConcurrentVectorValues(query.length);
        int ordinal = 0;

        ConcurrentHnswGraphBuilder<float[]> graphBuilder = ConcurrentHnswGraphBuilder.create(vectorValues,
                                                                                             VectorEncoding.FLOAT32,
                                                                                             VectorSimilarityFunction.COSINE,
                                                                                             16,
                                                                                             100);

        for (float[] vector : vectors)
        {
            vectorValues.add(ordinal, vector);
            graphBuilder.addGraphNode(ordinal++, vectorValues);
        }

        NeighborQueue queue = HnswGraphSearcher.search(query,
                                                       topK,
                                                       vectorValues,
                                                       VectorEncoding.FLOAT32,
                                                       VectorSimilarityFunction.COSINE,
                                                       graphBuilder.getGraph().getView(),
                                                       new Bits.MatchAllBits(vectors.size()),
                                                       Integer.MAX_VALUE);

        List<float[]> nearestNeighbors = new ArrayList<>();
        while (queue.size() > 0)
            nearestNeighbors.add(vectorValues.vectorValue(queue.pop()));

        return recallMatch(nearestNeighbors, result, topK);
    }

    private double recallMatch(List<float[]> expected, List<float[]> actual, int topK)
    {
        if (expected.size() == 0 && actual.size() == 0)
            return 1.0;

        int matches = 0;
        for (float[] in : expected)
        {
            for (float[] out : actual)
            {
                if (Arrays.compare(in, out) ==0)
                {
                    matches++;
                    break;
                }
            }
        }

        return (double) matches / topK;
    }

    private List<float[]> getVectorsFromResult(UntypedResultSet result)
    {
        List<float[]> vectors = new ArrayList<>();
        VectorType<?> vectorType = VectorType.getInstance(FloatType.instance, word2vec.dimension());

        // verify results are part of inserted vectors
        for (UntypedResultSet.Row row: result)
        {
            vectors.add(vectorType.composeAsFloat(row.getBytes("val")));
        }

        return vectors;
    }
}
