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

package org.apache.cassandra.distributed.test.sai;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class VectorDistributedTest extends TestBaseImpl
{
    @Rule
    public SAITester.FailureWatcher failureRule = new SAITester.FailureWatcher();

    private static final String CREATE_KEYSPACE = "CREATE KEYSPACE %%s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': %d}";
    private static final String CREATE_TABLE = "CREATE TABLE %%s (pk int primary key, val vector<float, %d>)";
    private static final String CREATE_INDEX = "CREATE INDEX ON %%s(%s) USING 'sai' WITH OPTIONS={'optimize_for':'recall'}";

    private static final VectorSimilarityFunction function = IndexWriterConfig.DEFAULT_SIMILARITY_FUNCTION;

    private static final double MIN_RECALL = 0.7;

    private static final int NUM_REPLICAS = 3;
    private static final int RF = 2;

    private static final AtomicInteger seq = new AtomicInteger();
    private static String table;

    private static Cluster cluster;

    private static int dimensionCount;

    @BeforeClass
    public static void setupCluster() throws Exception
    {
        cluster = Cluster.build(NUM_REPLICAS)
                         .withTokenCount(1)
                         .withDataDirCount(1) // VSTODO Vector memtable flush doesn't support multiple directories yet
                         .withConfig(config -> config.with(GOSSIP)
                                                     .with(NETWORK)
                                                     .set("memtable_allocation_type", Config.MemtableAllocationType.offheap_objects)
                                                     .set("memtable_heap_space", "20MiB"))
                         .start();

        cluster.schemaChange(withKeyspace(String.format(CREATE_KEYSPACE, RF)));
    }

    @AfterClass
    public static void closeCluster()
    {
        if (cluster != null)
            cluster.close();
    }

    @Before
    public void before()
    {
        table = "table_" + seq.getAndIncrement();
        dimensionCount = SAITester.getRandom().nextIntBetween(100, 2048);
    }

    @After
    public void after()
    {
        cluster.schemaChange(formatQuery("DROP TABLE IF EXISTS %s"));
    }

    @Test
    public void testVectorSearch()
    {
        cluster.schemaChange(formatQuery(String.format(CREATE_TABLE, dimensionCount)));
        cluster.schemaChange(formatQuery(String.format(CREATE_INDEX, "val")));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);

        int vectorCount = SAITester.getRandom().nextIntBetween(500, 1000);
        List<float[]> vectors = generateVectors(vectorCount);

        int pk = 0;
        for (float[] vector : vectors)
            execute("INSERT INTO %s (pk, val) VALUES (" + (pk++) + ", " + vectorString(vector) + " )");

        // query memtable index
        int limit = Math.min(SAITester.getRandom().nextIntBetween(10, 50), vectors.size());
        float[] queryVector = randomVector();
        Object[][] result = searchWithLimit(queryVector, limit);

        List<float[]> resultVectors = getVectors(result);
        assertDescendingScore(queryVector, resultVectors);

        assertThatThrownBy(() -> searchWithoutLimit(randomVector(), vectorCount))
        .hasMessageContaining(SelectStatement.TOPK_LIMIT_ERROR);

        int pageSize = SAITester.getRandom().nextIntBetween(40, 70);
        limit = SAITester.getRandom().nextIntBetween(20, 50);
        result = searchWithPageAndLimit(queryVector, pageSize, limit);

        resultVectors = getVectors(result);
        assertDescendingScore(queryVector, resultVectors);
        double memtableRecallWithPaging = getRecall(vectors, queryVector, resultVectors);
        assertThat(memtableRecallWithPaging).isGreaterThanOrEqualTo(MIN_RECALL);

        assertThatThrownBy(() -> searchWithPageWithoutLimit(randomVector()))
        .hasMessageContaining(SelectStatement.TOPK_LIMIT_ERROR);

        // query on-disk index
        cluster.forEach(n -> n.flush(KEYSPACE));

        limit = Math.min(SAITester.getRandom().nextIntBetween(10, 50), vectors.size());
        queryVector = randomVector();
        result = searchWithLimit(queryVector, limit);
        double sstableRecall = getRecall(vectors, queryVector, getVectors(result));
        assertThat(sstableRecall).isGreaterThanOrEqualTo(MIN_RECALL);
    }

    @Test
    public void testMultiSSTablesVectorSearch()
    {
        cluster.schemaChange(formatQuery(String.format(CREATE_TABLE, dimensionCount)));
        cluster.schemaChange(formatQuery(String.format(CREATE_INDEX, "val")));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);
        // disable compaction
        String tableName = table;
        cluster.forEach(n -> n.runOnInstance(() -> {
            Keyspace keyspace = Keyspace.open(KEYSPACE);
            keyspace.getColumnFamilyStore(tableName).disableAutoCompaction();
        }));

        int vectorCountPerSSTable = SAITester.getRandom().nextIntBetween(200, 500);
        int sstableCount = SAITester.getRandom().nextIntBetween(3, 5);
        List<float[]> allVectors = new ArrayList<>(sstableCount * vectorCountPerSSTable);

        int pk = 0;
        for (int i = 0; i < sstableCount; i++)
        {
            List<float[]> vectors = generateVectors(vectorCountPerSSTable);
            for (float[] vector : vectors)
                execute("INSERT INTO %s (pk, val) VALUES (" + (pk++) + ", " + vectorString(vector) + " )");

            allVectors.addAll(vectors);
            cluster.forEach(n -> n.flush(KEYSPACE));
        }

        // query multiple sstable indexes in multiple node
        int limit = Math.min(SAITester.getRandom().nextIntBetween(50, 100), allVectors.size());
        float[] queryVector = randomVector();
        Object[][] result = searchWithLimit(queryVector, limit);

        // expect recall to be at least 0.8
        List<float[]> resultVectors = getVectors(result);
        assertDescendingScore(queryVector, resultVectors);
        double recall = getRecall(allVectors, queryVector, getVectors(result));
        assertThat(recall).isGreaterThanOrEqualTo(MIN_RECALL);
    }

    @Test
    public void testPartitionRestrictedVectorSearch()
    {
        cluster.schemaChange(formatQuery(String.format(CREATE_TABLE, dimensionCount)));
        cluster.schemaChange(formatQuery(String.format(CREATE_INDEX, "val")));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);

        int vectorCount = SAITester.getRandom().nextIntBetween(500, 1000);
        List<float[]> vectors = generateVectors(vectorCount);

        int pk = 0;
        for (float[] vector : vectors)
            execute("INSERT INTO %s (pk, val) VALUES (" + (pk++) + ", " + vectorString(vector) + " )");

        // query memtable index
        for (int executionCount = 0; executionCount < 50; executionCount++)
        {
            int key = SAITester.getRandom().nextIntBetween(0, vectorCount - 1);
            float[] queryVector = randomVector();
            searchByKeyWithLimit(key, queryVector, vectors);
        }

        cluster.forEach(n -> n.flush(KEYSPACE));

        // query on-disk index
        for (int executionCount = 0; executionCount < 50; executionCount++)
        {
            int key = SAITester.getRandom().nextIntBetween(0, vectorCount - 1);
            float[] queryVector = randomVector();
            searchByKeyWithLimit(key, queryVector, vectors);
        }
    }

    @Test
    public void rangeRestrictedTest()
    {
        cluster.schemaChange(formatQuery(String.format(CREATE_TABLE, dimensionCount)));
        cluster.schemaChange(formatQuery(String.format(CREATE_INDEX, "val")));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);

        int vectorCount = SAITester.getRandom().nextIntBetween(500, 1000);
        List<float[]> vectors = IntStream.range(0, vectorCount).mapToObj(s -> randomVector()).collect(Collectors.toList());

        int pk = 0;
        Multimap<Long, float[]> vectorsByToken = ArrayListMultimap.create();
        for (float[] vector : vectors)
        {
            vectorsByToken.put(Murmur3Partitioner.instance.getToken(Int32Type.instance.decompose(pk)).getLongValue(), vector);
            execute("INSERT INTO %s (pk, val) VALUES (" + (pk++) + ',' + vectorString(vector) + " )");
        }

        // query memtable index
        for (int executionCount = 0; executionCount < 50; executionCount++)
        {
            int key1 = SAITester.getRandom().nextIntBetween(1, vectorCount * 2);
            long token1 = Murmur3Partitioner.instance.getToken(Int32Type.instance.decompose(key1)).getLongValue();
            int key2 = SAITester.getRandom().nextIntBetween(1, vectorCount * 2);
            long token2 = Murmur3Partitioner.instance.getToken(Int32Type.instance.decompose(key2)).getLongValue();

            long minToken = Math.min(token1, token2);
            long maxToken = Math.max(token1, token2);
            List<float[]> expected = vectorsByToken.entries().stream()
                                                   .filter(e -> e.getKey() >= minToken && e.getKey() <= maxToken)
                                                   .map(Map.Entry::getValue)
                                                   .collect(Collectors.toList());

            float[] queryVector = randomVector();
            List<float[]> resultVectors = searchWithRange(queryVector, minToken, maxToken, expected.size());
            if (expected.isEmpty())
                assertThat(resultVectors).isEmpty();
            else
            {
                double recall = getRecall(resultVectors, queryVector, expected);
                assertThat(recall).isGreaterThanOrEqualTo(0.6);
            }
        }

        cluster.forEach(n -> n.flush(KEYSPACE));

        // query on-disk index with existing key:
        for (int executionCount = 0; executionCount < 50; executionCount++)
        {
            int key1 = SAITester.getRandom().nextIntBetween(1, vectorCount * 2);
            long token1 = Murmur3Partitioner.instance.getToken(Int32Type.instance.decompose(key1)).getLongValue();
            int key2 = SAITester.getRandom().nextIntBetween(1, vectorCount * 2);
            long token2 = Murmur3Partitioner.instance.getToken(Int32Type.instance.decompose(key2)).getLongValue();

            long minToken = Math.min(token1, token2);
            long maxToken = Math.max(token1, token2);
            List<float[]> expected = vectorsByToken.entries().stream()
                                                   .filter(e -> e.getKey() >= minToken && e.getKey() <= maxToken)
                                                   .map(Map.Entry::getValue)
                                                   .collect(Collectors.toList());

            float[] queryVector = randomVector();

            List<float[]> resultVectors = searchWithRange(queryVector, minToken, maxToken, expected.size());
            if (expected.isEmpty())
                assertThat(resultVectors).isEmpty();
            else
            {
                double recall = getRecall(resultVectors, queryVector, expected);
                assertThat(recall).isGreaterThanOrEqualTo(0.8);
            }
        }
    }

    private List<float[]> searchWithRange(float[] queryVector, long minToken, long maxToken, int expectedSize)
    {
        Object[][] result = execute("SELECT val FROM %s WHERE token(pk) <= " + maxToken + " AND token(pk) >= " + minToken + " ORDER BY val ann of " + Arrays.toString(queryVector) + " LIMIT 1000");
        assertThat(result).hasNumberOfRows(expectedSize);
        return getVectors(result);
    }

    private Object[][] searchWithLimit(float[] queryVector, int limit)
    {
        Object[][] result = execute("SELECT val FROM %s ORDER BY val ann of " + Arrays.toString(queryVector) + " LIMIT " + limit);
        assertThat(result).hasNumberOfRows(limit);
        return result;
    }

    private void searchWithoutLimit(float[] queryVector, int results)
    {
        Object[][] result = execute("SELECT val FROM %s ORDER BY val ann of " + Arrays.toString(queryVector));
        assertThat(result).hasNumberOfRows(results);
    }


    private void searchWithPageWithoutLimit(float[] queryVector)
    {
        executeWithPaging("SELECT val FROM %s ORDER BY val ann of " + Arrays.toString(queryVector), 10);
    }

    private Object[][] searchWithPageAndLimit(float[] queryVector, int pageSize, int limit)
    {
        // we don't know how many will be returned in case of paging, because coordinator resumes from last-returned-row's partiton
        return executeWithPaging("SELECT val FROM %s ORDER BY val ann of " + Arrays.toString(queryVector) + " LIMIT " + limit, pageSize);
    }

    private void searchByKeyWithLimit(int key, float[] queryVector, List<float[]> vectors)
    {
        Object[][] result = execute("SELECT val FROM %s WHERE pk = " + key + " ORDER BY val ann of " + Arrays.toString(queryVector) + " LIMIT 1");
        assertThat(result).hasNumberOfRows(1);
        float[] output = getVectors(result).get(0);
        assertThat(output).isEqualTo(vectors.get(key));
    }

    private void assertDescendingScore(float[] queryVector, List<float[]> resultVectors)
    {
        float prevScore = -1;
        for (float[] current : resultVectors)
        {
            float score = function.compare(current, queryVector);
            if (prevScore >= 0)
                assertThat(score).isLessThanOrEqualTo(prevScore);

            prevScore = score;
        }
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

    private List<float[]> generateVectors(int vectorCount)
    {
        return IntStream.range(0, vectorCount).mapToObj(s -> randomVector()).collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    private List<float[]> getVectors(Object[][] result)
    {
        List<float[]> vectors = new ArrayList<>();

        // verify results are part of inserted vectors
        for (Object[] obj: result)
        {
            List<Float> list = (List<Float>) obj[0];
            float[] array = new float[list.size()];
            for (int index = 0; index < list.size(); index++)
                array[index] = list.get(index);
            vectors.add(array);
        }

        return vectors;
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
            rawVector[i] = SAITester.getRandom().nextFloat();
        }
        return rawVector;
    }

    private static Object[][] execute(String query)
    {
        return cluster.coordinator(1).execute(formatQuery(query), ConsistencyLevel.ONE);
    }

    private static Object[][] executeWithPaging(String query, int pageSize)
    {
        Iterator<Object[]> iterator = cluster.coordinator(1).executeWithPaging(formatQuery(query), ConsistencyLevel.ONE, pageSize);
        List<Object[]> list = new ArrayList<>();
        iterator.forEachRemaining(list::add);

        return list.toArray(new Object[0][]);
    }

    private static String formatQuery(String query)
    {
        return String.format(query, KEYSPACE + '.' + table);
    }
}
