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

import java.util.stream.IntStream;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.index.sai.disk.v1.SegmentBuilder;

import static org.assertj.core.api.Assertions.assertThat;

public class VectorComparisonCachingTest extends VectorTester
{

    @BeforeClass
    public static void setupClass() throws Exception
    {
        requireNetwork();
        startJMXServer();
        createMBeanServerConnection();
    }

    @Test
    public void testSearchLowRowCountHitsCachedScoresWithSegments() throws Throwable
    {
        createTable(KEYSPACE, "CREATE TABLE %s (pk int, ck int, vec vector<float, 2>, primary key(pk, ck))");
        // Use euclidean distance to more easily verify correctness of caching
        createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex' WITH OPTIONS = { 'similarity_function' : 'euclidean' }");

        // populate 3 sstables with 1 row each
        Vector<Float> q = vector(1f, 2f);
        execute("INSERT INTO %s (pk, ck, vec) VALUES (0, 1, ?)", vector(1f, 1f));
        flush();

        execute("INSERT INTO %s (pk, ck, vec) VALUES (0, 2, ?)", vector(1f, 2f));
        flush();

        execute("INSERT INTO %s (pk, ck, vec) VALUES (0, 3, ?)", vector(1f, 3f));
        flush();

        // 1 row per segment
        SegmentBuilder.updateLastValidSegmentRowId(0);
        // one sstable with 3 segments, each has one row
        compact(KEYSPACE);


        var similarityScoreCacheLookups = getMetricValue(objectNameNoIndex("Lookups", CQLTester.KEYSPACE, currentTable(), "SimilarityScoreCache"));
        var similarityScoreCacheHits = getMetricValue(objectNameNoIndex("Hits", CQLTester.KEYSPACE, currentTable(), "SimilarityScoreCache"));

        assertThat(similarityScoreCacheLookups).isEqualTo(0L);
        assertThat(similarityScoreCacheHits).isEqualTo(0L);

        var result = execute("SELECT pk, ck FROM %s ORDER BY vec ANN OF ? LIMIT 100", q);
        assertRows(result, row(0, 2), row(0, 1), row(0, 3));
        similarityScoreCacheLookups = getMetricValue(objectNameNoIndex("Lookups", CQLTester.KEYSPACE, currentTable(), "SimilarityScoreCache"));
        similarityScoreCacheHits = getMetricValue(objectNameNoIndex("Hits", CQLTester.KEYSPACE, currentTable(), "SimilarityScoreCache"));
        assertThat(similarityScoreCacheLookups).isEqualTo(3L);
        // Non-hybrid queries do not have brute force search, so we expect cache hits here
        assertThat(similarityScoreCacheHits).isEqualTo(3L);
    }

    @Test
    public void testSearchHighRowCountMissesCachedScoresWithSegments() throws Throwable
    {
        createTable(KEYSPACE, "CREATE TABLE %s (pk int, ck int, vec vector<float, 2>, primary key(pk, ck))");
        // Use euclidean distance to more easily verify correctness of caching
        createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex' WITH OPTIONS = { 'similarity_function' : 'euclidean' }");

        // Choose the first vector for simplicity of ordering verification
        Vector<Float> q = vector(1f, 0f);
        // populate 3 sstables with 1000 rows each
        for (int i = 1; i < 3; i++)
        {
            for (int j = 0; j < 100; j++)
            {
                // Vectors are stored as [i, j]
                execute("INSERT INTO %s (pk, ck, vec) VALUES (0, ?, ?)", j, vector((float) i, (float) j));
            }
            flush();
        }

        // one sstable with 3 segments
        compact(KEYSPACE);


        var similarityScoreCacheLookups = getMetricValue(objectNameNoIndex("Lookups", CQLTester.KEYSPACE, currentTable(), "SimilarityScoreCache"));
        var similarityScoreCacheHits = getMetricValue(objectNameNoIndex("Hits", CQLTester.KEYSPACE, currentTable(), "SimilarityScoreCache"));

        assertThat(similarityScoreCacheLookups).isEqualTo(0L);
        assertThat(similarityScoreCacheHits).isEqualTo(0L);

        var expected = IntStream.range(0, 100).mapToObj(i -> row(0, i)).toArray();

        var result = execute("SELECT pk, ck FROM %s ORDER BY vec ANN OF ? LIMIT 100", q);
        assertThat(result.stream().map(r -> row(r.getInt("pk"), r.getInt("ck"))).toArray()).isEqualTo(expected);
        similarityScoreCacheLookups = getMetricValue(objectNameNoIndex("Lookups", CQLTester.KEYSPACE, currentTable(), "SimilarityScoreCache"));
        similarityScoreCacheHits = getMetricValue(objectNameNoIndex("Hits", CQLTester.KEYSPACE, currentTable(), "SimilarityScoreCache"));
        assertThat(similarityScoreCacheLookups).isEqualTo(100L);
        assertThat(similarityScoreCacheHits).isEqualTo(100L);
    }

    @Test
    public void testHybridSearchLowRowCountMissesCachedScoresWithSegments() throws Throwable
    {
        createTable(KEYSPACE, "CREATE TABLE %s (pk int, ck int, val int, vec vector<float, 2>, primary key(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        // Use euclidean distance to more easily verify correctness of caching
        createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex' WITH OPTIONS = { 'similarity_function' : 'euclidean' }");

        // populate 3 sstables with 1 row each
        Vector<Float> q = vector(1f, 2f);
        execute("INSERT INTO %s (pk, ck, val, vec) VALUES (0, 1, 0, ?)", vector(1f, 1f));
        flush();

        execute("INSERT INTO %s (pk, ck, val, vec) VALUES (0, 2, 0, ?)", vector(1f, 2f));
        flush();

        execute("INSERT INTO %s (pk, ck, val, vec) VALUES (0, 3, 0, ?)", vector(1f, 3f));
        flush();

        // 1 row per segment
        SegmentBuilder.updateLastValidSegmentRowId(0);
        // one sstable with 3 segments, each has one row
        compact(KEYSPACE);


        var similarityScoreCacheLookups = getMetricValue(objectNameNoIndex("Lookups", CQLTester.KEYSPACE, currentTable(), "SimilarityScoreCache"));
        var similarityScoreCacheHits = getMetricValue(objectNameNoIndex("Hits", CQLTester.KEYSPACE, currentTable(), "SimilarityScoreCache"));

        assertThat(similarityScoreCacheLookups).isEqualTo(0L);
        assertThat(similarityScoreCacheHits).isEqualTo(0L);

        var result = execute("SELECT pk, ck FROM %s WHERE val = 0 ORDER BY vec ANN OF ? LIMIT 100", q);
        assertRows(result, row(0, 2), row(0, 1), row(0, 3));
        similarityScoreCacheLookups = getMetricValue(objectNameNoIndex("Lookups", CQLTester.KEYSPACE, currentTable(), "SimilarityScoreCache"));
        similarityScoreCacheHits = getMetricValue(objectNameNoIndex("Hits", CQLTester.KEYSPACE, currentTable(), "SimilarityScoreCache"));
        assertThat(similarityScoreCacheLookups).isEqualTo(3L);
        // We miss the cache here because we compute the topk with brute force
        assertThat(similarityScoreCacheHits).isEqualTo(0L);
    }

    @Test
    public void testHybridSearchHighRowCountHitsCachedScoresWithSegments() throws Throwable
    {
        createTable(KEYSPACE, "CREATE TABLE %s (pk int, ck int, val int, vec vector<float, 2>, primary key(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        // Use euclidean distance to more easily verify correctness of caching
        createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex' WITH OPTIONS = { 'similarity_function' : 'euclidean' }");

        // Choose the first vector for simplicity of ordering verification
        Vector<Float> q = vector(1f, 0f);
        // populate 3 sstables with 1000 rows each
        for (int i = 1; i < 3; i++)
        {
            for (int j = 0; j < 100; j++)
            {
                // Vectors are stored as [i, j]
                execute("INSERT INTO %s (pk, ck, val, vec) VALUES (0, ?, 0, ?)", j, vector((float) i, (float) j));
            }
            flush();
        }

        // one sstable with 3 segments
        compact(KEYSPACE);


        var similarityScoreCacheLookups = getMetricValue(objectNameNoIndex("Lookups", CQLTester.KEYSPACE, currentTable(), "SimilarityScoreCache"));
        var similarityScoreCacheHits = getMetricValue(objectNameNoIndex("Hits", CQLTester.KEYSPACE, currentTable(), "SimilarityScoreCache"));

        assertThat(similarityScoreCacheLookups).isEqualTo(0L);
        assertThat(similarityScoreCacheHits).isEqualTo(0L);

        var expected = IntStream.range(0, 100).mapToObj(i -> row(0, i)).toArray();

        var result = execute("SELECT pk, ck FROM %s WHERE val = 0 ORDER BY vec ANN OF ? LIMIT 100", q);
        assertThat(result.stream().map(r -> row(r.getInt("pk"), r.getInt("ck"))).toArray()).isEqualTo(expected);
        similarityScoreCacheLookups = getMetricValue(objectNameNoIndex("Lookups", CQLTester.KEYSPACE, currentTable(), "SimilarityScoreCache"));
        similarityScoreCacheHits = getMetricValue(objectNameNoIndex("Hits", CQLTester.KEYSPACE, currentTable(), "SimilarityScoreCache"));
        assertThat(similarityScoreCacheLookups).isEqualTo(100L);
        assertThat(similarityScoreCacheHits).isEqualTo(100L);
    }
}
