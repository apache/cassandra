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

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

public class VectorRecallOptimizedTest extends VectorTester
{
    @Test
    public void latencyVsRecallTest() throws Throwable
    {
        // we don't have an easy way to check nodes visited at this high level,
        // so we'll do enough queries that it should be visible in the similarity of the results found
        createTable("CREATE TABLE %s (pk int, v vector<float, 100>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX v_idx ON %s(v) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        IntStream.range(0, 10_000).parallel().forEach(i -> {
            try
            {
                execute("INSERT INTO %s (pk, v) VALUES (?, ?)", i, randomVector(100));
            }
            catch (Throwable e)
            {
                throw new RuntimeException(e);
            }
        });
        flush();

        var queryVectors = IntStream.range(0, 10).mapToObj(__ -> randomVector(100)).collect(Collectors.toList());

        double latencyOptimizedSimilarity = 0;
        for (int k = 1; k <= 128; k *= 2)
        {
            for (var q : queryVectors)
            {
                UntypedResultSet result = execute("SELECT similarity_cosine(?, v) as s FROM %s ORDER BY v ann of ? LIMIT ?",
                                                  q, q, k);
                assertThat(result).hasSize(k);
                for (var row : result)
                    latencyOptimizedSimilarity += row.getFloat("s");
            }
        }

        dropIndex("DROP INDEX %s.v_idx");
        createIndex("CREATE CUSTOM INDEX v_idx ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'optimize_for': 'recall'}");
        waitForIndexQueryable(300); // CI is slow

        double recallOptimizedSimilarity = 0;
        for (int k = 1; k <= 128; k *= 2)
        {
            for (var q : queryVectors)
            {
                UntypedResultSet result = execute("SELECT similarity_cosine(?, v) as s FROM %s ORDER BY v ann of ? LIMIT ?",
                                                  q, q, k);
                assertThat(result).hasSize(k);
                for (var row : result)
                    recallOptimizedSimilarity += row.getFloat("s");

            }
        }

        assertTrue(String.format("%.1s >= %.1s", latencyOptimizedSimilarity, recallOptimizedSimilarity),
                   latencyOptimizedSimilarity < recallOptimizedSimilarity);
    }
}
