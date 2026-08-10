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

import java.util.Collection;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph;
import org.apache.cassandra.index.sai.disk.vector.JVectorVersionUtil;
import org.apache.cassandra.index.sai.metrics.ColumnQueryMetrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class VectorMetricsTest extends VectorTester
{
    @Parameterized.Parameter
    public Version version;

    @Parameterized.Parameters(name = "version={0}")
    public static Collection<Object[]> data()
    {
        // Test all versions that support vector indexes
        return Version.ALL.stream()
                          .filter(v -> v.onOrAfter(Version.JVECTOR_EARLIEST))
                          .map(v -> new Object[]{ v })
                          .collect(Collectors.toList());
    }

    @BeforeClass
    public static void setUpClass()
    {
        VectorTester.setUpClass();
    }

    @Before
    @Override
    public void setup() throws Throwable
    {
        super.setup();
        SAIUtil.setCurrentVersion(version);
    }

    @Test
    public void testBasicColumnQueryMetricsVectorIndexMetrics()
    {
        createTable("CREATE TABLE %s (pk int, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        disableCompaction();

        // Get the metrics
        var index = getCurrentColumnFamilyStore().indexManager.listIndexes().iterator().next();
        assert index instanceof StorageAttachedIndex;
        var saiIndex = (StorageAttachedIndex) index;
        var vectorMetrics = (ColumnQueryMetrics.VectorIndexMetrics) saiIndex.getIndexContext().getColumnQueryMetrics();

        // Expect all metrics to be 0
        assertEquals(0, vectorMetrics.annNodesVisited.getCount());
        assertEquals(0, vectorMetrics.annNodesReranked.getCount());
        assertEquals(0, vectorMetrics.annNodesExpanded.getCount());
        assertEquals(0, vectorMetrics.annNodesExpandedBaseLayer.getCount());
        assertEquals(0, vectorMetrics.annGraphSearches.getCount());
        assertEquals(0, vectorMetrics.annGraphResumes.getCount());
        assertEquals(0, vectorMetrics.bruteForceNodesVisited.getCount());
        assertEquals(0, vectorMetrics.bruteForceNodesReranked.getCount());
        assertEquals(0, vectorMetrics.quantizationMemoryBytes.sum());
        assertEquals(0, vectorMetrics.ordinalsMapMemoryBytes.sum());
        assertEquals(0, vectorMetrics.onDiskGraphsCount.sum());
        assertEquals(0, vectorMetrics.onDiskGraphVectorsCount.sum());
        assertEquals(0, vectorMetrics.annGraphSearchLatency.getCount());

        for (int i = 0; i < CassandraOnHeapGraph.MIN_PQ_ROWS; i++)
            execute("INSERT INTO %s (pk, val) VALUES (?, ?)", i, vector(1 + i, 2 + i, 3 + i));
        flush();

        // FA always uses FusedPQ; FB+ uses it only when cassandra.sai.vector.enable_fused=true (default false).
        // Pre-FA versions never use FusedPQ.
        boolean fusedPQ = JVectorVersionUtil.shouldWriteFused(version);
        long pqMemoryAfterFlush = vectorMetrics.quantizationMemoryBytes.sum();

        if (fusedPQ)
        {
            // With FusedPQ, quantized vectors are stored inline with graph nodes, so no separate PQ memory
            assertEquals("Version " + version + " should have no separate PQ memory with FusedPQ",
                        0L, pqMemoryAfterFlush);
        }
        else
        {
            // Without FusedPQ, PQ vectors are stored separately, so we expect some memory usage
            assertTrue("Version " + version + " should have PQ memory without FusedPQ",
                      pqMemoryAfterFlush > 0);
        }

        assertEquals(0, vectorMetrics.ordinalsMapMemoryBytes.sum()); // unique vectors means no cache required
        assertEquals(1, vectorMetrics.onDiskGraphsCount.sum());
        assertEquals(CassandraOnHeapGraph.MIN_PQ_ROWS, vectorMetrics.onDiskGraphVectorsCount.sum());

        compact();
        long pqMemoryAfterCompaction = vectorMetrics.quantizationMemoryBytes.sum();

        if (fusedPQ)
        {
            // With FusedPQ, quantized vectors are stored inline with graph nodes, so no separate PQ memory
            assertEquals("Version " + version + " should have no separate PQ memory with FusedPQ after compaction",
                        0L, pqMemoryAfterCompaction);
        }
        else
        {
            // Without FusedPQ, PQ vectors are stored separately, so we expect some memory usage
            assertTrue("Version " + version + " should have PQ memory without FusedPQ after compaction",
                      pqMemoryAfterCompaction > 0);
        }
        
        assertEquals(0, vectorMetrics.ordinalsMapMemoryBytes.sum()); // unique vectors means no cache required
        assertEquals(1, vectorMetrics.onDiskGraphsCount.sum());
        assertEquals(CassandraOnHeapGraph.MIN_PQ_ROWS, vectorMetrics.onDiskGraphVectorsCount.sum());

        // Now run a pure ann query and verify we hit the ann graph
        var result = execute("SELECT pk FROM %s ORDER BY val ANN OF [2.5, 3.5, 4.5] LIMIT 2");
        assertThat(result).hasSize(2);

        assertTrue(vectorMetrics.annNodesVisited.getCount() > 0);
        assertTrue(vectorMetrics.annNodesReranked.getCount() > 0);
        assertTrue(vectorMetrics.annNodesExpanded.getCount() > 0);
        assertTrue(vectorMetrics.annNodesExpandedBaseLayer.getCount() > 0);
        assertEquals(1, vectorMetrics.annGraphSearches.getCount());
        // We shouldn't need to resume when searching in this scenario
        assertEquals(0, vectorMetrics.annGraphResumes.getCount());
        assertEquals(1, vectorMetrics.annGraphSearchLatency.getCount());

        // Now do a hybrid query that is guaranteed to use brute force. (We have to override this because
        // the super class sets it with a random value that can lead to graph search.)
        setMaxBruteForceRows(100);
        result = execute("SELECT pk FROM %s WHERE pk = 0 ORDER BY val ANN OF [2.5, 3.5, 4.5] LIMIT 2");
        assertThat(result).hasSize(1);

        // We don't do approximate scores if there are few enough rows
        assertEquals(0, vectorMetrics.bruteForceNodesVisited.getCount());
        assertEquals(1, vectorMetrics.bruteForceNodesReranked.getCount());
        assertEquals(1, vectorMetrics.annGraphSearchLatency.getCount());

        // Confirm that truncating the table which will remove the index also drops the gauges
        truncate(false);
        assertEquals(0, vectorMetrics.quantizationMemoryBytes.sum());
        assertEquals(0, vectorMetrics.ordinalsMapMemoryBytes.sum());
        assertEquals(0, vectorMetrics.onDiskGraphsCount.sum());
        assertEquals(0, vectorMetrics.onDiskGraphVectorsCount.sum());
    }
}
