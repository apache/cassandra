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

package org.apache.cassandra.index.sai.disk.hnsw;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.index.sai.disk.hnsw.CompactionVectorValues;
import org.apache.cassandra.index.sai.disk.hnsw.OnDiskVectors;
import org.apache.cassandra.index.sai.disk.hnsw.VectorCache;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.lucene.util.hnsw.HnswGraph;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.assertj.core.api.Assertions.assertThat;

public class VectorCacheTest
{
    private int dimension;
    private int expectedCached;
    private CompactionVectorValues vectorValues;
    private Path testDirectory;
    private OnDiskVectors onDiskVectors;
    private HnswGraph hnsw;
    private VectorCache cache;

    @Before
    public void setup() throws IOException
    {
        dimension = 10;
        var topLevelCount = 20;
        var M = 16;
        expectedCached = M >> 1; // ensures BFS should visit entry node + a proper subset of its neighbors
        var cacheCapacity = expectedCached * dimension * Float.BYTES;
        var levels = 2;
        // RandomlyConnectedHnswGraph cuts number of nodes in half as we go up levels
        var totalOrdinals = (1 << (levels - 1)) * topLevelCount;
        hnsw = new RandomlyConnectedHnswGraph.Builder().addLevels(levels, totalOrdinals, M).build();

        testDirectory = Files.createTempDirectory("VectorCacheTest");
        var vectorFile = new File(testDirectory, "vectors");
        generateVectors(totalOrdinals);
        var offset = writeVectorsToDisk(vectorFile);
        try (var fh = new FileHandle.Builder(vectorFile).mmapped(true).complete()) {
            onDiskVectors = new OnDiskVectors(fh, offset);
            cache = VectorCache.load(hnsw, onDiskVectors, cacheCapacity);
        }
    }

    private void generateVectors(int totalOrdinals)
    {
        var type = VectorType.getInstance(FloatType.instance, dimension);
        vectorValues = new CompactionVectorValues(type);
        for (int i = 0; i < totalOrdinals; i++)
        {
            List<Float> rawVector = new ArrayList<>(Collections.nCopies(dimension, (float) i));
            var bb = type.getSerializer().serialize(rawVector);
            vectorValues.add(i, bb);
        }
    }

    private long writeVectorsToDisk(File vectorFile) throws IOException
    {
        var writerOption = SequentialWriterOption.newBuilder().finishOnClose(true).build();
        var vectorsWriter = new SequentialWriter(vectorFile, writerOption);
        var offset = vectorsWriter.getOnDiskFilePointer();
        vectorValues.write(vectorsWriter);
        vectorsWriter.close();
        return offset;
    }

    @After
    public void tearDown()
    {
        org.apache.commons.io.FileUtils.deleteQuietly(testDirectory.toFile());
    }

    @Test
    public void testOnlyEntryNodeAndNeighborsCached() throws IOException
    {
        int n = hnsw.size();
        int topLevel = hnsw.numLevels() - 1;
        int entryNode = hnsw.entryNode();

        Set<Integer> expectedCachedNeighbors = new HashSet<>(Set.of(entryNode));
        hnsw.seek(topLevel, entryNode);
        for (int i = 0; i < expectedCached - 1; i++)
        {
            var neighbor = hnsw.nextNeighbor();
            assertThat(neighbor).isNotEqualTo(NO_MORE_DOCS);
            expectedCachedNeighbors.add(neighbor);
        }

        for (int i = 0; i < n; i++)
        {
            var raw = cache.get(i);
            // we only expect entry node and first expectedCached - 1 neighbors to be visited by BFS and cached
            if (expectedCachedNeighbors.contains(i))
            {
                assertThat(raw).isNotEqualTo(null);
                var expectedRaw = vectorValues.vectorValue(i);
                assertThat(raw).isEqualTo(expectedRaw);
            }
            else
            {
                assertThat(raw).isEqualTo(null);
            }
        }
    }
}