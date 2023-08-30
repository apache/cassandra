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

package org.apache.cassandra.index.sai.disk.v1.vector.hnsw;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.io.IndexFileUtils;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.assertj.core.api.Assertions.assertThat;

public class OnDiskHnswGraphTest extends SAITester
{

    private Path testDirectory;

    private FullyConnectedHnswGraph oneLevelGraph;
    private FullyConnectedHnswGraph twoLevelGraph;
    private FullyConnectedHnswGraph threeLevelGraph;

    @Before
    public void setup() throws IOException
    {
        // Build FullyConnectedHnswGraph instances here to be used in the tests

        // Example:
        oneLevelGraph = new FullyConnectedHnswGraph.Builder()
                        .setEntryNode(0)
                        .addLevel(0, List.of(0, 1, 2, 3, 4, 5))
                        .build();

        twoLevelGraph = new FullyConnectedHnswGraph.Builder()
                        .setEntryNode(0)
                        .addLevel(0, List.of(0, 1, 2, 3, 4, 5))
                        .addLevel(1, List.of(0, 1, 2))
                        .build();

        threeLevelGraph = new FullyConnectedHnswGraph.Builder()
                          .setEntryNode(0)
                          .addLevel(0, List.of(0, 1, 2, 3, 4, 5))
                          .addLevel(1, List.of(0, 1, 3, 5))
                          .addLevel(2, List.of(0, 1, 3))
                          .build();

        testDirectory = Files.createTempDirectory("OnDiskHnswGraphTest");
    }

    @After
    public void tearDown() {
        // Use Apache Commons IOUtils to delete the directory
        org.apache.commons.io.FileUtils.deleteQuietly(testDirectory.toFile());
    }

    private void validateGraph(HnswGraph original, OnDiskHnswGraph onDisk) throws IOException {
        try (var view = onDisk.getView())
        {
            assertThat(view.size()).isEqualTo(original.size());
            assertThat(view.entryNode()).isEqualTo(original.entryNode());
            assertThat(view.numLevels()).isEqualTo(original.numLevels());

            for (int i = 0; i < original.numLevels(); i++) {
                // Check the nodes and neighbors at each level
                var nodes = assertEqualNodes(original.getNodesOnLevel(i), view.getNodesOnLevel(i));

                // For each node, check its neighbors
                for (int j : nodes) {
                    original.seek(i, j);
                    view.seek(i, j);
                    int n1;
                    do
                    {
                        n1 = view.nextNeighbor();
                        var n2 = original.nextNeighbor();
                        assertThat(n1).isEqualTo(n2);
                    } while (n1 != NO_MORE_DOCS);
                }
            }
        }
    }

    private ArrayList<Integer> assertEqualNodes(HnswGraph.NodesIterator nodesExpected, HnswGraph.NodesIterator nodesActual)
    {
        var L1 = new ArrayList<Integer>();
        var L2 = new ArrayList<Integer>();
        while (nodesExpected.hasNext()) {
            L1.add(nodesExpected.next());
        }
        while (nodesActual.hasNext()) {
            L2.add(nodesActual.next());
        }
        L1.sort(Integer::compareTo);
        L2.sort(Integer::compareTo);
        assertThat(L2).isEqualTo(L1);
        return L1;
    }

    private static long writeGraph(ExtendedHnswGraph hnsw, File outputFile) throws IOException
    {
        HnswGraphWriter writer = new HnswGraphWriter(hnsw);
        try (var indexOutputWriter = IndexFileUtils.instance.openOutput(outputFile))
        {
            return writer.write(indexOutputWriter);
        }
    }

    private static OnDiskHnswGraph createOnDiskGraph(File outputFile, int cacheRamBudget) throws IOException
    {
        var builder = new FileHandle.Builder(outputFile).mmapped(true);
        return new OnDiskHnswGraph(builder.complete(), 0, outputFile.length(), cacheRamBudget);
    }

    @Test
    public void testSimpleGraphs() throws IOException {
        var outputPath = testDirectory.resolve("test_graph");
        var outputFile = new File(outputPath);
        for (var g : List.of(oneLevelGraph, twoLevelGraph, threeLevelGraph))
        {
            writeGraph(g, outputFile);
            try (var onDiskGraph = createOnDiskGraph(outputFile, 0))
            {
                validateGraph(g, onDiskGraph);
            }
        }
    }

    private static class GraphWithOffsets
    {
        public final ExtendedHnswGraph hnsw;
        public final long startOffset;
        public final long endOffset;

        public GraphWithOffsets(ExtendedHnswGraph hnsw, long startOffset, long endOffset)
        {
            this.hnsw = hnsw;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
        }
    }

    @Test
    public void testAppendedGraphs() throws IOException
    {
        var outputPath = testDirectory.resolve("test_graph");
        var outputFile = new File(outputPath);
        List<GraphWithOffsets> graphOffsets;
        try (var out = IndexFileUtils.instance.openOutput(outputFile))
        {
            graphOffsets = List.of(oneLevelGraph, twoLevelGraph, threeLevelGraph)
                    .stream()
                    .map(g -> {
                        try
                        {
                            return new GraphWithOffsets(g, out.getFilePointer(), new HnswGraphWriter(g).write(out));
                        }
                        catch (IOException e)
                        {
                            throw new RuntimeException(e);
                        }
                    }).collect(Collectors.toList());
        }
        for (var g: graphOffsets)
        {
            var builder = new FileHandle.Builder(outputFile).mmapped(true);
            try (var onDiskGraph = new OnDiskHnswGraph(builder.complete(), g.startOffset, g.endOffset, 0))
            {
                validateGraph(g.hnsw, onDiskGraph);
            }
        }
    }

    @Test
    public void testCaching() throws IOException {
        // Write the graph to the disk
        File outputFile = new File(testDirectory, "test_graph");
        writeGraph(threeLevelGraph, outputFile);

        BiFunction<OnDiskHnswGraph, Integer, Integer> nodeIdBytes = (g, i) -> {
            try (var v = g.getView())
            {
                return v.getNodesOnLevel(i).size() * Integer.BYTES;
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        };
        BiFunction<OnDiskHnswGraph, Integer, Integer> offsetBytes = (g, i) -> {
            try (var v = g.getView())
            {
                return v.getNodesOnLevel(i).size() * Long.BYTES;
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        };
        BiFunction<OnDiskHnswGraph, Integer, Long> neighborBytes = (g, i) -> g.levelSize(i) - (nodeIdBytes.apply(g, i) + offsetBytes.apply(g, i));

        // test graph that caches just the offsets of the top levels
        var onDiskGraph = createOnDiskGraph(outputFile, 0);
        validateGraph(threeLevelGraph, onDiskGraph);
        assertThat(onDiskGraph.cachedLevels[2].containsNeighbors()).isFalse();
        onDiskGraph.close();

        // test graph that caches the entire structure
        onDiskGraph = createOnDiskGraph(outputFile, 1024);
        validateGraph(threeLevelGraph, onDiskGraph);
        assertThat(onDiskGraph.cachedLevels[2].containsNeighbors()).isTrue();
        assertThat(onDiskGraph.cachedLevels[1].containsNeighbors()).isTrue();
        assertThat(onDiskGraph.cachedLevels[0].containsNeighbors()).isTrue();
        onDiskGraph.close();
    }

    @Test
    public void testLargeGraph() throws IOException
    {
        logger.debug("constructing graph");
        var graph = new RandomlyConnectedHnswGraph.Builder().addLevels(10, 100_000, 16).build();

        logger.debug("writing graph");
        File outputFile = new File(testDirectory, "test_graph");
        writeGraph(graph, outputFile);
        logger.debug("Graph is " + outputFile.length() + " bytes");

        logger.debug("initializing OnDiskHnswGraph");
        OnDiskHnswGraph onDiskGraph = createOnDiskGraph(outputFile, 0);

        logger.debug("validating graph");
        validateGraph(graph, onDiskGraph);

        logger.debug("random queries");
        try (var view = onDiskGraph.getView())
        {
            for (int i = 0; i < 1000; i++)
            {
                // pick a random node from a random level in the original graph
                int level = ThreadLocalRandom.current().nextInt(graph.numLevels());
                var nodes = new ArrayList<>(graph.rawNodesOnLevel(level).keySet());
                int node = nodes.get(ThreadLocalRandom.current().nextInt(nodes.size()));

                view.seek(level, node);
                while (true)
                {
                    int neighbor = view.nextNeighbor();
                    if (neighbor == NO_MORE_DOCS)
                        break;
                }
            }
        }

        onDiskGraph.close();
    }
}

