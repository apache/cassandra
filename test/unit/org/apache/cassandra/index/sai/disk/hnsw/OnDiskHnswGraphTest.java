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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.lucene.util.hnsw.HnswGraph;

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

    private FullyConnectedHnswGraph buildGraph(int entryNode, Map<Integer, List<Integer>> nodes) {
        FullyConnectedHnswGraph.Builder builder = new FullyConnectedHnswGraph.Builder().setEntryNode(entryNode);
        nodes.forEach(builder::addLevel);
        return builder.build();
    }

    private void validateGraph(HnswGraph original, OnDiskHnswGraph onDisk) throws IOException {
        assertThat(onDisk.size()).isEqualTo(original.size());
        assertThat(onDisk.entryNode()).isEqualTo(original.entryNode());
        assertThat(onDisk.numLevels()).isEqualTo(original.numLevels());

        for (int i = 0; i < original.numLevels(); i++) {
            // Check the nodes and neighbors at each level
            var nodes = assertEqualNodes(original.getNodesOnLevel(i), onDisk.getNodesOnLevel(i));

            // For each node, check its neighbors
            for (int j : nodes) {
                original.seek(i, j);
                onDisk.seek(i, j);
                int n1;
                do
                {
                    n1 = onDisk.nextNeighbor();
                    var n2 = original.nextNeighbor();
                    assertThat(n1).isEqualTo(n2);
                } while (n1 != NO_MORE_DOCS);
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

    private static OnDiskHnswGraph createOnDiskGraph(File outputFile, int cacheRamBudget) throws IOException
    {
        return new OnDiskHnswGraph(new FileHandle.Builder(outputFile).complete(), 0, outputFile.length(), cacheRamBudget);
    }

    @Test
    public void testOneLevelGraph() throws IOException {
        var outputPath = testDirectory.resolve("one_level_graph");
        var outputFile = new File(outputPath);
        new HnswGraphWriter(oneLevelGraph).write(outputFile);
        OnDiskHnswGraph onDiskGraph = createOnDiskGraph(outputFile, 0);
        validateGraph(oneLevelGraph, onDiskGraph);
        onDiskGraph.close();
    }

    @Test
    public void testTwoLevelGraph() throws IOException {
        var outputPath = testDirectory.resolve("two_level_graph");
        var outputFile = new File(outputPath);
        new HnswGraphWriter(twoLevelGraph).write(outputFile);
        OnDiskHnswGraph onDiskGraph = createOnDiskGraph(outputFile, 0);
        validateGraph(twoLevelGraph, onDiskGraph);
        onDiskGraph.close();
    }

    @Test
    public void testThreeLevelGraph() throws IOException {
        var outputPath = testDirectory.resolve("three_level_graph");
        var outputFile = new File(outputPath);
        new HnswGraphWriter(threeLevelGraph).write(outputFile);
        OnDiskHnswGraph onDiskGraph = createOnDiskGraph(outputFile, 0);
        validateGraph(threeLevelGraph, onDiskGraph);
        onDiskGraph.close();
    }

    @Test
    public void testCaching() throws IOException {
        // Write the graph to the disk
        File outputFile = new File(testDirectory, "test_graph");
        new HnswGraphWriter(threeLevelGraph).write(outputFile);

        var onDiskGraph = createOnDiskGraph(outputFile, 0);
        BiFunction<OnDiskHnswGraph, Integer, Integer> nodeIdBytes = (g, i) -> {
            try
            {
                return g.getNodesOnLevel(i).size() * Integer.BYTES;
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        };
        BiFunction<OnDiskHnswGraph, Integer, Integer> offsetBytes = (g, i) -> {
            try
            {
                return g.getNodesOnLevel(i).size() * Long.BYTES;
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        };
        BiFunction<OnDiskHnswGraph, Integer, Long> neighborBytes = (g, i) -> g.levelSize(i) - (nodeIdBytes.apply(g, i) + offsetBytes.apply(g, i));

        // test graph that caches just the offsets of the top level
        int ramBudget = Math.toIntExact(nodeIdBytes.apply(onDiskGraph, 2) + offsetBytes.apply(onDiskGraph, 2));
        onDiskGraph = createOnDiskGraph(outputFile, ramBudget);
        validateGraph(threeLevelGraph, onDiskGraph);
        assertThat(onDiskGraph.cachedLevels[2].containsNeighbors()).isFalse();
        assertThat(onDiskGraph.cachedLevels[1]).isNull();
        assertThat(onDiskGraph.getCacheSizeInBytes()).isEqualTo(ramBudget);

        // test graph that caches just the entire top level
        ramBudget = Math.toIntExact(nodeIdBytes.apply(onDiskGraph, 2) + neighborBytes.apply(onDiskGraph, 2));
        onDiskGraph = createOnDiskGraph(outputFile, ramBudget);
        validateGraph(threeLevelGraph, onDiskGraph);
        assertThat(onDiskGraph.cachedLevels[2].containsNeighbors()).isTrue();
        assertThat(onDiskGraph.cachedLevels[1]).isNull();
        assertThat(onDiskGraph.getCacheSizeInBytes()).isEqualTo(ramBudget);

        // test graph that caches the entire top level, and offsets from the next
        ramBudget += Math.toIntExact(nodeIdBytes.apply(onDiskGraph, 1) + offsetBytes.apply(onDiskGraph, 1));
        onDiskGraph = createOnDiskGraph(outputFile, ramBudget);
        validateGraph(threeLevelGraph, onDiskGraph);
        assertThat(onDiskGraph.cachedLevels[2].containsNeighbors()).isTrue();
        assertThat(onDiskGraph.cachedLevels[1].containsNeighbors()).isFalse();
        assertThat(onDiskGraph.cachedLevels[0]).isNull();
        assertThat(onDiskGraph.getCacheSizeInBytes()).isEqualTo(ramBudget);

        // test graph that caches the entire structure
        onDiskGraph = createOnDiskGraph(outputFile, 1024);
        validateGraph(threeLevelGraph, onDiskGraph);
        assertThat(onDiskGraph.cachedLevels[2].containsNeighbors()).isTrue();
        assertThat(onDiskGraph.cachedLevels[1].containsNeighbors()).isTrue();
        assertThat(onDiskGraph.cachedLevels[0].containsNeighbors()).isTrue();
    }

    @Test
    public void testLargeGraph() throws IOException
    {
        System.out.println("constructing graph");
        var graph = new RandomlyConnectedHnswGraph.Builder().addLevels(10, 1_000_000, 16).build();
        System.out.println("writing graph");
        File outputFile = new File(testDirectory, "test_graph");
        new HnswGraphWriter(graph).write(outputFile);
        System.out.println("Graph is " + outputFile.length() + " bytes");
        OnDiskHnswGraph onDiskGraph = createOnDiskGraph(outputFile, 0);
        System.out.println("validating graph");
        validateGraph(graph, onDiskGraph);

        System.out.println("random queries");
        for (int i = 0; i < 1000; i++)
        {
            // pick a random node from a random level in the original graph
            int level = ThreadLocalRandom.current().nextInt(graph.numLevels());
            var nodes = new ArrayList<>(graph.rawNodesOnLevel(level).keySet());
            int node = nodes.get(ThreadLocalRandom.current().nextInt(nodes.size()));

            onDiskGraph.seek(level, node);
            while (true)
            {
                int neighbor = onDiskGraph.nextNeighbor();
                if (neighbor == NO_MORE_DOCS)
                    break;
            }
        }
    }
}

