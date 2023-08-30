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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.cassandra.utils.Pair;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

public class RandomlyConnectedHnswGraph extends ExtendedHnswGraph
{

    private final int entryNode;
    private final Map<Integer, Map<Integer, List<Integer>>> nodes;

    private Pair<Integer, Integer> currentLevelNode;
    private Iterator<Integer> currentNeighbors;

    public RandomlyConnectedHnswGraph(Map<Integer, Map<Integer, List<Integer>>> nodes)
    {
        this.nodes = nodes;
        // entry node is a random node on the top level
        var topLevel = nodes.keySet().stream().mapToInt(i -> i).max().orElseThrow();
        var topNodes = nodes.get(topLevel).entrySet();
        var randomNode = topNodes.stream().skip(new Random().nextInt(topNodes.size())).findFirst().get();
        this.entryNode = randomNode.getKey();
    }

    public Map<Integer, List<Integer>> rawNodesOnLevel(int level)
    {
        return nodes.get(level);
    }

    @Override
    public NodesIterator getNodesOnLevel(int level) {
        return new OnDiskHnswGraph.AbstractNodesIterator(nodes.get(level).size()) {
            private final Iterator<Integer> it = nodes.get(level).keySet().iterator();

            @Override
            public int nextInt() {
                return it.next();
            }

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }
        };
    }

    @Override
    public int getNeighborCount(int level, int node) throws IOException
    {
        return nodes.get(level).get(node).size();
    }

    @Override
    public void seek(int level, int node) throws IOException
    {
        currentLevelNode = Pair.create(level, node);
        currentNeighbors = nodes.get(level).get(node).iterator();
    }

    @Override
    public int size()
    {
        return nodes.get(0).size();
    }

    @Override
    public int nextNeighbor() throws IOException
    {
        return currentNeighbors.hasNext() ? currentNeighbors.next() : NO_MORE_DOCS;
    }

    @Override
    public int numLevels() throws IOException
    {
        return nodes.size();
    }

    @Override
    public int entryNode() throws IOException
    {
        return entryNode;
    }

    public static class Builder {
        private final Map<Integer, Map<Integer, List<Integer>>> nodes = new HashMap<>();
        private final Random random = new Random();

        public Builder addLevel(int level, List<Integer> nodeIds, int M) {
            Map<Integer, List<Integer>> nodeConnections = new HashMap<>();
            for (Integer nodeId : nodeIds)
            {
                List<Integer> neighbors = new ArrayList<>(M);
                for (int i = 0; i < M; i++)
                {
                    int neighbor = random.nextInt(nodeIds.size());
                    if (neighbor == nodeId)
                        neighbor = (neighbor + 1) % nodeIds.size();
                    neighbors.add(neighbor);
                }
                nodeConnections.put(nodeId, neighbors);
            }
            this.nodes.put(level, nodeConnections);
            return this;
        }

        public Builder addLevels(int levels, int totalNodes, int M)
        {
            List<Integer> nodeIds = IntStream.range(0, totalNodes).boxed().collect(Collectors.toList());
            for (int level = 0; level < levels; level++)
            {
                addLevel(level, nodeIds, M);
                Collections.shuffle(nodeIds);
                nodeIds = new ArrayList<>(nodeIds.subList(0, nodeIds.size() / 2));
            }
            return this;
        }

        public RandomlyConnectedHnswGraph build() {
            return new RandomlyConnectedHnswGraph(nodes);
        }
    }
}

