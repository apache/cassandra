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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.index.sai.disk.v1.vector.hnsw.OnDiskHnswGraph.AbstractNodesIterator;
import org.apache.cassandra.utils.Pair;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

public class FullyConnectedHnswGraph extends ExtendedHnswGraph
{

    private final int entryNode;
    private final Map<Integer, List<Integer>> nodes;
    private Pair<Integer, Integer> currentLevelNode;
    private Iterator<Integer> currentNeighbors;

    private FullyConnectedHnswGraph(int entryNode, Map<Integer, List<Integer>> nodes) {
        this.entryNode = entryNode;
        this.nodes = nodes;
    }

    @Override
    public NodesIterator getNodesOnLevel(int level) {
        return new AbstractNodesIterator(nodes.get(level).size()) {
            private final Iterator<Integer> it = nodes.get(level).iterator();

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
        return nodes.get(level).size() - 1;
    }

    @Override
    public void seek(int level, int node) throws IOException
    {
        currentLevelNode = Pair.create(level, node);
        currentNeighbors = nodes.get(level).stream().filter(n -> n != node).iterator();
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
        private int entryNode;
        private final Map<Integer, List<Integer>> nodes = new HashMap<>();

        public Builder setEntryNode(int entryNode) {
            this.entryNode = entryNode;
            return this;
        }

        public Builder addLevel(int level, List<Integer> nodes) {
            this.nodes.put(level, nodes);
            return this;
        }

        public FullyConnectedHnswGraph build() {
            return new FullyConnectedHnswGraph(entryNode, nodes);
        }
    }
}

