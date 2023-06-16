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
import java.util.Arrays;

import org.apache.lucene.util.hnsw.ConcurrentNeighborSet;
import org.apache.lucene.util.hnsw.ConcurrentOnHeapHnswGraph;
import org.apache.lucene.util.hnsw.HnswGraph;

public class ExtendedConcurrentHnswGraph extends ExtendedHnswGraph
{
    private final ConcurrentOnHeapHnswGraph graph;
    private final HnswGraph view;

    public ExtendedConcurrentHnswGraph(ConcurrentOnHeapHnswGraph graph)
    {
        super();
        this.graph = graph;
        this.view = graph.getView();
    }

    // this is not guaranteed to be consistent with the view, but it's only used
    // when we're writing to disk and there aren't other concurrent operations
    @Override
    public int getNeighborCount(int level, int node)
    {
        var neighbors = graph.getNeighbors(level, node);
        assert neighbors != null : String.format("Node %d not found on on level %d among %s",
                                                 node, level, Arrays.toString(getSortedNodes(view, level)));
        return neighbors.size();
    }

    @Override
    public void seek(int level, int node) throws IOException
    {
        view.seek(level, node);
    }

    @Override
    public int size()
    {
        return view.size();
    }

    @Override
    public int nextNeighbor() throws IOException
    {
        return view.nextNeighbor();
    }

    @Override
    public int numLevels() throws IOException
    {
        return view.numLevels();
    }

    @Override
    public int entryNode() throws IOException
    {
        return view.entryNode();
    }

    @Override
    public NodesIterator getNodesOnLevel(int i) throws IOException
    {
        return view.getNodesOnLevel(i);
    }

    @Override
    public long ramBytesUsed()
    {
        return graph.ramBytesUsed();
    }
}
