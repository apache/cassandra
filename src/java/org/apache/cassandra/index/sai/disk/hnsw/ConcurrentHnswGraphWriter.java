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
import java.util.HashMap;

import org.apache.cassandra.index.sai.disk.io.IndexFileUtils;
import org.apache.cassandra.io.util.File;
import org.apache.lucene.util.hnsw.ConcurrentOnHeapHnswGraph;
import org.apache.lucene.util.hnsw.HnswGraph;

import static org.apache.cassandra.index.sasi.disk.OnDiskIndexBuilder.WRITER_OPTION;

public class ConcurrentHnswGraphWriter
{
    private final ConcurrentOnHeapHnswGraph hnsw;

    public ConcurrentHnswGraphWriter(ConcurrentOnHeapHnswGraph hnsw)
    {
        this.hnsw = hnsw;
    }

    private long levelSize(int level)
    {
        long size = 4; // number of nodes on level
        var nodesOnLevel = hnsw.getNodesOnLevel(level);
        while (nodesOnLevel.hasNext())
        {
            size += 4L + 8L; // node id and offset
            size += neighborSize(level, nodesOnLevel.nextInt());
        }
        return size;
    }

    private long neighborSize(int level, int node)
    {
        // node neighbor count, and node neighbors
        return 4L * (1 + hnsw.getNeighbors(level, node).size());
    }

    public void write(File file) throws IOException {
        try (var indexOutputWriter = IndexFileUtils.instance.openOutput(file))
        {
            var out = indexOutputWriter.asSequentialWriter();
            // hnsw info we want to be able to provide without reading the whole thing
            out.writeInt(hnsw.size());
            out.writeInt(hnsw.numLevels());
            out.writeInt(hnsw.entryNode());

            long firstLevelOffset = 12 // header
                                    + 8L * hnsw.numLevels(); // offsets for each level
            // Write offsets for each level
            long nextLevelOffset = firstLevelOffset;
            var levelOffsets = new HashMap<Integer, Long>(); // TODO remove this once the code is debugged
            for (var level = 0; level < hnsw.numLevels(); level++)
            {
                out.writeLong(nextLevelOffset);
                levelOffsets.put(level, nextLevelOffset);
                nextLevelOffset += levelSize(level);
            }
            assert out.position() == firstLevelOffset : String.format("first level offset mismatch: %s actual vs %s expected", out.position(), firstLevelOffset);

            for (var level = 0; level < hnsw.numLevels(); level++)
            {
                var levelOffset = out.position();
                assert levelOffset == levelOffsets.get(level) : String.format("level %s offset mismatch: %s actual vs %s expected", level, levelOffset, levelOffsets.get(level));
                // write the number of nodes on the level
                var sortedNodes = getSortedNodes(hnsw.getNodesOnLevel(level));
                out.writeInt(sortedNodes.length);

                long nextNodeOffset = out.position() + (4L + 8L) * sortedNodes.length;
                var nodeOffsets = new HashMap<Integer, Long>(); // TODO remove this once the code is debugged
                for (var node : sortedNodes)
                {
                    out.writeInt(node);
                    out.writeLong(nextNodeOffset);
                    nodeOffsets.put(node, nextNodeOffset);
                    nextNodeOffset += neighborSize(level, node);
                }

                // for each node on the level, write its neighbors
                for (var node : sortedNodes)
                {
                    assert out.position() == nodeOffsets.get(node) : String.format("level %s node %s offset mismatch: %s actual vs %s expected", level, node, out.position(), nodeOffsets.get(node));
                    var neighborSet = hnsw.getNeighbors(level, node);
                    out.writeInt(neighborSet.size());
                    neighborSet.forEach((ordinal, score_) -> out.writeInt(ordinal));
                }
                long expectedPosition = levelOffset + levelSize(level);
                assert out.position() == expectedPosition : String.format("level %s offset mismatch: %s actual vs %s expected", level, out.position(), expectedPosition);
            }
            assert out.position() == nextLevelOffset : String.format("final level offset mismatch: %s actual vs %s expected", out.position(), nextLevelOffset);
        }
    }

    private static int[] getSortedNodes(HnswGraph.NodesIterator nodesOnLevel) {
        var sortedNodes = new int[nodesOnLevel.size()];

        for(var n = 0; nodesOnLevel.hasNext(); n++) {
            sortedNodes[n] = nodesOnLevel.nextInt();
        }

        Arrays.sort(sortedNodes);
        return sortedNodes;
    }
}
