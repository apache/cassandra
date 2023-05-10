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

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.lucene.util.hnsw.HnswGraph;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

public class OnDiskHnswGraph extends HnswGraph
{
    private final RandomAccessReader reader;
    private final int size;
    private final int numLevels;
    private final int entryNode;

    private int currentNeighborCount;
    private int currentNeighborsRead;

    public OnDiskHnswGraph(File file) throws IOException {
        this.reader = RandomAccessReader.open(file);

        size = reader.readInt();
        numLevels = reader.readInt();
        entryNode = reader.readInt();
    }

    @Override
    public void seek(int level, int target) throws IOException {
        reader.seek(12L + 8L * level);
        long offset = reader.readLong();
        reader.seek(offset);
        int numNodes = reader.readInt();
        long firstOffset = reader.getFilePointer();
        long lastOffset = firstOffset + numNodes * 12L;
        long entryOffset = binarySearchNodeOffset(level, firstOffset, lastOffset, target);
        reader.seek(entryOffset);
        var diskNodeId = reader.readInt();
        assert diskNodeId == target : String.format("Expected node %d, but found %d", target, diskNodeId);
        long neighborsOffset = reader.readLong();
        reader.seek(neighborsOffset);
        currentNeighborCount = reader.readInt();
        currentNeighborsRead = 0;
    }

    private long binarySearchNodeOffset(int level, long firstOffset, long lastOffset, int target) throws IOException {
        long left = firstOffset;
        long right = lastOffset - 12;

        while (left <= right) {
            long mid = left + ((right - left) / 2 / 12) * 12;
            reader.seek(mid);
            int midValue = reader.readInt();

            if (midValue < target) {
                left = mid + 12;
            } else if (midValue > target) {
                right = mid - 12;
            } else {
                return mid;
            }
        }

        throw new IllegalStateException(String.format("Node %d not found on level %d in %s", target, level, reader.getPath()));
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public int nextNeighbor() throws IOException {
        if (currentNeighborsRead++ < currentNeighborCount)
        {
            return reader.readInt();
        }
        return NO_MORE_DOCS;
    }

    @Override
    public int numLevels() {
        return numLevels;
    }

    @Override
    public int entryNode() {
        return entryNode;
    }

    @Override
    public NodesIterator getNodesOnLevel(int level) throws IOException {
        reader.seek(12L + 8L * level);
        long offset = reader.readLong();
        reader.seek(offset);
        int numNodes = reader.readInt();
        return new NodesIterator(numNodes)
        {
            private int nodesRead = 0;

            @Override
            public int consume(int[] ints)
            {
                int i = 0;
                while (i < ints.length && hasNext())
                {
                    ints[i++] = nextInt();
                }
                return i;
            }

            @Override
            public int nextInt()
            {
                try
                {
                    int value = reader.readInt();
                    nodesRead++;
                    return value;
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public boolean hasNext()
            {
                return nodesRead < size;
            }
        };
    }

    public void close()
    {
        reader.close();
    }
}


