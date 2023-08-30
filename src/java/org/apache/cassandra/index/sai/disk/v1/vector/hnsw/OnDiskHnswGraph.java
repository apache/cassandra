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
import java.util.Arrays;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

public class OnDiskHnswGraph extends HnswGraph implements AutoCloseable
{
    private final FileHandle fh;
    private final long segmentSize;
    private final int size;
    private final int numLevels;
    private final int entryNode;

    private final long[] levelOffsets;

    @VisibleForTesting
    final CachedLevel[] cachedLevels;
    private final int cacheSizeInBytes;

    public OnDiskHnswGraph(FileHandle fh, long segmentOffset, long segmentLength, int neighborsRamBudget)
    {
        this.fh = fh;
        try (var reader = fh.createReader())
        {
            try
            {
                reader.seek(segmentOffset);
                segmentSize = segmentOffset + segmentLength;

                size = reader.readInt();
                numLevels = reader.readInt();
                cachedLevels = new CachedLevel[numLevels];
                entryNode = reader.readInt();

                // always load the level offsets
                levelOffsets = new long[numLevels];
                for (int i = 0; i < numLevels; i++) {
                    levelOffsets[i] = reader.readLong();
                }
            }
            catch (Exception e)
            {
                throw new RuntimeException("Error initializing OnDiskHnswGraph at offset " + segmentOffset, e);
            }

            cacheSizeInBytes = loadCache(segmentOffset, neighborsRamBudget, reader);
        }
    }

    private int loadCache(long segmentOffset, int neighborsRamBudget, RandomAccessReader reader)
    {
        int cacheSizeInBytes = 0;
        try
        {
            // cache full levels including neighbors up to neighborsRamBudget, starting with the top level,
            // but always cache all levels above the bottom two levels -- this will be ~1% of the graph.
            // then on L1, cache at least the offsets
            // L0 we do not cache since we only need one extra seek (no bsearch) to read the neighbors offset
            int level = numLevels - 1;
            if (neighborsRamBudget > 0) // for testing we allow disabling by setting budget=0
            {
                for (; level >= 0; level--)
                {
                    reader.seek(levelOffsets[level]);
                    int numNodes = reader.readInt();
                    long nodeIdsSize = (long) numNodes * Integer.BYTES;
                    long offsetsSize = (long) numNodes * Long.BYTES;
                    long neighborsSize = levelSize(level) - (offsetsSize + nodeIdsSize);

                    if (level <= 1 && cacheSizeInBytes + nodeIdsSize + neighborsSize > neighborsRamBudget)
                        break;

                    // Cache entire level including neighbors
                    int[] nodeIds = new int[numNodes];
                    int[][] neighbors = new int[numNodes][];

                    // Read node IDs
                    for (int i = 0; i < numNodes; i++)
                    {
                        nodeIds[i] = reader.readInt();  // read node id
                        reader.skipBytes(Long.BYTES);   // skip offset
                    }

                    // Read neighbors
                    for (int i = 0; i < numNodes; i++)
                    {
                        int numNeighbors = reader.readInt();
                        neighbors[i] = new int[numNeighbors];
                        for (int j = 0; j < numNeighbors; j++)
                        {
                            neighbors[i][j] = reader.readInt();
                        }
                    }

                    cachedLevels[level] = new CachedLevel(level, nodeIds, neighbors);
                    cacheSizeInBytes += nodeIdsSize + neighborsSize;
                }
            }

            // Cache node offsets for levels 1 and up, if their neighbors aren't already cached
            for ( ; level >= 1; level--)
            {
                reader.seek(levelOffsets[level]);
                int numNodes = reader.readInt();
                int[] nodeIds = new int[numNodes];
                long[] offsets = new long[numNodes];
                long nodeIdsSize = (long) numNodes * Integer.BYTES;
                long offsetsSize = (long) numNodes * Long.BYTES;
                for (int i = 0; i < numNodes; i++) {
                    nodeIds[i] = reader.readInt();
                    offsets[i] = reader.readLong();
                }
                cachedLevels[level] = new CachedLevel(level, nodeIds, offsets);
                cacheSizeInBytes += nodeIdsSize + offsetsSize;
            }
        }
        catch (Exception e)
        {
            var summary = String.format("Size: %d, Entry node: %d, Level offsets: %s",
                                        size, entryNode, Arrays.toString(levelOffsets));
            throw new RuntimeException(String.format("Error initializing OnDiskHnswGraph [%s] at offset %d",
                                                     summary, segmentOffset), e);
        }
        return cacheSizeInBytes;
    }

    int getCacheSizeInBytes() {
        return cacheSizeInBytes;
    }

    @Override
    public void seek(int level, int target) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    private static long levelNodeOf(int level, int target)
    {
        return ((long) level << 32) | target;
    }

    @Override
    public int size() {
        return size;
    }

    @VisibleForTesting
    long levelSize(int i) {
        long currentLevelOffset = levelOffsets[i];
        long nextLevelOffset;
        int topLevel = numLevels - 1;
        if (i < topLevel) {
            nextLevelOffset = levelOffsets[i + 1];
        } else {
            nextLevelOffset = segmentSize;
        }
        return nextLevelOffset - currentLevelOffset;
    }

    @Override
    public int nextNeighbor()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int numLevels() {
        return numLevels;
    }

    @Override
    public int entryNode() {
        return entryNode;
    }

    /** return an HnswGraph that can be safely queried concurrently */
    public OnDiskView getView()
    {
        return new OnDiskView(fh.createReader());
    }

    public class OnDiskView extends HnswGraph implements AutoCloseable
    {
        private final RandomAccessReader reader;
        private int currentNeighborsRead;
        private long currentCachedLevelNode = -1;
        private int[] currentNeighbors;

        public OnDiskView(RandomAccessReader reader)
        {
            super();
            this.reader = reader;
        }

        @Override
        public void seek(int level, int target) throws IOException
        {
            if (currentNeighborsRead == 0 && currentCachedLevelNode == levelNodeOf(level, target))
            {
                // seek was called redundantly (usually because getNeighborCount was also called)
                return;
            }

            currentCachedLevelNode = -1;
            currentNeighbors = null;
            currentNeighborsRead = 0;
            long neighborsOffset;

            var cachedLevel = cachedLevels[level];
            if (cachedLevel != null)
            {
                if (cachedLevel.containsNeighbors())
                {
                    currentNeighbors = cachedLevel.neighborsFor(target);
                    currentCachedLevelNode = levelNodeOf(level, target);
                    return;
                }

                // get the offset of the node's index entry from the cache, if present
                neighborsOffset = cachedLevel.offsetFor(target);
            }
            else
            {
                assert level == 0 : level; // other levels should all have a cache entry
                // level 0 ordinals are consecutive so we can easily compute the location from which to read the offset
                long neighborsOffsetOffset = levelOffsets[0] + Integer.BYTES + (long) target * (Integer.BYTES + Long.BYTES);
                reader.seek(neighborsOffsetOffset + 4);
                neighborsOffset = reader.readLong();
            }

            // seek to the neighbor list
            reader.seek(neighborsOffset);
            currentNeighbors = new int[reader.readInt()];
            reader.readIntsAt(reader.getPosition(), currentNeighbors);
        }

        @Override
        public int size()
        {
            return OnDiskHnswGraph.this.size();
        }

        @Override
        public int nextNeighbor()
        {
            if (currentNeighborsRead++ < currentNeighbors.length)
            {
                return currentNeighbors[currentNeighborsRead - 1];
            }
            return NO_MORE_DOCS;
        }

        @Override
        public int numLevels()
        {
            return OnDiskHnswGraph.this.numLevels();
        }

        @Override
        public int entryNode()
        {
            return OnDiskHnswGraph.this.entryNode();
        }

        // getNodesOnLevel is only used when scanning the entire graph, i.e., during compaction (or tests)
        @Override
        public NodesIterator getNodesOnLevel(int level) throws IOException
        {
            if (cachedLevels[level] != null)
            {
                var nodes = cachedLevels[level].nodesOnLevel();
                var it = Arrays.stream(nodes).iterator();
                return new AbstractNodesIterator(nodes.length)
                {
                    @Override
                    public int nextInt()
                    {
                        return it.nextInt();
                    }

                    @Override
                    public boolean hasNext()
                    {
                        return it.hasNext();
                    }
                };
            }

            reader.seek(levelOffsets[level]);
            int numNodes = reader.readInt();
            return new AbstractNodesIterator(numNodes)
            {
                private int nodesRead = 0;

                @Override
                public int nextInt()
                {
                    try
                    {
                        int value = reader.readInt();
                        reader.skipBytes(Long.BYTES);
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

        @Override
        public void close()
        {
            reader.close();
        }
    }

    @Override
    public NodesIterator getNodesOnLevel(int level)
    {
        throw new UnsupportedOperationException();
    }

    public void close()
    {
        fh.close();
    }

    @VisibleForTesting
    static class CachedLevel
    {
        private final int level;
        private final int[] nodeIds;
        private final long[] offsets;
        private final int[][] neighbors;

        public CachedLevel(int level, int[] nodeIds, long[] offsets)
        {
            this.level = level;
            this.nodeIds = nodeIds;
            this.offsets = offsets;
            this.neighbors = null;
        }

        public CachedLevel(int level, int[] nodeIds, int[][] neighbors)
        {
            this.level = level;
            this.nodeIds = nodeIds;
            this.neighbors = neighbors;
            offsets = null;
        }

        public boolean containsNeighbors() {
            return neighbors != null;
        }

        public long offsetFor(int nodeId)
        {
            int i = Arrays.binarySearch(nodeIds, nodeId);
            if (i < 0)
                throw new IllegalStateException("Node " + nodeId + " not found in level " + level);
            return offsets[i];
        }

        public int[] neighborsFor(int nodeId)
        {
            int i = Arrays.binarySearch(nodeIds, nodeId);
            if (i < 0)
                throw new IllegalStateException("Node " + nodeId + " not found in level " + level);
            return neighbors[i];
        }

        public int[] nodesOnLevel()
        {
            return nodeIds;
        }
    }

    @VisibleForTesting
    abstract static class AbstractNodesIterator extends NodesIterator
    {
        public AbstractNodesIterator(int size)
        {
            super(size);
        }

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
    }
}


