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
package org.apache.cassandra.index.sai.disk.v1.bbtree;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Stopwatch;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.agrona.collections.IntArrayList;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.disk.v1.postings.MergePostingList;
import org.apache.cassandra.index.sai.disk.v1.postings.PackedLongsPostingList;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsWriter;
import org.apache.cassandra.index.sai.postings.PeekablePostingList;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.packed.PackedLongValues;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * Writes leaf postings and auxiliary posting lists for bbtree nodes. If a node has a posting list attached,
 * it will contain every row id from all leaves reachable from that node.
 * <p>
 * Writer is stateful, because it needs to collect data from the balanced tree data structure first to find set of eligible
 * nodes and leaf nodes reachable from them.
 * <p>
 * The leaf blocks are written in value order (in the order we pass them to the {@link BlockBalancedTreeWriter}).
 * This allows us to skip reading the leaves, instead just order leaf blocks by their offset in the index file,
 * and correlate them with buffered posting lists.
 */
@NotThreadSafe
public class BlockBalancedTreePostingsWriter implements BlockBalancedTreeWalker.TraversalCallback
{
    private static final Logger logger = LoggerFactory.getLogger(BlockBalancedTreePostingsWriter.class);

    private final TreeMap<Long, Integer> leafOffsetToNodeID = new TreeMap<>(Long::compareTo);
    private final Multimap<Integer, Integer> nodeToChildLeaves = HashMultimap.create();

    /**
     * Minimum number of reachable leaves for a given node to be eligible for an auxiliary posting list.
     */
    private final int minimumPostingsLeaves;
    /**
     * Skip, or the sampling interval, for selecting a balanced tree level that is eligible for an auxiliary posting list.
     * Sampling starts from 0, but the balanced tree root node is at level 1. For skip = 4, eligible levels are 4, 8, 12, etc. (no
     * level 0, because there is no node at level 0).
     */
    private final int postingsSkip;

    int numNonLeafPostings = 0;
    int numLeafPostings = 0;

    public BlockBalancedTreePostingsWriter()
    {
        minimumPostingsLeaves = CassandraRelevantProperties.SAI_MINIMUM_POSTINGS_LEAVES.getInt();
        postingsSkip = CassandraRelevantProperties.SAI_POSTINGS_SKIP.getInt();
    }

    /**
     * Called when a leaf node is hit as we traverse the packed index.
     *
     * @param leafNodeID the current leaf node ID in the packed inded
     * @param leafBlockFP the file pointer to the on-disk leaf block
     * @param pathToRoot the path to the root leaf above this leaf. Contains all the intermediate leaf node IDs.
     */
    @Override
    public void onLeaf(int leafNodeID, long leafBlockFP, IntArrayList pathToRoot)
    {
        checkArgument(!pathToRoot.containsInt(leafNodeID));
        checkArgument(pathToRoot.isEmpty() || leafNodeID > pathToRoot.get(pathToRoot.size() - 1));

        leafOffsetToNodeID.put(leafBlockFP, leafNodeID);
        for (int i = 0; i < pathToRoot.size(); i++)
        {
            int level = i + 1;
            if (isLevelEligibleForPostingList(level))
            {
                int nodeID = pathToRoot.get(i);
                nodeToChildLeaves.put(nodeID, leafNodeID);
            }
        }
    }

    /**
     * Writes merged posting lists for eligible internal nodes and leaf postings for each leaf in the tree.
     * The merged postings list for an internal node contains all postings from the postings lists of leaf nodes
     * in the subtree rooted at that node.
     * <p>
     * After writing out the postings, it writes a map of node ID -> postings file pointer for all
     * nodes with an attached postings list. It then returns the file pointer to this map.
     */
    public long finish(IndexOutputWriter out, List<PackedLongValues> leafPostings, IndexIdentifier indexIdentifier) throws IOException
    {
        checkState(leafPostings.size() == leafOffsetToNodeID.size(),
                   "Expected equal number of postings lists (%s) and leaf offsets (%s).",
                   leafPostings.size(), leafOffsetToNodeID.size());

        try (PostingsWriter postingsWriter = new PostingsWriter(out))
        {
            Iterator<PackedLongValues> postingsIterator = leafPostings.iterator();
            Map<Integer, PackedLongValues> leafToPostings = new HashMap<>();
            leafOffsetToNodeID.forEach((fp, nodeID) -> leafToPostings.put(nodeID, postingsIterator.next()));

            long postingsRamBytesUsed = leafPostings.stream()
                                                .mapToLong(PackedLongValues::ramBytesUsed)
                                                .sum();

            List<Integer> internalNodeIDs = nodeToChildLeaves.keySet()
                                                             .stream()
                                                             .filter(i -> nodeToChildLeaves.get(i).size() >= minimumPostingsLeaves)
                                                             .collect(Collectors.toList());

            Collection<Integer> leafNodeIDs = leafOffsetToNodeID.values();

            logger.debug(indexIdentifier.logMessage("Writing posting lists for {} internal and {} leaf balanced tree nodes. Leaf postings memory usage: {}."),
                         internalNodeIDs.size(), leafNodeIDs.size(), FBUtilities.prettyPrintMemory(postingsRamBytesUsed));

            long startFP = out.getFilePointer();
            Stopwatch flushTime = Stopwatch.createStarted();
            TreeMap<Integer, Long> nodeIDToPostingsFilePointer = new TreeMap<>();
            PriorityQueue<PeekablePostingList> postingLists = new PriorityQueue<>(minimumPostingsLeaves, Comparator.comparingLong(PeekablePostingList::peek));
            for (int nodeID : Iterables.concat(internalNodeIDs, leafNodeIDs))
            {
                Collection<Integer> leaves = nodeToChildLeaves.get(nodeID);

                if (leaves.isEmpty())
                {
                    leaves = Collections.singletonList(nodeID);
                    numLeafPostings++;
                }
                else
                {
                    numNonLeafPostings++;
                }

                for (Integer leaf : leaves)
                    postingLists.add(PeekablePostingList.makePeekable(new PackedLongsPostingList(leafToPostings.get(leaf))));

                try (PostingList mergedPostingList = MergePostingList.merge(postingLists))
                {
                    long postingFilePosition = postingsWriter.write(mergedPostingList);
                    // During compaction, we could end up with an empty postings due to deletions.
                    // The writer will return a fp of -1 if no postings were written.
                    if (postingFilePosition >= 0)
                        nodeIDToPostingsFilePointer.put(nodeID, postingFilePosition);
                }
                postingLists.clear();
            }
            flushTime.stop();
            logger.debug(indexIdentifier.logMessage("Flushed {} of posting lists for balanced tree nodes in {} ms."),
                         FBUtilities.prettyPrintMemory(out.getFilePointer() - startFP),
                         flushTime.elapsed(TimeUnit.MILLISECONDS));

            long indexFilePointer = out.getFilePointer();
            writeMap(nodeIDToPostingsFilePointer, out);
            postingsWriter.complete();
            return indexFilePointer;
        }
    }

    private boolean isLevelEligibleForPostingList(int level)
    {
        return level > 1 && level % postingsSkip == 0;
    }

    private void writeMap(Map<Integer, Long> map, IndexOutput out) throws IOException
    {
        out.writeVInt(map.size());

        for (Map.Entry<Integer, Long> e : map.entrySet())
        {
            out.writeVInt(e.getKey());
            out.writeVLong(e.getValue());
        }
    }
}
