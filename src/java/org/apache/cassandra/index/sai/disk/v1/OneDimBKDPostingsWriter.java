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
package org.apache.cassandra.index.sai.disk.v1;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
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

import com.google.common.base.Stopwatch;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.agrona.collections.IntArrayList;
import org.apache.cassandra.index.sai.disk.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.packed.PackedLongValues;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * Writes auxiliary posting lists for bkd tree nodes. If a node has a posting list attached, it will contain every row
 * id
 * from all leaves reachable from that node.
 *
 * Writer is stateful, because it needs to collect data from bkd index data structure first to find set of eligible
 * nodes and leaf nodes reachable from them.
 *
 * This is an optimised writer for 1-dim points, where we know that leaf blocks are written in value order (in this
 * order we pass them to the {@link BKDWriter}). That allows us to skip reading the leaves, instead just order leaf
 * blocks by their offset in the index file, and correlate them with buffered posting lists. We can't make this
 * assumption for multi-dim case.
 */
public class OneDimBKDPostingsWriter implements TraversingBKDReader.IndexTreeTraversalCallback
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final List<PackedLongValues> postings;
    private final TreeMap<Long, Integer> leafOffsetToNodeID = new TreeMap<>(Long::compareTo);
    private final Multimap<Integer, Integer> nodeToChildLeaves = HashMultimap.create();

    private final IndexWriterConfig config;
    private final IndexComponents components;
    int numNonLeafPostings = 0;
    int numLeafPostings = 0;

    OneDimBKDPostingsWriter(List<PackedLongValues> postings, IndexWriterConfig config, IndexComponents indexComponents)
    {
        this.postings = postings;
        this.config = config;
        this.components = indexComponents;
    }

    @Override
    public void onLeaf(int leafNodeID, long leafBlockFP, IntArrayList pathToRoot)
    {
        checkArgument(!pathToRoot.containsInt(leafNodeID));
        checkArgument(pathToRoot.isEmpty() || leafNodeID > pathToRoot.get(pathToRoot.size() - 1));

        leafOffsetToNodeID.put(leafBlockFP, leafNodeID);
        for (int i = 0; i < pathToRoot.size(); i++)
        {
            final int level = i + 1;
            if (isLevelEligibleForPostingList(level))
            {
                final int nodeID = pathToRoot.get(i);
                nodeToChildLeaves.put(nodeID, leafNodeID);
            }
        }
    }

    @SuppressWarnings("resource")
    public long finish(IndexOutput out) throws IOException
    {
        checkState(postings.size() == leafOffsetToNodeID.size(),
                   "Expected equal number of postings lists (%s) and leaf offsets (%s).",
                   postings.size(), leafOffsetToNodeID.size());

        final PostingsWriter postingsWriter = new PostingsWriter(out);

        final Iterator<PackedLongValues> postingsIterator = postings.iterator();
        final Map<Integer, PackedLongValues> leafToPostings = new HashMap<>();
        leafOffsetToNodeID.forEach((fp, nodeID) -> leafToPostings.put(nodeID, postingsIterator.next()));

        final long postingsRamBytesUsed = postings.stream()
                                                  .mapToLong(PackedLongValues::ramBytesUsed)
                                                  .sum();

        final List<Integer> internalNodeIDs =
                nodeToChildLeaves.keySet()
                                 .stream()
                                 .filter(i -> nodeToChildLeaves.get(i).size() >= config.getBkdPostingsMinLeaves())
                                 .collect(Collectors.toList());

        final Collection<Integer> leafNodeIDs = leafOffsetToNodeID.values();

        logger.debug(components.logMessage("Writing posting lists for {} internal and {} leaf kd-tree nodes. Leaf postings memory usage: {}."),
                     internalNodeIDs.size(), leafNodeIDs.size(), FBUtilities.prettyPrintMemory(postingsRamBytesUsed));

        final long startFP = out.getFilePointer();
        final Stopwatch flushTime = Stopwatch.createStarted();
        final TreeMap<Integer, Long> nodeIDToPostingsFilePointer = new TreeMap<>();
        for (int nodeID : Iterables.concat(internalNodeIDs, leafNodeIDs))
        {
            Collection<Integer> leaves = nodeToChildLeaves.get(nodeID);

            if (leaves.size() == 0)
            {
                leaves = Collections.singletonList(nodeID);
                numLeafPostings++;
            }
            else
            {
                numNonLeafPostings++;
            }

            final PriorityQueue<PostingList.PeekablePostingList> postingLists = new PriorityQueue<>(100, Comparator.comparingLong(PostingList.PeekablePostingList::peek));
            for (Integer leaf : leaves)
                postingLists.add(new PackedLongsPostingList(leafToPostings.get(leaf)).peekable());

            final PostingList mergedPostingList = MergePostingList.merge(postingLists);
            final long postingFilePosition = postingsWriter.write(mergedPostingList);
            // During compaction we could end up with an empty postings due to deletions.
            // The writer will return a fp of -1 if no postings were written.
            if (postingFilePosition >= 0)
                nodeIDToPostingsFilePointer.put(nodeID, postingFilePosition);
        }
        flushTime.stop();
        logger.debug(components.logMessage("Flushed {} of posting lists for kd-tree nodes in {} ms."),
                     FBUtilities.prettyPrintMemory(out.getFilePointer() - startFP),
                     flushTime.elapsed(TimeUnit.MILLISECONDS));


        final long indexFilePointer = out.getFilePointer();
        writeMap(nodeIDToPostingsFilePointer, out);
        postingsWriter.complete();
        return indexFilePointer;
    }

    private boolean isLevelEligibleForPostingList(int level)
    {
        return level > 1 && level % config.getBkdPostingsSkip() == 0;
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
