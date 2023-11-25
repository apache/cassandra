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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.agrona.collections.IntArrayList;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.ArrayPostingList;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsReader;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.index.sai.utils.SAIRandomizedTester;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class BlockBalancedTreePostingsWriterTest extends SAIRandomizedTester
{
    private IndexDescriptor indexDescriptor;
    private IndexIdentifier indexIdentifier;

    @Before
    public void setup() throws Throwable
    {
        indexDescriptor = newIndexDescriptor();
        indexIdentifier = SAITester.createIndexIdentifier("test", "test", newIndex());
    }

    @Test
    public void shouldWritePostingsForEligibleNodes() throws Exception
    {
        List<PackedLongValues> leaves =
        Arrays.asList(postings(1, 5, 7), postings(3, 4, 6), postings(2, 8, 10), postings(11, 12, 13));

        setBDKPostingsWriterSizing(1, 2);
        BlockBalancedTreePostingsWriter writer = new BlockBalancedTreePostingsWriter();

        // should build postings for nodes 2 & 3 (lvl 2) and 8, 10, 12, 14 (lvl 4)
        writer.onLeaf(64, 1, pathToRoot(1, 2, 4, 8, 16));
        writer.onLeaf(80, 2, pathToRoot(1, 2, 5, 10, 20));
        writer.onLeaf(96, 3, pathToRoot(1, 3, 6, 12, 24));
        writer.onLeaf(112, 4, pathToRoot(1, 3, 7, 14, 28));

        long fp;
        try (IndexOutputWriter output = indexDescriptor.openPerIndexOutput(IndexComponent.POSTING_LISTS, indexIdentifier))
        {
            fp = writer.finish(output, leaves, indexIdentifier);
        }

        BlockBalancedTreePostingsIndex postingsIndex = new BlockBalancedTreePostingsIndex(indexDescriptor.createPerIndexFileHandle(IndexComponent.POSTING_LISTS, indexIdentifier, null), fp);
        assertEquals(10, postingsIndex.size());

        // Internal postings...
        assertTrue(postingsIndex.exists(2));
        assertTrue(postingsIndex.exists(3));
        assertTrue(postingsIndex.exists(8));
        assertTrue(postingsIndex.exists(10));
        assertTrue(postingsIndex.exists(12));
        assertTrue(postingsIndex.exists(14));

        assertPostingReaderEquals(postingsIndex, 2, 1, 3, 4, 5, 6, 7);
        assertPostingReaderEquals(postingsIndex, 3, 2, 8, 10, 11, 12, 13);
        assertPostingReaderEquals(postingsIndex, 8, 1, 5, 7);
        assertPostingReaderEquals(postingsIndex, 10, 3, 4, 6);
        assertPostingReaderEquals(postingsIndex, 12, 2, 8, 10);
        assertPostingReaderEquals(postingsIndex, 14, 11, 12, 13);

        // Leaf postings...
        assertTrue(postingsIndex.exists(64));
        assertTrue(postingsIndex.exists(80));
        assertTrue(postingsIndex.exists(96));
        assertTrue(postingsIndex.exists(112));

        assertPostingReaderEquals(postingsIndex, 64, 1, 5, 7);
        assertPostingReaderEquals(postingsIndex, 80, 3, 4, 6);
        assertPostingReaderEquals(postingsIndex, 96, 2, 8, 10);
        assertPostingReaderEquals(postingsIndex, 112, 11, 12, 13);
    }

    @Test
    public void shouldSkipPostingListWhenSamplingMisses() throws Exception
    {
        List<PackedLongValues> leaves = Collections.singletonList(postings(1, 2, 3));

        setBDKPostingsWriterSizing(1, 5);
        BlockBalancedTreePostingsWriter writer = new BlockBalancedTreePostingsWriter();

        // The tree is too short to have any internal posting lists.
        writer.onLeaf(16, 1, pathToRoot(1, 2, 4, 8));

        long fp;
        try (IndexOutputWriter output = indexDescriptor.openPerIndexOutput(IndexComponent.POSTING_LISTS, indexIdentifier))
        {
            fp = writer.finish(output, leaves, indexIdentifier);
        }

        // There is only a single posting list...the leaf posting list.
        BlockBalancedTreePostingsIndex postingsIndex = new BlockBalancedTreePostingsIndex(indexDescriptor.createPerIndexFileHandle(IndexComponent.POSTING_LISTS, indexIdentifier, null), fp);
        assertEquals(1, postingsIndex.size());
    }

    @Test
    public void shouldSkipPostingListWhenTooFewLeaves() throws Exception
    {
        List<PackedLongValues> leaves = Collections.singletonList(postings(1, 2, 3));

        setBDKPostingsWriterSizing(2, 2);
        BlockBalancedTreePostingsWriter writer = new BlockBalancedTreePostingsWriter();

        // The tree is too short to have any internal posting lists.
        writer.onLeaf(16, 1, pathToRoot(1, 2, 4, 8));

        long fp;
        try (IndexOutputWriter output = indexDescriptor.openPerIndexOutput(IndexComponent.POSTING_LISTS, indexIdentifier))
        {
            fp = writer.finish(output, leaves, indexIdentifier);
        }

        // There is only a single posting list...the leaf posting list.
        BlockBalancedTreePostingsIndex postingsIndex = new BlockBalancedTreePostingsIndex(indexDescriptor.createPerIndexFileHandle(IndexComponent.POSTING_LISTS, indexIdentifier, null), fp);
        assertEquals(1, postingsIndex.size());
    }

    private void assertPostingReaderEquals(BlockBalancedTreePostingsIndex postingsIndex, int nodeID, long... postings) throws IOException
    {
        assertPostingReaderEquals(indexDescriptor.openPerIndexInput(IndexComponent.POSTING_LISTS, indexIdentifier),
                                  postingsIndex.getPostingsFilePointer(nodeID),
                                  new ArrayPostingList(postings));
    }

    private void assertPostingReaderEquals(IndexInput input, long offset, PostingList expected) throws IOException
    {
        try (PostingsReader reader = new PostingsReader(input, offset, mock(QueryEventListener.PostingListEventListener.class)))
        {
            assertPostingListEquals(expected, reader);
        }
    }

    private PackedLongValues postings(int... postings)
    {
        final PackedLongValues.Builder builder = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
        for (int posting : postings)
        {
            builder.add(posting);
        }
        return builder.build();
    }

    private IntArrayList pathToRoot(int... nodes)
    {
        final IntArrayList path = new IntArrayList();
        for (int node : nodes)
        {
            path.add(node);
        }
        return path;
    }
}
