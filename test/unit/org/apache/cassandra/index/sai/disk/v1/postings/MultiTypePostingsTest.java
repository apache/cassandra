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
package org.apache.cassandra.index.sai.disk.v1.postings;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.ArrayPostingList;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.SAICodecUtils;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.index.sai.utils.SAIRandomizedTester;
import org.apache.lucene.store.IndexInput;

import static org.junit.Assert.assertEquals;

/**
 * Tests for multi-type postings (exactMatch + prefix) written by {@link PostingsWriter#writeWithFilterTypes}
 * and read by {@link PostingsReader#forOperator}.
 */
public class MultiTypePostingsTest extends SAIRandomizedTester
{
    private IndexDescriptor indexDescriptor;
    private IndexIdentifier indexIdentifier;

    @Before
    public void setup() throws Throwable
    {
        indexDescriptor = newIndexDescriptor();
        String index = newIndex();
        indexIdentifier = SAITester.createIndexIdentifier(indexDescriptor.sstableDescriptor.ksname,
                                                          indexDescriptor.sstableDescriptor.cfname,
                                                          index);
    }

    /**
     * Write exactMatch + prefix postings, read back with EQ and LIKE_PREFIX operators.
     * Stream: [0, 3, 10, 20, 30, 5, 15, 25]
     */
    @Test
    public void testExactMatchAndPrefixPostings() throws Exception
    {
        final int blockSize = 128;
        ArrayPostingList postingList = new ArrayPostingList(
            0, 3,        // metadata: exactMatchStartIndex=0, prefixStartIndex=3
            10, 20, 30,  // exactMatch postings
            5, 15, 25    // prefix postings
        );

        long summaryOffset;
        try (PostingsWriter writer = new PostingsWriter(indexDescriptor, indexIdentifier, blockSize))
        {
            summaryOffset = writer.writeWithFilterTypes(postingList);
            writer.complete();
        }

        // Verify summary metadata
        try (IndexInput input = indexDescriptor.openPerIndexInput(IndexComponent.POSTING_LISTS, indexIdentifier))
        {
            SAICodecUtils.validate(input);
            PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(input, summaryOffset);
            assertEquals(blockSize, summary.blockSize);
            assertEquals(6, summary.numPostings);
            assertEquals(2, summary.filterTypes);
            assertEquals(0, summary.typeStartIndices[0]);
            assertEquals(3, summary.typeStartIndices[1]);
        }

        // EQ -> only exactMatch postings [10, 20, 30]
        try (IndexInput summaryInput = indexDescriptor.openPerIndexInput(IndexComponent.POSTING_LISTS, indexIdentifier);
             IndexInput postingsInput = indexDescriptor.openPerIndexInput(IndexComponent.POSTING_LISTS, indexIdentifier))
        {
            PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(summaryInput, summaryOffset);
            try (PostingsReader reader = PostingsReader.forOperator(postingsInput, summary, Expression.IndexOperator.EQ, new NoOpListener()))
            {
                assertEquals(3, reader.size());
                assertEquals(10, reader.nextPosting());
                assertEquals(20, reader.nextPosting());
                assertEquals(30, reader.nextPosting());
                assertEquals(PostingList.END_OF_STREAM, reader.nextPosting());
            }
        }

        // LIKE_PREFIX -> all postings [10, 20, 30, 5, 15, 25]
        try (IndexInput summaryInput = indexDescriptor.openPerIndexInput(IndexComponent.POSTING_LISTS, indexIdentifier);
             IndexInput postingsInput = indexDescriptor.openPerIndexInput(IndexComponent.POSTING_LISTS, indexIdentifier))
        {
            PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(summaryInput, summaryOffset);
            try (PostingsReader reader = PostingsReader.forOperator(postingsInput, summary, Expression.IndexOperator.LIKE_PREFIX, new NoOpListener()))
            {
                assertEquals(6, reader.size());
                assertEquals(10, reader.nextPosting());
                assertEquals(20, reader.nextPosting());
                assertEquals(30, reader.nextPosting());
                assertEquals(5, reader.nextPosting());
                assertEquals(15, reader.nextPosting());
                assertEquals(25, reader.nextPosting());
                assertEquals(PostingList.END_OF_STREAM, reader.nextPosting());
            }
        }
    }

    /**
     * Single-type write() - no prefix concept. filterTypes=1, startIndex=size (no prefix postings).
     * EQ should return all postings since there are no filter types to distinguish.
     */
    @Test
    public void testSingleTypeWrite() throws Exception
    {
        final int blockSize = 128;
        ArrayPostingList postingList = new ArrayPostingList(10, 20, 30, 40, 50);

        long summaryOffset;
        try (PostingsWriter writer = new PostingsWriter(indexDescriptor, indexIdentifier, blockSize))
        {
            summaryOffset = writer.write(postingList);
            writer.complete();
        }

        // Verify summary metadata
        try (IndexInput input = indexDescriptor.openPerIndexInput(IndexComponent.POSTING_LISTS, indexIdentifier))
        {
            SAICodecUtils.validate(input);
            PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(input, summaryOffset);
            assertEquals(2, summary.filterTypes);
            assertEquals(5, summary.numPostings);
            // startIndices=[0, 5] indicating all postings are exactMatch, no prefix postings
            assertEquals(0, summary.typeStartIndices[0]);
            assertEquals(5, summary.typeStartIndices[1]);
        }

        // EQ returns all 5 postings
        try (IndexInput summaryInput = indexDescriptor.openPerIndexInput(IndexComponent.POSTING_LISTS, indexIdentifier);
             IndexInput postingsInput = indexDescriptor.openPerIndexInput(IndexComponent.POSTING_LISTS, indexIdentifier))
        {
            PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(summaryInput, summaryOffset);
            try (PostingsReader reader = PostingsReader.forOperator(postingsInput, summary, Expression.IndexOperator.EQ, new NoOpListener()))
            {
                assertEquals(5, reader.size());
                for (long expected : new long[]{10, 20, 30, 40, 50})
                    assertEquals(expected, reader.nextPosting());
                assertEquals(PostingList.END_OF_STREAM, reader.nextPosting());
            }
        }
    }

    /**
     * No prefix postings - prefixStartIndex == numPostings.
     */
    @Test
    public void testNoPrefixPostings() throws Exception
    {
        final int blockSize = 128;
        ArrayPostingList postingList = new ArrayPostingList(0, 4, 10, 20, 30, 40);

        long summaryOffset;
        try (PostingsWriter writer = new PostingsWriter(indexDescriptor, indexIdentifier, blockSize))
        {
            summaryOffset = writer.writeWithFilterTypes(postingList);
            writer.complete();
        }

        // Both EQ and LIKE_PREFIX return all 4
        for (Expression.IndexOperator op : new Expression.IndexOperator[]{Expression.IndexOperator.EQ, Expression.IndexOperator.LIKE_PREFIX})
        {
            try (IndexInput summaryInput = indexDescriptor.openPerIndexInput(IndexComponent.POSTING_LISTS, indexIdentifier);
                 IndexInput postingsInput = indexDescriptor.openPerIndexInput(IndexComponent.POSTING_LISTS, indexIdentifier))
            {
                PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(summaryInput, summaryOffset);
                try (PostingsReader reader = PostingsReader.forOperator(postingsInput, summary, op, new NoOpListener()))
                {
                    assertEquals(4, reader.size());
                    for (long expected : new long[]{10, 20, 30, 40})
                        assertEquals(expected, reader.nextPosting());
                    assertEquals(PostingList.END_OF_STREAM, reader.nextPosting());
                }
            }
        }
    }

    /**
     * Small block size - tests block padding at type boundary and delta base reset.
     * 5 exactMatch postings (block 0 full, block 1 partial, padded), 3 prefix postings (block 2 fresh).
     * Prefix row IDs are less than exactMatch row IDs to verify delta base reset works.
     */
    @Test
    public void testTypeBoundaryWithSmallBlockSize() throws Exception
    {
        final int blockSize = 4;
        ArrayPostingList postingList = new ArrayPostingList(
            0, 5,
            10, 20, 30, 40, 50,  // exactMatch
            3, 7, 12             // prefix (3 < 50, delta base reset needed)
        );

        long summaryOffset;
        try (PostingsWriter writer = new PostingsWriter(indexDescriptor, indexIdentifier, blockSize))
        {
            summaryOffset = writer.writeWithFilterTypes(postingList);
            writer.complete();
        }

        // EQ: [10, 20, 30, 40, 50]
        try (IndexInput summaryInput = indexDescriptor.openPerIndexInput(IndexComponent.POSTING_LISTS, indexIdentifier);
             IndexInput postingsInput = indexDescriptor.openPerIndexInput(IndexComponent.POSTING_LISTS, indexIdentifier))
        {
            PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(summaryInput, summaryOffset);
            try (PostingsReader reader = PostingsReader.forOperator(postingsInput, summary, Expression.IndexOperator.EQ, new NoOpListener()))
            {
                assertEquals(5, reader.size());
                for (long expected : new long[]{10, 20, 30, 40, 50})
                    assertEquals(expected, reader.nextPosting());
                assertEquals(PostingList.END_OF_STREAM, reader.nextPosting());
            }
        }

        // LIKE_PREFIX: [10, 20, 30, 40, 50, 3, 7, 12]
        try (IndexInput summaryInput = indexDescriptor.openPerIndexInput(IndexComponent.POSTING_LISTS, indexIdentifier);
             IndexInput postingsInput = indexDescriptor.openPerIndexInput(IndexComponent.POSTING_LISTS, indexIdentifier))
        {
            PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(summaryInput, summaryOffset);
            try (PostingsReader reader = PostingsReader.forOperator(postingsInput, summary, Expression.IndexOperator.LIKE_PREFIX, new NoOpListener()))
            {
                assertEquals(8, reader.size());
                for (long expected : new long[]{10, 20, 30, 40, 50, 3, 7, 12})
                    assertEquals(expected, reader.nextPosting());
                assertEquals(PostingList.END_OF_STREAM, reader.nextPosting());
            }
        }
    }

    /**
     * Exact block boundary - 4 exactMatch fills block 0 exactly, no padding needed.
     * Prefix postings start fresh in block 1.
     */
    @Test
    public void testExactBlockBoundaryAlignment() throws Exception
    {
        final int blockSize = 4;
        ArrayPostingList postingList = new ArrayPostingList(
            0, 4,
            10, 20, 30, 40,  // exactMatch (fills block 0 exactly)
            2, 8, 14         // prefix (block 1 with fresh firstPosting)
        );

        long summaryOffset;
        try (PostingsWriter writer = new PostingsWriter(indexDescriptor, indexIdentifier, blockSize))
        {
            summaryOffset = writer.writeWithFilterTypes(postingList);
            writer.complete();
        }

        // EQ: [10, 20, 30, 40]
        try (IndexInput summaryInput = indexDescriptor.openPerIndexInput(IndexComponent.POSTING_LISTS, indexIdentifier);
             IndexInput postingsInput = indexDescriptor.openPerIndexInput(IndexComponent.POSTING_LISTS, indexIdentifier))
        {
            PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(summaryInput, summaryOffset);
            try (PostingsReader reader = PostingsReader.forOperator(postingsInput, summary, Expression.IndexOperator.EQ, new NoOpListener()))
            {
                assertEquals(4, reader.size());
                for (long expected : new long[]{10, 20, 30, 40})
                    assertEquals(expected, reader.nextPosting());
                assertEquals(PostingList.END_OF_STREAM, reader.nextPosting());
            }
        }

        // LIKE_PREFIX: [10, 20, 30, 40, 2, 8, 14]
        try (IndexInput summaryInput = indexDescriptor.openPerIndexInput(IndexComponent.POSTING_LISTS, indexIdentifier);
             IndexInput postingsInput = indexDescriptor.openPerIndexInput(IndexComponent.POSTING_LISTS, indexIdentifier))
        {
            PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(summaryInput, summaryOffset);
            try (PostingsReader reader = PostingsReader.forOperator(postingsInput, summary, Expression.IndexOperator.LIKE_PREFIX, new NoOpListener()))
            {
                assertEquals(7, reader.size());
                for (long expected : new long[]{10, 20, 30, 40, 2, 8, 14})
                    assertEquals(expected, reader.nextPosting());
                assertEquals(PostingList.END_OF_STREAM, reader.nextPosting());
            }
        }
    }

    private static class NoOpListener implements QueryEventListener.PostingListEventListener
    {
        @Override
        public void onAdvance() {}

        @Override
        public void postingDecoded(long postingsDecoded) {}
    }
}
