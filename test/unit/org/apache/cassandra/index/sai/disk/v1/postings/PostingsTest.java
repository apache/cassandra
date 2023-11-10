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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.disk.ArrayPostingList;
import org.apache.cassandra.index.sai.disk.v1.SAICodecUtils;
import org.apache.cassandra.index.sai.utils.SAIRandomizedTester;
import org.apache.lucene.store.IndexInput;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PostingsTest extends SAIRandomizedTester
{
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

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

    @Test
    public void testSingleBlockPostingList() throws Exception
    {
        final int blockSize = 1 << between(3, 8);
        final ArrayPostingList expectedPostingList = new ArrayPostingList(10, 20, 30, 40, 50, 60);

        long postingPointer;
        try (PostingsWriter writer = new PostingsWriter(indexDescriptor, indexIdentifier, blockSize))
        {
            postingPointer = writer.write(expectedPostingList);
            writer.complete();
        }

        IndexInput input = indexDescriptor.openPerIndexInput(IndexComponent.POSTING_LISTS, indexIdentifier);
        SAICodecUtils.validate(input);
        input.seek(postingPointer);

        final PostingsReader.BlocksSummary summary = assertBlockSummary(blockSize, expectedPostingList, input);
        assertEquals(1, summary.offsets.length());

        CountingPostingListEventListener listener = new CountingPostingListEventListener();
        PostingsReader reader = new PostingsReader(input, postingPointer, listener);

        expectedPostingList.reset();
        assertEquals(expectedPostingList.getOrdinal(), reader.getOrdinal());
        assertEquals(expectedPostingList.size(), reader.size());

        long actualRowID;
        while ((actualRowID = reader.nextPosting()) != PostingList.END_OF_STREAM)
        {
            assertEquals(expectedPostingList.nextPosting(), actualRowID);
            assertEquals(expectedPostingList.getOrdinal(), reader.getOrdinal());
        }
        assertEquals(PostingList.END_OF_STREAM, expectedPostingList.nextPosting());
        assertEquals(0, listener.advances);
        reader.close();
        assertEquals(reader.size(), listener.decodes);

        input = indexDescriptor.openPerIndexInput(IndexComponent.POSTING_LISTS, indexIdentifier);
        listener = new CountingPostingListEventListener();
        reader = new PostingsReader(input, postingPointer, listener);

        assertEquals(50, reader.advance(45));
        assertEquals(60, reader.advance(60));
        assertEquals(PostingList.END_OF_STREAM, reader.nextPosting());
        assertEquals(2, listener.advances);
        reader.close();

        assertEquals(reader.size(), listener.decodes); // nothing more was decoded
    }

    @Test
    public void testMultiBlockPostingList() throws Exception
    {
        final int numPostingLists = 1 << between(1, 5);
        final int blockSize = 1 << between(5, 8);
        final int numPostings = getRandom().nextIntBetween(1 << 11, 1 << 16);
        final ArrayPostingList[] expected = new ArrayPostingList[numPostingLists];
        final long[] postingPointers = new long[numPostingLists];

        try (PostingsWriter writer = new PostingsWriter(indexDescriptor, indexIdentifier, blockSize))
        {
            for (int i = 0; i < numPostingLists; ++i)
            {
                final long[] postings = randomPostings(numPostings);
                final ArrayPostingList postingList = new ArrayPostingList(postings);
                expected[i] = postingList;
                postingPointers[i] = writer.write(postingList);
            }
            writer.complete();
        }

        try (IndexInput input = indexDescriptor.openPerIndexInput(IndexComponent.POSTING_LISTS, indexIdentifier))
        {
            SAICodecUtils.validate(input);
        }

        for (int i = 0; i < numPostingLists; ++i)
        {
            IndexInput input = indexDescriptor.openPerIndexInput(IndexComponent.POSTING_LISTS, indexIdentifier);
            input.seek(postingPointers[i]);
            ArrayPostingList expectedPostingList = expected[i];
            PostingsReader.BlocksSummary summary = assertBlockSummary(blockSize, expectedPostingList, input);
            assertTrue(summary.offsets.length() > 1);

            CountingPostingListEventListener listener = new CountingPostingListEventListener();
            try (PostingsReader reader = new PostingsReader(input, postingPointers[i], listener))
            {
                expectedPostingList.reset();
                assertEquals(expectedPostingList.getOrdinal(), reader.getOrdinal());
                assertEquals(expectedPostingList.size(), reader.size());

                assertPostingListEquals(expectedPostingList, reader);
                assertEquals(0, listener.advances);
            }

            // test random advances through the posting list
            listener = new CountingPostingListEventListener();
            input = indexDescriptor.openPerIndexInput(IndexComponent.POSTING_LISTS, indexIdentifier);
            try (PostingsReader reader = new PostingsReader(input, postingPointers[i], listener))
            {
                expectedPostingList.reset();
                int advances = 0;
                for (int p = 0; p < numPostings - blockSize; p += getRandom().nextIntBetween(1, blockSize / 2))
                {
                    advances++;
                    assertEquals(expectedPostingList.advance(p), reader.advance(p));
                }
                assertEquals(advances, listener.advances);
            }

            // test skipping to the last block
            listener = new CountingPostingListEventListener();
            input = indexDescriptor.openPerIndexInput(IndexComponent.POSTING_LISTS, indexIdentifier);
            try (PostingsReader reader = new PostingsReader(input, postingPointers[i], listener))
            {
                long tokenToAdvance = -1;
                expectedPostingList.reset();
                for (int p = 0; p < numPostings - 7; ++p)
                {
                    tokenToAdvance = expectedPostingList.nextPosting();
                }

                expectedPostingList.reset();
                assertEquals(expectedPostingList.advance(tokenToAdvance),
                             reader.advance(tokenToAdvance));

                assertPostingListEquals(expectedPostingList, reader);
                assertEquals(1, listener.advances);
            }
        }
    }

    @Test
    public void testDuplicatePostings() throws Exception
    {
        int blockSize = 4;
        // For the duplicate testing code to work we need to have a block full of duplicate values
        // with the end value of the preceeding block having the same duplicate value.
        final ArrayPostingList expectedPostingList = new ArrayPostingList(0, 1, 1, 3, 3, 5, 5, 7, 7, 7, 7, 7, 7, 9, 9, 10, 11, 12);

        long postingPointer;
        try (PostingsWriter writer = new PostingsWriter(indexDescriptor, indexIdentifier, blockSize))
        {
            postingPointer = writer.write(expectedPostingList);
            writer.complete();
        }

        IndexInput input = indexDescriptor.openPerIndexInput(IndexComponent.POSTING_LISTS, indexIdentifier);
        CountingPostingListEventListener listener = new CountingPostingListEventListener();
        try (PostingsReader reader = new PostingsReader(input, postingPointer, listener))
        {
            assertEquals(7L, reader.advance(7));
        }
    }

    @Test
    public void testAdvance() throws Exception
    {
        final int blockSize = 4; // 4 postings per FoR block
        final int maxSegmentRowID = 30;
        final long[] postings = LongStream.range(0, maxSegmentRowID).toArray(); // 30 postings = 7 FoR blocks + 1 VLong block
        final ArrayPostingList expected = new ArrayPostingList(postings);

        long fp;
        try (PostingsWriter writer = new PostingsWriter(indexDescriptor, indexIdentifier, blockSize))
        {
            fp = writer.write(expected);
            writer.complete();
        }

        try (IndexInput input = indexDescriptor.openPerIndexInput(IndexComponent.POSTING_LISTS, indexIdentifier))
        {
            SAICodecUtils.validate(input);
            input.seek(fp);

            final PostingsReader.BlocksSummary summary = assertBlockSummary(blockSize, expected, input);
            assertEquals((int) Math.ceil((double) maxSegmentRowID / blockSize), summary.offsets.length());

            for (int i = 0; i < summary.maxValues.length(); i++)
            {
                assertEquals(Math.min(maxSegmentRowID - 1, (i + 1) * blockSize - 1), summary.maxValues.get(i));
            }
        }

        // exact advance
        testAdvance(fp, expected, new long[]{ 3, 7, 11, 15, 19 });
        // non-exact advance
        testAdvance(fp, expected, new long[]{ 2, 6, 12, 17, 25 });

        // exact advance
        testAdvance(fp, expected, new long[]{ 3, 5, 7, 12 });
        // non-exact advance
        testAdvance(fp, expected, new long[]{ 2, 7, 9, 11 });
    }

    @Test
    public void testAdvanceOnRandomizedData() throws IOException
    {
        final int blockSize = 4;
        final int numPostings = nextInt(64, 64_000);
        final long[] postings = randomPostings(numPostings);

        final ArrayPostingList expected = new ArrayPostingList(postings);

        long fp;
        try (PostingsWriter writer = new PostingsWriter(indexDescriptor, indexIdentifier, blockSize))
        {
            fp = writer.write(expected);
            writer.complete();
        }

        try (IndexInput input = indexDescriptor.openPerIndexInput(IndexComponent.POSTING_LISTS, indexIdentifier))
        {
            SAICodecUtils.validate(input);
            input.seek(fp);

            final PostingsReader.BlocksSummary summary = assertBlockSummary(blockSize, expected, input);
            assertEquals((int) Math.ceil((double) numPostings / blockSize), summary.offsets.length());

            for (int i = 0; i < summary.maxValues.length(); i++)
            {
                assertEquals(postings[Math.min(numPostings - 1, (i + 1) * blockSize - 1)], summary.maxValues.get(i));
            }
        }

        testAdvance(fp, expected, postings);
    }

    @Test
    @SuppressWarnings("all")
    public void testNullPostingList() throws IOException
    {
        try (PostingsWriter writer = new PostingsWriter(indexDescriptor, indexIdentifier))
        {
            expectedException.expect(IllegalArgumentException.class);
            writer.write(null);
            writer.complete();
        }
    }

    @Test
    public void testEmptyPostingList() throws IOException
    {
        try (PostingsWriter writer = new PostingsWriter(indexDescriptor, indexIdentifier))
        {
            expectedException.expect(IllegalArgumentException.class);
            writer.write(new ArrayPostingList());
        }
    }

    @Test
    public void testNonAscendingPostingList() throws IOException
    {
        try (PostingsWriter writer = new PostingsWriter(indexDescriptor, indexIdentifier))
        {
            expectedException.expect(IllegalArgumentException.class);
            writer.write(new ArrayPostingList(1, 0));
        }
    }

    private void testAdvance(long fp, ArrayPostingList expected, long[] targetIDs) throws IOException
    {
        expected.reset();
        final CountingPostingListEventListener listener = new CountingPostingListEventListener();
        PostingsReader reader = openReader(fp, listener);
        for (int i = 0; i < 2; ++i)
        {
            assertEquals(expected.nextPosting(), reader.nextPosting());
            assertEquals(expected.getOrdinal(), reader.getOrdinal());
        }

        for (long target : targetIDs)
        {
            final long actualRowId = reader.advance(target);
            final long expectedRowId = expected.advance(target);

            assertEquals(expectedRowId, actualRowId);

            assertEquals(expected.getOrdinal(), reader.getOrdinal());
        }

        // check if iterator is correctly positioned
        assertPostingListEquals(expected, reader);
        // check if reader emitted all events
        assertEquals(targetIDs.length, listener.advances);

        reader.close();
    }

    private PostingsReader openReader(long fp, QueryEventListener.PostingListEventListener listener) throws IOException
    {
        IndexInput input = indexDescriptor.openPerIndexInput(IndexComponent.POSTING_LISTS, indexIdentifier);
        input.seek(fp);
        return new PostingsReader(input, fp, listener);
    }

    private PostingsReader.BlocksSummary assertBlockSummary(int blockSize, PostingList expected, IndexInput input) throws IOException
    {
        final PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(input, input.getFilePointer());
        assertEquals(blockSize, summary.blockSize);
        assertEquals(expected.size(), summary.numPostings);
        assertTrue(summary.offsets.length() > 0);
        assertEquals(summary.offsets.length(), summary.maxValues.length());
        return summary;
    }

    private long[] randomPostings(int numPostings)
    {
        final AtomicInteger rowId = new AtomicInteger();
        // postings with duplicates
        return LongStream.generate(() -> rowId.getAndAdd(getRandom().nextIntBetween(0, 3)))
                         .limit(numPostings)
                         .toArray();
    }

    static class CountingPostingListEventListener implements QueryEventListener.PostingListEventListener
    {
        int advances;
        int decodes;

        @Override
        public void onAdvance()
        {
            advances++;
        }

        @Override
        public void postingDecoded(long postingsDecoded)
        {
            this.decodes += postingsDecoded;
        }
    }
}
