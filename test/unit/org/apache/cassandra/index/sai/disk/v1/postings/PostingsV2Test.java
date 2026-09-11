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

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.ArrayPostingList;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.index.sai.utils.SAIRandomizedTester;
import org.apache.lucene.store.IndexInput;

import static org.junit.Assert.assertEquals;

public class PostingsV2Test extends SAIRandomizedTester
{
    private IndexDescriptor indexDescriptor;
    private IndexIdentifier indexIdentifier;

    @Before
    public void setup() throws Throwable
    {
        indexDescriptor = newIndexDescriptor();
        indexIdentifier = SAITester.createIndexIdentifier(indexDescriptor.sstableDescriptor.ksname,
                                                          indexDescriptor.sstableDescriptor.cfname,
                                                          newIndex());
    }

    private IndexInput openInput() throws IOException
    {
        return indexDescriptor.openPerIndexInput(IndexComponent.POSTING_LISTS, indexIdentifier);
    }

    @Test
    public void testV2ExactSectionOnly() throws IOException
    {
        long summaryOffset;
        try (PostingsWriter writer = new PostingsWriter(indexDescriptor, indexIdentifier))
        {
            summaryOffset = writer.writeV2(new ArrayPostingList(0, 1, 2), null);
            writer.complete();
        }
        try (IndexInput input = openInput())
        {
            PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(input, summaryOffset, true);
            assertEquals(3, summary.prefixIndex);
            assertEquals(3, summary.suffixIndex);

            try (PostingsReader reader = PostingsReader.exactSection(input, summary,
                                                                    QueryEventListener.PostingListEventListener.NO_OP))
            {
                assertEquals(0L, reader.nextPosting());
                assertEquals(1L, reader.nextPosting());
                assertEquals(2L, reader.nextPosting());
                assertEquals(PostingList.END_OF_STREAM, reader.nextPosting());
            }
            assertEquals(null, PostingsReader.prefixSection(input, summary,
                                                            QueryEventListener.PostingListEventListener.NO_OP));
        }
    }

    @Test
    public void testV2WithPrefixSection() throws IOException
    {
        // exact=[5,10] prefix=[1,3,5,7,10]
        long summaryOffset;
        try (PostingsWriter writer = new PostingsWriter(indexDescriptor, indexIdentifier))
        {
            summaryOffset = writer.writeV2(new ArrayPostingList(5, 10),
                                           new ArrayPostingList(1, 3, 5, 7, 10));
            writer.complete();
        }
        try (IndexInput input = openInput())
        {
            PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(input, summaryOffset, true);
            assertEquals(2, summary.prefixIndex);   // 2 exact postings
            assertEquals(7, summary.suffixIndex);   // 2 exact + 5 prefix

            // Exact section reads [0, prefixIndex), sorted ascending.
            try (PostingsReader exact = PostingsReader.exactSection(input, summary,
                                                                   QueryEventListener.PostingListEventListener.NO_OP))
            {
                assertEquals(5L, exact.nextPosting());
                assertEquals(10L, exact.nextPosting());
                assertEquals(PostingList.END_OF_STREAM, exact.nextPosting());
            }
        }
        try (IndexInput input = openInput())
        {
            PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(input, summaryOffset, true);
            // Prefix section reads [prefixIndex, suffixIndex), sorted ascending.
            try (PostingsReader prefix = PostingsReader.prefixSection(input, summary,
                                                                     QueryEventListener.PostingListEventListener.NO_OP))
            {
                assertEquals(1L, prefix.nextPosting());
                assertEquals(3L, prefix.nextPosting());
                assertEquals(5L, prefix.nextPosting());
                assertEquals(7L, prefix.nextPosting());
                assertEquals(10L, prefix.nextPosting());
                assertEquals(PostingList.END_OF_STREAM, prefix.nextPosting());
            }
        }
    }

    @Test
    public void testV2LargeSectionsWithAdvance() throws IOException
    {
        // Build large exact and prefix sections spanning multiple FOR blocks.
        long[] exact = new long[300];
        for (int i = 0; i < exact.length; i++)
            exact[i] = i * 2L;
        long[] prefix = new long[300];
        for (int i = 0; i < prefix.length; i++)
            prefix[i] = i * 3L;

        long summaryOffset;
        try (PostingsWriter writer = new PostingsWriter(indexDescriptor, indexIdentifier))
        {
            summaryOffset = writer.writeV2(new ArrayPostingList(exact), new ArrayPostingList(prefix));
            writer.complete();
        }
        try (IndexInput input = openInput())
        {
            PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(input, summaryOffset, true);
            assertEquals(300, summary.prefixIndex);
            assertEquals(600, summary.suffixIndex);

            try (PostingsReader prefixReader = PostingsReader.prefixSection(input, summary,
                                                                           QueryEventListener.PostingListEventListener.NO_OP))
            {
                // advance into the middle of the prefix section
                assertEquals(300L, prefixReader.advance(299L)); // first prefix value >= 299 is 300 (=100*3)
                assertEquals(303L, prefixReader.nextPosting());
            }
        }
    }

    @Test
    public void testV1BackwardCompat() throws IOException
    {
        long summaryOffset;
        try (PostingsWriter writer = new PostingsWriter(indexDescriptor, indexIdentifier))
        {
            summaryOffset = writer.write(new ArrayPostingList(0, 1, 2));
            writer.complete();
        }
        try (IndexInput input = openInput())
        {
            PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(input, summaryOffset);
            assertEquals(3, summary.numPostings);
            try (PostingsReader reader = new PostingsReader(input, summary,
                                                            QueryEventListener.PostingListEventListener.NO_OP))
            {
                assertEquals(0L, reader.nextPosting());
                assertEquals(1L, reader.nextPosting());
                assertEquals(2L, reader.nextPosting());
                assertEquals(PostingList.END_OF_STREAM, reader.nextPosting());
            }
        }
    }
}
