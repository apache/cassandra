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
package org.apache.cassandra.index.sai.memory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.index.sai.utils.SAIRandomizedTester;
import org.apache.cassandra.index.sai.utils.TermsIterator;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.lucene.util.BytesRef;

import static org.apache.cassandra.utils.ByteBufferUtil.string;
import static org.junit.Assert.assertEquals;

public class RAMStringIndexerTest extends SAIRandomizedTester
{
    @Test
    public void test() throws Exception
    {
        RAMStringIndexer indexer = new RAMStringIndexer();

        indexer.add(new BytesRef("0"), 100);
        indexer.add(new BytesRef("2"), 102);
        indexer.add(new BytesRef("0"), 200);
        indexer.add(new BytesRef("2"), 202);
        indexer.add(new BytesRef("2"), 302);

        List<List<Long>> matches = new ArrayList<>();
        matches.add(Arrays.asList(100L, 200L));
        matches.add(Arrays.asList(102L, 202L, 302L));

        try (TermsIterator terms = indexer.getTermsWithPostings())
        {
            int ord = 0;
            while (terms.hasNext())
            {
                terms.next();
                try (PostingList postings = terms.postings())
                {
                    List<Long> results = new ArrayList<>();
                    long segmentRowId;
                    while ((segmentRowId = postings.nextPosting()) != PostingList.END_OF_STREAM)
                    {
                        results.add(segmentRowId);
                    }
                    assertEquals(matches.get(ord++), results);
                }
            }
        }
    }

    @Test
    public void testLargeSegment() throws IOException
    {
        final RAMStringIndexer indexer = new RAMStringIndexer();
        final int numTerms = getRandom().nextIntBetween(1 << 10, 1 << 13);
        final int numPostings = getRandom().nextIntBetween(1 << 5, 1 << 10);

        for (int id = 0; id < numTerms; ++id)
        {
            final BytesRef term = new BytesRef(String.format("%04d", id));
            for (int posting = 0; posting < numPostings; ++posting)
            {
                indexer.add(term, posting);
            }
        }

        final TermsIterator terms = indexer.getTermsWithPostings();

        ByteComparable term;
        long termOrd = 0L;
        while (terms.hasNext())
        {
            term = terms.next();
            final ByteBuffer decoded = ByteBuffer.wrap(ByteSourceInverse.readBytes(term.asComparableBytes(ByteComparable.Version.OSS50)));
            assertEquals(String.format("%04d", termOrd), string(decoded));

            try (PostingList postingList = terms.postings())
            {
                assertEquals(numPostings, postingList.size());
                for (int i = 0; i < numPostings; ++i)
                {
                    assertEquals(i, postingList.nextPosting());
                }
                assertEquals(PostingList.END_OF_STREAM, postingList.nextPosting());
            }
            termOrd++;
        }

        assertEquals(numTerms, termOrd);
    }
}
