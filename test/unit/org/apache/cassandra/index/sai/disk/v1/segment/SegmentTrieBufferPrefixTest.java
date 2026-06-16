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
package org.apache.cassandra.index.sai.disk.v1.segment;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.index.sai.utils.IndexEntry;
import org.apache.cassandra.index.sai.utils.SAIRandomizedTester;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SegmentTrieBufferPrefixTest extends SAIRandomizedTester
{
    private void add(SegmentTrieBuffer buf, String term, int rowId)
    {
        byte[] bytes = term.getBytes(StandardCharsets.UTF_8);
        ByteComparable bc = v -> ByteSource.of(bytes, v);
        buf.add(bc, bytes.length, rowId);
    }

    /** A single buffered entry decoded from the V2 (header + sections) posting list. */
    private static class Decoded
    {
        final int exactCount;
        final int prefixCount;
        final List<Long> exact = new ArrayList<>();
        final List<Long> prefix = new ArrayList<>();

        Decoded(PostingList postings) throws Exception
        {
            this.exactCount = (int) postings.nextPosting();
            int total = (int) postings.nextPosting();
            this.prefixCount = total - exactCount;
            for (int i = 0; i < exactCount; i++)
                exact.add(postings.nextPosting());
            for (int i = 0; i < prefixCount; i++)
                prefix.add(postings.nextPosting());
        }
    }

    private List<Decoded> decodeAll(SegmentTrieBuffer buf) throws Exception
    {
        List<Decoded> result = new ArrayList<>();
        Iterator<IndexEntry> it = buf.iterator();
        while (it.hasNext())
            result.add(new Decoded(it.next().postingList));
        return result;
    }

    @Test
    public void testPrefixAccumulatedAtEligibleDepths() throws Exception
    {
        // skip = 1: accumulate prefix at every depth.
        SegmentTrieBuffer buf = new SegmentTrieBuffer(depth -> depth % 1 == 0);

        add(buf, "apple", 0);
        add(buf, "application", 1);
        add(buf, "apt", 5);

        boolean foundApNode = false;   // "ap" prefix node should cover all three rows [0,1,5]
        boolean foundLeaf = false;     // a leaf with a single exact posting

        for (Decoded d : decodeAll(buf))
        {
            if (d.exactCount == 0 && d.prefixCount == 3)
            {
                foundApNode = true;
                assertEquals(List.of(0L, 1L, 5L), d.prefix);
            }
            if (d.exactCount == 1 && d.prefixCount == 0)
                foundLeaf = true;
        }

        assertTrue("Expected an intermediate prefix node covering 3 rows", foundApNode);
        assertTrue("Expected leaf nodes with a single exact posting", foundLeaf);
    }

    @Test
    public void testPrefixSkipGating() throws Exception
    {
        // skip = 2: accumulate prefix only at even depths (2, 4, ...).
        SegmentTrieBuffer buf = new SegmentTrieBuffer(depth -> depth % 2 == 0);

        add(buf, "apple", 0);
        add(buf, "application", 1);

        // Depth-1 node "a" must NOT have prefix postings; depth-2 node "ap" must.
        boolean foundEvenDepthPrefixNode = false;
        for (Decoded d : decodeAll(buf))
        {
            if (d.exactCount == 0 && d.prefixCount == 2)
            {
                foundEvenDepthPrefixNode = true;
                assertEquals(List.of(0L, 1L), d.prefix);
            }
        }
        assertTrue("Expected an even-depth prefix node covering both rows", foundEvenDepthPrefixNode);
    }

    @Test
    public void testNoPolicyMeansRawExactPostings() throws Exception
    {
        SegmentTrieBuffer buf = new SegmentTrieBuffer(); // null policy = V1 raw

        add(buf, "apple", 0);
        add(buf, "application", 1);

        // V1 buffer emits raw exact postings (no header). Each leaf has exactly one row.
        Iterator<IndexEntry> it = buf.iterator();
        int entries = 0;
        while (it.hasNext())
        {
            PostingList postings = it.next().postingList;
            assertEquals(1, postings.size());
            assertTrue(postings.nextPosting() != PostingList.END_OF_STREAM);
            assertEquals(PostingList.END_OF_STREAM, postings.nextPosting());
            entries++;
        }
        assertEquals(2, entries); // only the two leaf terms, no intermediate nodes
    }
}
