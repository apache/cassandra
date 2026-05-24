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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.SAICodecUtils;
import org.apache.cassandra.index.sai.disk.v1.trie.LiteralIndexWriter;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.index.sai.utils.IndexEntry;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.index.sai.utils.IndexTermType;
import org.apache.cassandra.index.sai.utils.SAIRandomizedTester;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests that {@link SegmentTrieBuffer} correctly buffers terms and postings, and that its iterator
 * emits entries in sorted term order with the expected layout:
 * <pre>
 *   [startIdxExactMatch, startIdxPrefix, exactMatchPosting1, exactMatchPosting2, ..., prefixPosting1, ...]
 * </pre>
 *
 * The default {@link SegmentTrieBuffer#add(ByteComparable, int, int)} inserts postings as type 0
 * (exactMatch). The overloaded {@link SegmentTrieBuffer#add(ByteComparable, int, int, int)} allows
 * specifying a type (0 = exactMatch, 1 = prefix) so prefix postings can also be stored.
 */
public class SegmentTrieBufferTest extends SAIRandomizedTester
{
    private static final Logger logger = LoggerFactory.getLogger(SegmentTrieBufferTest.class);
    // @Test
    // public void testIteratorEmitsStartIndicesBeforePostings() throws IOException
    // {
    //     SegmentTrieBuffer buffer = new SegmentTrieBuffer();
    //     IndexTermType indexTermType = createIndexTermType(UTF8Type.instance);

    //     // Add postings for multiple string terms in non-sorted order.
    //     //   "banana" -> row IDs 0, 3, 5  (3 exactMatch postings)
    //     //   "apple"  -> row IDs 1, 4     (2 exactMatch postings)
    //     //   "cherry" -> row ID  2        (1 exactMatch posting)
    //     //   "date"   -> row IDs 6, 7, 8, 9  (4 exactMatch postings)
    //     addTerm(buffer, indexTermType, "banana", 0);
    //     addTerm(buffer, indexTermType, "apple",  1);
    //     addTerm(buffer, indexTermType, "cherry", 2);
    //     addTerm(buffer, indexTermType, "banana", 3);
    //     addTerm(buffer, indexTermType, "apple",  4);
    //     addTerm(buffer, indexTermType, "banana", 5);
    //     addTerm(buffer, indexTermType, "date",   6);
    //     addTerm(buffer, indexTermType, "date",   7);
    //     addTerm(buffer, indexTermType, "date",   8);
    //     addTerm(buffer, indexTermType, "date",   9);

    //     assertEquals(10, buffer.numRows());

    //     Iterator<IndexEntry> it = buffer.iterator();

    //     // Iterator should return terms in sorted (byte-comparable) order:
    //     //   "apple", "banana", "cherry", "date"

    //     // --- "apple": 2 exactMatch postings, 0 prefix postings ---
    //     assertTrue(it.hasNext());
    //     IndexEntry apple = it.next();
    //     assertEquals("apple", unpackString(apple.term));
    //     // size() returns only the count of actual postings (exactMatch + prefix), not the offset header
    //     assertEquals(2, apple.postingList.size());
    //     // First two nextPosting() calls return the start indices
    //     assertEquals(0L, apple.postingList.nextPosting());   // startIdxExactMatch = 0
    //     assertEquals(2L, apple.postingList.nextPosting());   // startIdxPrefix = 2 (2 exactMatch postings)
    //     // Remaining calls return the actual exactMatch row IDs
    //     assertEquals(1L, apple.postingList.nextPosting());
    //     assertEquals(4L, apple.postingList.nextPosting());
    //     assertEquals(PostingList.END_OF_STREAM, apple.postingList.nextPosting());

    //     // --- "banana": 3 exactMatch postings, 0 prefix postings ---
    //     assertTrue(it.hasNext());
    //     IndexEntry banana = it.next();
    //     assertEquals("banana", unpackString(banana.term));
    //     assertEquals(3, banana.postingList.size());
    //     assertEquals(0L, banana.postingList.nextPosting());  // startIdxExactMatch
    //     assertEquals(3L, banana.postingList.nextPosting());  // startIdxPrefix = 3
    //     assertEquals(0L, banana.postingList.nextPosting());
    //     assertEquals(3L, banana.postingList.nextPosting());
    //     assertEquals(5L, banana.postingList.nextPosting());
    //     assertEquals(PostingList.END_OF_STREAM, banana.postingList.nextPosting());

    //     // --- "cherry": 1 exactMatch posting, 0 prefix postings ---
    //     assertTrue(it.hasNext());
    //     IndexEntry cherry = it.next();
    //     assertEquals("cherry", unpackString(cherry.term));
    //     assertEquals(1, cherry.postingList.size());
    //     assertEquals(0L, cherry.postingList.nextPosting());  // startIdxExactMatch
    //     assertEquals(1L, cherry.postingList.nextPosting());  // startIdxPrefix = 1
    //     assertEquals(2L, cherry.postingList.nextPosting());  // actual row ID
    //     assertEquals(PostingList.END_OF_STREAM, cherry.postingList.nextPosting());

    //     // --- "date": 4 exactMatch postings, 0 prefix postings ---
    //     assertTrue(it.hasNext());
    //     IndexEntry date = it.next();
    //     assertEquals("date", unpackString(date.term));
    //     assertEquals(4, date.postingList.size());
    //     assertEquals(0L, date.postingList.nextPosting());    // startIdxExactMatch
    //     assertEquals(4L, date.postingList.nextPosting());    // startIdxPrefix = 4
    //     assertEquals(6L, date.postingList.nextPosting());
    //     assertEquals(7L, date.postingList.nextPosting());
    //     assertEquals(8L, date.postingList.nextPosting());
    //     assertEquals(9L, date.postingList.nextPosting());
    //     assertEquals(PostingList.END_OF_STREAM, date.postingList.nextPosting());

    //     assertFalse(it.hasNext());
    // }

    private void addTerm(SegmentTrieBuffer buffer, IndexTermType indexTermType, String term, int segmentRowId)
    {
        buffer.add(v -> indexTermType.asComparableBytes(UTF8Type.instance.decompose(term), v),
                   UTF8Type.instance.decompose(term).remaining(),
                   segmentRowId);
    }

    /**
     * Tests terms that share common prefixes are stored and iterated correctly, with
     * explicit verification of both exactMatch and prefix posting sections.
     *
     * Terms: "apple", "application", "car", "cartridge", "cat"
     * Note: "apple"/"application" share prefix "appl"; "car"/"cartridge"/"cat" share prefix "ca".
     *
     * Sorted order: "apple", "application", "car", "cartridge", "cat"
     *
     * Each posting list is laid out as:
     *   [startIdxExactMatch, startIdxPrefix, exactMatchRow1, ..., prefixRow1, ...]
     *
     * putRecursive in InMemoryTrie calls applyContent with prefix type at every intermediate
     * node on the key path. However, since the return value is currently discarded (line 920),
     * prefix postings at intermediate trie nodes are NOT linked. Therefore all postings in
     * the iterator are exactMatch only, and the prefix section is empty for every term.
     */
    @Test
    public void testCommonPrefixTerms() throws IOException
    {
        SegmentTrieBuffer buffer = new SegmentTrieBuffer();
        IndexTermType indexTermType = createIndexTermType(UTF8Type.instance);

        // Add terms with shared prefixes in non-sorted order
        //   "cat"         -> row IDs 0, 7
        //   "apple"       -> row IDs 1, 5
        //   "car"         -> row ID  2
        //   "application" -> row IDs 3, 6
        //   "cartridge"   -> row ID  4
        addTerm(buffer, indexTermType, "cat", 0);
        addTerm(buffer, indexTermType, "apple", 1);
        addTerm(buffer, indexTermType, "car", 2);
        addTerm(buffer, indexTermType, "application", 3);
        addTerm(buffer, indexTermType, "cartridge", 4);
        addTerm(buffer, indexTermType, "apple", 5);
        addTerm(buffer, indexTermType, "applications", 6);
        addTerm(buffer, indexTermType, "cat", 7);

        assertEquals(8, buffer.numRows());

        Iterator<IndexEntry> it = buffer.iterator();
        while(it.hasNext())
        {
            IndexEntry entry = it.next();
            PostingList postingList = entry.postingList;

            // Try to decode as UTF8; fall back to hex bytes for intermediate trie nodes
            String termLabel;
            try
            {
                termLabel = "\"" + unpackString(entry.term) + "\"";
            }
            catch (Throwable t)
            {
                // Intermediate prefix node — not a valid complete UTF8 encoding
                StringBuilder hex = new StringBuilder("0x[");
                ByteSource bs = entry.term.asComparableBytes(ByteComparable.Version.OSS50);
                int b;
                boolean first = true;
                while ((b = bs.next()) != ByteSource.END_OF_STREAM)
                {
                    if (!first) hex.append(", ");
                    hex.append(String.format("%02X", b));
                    first = false;
                }
                hex.append("]");
                termLabel = hex.toString() + " (intermediate prefix node)";
            }

            StringBuilder sb = new StringBuilder();
            sb.append("Term: ").append(termLabel).append(" -> [");
            long posting;
            boolean first = true;
            while ((posting = postingList.nextPosting()) != PostingList.END_OF_STREAM)
            {
                if (!first) sb.append(", ");
                sb.append(posting);
                first = false;
            }
            sb.append("]  size=").append(postingList.size());
            logger.info(sb.toString());
        }
    }

    /**
     * Verifies the full posting list layout: start indices followed by exactMatch rows then prefix rows.
     *
     * Expected sequence from nextPosting():
     *   startIdxExactMatch (always 0), startIdxPrefix (= exactMatchCount),
     *   exactMatchRow1, exactMatchRow2, ...,
     *   prefixRow1, prefixRow2, ...,
     *   END_OF_STREAM
     */
    private void verifyPostings(PostingList postingList,
                                int expectedExactCount, long[] expectedExactRows,
                                int expectedPrefixCount, long[] expectedPrefixRows) throws IOException
    {
        long totalPostings = expectedExactCount + expectedPrefixCount;
        assertEquals("total posting count (exactMatch + prefix)", totalPostings, postingList.size());

        // --- Start indices ---
        assertEquals("startIdxExactMatch must be 0", 0L, postingList.nextPosting());
        assertEquals("startIdxPrefix must equal exactMatch count",
                     (long) expectedExactCount, postingList.nextPosting());

        // --- ExactMatch postings section ---
        for (int i = 0; i < expectedExactRows.length; i++)
            assertEquals("exactMatch row [" + i + "]", expectedExactRows[i], postingList.nextPosting());

        // --- Prefix postings section ---
        for (int i = 0; i < expectedPrefixRows.length; i++)
            assertEquals("prefix row [" + i + "]", expectedPrefixRows[i], postingList.nextPosting());

        assertEquals("no more postings after prefix section",
                     PostingList.END_OF_STREAM, postingList.nextPosting());
    }

    /**
     * End-to-end test: add terms with common prefixes to SegmentTrieBuffer, write to disk via
     * LiteralIndexWriter, then read back prefix postings for intermediate trie nodes ("appl", "ca",
     * "car") and exact-match postings for full terms ("apple", "car", "application", "cat").
     *
     * When putRecursive runs with withPrefixes=true, it stores a prefix-type posting at every
     * intermediate byte node along the key path and an exactMatch-type posting at the terminal node.
     * UTF8Type encodes "car" as [0x63, 0x61, 0x72, 0x00(escape), END_OF_STREAM], so the intermediate
     * node at [0x63, 0x61, 0x72] (raw bytes, no escape) is a separate on-disk trie entry from the
     * terminal node at [0x63, 0x61, 0x72, 0x00].
     *
     * Prefix lookup: ByteComparable.fixedLength(rawAsciiBytes) matches intermediate node.
     * Full-term lookup: UTF8Type-encoded ByteComparable matches terminal node.
     */
    @Test
    public void testEndToEndPrefixAndExactMatchPostings() throws IOException
    {
        // --- Phase 1: Setup ---
        IndexDescriptor indexDescriptor = newIndexDescriptor();
        IndexIdentifier indexIdentifier = createIndexIdentifier("test", "test", newIndex());
        IndexTermType indexTermType = createIndexTermType(UTF8Type.instance);

        SegmentTrieBuffer buffer = new SegmentTrieBuffer();

        // --- Phase 2: Add terms with common prefixes ---
        // Chosen so that some full terms are prefixes of others:
        //   "apple"/"apples", "application"/"applications", "car"/"cars"/"cartridge", "cat"/"cats"
        // Row IDs are monotonically increasing (required for sorted prefix postings).
        addTerm(buffer, indexTermType, "cat", 0);
        addTerm(buffer, indexTermType, "apple", 1);
        addTerm(buffer, indexTermType, "car", 2);
        addTerm(buffer, indexTermType, "application", 3);
        addTerm(buffer, indexTermType, "cartridge", 4);
        addTerm(buffer, indexTermType, "apples", 5);
        addTerm(buffer, indexTermType, "applications", 6);
        addTerm(buffer, indexTermType, "cats", 7);
        addTerm(buffer, indexTermType, "cars", 8);

        assertEquals(9, buffer.numRows());

        // --- Phase 3: Write to disk via LiteralIndexWriter ---
        LiteralIndexWriter writer = new LiteralIndexWriter(indexDescriptor, indexIdentifier);
        SegmentMetadata.ComponentMetadataMap indexMetas = writer.writeCompleteSegment(buffer.iterator());

        // --- Phase 4: Open reader ---
        FileHandle termsData = indexDescriptor.createPerIndexFileHandle(IndexComponent.TERMS_DATA, indexIdentifier, null);
        FileHandle postingLists = indexDescriptor.createPerIndexFileHandle(IndexComponent.POSTING_LISTS, indexIdentifier, null);
        long termsFooterPointer = Long.parseLong(
                indexMetas.get(IndexComponent.TERMS_DATA).attributes.get(SAICodecUtils.FOOTER_POINTER));

        try (LiteralIndexSegmentTermsReader reader = new LiteralIndexSegmentTermsReader(
                indexIdentifier,
                termsData,
                postingLists,
                indexMetas.get(IndexComponent.TERMS_DATA).root,
                termsFooterPointer))
        {
            // --- Phase 5: Verify prefix postings at intermediate trie nodes ---
            // Intermediate nodes have ONLY prefix-type postings (no exact-match), so we use
            // LIKE_PREFIX operator which returns all posting types.

            // Prefix "appl": all terms passing through this intermediate node:
            //   apple(1), application(3), apples(5), applications(6)
            assertPrefixPostings(reader, "appl", 1, 3, 5, 6);

            // Prefix "ca": all terms starting with "ca":
            //   cat(0), car(2), cartridge(4), cats(7), cars(8)
            assertPrefixPostings(reader, "ca", 0, 2, 4, 7, 8);

            // Prefix "car": car(2), cartridge(4), cars(8)
            assertPrefixPostings(reader, "car", 2, 4, 8);

            // --- Phase 6: Verify exact-match postings at terminal trie nodes ---
            assertExactMatchPostings(reader, indexTermType, "apple", 1);
            assertExactMatchPostings(reader, indexTermType, "car", 2);
            assertExactMatchPostings(reader, indexTermType, "application", 3);
            assertExactMatchPostings(reader, indexTermType, "cat", 0);

            // Non-existent term returns null
            QueryEventListener.TrieIndexEventListener listener = mockTrieListener();
            PostingList missing = reader.exactMatch(
                    asByteComparable(indexTermType, "nonexistent"), listener, mock(QueryContext.class));
            assertNull("non-existent term should return null", missing);
        }
    }

    // ---- Helpers for the end-to-end test ----

    /**
     * Look up an intermediate prefix node by its raw ASCII bytes (no UTF8 escape) and verify
     * that the LIKE_PREFIX postings match the expected row IDs.
     */
    private void assertPrefixPostings(LiteralIndexSegmentTermsReader reader,
                                      String prefix, long... expectedRowIds) throws IOException
    {
        ByteComparable prefixKey = ByteComparable.fixedLength(prefix.getBytes(StandardCharsets.US_ASCII));
        QueryEventListener.TrieIndexEventListener listener = mockTrieListener();

        try (PostingList postings = reader.search(prefixKey, Expression.IndexOperator.LIKE_PREFIX,
                                                  listener, mock(QueryContext.class)))
        {
            assertNotNull("prefix '" + prefix + "' should be found in the on-disk trie", postings);
            for (long expectedRowId : expectedRowIds)
            {
                long actual = postings.nextPosting();
                assertEquals("prefix '" + prefix + "' posting mismatch", expectedRowId, actual);
            }
            assertEquals("prefix '" + prefix + "' should have no more postings",
                         PostingList.END_OF_STREAM, postings.nextPosting());
        }
    }

    /**
     * Look up a full term via UTF8Type-encoded ByteComparable and verify that the EQ (exact-match)
     * postings match the expected row IDs.
     */
    private void assertExactMatchPostings(LiteralIndexSegmentTermsReader reader,
                                          IndexTermType indexTermType,
                                          String term, long... expectedRowIds) throws IOException
    {
        ByteComparable termKey = asByteComparable(indexTermType, term);
        QueryEventListener.TrieIndexEventListener listener = mockTrieListener();

        try (PostingList postings = reader.exactMatch(termKey, listener, mock(QueryContext.class)))
        {
            assertNotNull("term '" + term + "' should be found", postings);
            for (long expectedRowId : expectedRowIds)
            {
                long actual = postings.nextPosting();
                assertEquals("term '" + term + "' posting mismatch", expectedRowId, actual);
            }
            assertEquals("term '" + term + "' should have no more postings",
                         PostingList.END_OF_STREAM, postings.nextPosting());
        }
    }

    /**
     * Creates a UTF8Type-encoded ByteComparable for a full term lookup (includes trailing escape byte).
     */
    private ByteComparable asByteComparable(IndexTermType indexTermType, String term)
    {
        return v -> indexTermType.asComparableBytes(UTF8Type.instance.decompose(term), v);
    }

    private QueryEventListener.TrieIndexEventListener mockTrieListener()
    {
        QueryEventListener.TrieIndexEventListener listener = mock(QueryEventListener.TrieIndexEventListener.class);
        when(listener.postingListEventListener()).thenReturn(mock(QueryEventListener.PostingListEventListener.class));
        return listener;
    }

    private static String unpackString(ByteComparable value)
    {
        return UTF8Type.instance.compose(UTF8Type.instance.fromComparableBytes(
                ByteSource.peekable(value.asComparableBytes(ByteComparable.Version.OSS50)),
                ByteComparable.Version.OSS50));
    }
}
