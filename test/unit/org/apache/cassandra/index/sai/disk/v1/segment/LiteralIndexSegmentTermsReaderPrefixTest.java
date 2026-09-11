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
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.SAICodecUtils;
import org.apache.cassandra.index.sai.disk.v1.trie.LiteralIndexWriter;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.index.sai.utils.SAIRandomizedTester;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Direct-reader tests for {@link LiteralIndexSegmentTermsReader#prefixMatchWithStats}.
 * Verifies both correct row IDs and the traversal path taken by
 * {@link LiteralIndexSegmentTermsReader.PrefixQuery#collectFromNode}.
 *
 * Four scenarios:
 * <ol>
 *   <li>No match — prefix not in the trie.</li>
 *   <li>Combined section at the prefix node itself — DFS stops immediately.</li>
 *   <li>Combined sections 2 levels below the prefix node — DFS walks empty intermediates.</li>
 *   <li>No sections anywhere — DFS traverses all leaves, exact-section read per term.</li>
 * </ol>
 *
 * Mirrors the {@code BlockBalancedTreeReaderTest} pattern: write the index directly via
 * {@link LiteralIndexWriter}, open the reader directly, no CQL stack.
 */
public class LiteralIndexSegmentTermsReaderPrefixTest extends SAIRandomizedTester
{
    // Combined sections are written at depth % skip == 0 AND prefixCount >= minimumLeaves.
    private static final int POSTINGS_SKIP = 3;
    private static final int MIN_POSTINGS_LEAVES = 64;

    @Before
    public void configurePrefixThresholds()
    {
        CassandraRelevantProperties.SAI_POSTINGS_SKIP.setString(String.valueOf(POSTINGS_SKIP));
        CassandraRelevantProperties.SAI_MINIMUM_POSTINGS_LEAVES.setString(String.valueOf(MIN_POSTINGS_LEAVES));
    }

    // -------------------------------------------------------------------------
    // helpers
    // -------------------------------------------------------------------------

    private LiteralIndexSegmentTermsReader buildReader(List<TermPostings> termPostings) throws Exception
    {
        IndexDescriptor desc = newIndexDescriptor();
        IndexIdentifier id = createIndexIdentifier("ks", "tbl", newIndex());

        // SegmentTrieBuffer accumulates prefix postings at the right depths and emits the V2
        // header (exactCount, totalCount, rows...) that LiteralIndexWriter.writeCompleteSegment expects.
        int skip = CassandraRelevantProperties.SAI_POSTINGS_SKIP.getInt();
        SegmentTrieBuffer buffer = new SegmentTrieBuffer(depth -> depth % skip == 0);

        for (TermPostings tp : termPostings)
        {
            ByteComparable term = ByteComparable.fixedLength(tp.term.getBytes());
            for (long rowId : tp.rowIds)
                buffer.add(term, tp.term.length(), (int) rowId);
        }

        LiteralIndexWriter writer = new LiteralIndexWriter(desc, id);
        SegmentMetadata.ComponentMetadataMap meta = writer.writeCompleteSegment(buffer.iterator(), true);

        FileHandle termsData = desc.createPerIndexFileHandle(IndexComponent.TERMS_DATA, id, null);
        FileHandle postingLists = desc.createPerIndexFileHandle(IndexComponent.POSTING_LISTS, id, null);
        long footerPointer = Long.parseLong(
            meta.get(IndexComponent.TERMS_DATA).attributes.get(SAICodecUtils.FOOTER_POINTER));

        return new LiteralIndexSegmentTermsReader(
            id, termsData, postingLists,
            meta.get(IndexComponent.TERMS_DATA).root,
            footerPointer, true /* isV2 */);
    }

    private QueryEventListener.TrieIndexEventListener mockListener()
    {
        QueryEventListener.TrieIndexEventListener l = mock(QueryEventListener.TrieIndexEventListener.class);
        when(l.postingListEventListener()).thenReturn(mock(QueryEventListener.PostingListEventListener.class));
        return l;
    }

    private TreeSet<Long> drain(PostingList pl) throws IOException
    {
        TreeSet<Long> ids = new TreeSet<>();
        if (pl == null) return ids;
        try (PostingList p = pl)
        {
            long id;
            while ((id = p.nextPosting()) != PostingList.END_OF_STREAM)
                ids.add(id);
        }
        return ids;
    }

    private static ByteComparable bc(String s)
    {
        return ByteComparable.fixedLength(s.getBytes());
    }

    /** A term and its associated row IDs. */
    private static class TermPostings
    {
        final String term;
        final long[] rowIds;

        TermPostings(String term, long... rowIds)
        {
            this.term = term;
            this.rowIds = rowIds;
        }
    }

    // -------------------------------------------------------------------------
    // tests
    // -------------------------------------------------------------------------

    /**
     * Dense 3-level trie under "abc": abc_[a-c]_[x-z]_NN (45 terms, 3×3×5 rows).
     * Query prefix "xyz" shares no bytes with any term.
     *
     * Expected: null result, all stats zero — DFS never starts.
     */
    @Test
    public void testNoMatch() throws Exception
    {
        List<TermPostings> data = new ArrayList<>();
        long rowId = 0;
        for (char g : new char[]{'a', 'b', 'c'})
            for (char s : new char[]{'x', 'y', 'z'})
                for (int n = 0; n < 5; n++)
                    data.add(new TermPostings("abc_" + g + "_" + s + "_" + String.format("%02d", n), rowId++));

        try (LiteralIndexSegmentTermsReader reader = buildReader(data))
        {
            LiteralIndexSegmentTermsReader.TraversalStats stats = new LiteralIndexSegmentTermsReader.TraversalStats();
            PostingList result = reader.prefixMatchWithStats(bc("xyz"), bc("xyz~"), mockListener(), mock(QueryContext.class), stats);

            assertNull("Expected null for unmatched prefix", result);
            assertEquals(0, stats.combinedSectionHits);
            assertEquals(0, stats.exactSectionHits);
            assertEquals(0, stats.emptyNodes);
        }
    }

    /**
     * Prefix "ge_" sits at trie depth 3 (depth 3 % skip=3 == 0).
     * Under it: 3 groups × 3 sub-groups × 10 rows = 90 rows (≥ 64 → combined section at "ge_").
     * Noise: "ga_*" and "gb_*" branch at depth 2; "other_*" branches at root.
     *
     * Expected: single combined-section hit; emptyNodes=0 proves DFS stopped at "ge_" without recursing.
     */
    @Test
    public void testCombinedSectionAtPrefixNode() throws Exception
    {
        List<TermPostings> data = new ArrayList<>();
        TreeSet<Long> expectedIds = new TreeSet<>();
        long rowId = 1000;

        // Target: ge_[a-c]_[p-r]_NNNN — 3×3×10 = 90 rows
        for (char g : new char[]{'a', 'b', 'c'})
            for (char s : new char[]{'p', 'q', 'r'})
                for (int n = 0; n < 10; n++, rowId++)
                {
                    data.add(new TermPostings("ge_" + g + "_" + s + "_" + String.format("%04d", n), rowId));
                    expectedIds.add(rowId);
                }

        // Noise branching at depth 2 under same root letter 'g': ga_*, gb_*
        for (char g : new char[]{'a', 'b'})
            for (char s : new char[]{'x', 'y', 'z'})
                for (int n = 0; n < 10; n++, rowId++)
                    data.add(new TermPostings("g" + g + "_" + s + "_" + String.format("%04d", n), rowId));

        // Noise at a different root: other_[a-c]_NN
        for (char g : new char[]{'a', 'b', 'c'})
            for (int n = 0; n < 10; n++, rowId++)
                data.add(new TermPostings("other_" + g + "_" + String.format("%02d", n), rowId));

        try (LiteralIndexSegmentTermsReader reader = buildReader(data))
        {
            LiteralIndexSegmentTermsReader.TraversalStats stats = new LiteralIndexSegmentTermsReader.TraversalStats();
            PostingList result = reader.prefixMatchWithStats(bc("ge_"), bc("ge~"), mockListener(), mock(QueryContext.class), stats);

            assertEquals("Expected 90 matching rows", expectedIds, drain(result));
            assertEquals("Expected single combined-section hit at prefix node", 1, stats.combinedSectionHits);
            assertEquals(0, stats.exactSectionHits);
            // emptyNodes=0: DFS returned at "ge_" without recursing into any child.
            assertEquals("Expected no recursion past prefix node", 0, stats.emptyNodes);
        }
    }

    /**
     * Prefix "r" (depth 1, 1 % 3 ≠ 0) has no combined section.
     * Children "ra", "rb" (depth 2, 2 % 3 ≠ 0) also have none.
     * Grandchildren "rax", "ray", "rbx", "rby" sit at depth 3 (3 % 3 == 0),
     * each with 80 rows (≥ 64) → combined sections written there.
     *
     * DFS path: r (empty) → ra (empty) → rax (combined! stop), ray (combined! stop)
     *                        rb (empty) → rbx (combined! stop), rby (combined! stop)
     *
     * Expected: combinedSectionHits=4, emptyNodes≥3 (r, ra, rb are all intermediate).
     */
    @Test
    public void testCombinedSectionInGrandchildren() throws Exception
    {
        List<TermPostings> data = new ArrayList<>();
        TreeSet<Long> expectedIds = new TreeSet<>();
        long rowId = 500;

        // Target: r[a-b][x-y]_NNNN — 4 sub-groups × 80 rows = 320 rows.
        // rax/ray/rbx/rby nodes are at trie depth 3 (3 chars deep), so depth % 3 == 0.
        for (char mid : new char[]{'a', 'b'})
            for (char leaf : new char[]{'x', 'y'})
                for (int n = 0; n < 80; n++, rowId++)
                {
                    data.add(new TermPostings("r" + mid + leaf + "_" + String.format("%04d", n), rowId));
                    expectedIds.add(rowId);
                }

        // Noise at a different root: m[a-c][x-z]_NN
        for (char g : new char[]{'a', 'b', 'c'})
            for (char s : new char[]{'x', 'y', 'z'})
                for (int n = 0; n < 5; n++, rowId++)
                    data.add(new TermPostings("m" + g + s + "_" + String.format("%02d", n), rowId));

        try (LiteralIndexSegmentTermsReader reader = buildReader(data))
        {
            LiteralIndexSegmentTermsReader.TraversalStats stats = new LiteralIndexSegmentTermsReader.TraversalStats();
            // Query "r": depth 1, no combined section. DFS descends through ra/rb (depth 2)
            // then stops at rax/ray/rbx/rby (depth 3, combined sections).
            PostingList result = reader.prefixMatchWithStats(bc("r"), bc("r~"), mockListener(), mock(QueryContext.class), stats);

            assertEquals("Expected 320 matching rows", expectedIds, drain(result));
            assertEquals("Expected 4 combined-section hits at grandchild nodes", 4, stats.combinedSectionHits);
            assertEquals(0, stats.exactSectionHits);
            assertTrue("Expected empty intermediate nodes (r, ra, rb)", stats.emptyNodes >= 3);
        }
    }

    /**
     * Prefix "s_" over a dense 3-level trie: s_[a-c]_[x-z]_[0-2] (27 terms × 2 rowIds = 54 total).
     * 54 < MIN_POSTINGS_LEAVES=64 → no combined section at any node, even at depth 6.
     *
     * DFS traverses all 3 levels: s_a_/s_b_/s_c_ → x_/y_/z_ → 0/1/2.
     * Each of the 27 leaf terms has only an exact section.
     *
     * Expected: combinedSectionHits=0, exactSectionHits=27, emptyNodes>0.
     */
    @Test
    public void testFullSubtreeTraversalNoSections() throws Exception
    {
        List<TermPostings> data = new ArrayList<>();
        TreeSet<Long> expectedIds = new TreeSet<>();
        long rowId = 10;

        // Target: s_[a-c]_[x-z]_[0-2] — 27 terms, 2 rows each (54 total, below 64 threshold)
        for (char g : new char[]{'a', 'b', 'c'})
            for (char s : new char[]{'x', 'y', 'z'})
                for (char t : new char[]{'0', '1', '2'})
                {
                    data.add(new TermPostings("s_" + g + "_" + s + "_" + t, rowId, rowId + 1));
                    expectedIds.add(rowId);
                    expectedIds.add(rowId + 1);
                    rowId += 2;
                }

        // Noise at a different root: t_[a-c]_[x-z]_N
        for (char g : new char[]{'a', 'b', 'c'})
            for (char s : new char[]{'x', 'y'})
                for (int n = 0; n < 3; n++, rowId++)
                    data.add(new TermPostings("t_" + g + "_" + s + "_" + n, rowId));

        try (LiteralIndexSegmentTermsReader reader = buildReader(data))
        {
            LiteralIndexSegmentTermsReader.TraversalStats stats = new LiteralIndexSegmentTermsReader.TraversalStats();
            PostingList result = reader.prefixMatchWithStats(bc("s_"), bc("s~"), mockListener(), mock(QueryContext.class), stats);

            assertEquals("Expected 54 matching rows", expectedIds, drain(result));
            assertEquals("Expected no combined-section hits", 0, stats.combinedSectionHits);
            assertEquals("Expected one exact-section hit per leaf term", 27, stats.exactSectionHits);
            assertTrue("Expected empty intermediate nodes within s_ subtree", stats.emptyNodes > 0);
        }
    }
}
