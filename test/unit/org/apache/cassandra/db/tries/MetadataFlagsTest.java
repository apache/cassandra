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

package org.apache.cassandra.db.tries;

import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MetadataFlagsTest
{
    private static final long FLAG1 = 0x0002L;
    private static final long FLAG2 = 0x0004L;
    private static final long FLAG3 = 0x0008L;

    @BeforeClass
    public static void enableVerification()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
    }

    @Test
    public void testCompareIgnoresFlags()
    {

        long pos = Cursor.encode(5, 0x41, Direction.FORWARD);
        long posWithFlag1 = pos | FLAG1;
        long posWithFlag2 = pos | FLAG2;

        assertEquals("Compare should ignore flags", 0, Cursor.compare(posWithFlag1, posWithFlag2));
        assertEquals("Compare should ignore flags (reverse)", 0, Cursor.compare(posWithFlag2, posWithFlag1));
        assertEquals("Compare with root should ignore flags", 0, Cursor.compare(Cursor.rootPosition(Direction.FORWARD) | FLAG1, Cursor.rootPosition(Direction.FORWARD)));
    }

    @Test
    public void testMergeCursorUnionsFlags()
    {

        long pos = Cursor.rootPosition(Direction.FORWARD);
        Cursor<Integer> c1 = new MockCursor<>(pos | FLAG1);
        Cursor<Integer> c2 = new MockCursor<>(pos | FLAG2);

        MergeCursor.Plain<Integer> merge = new MergeCursor.Plain<>(Trie.throwingResolver(), c1, c2);
        
        long expected = pos | FLAG1 | FLAG2;
        assertEquals("MergeCursor should union flags from both sources", expected, merge.encodedPosition());
    }

    @Test
    public void testCollectionMergeCursorUnionsFlags()
    {

        long pos = Cursor.rootPosition(Direction.FORWARD);
        List<Cursor<Integer>> inputs = Arrays.asList(
            new MockCursor<>(pos | FLAG1),
            new MockCursor<>(pos | FLAG2),
            new MockCursor<>(pos | FLAG3)
        );

        CollectionMergeCursor<Integer, Cursor<Integer>> merge = new CollectionMergeCursor.Plain<>(
            Trie.throwingResolver(), Direction.FORWARD, inputs, Cursor::tailCursor
        );

        long expected = pos | FLAG1 | FLAG2 | FLAG3;
        assertEquals("CollectionMergeCursor should union flags from all sources", expected, merge.encodedPosition());
    }

    @Test
    public void testIntersectionCursorPreservesFlags()
    {

        long pos = Cursor.rootPosition(Direction.FORWARD);
        Cursor<Integer> source = new MockCursor<>(pos | FLAG1);
        TrieSetCursor set = new MockTrieSetCursor(pos | FLAG2, TrieSetCursor.RangeState.CONTAINED);

        IntersectionCursor.BySet<Integer, Cursor<Integer>> intersect = new IntersectionCursor.Plain<>(source, set);
        
        // IntersectionCursor.encodedPosition returns source.encodedPosition()
        assertEquals("IntersectionCursor should preserve source flags", pos | FLAG1, intersect.encodedPosition());
    }

    @Test
    public void testIntersectionCursorContentBit()
    {

        long posWithContent = Cursor.rootPosition(Direction.FORWARD) | FLAG1 | Cursor.MAY_HAVE_CONTENT_BIT;
        
        // Plain IntersectionCursor should PRESERVE content bit even if NOT_CONTAINED
        Cursor<Integer> source1 = new MockCursor<>(posWithContent);
        TrieSetCursor set1 = new MockTrieSetCursor(Cursor.rootPosition(Direction.FORWARD), TrieSetCursor.RangeState.NOT_CONTAINED);
        IntersectionCursor.BySet<Integer, Cursor<Integer>> intersect1 = new IntersectionCursor.Plain<>(source1, set1);
        assertEquals("Plain IntersectionCursor should preserve content bit", 
                     posWithContent, intersect1.encodedPosition());

        // IntersectionCursor.PlainSlice should CLEAR content bit if NOT_CONTAINED
        Cursor<Integer> source2 = new MockCursor<>(posWithContent);
        TrieSetCursor set2 = new MockTrieSetCursor(Cursor.rootPosition(Direction.FORWARD), TrieSetCursor.RangeState.NOT_CONTAINED);
        IntersectionCursor.BySet<Integer, Cursor<Integer>> intersect2 = new IntersectionCursor.PlainSlice<>(source2, set2);
        assertEquals("PlainSlice IntersectionCursor should clear content bit when NOT_CONTAINED", 
                     posWithContent & ~Cursor.MAY_HAVE_CONTENT_BIT, intersect2.encodedPosition());
    }

    @Test
    public void testNegatedCursorContentBit()
    {

        long posWithContent = Cursor.encode(1, 0x41, Direction.FORWARD) | FLAG1 | Cursor.MAY_HAVE_CONTENT_BIT;
        long posWithoutContent = posWithContent & ~Cursor.MAY_HAVE_CONTENT_BIT;

        // Case 1: Source is NOT_CONTAINED -> negated should have content bit SET
        TrieSetCursor source1 = new MockTrieSetCursor(posWithoutContent, TrieSetCursor.RangeState.NOT_CONTAINED);
        TrieSetCursor negated1 = source1.negated();
        // Negated root logic: if overriding == NOT_CONTAINED, it sets the bit
        assertEquals("Negated cursor should set content bit when source is NOT_CONTAINED",
                     posWithoutContent | Cursor.MAY_HAVE_CONTENT_BIT, negated1.encodedPosition());

        // Case 2: Source is CONTAINED -> negated should have content bit CLEARED
        TrieSetCursor source2 = new MockTrieSetCursor(posWithContent, TrieSetCursor.RangeState.CONTAINED);
        TrieSetCursor negated2 = source2.negated();
        assertEquals("Negated cursor should clear content bit when source is CONTAINED",
                     posWithContent & ~Cursor.MAY_HAVE_CONTENT_BIT, negated2.encodedPosition());
    }

    @Test
    public void testNegatedCursorPreservesFlags()
    {

        // Let's test ROOT
        long rootPos = Cursor.rootPosition(Direction.FORWARD);
        TrieSetCursor sourceRoot = new MockTrieSetCursor(rootPos | FLAG1, TrieSetCursor.RangeState.NOT_CONTAINED);
        TrieSetCursor negatedRoot = sourceRoot.negated();
        // Negated.encodedPosition for ROOT returns source.encodedPosition() | MAY_HAVE_CONTENT_BIT
        assertEquals("Negated cursor should preserve flags at root", rootPos | FLAG1 | Cursor.MAY_HAVE_CONTENT_BIT, negatedRoot.encodedPosition());
    }

    @Test
    public void testComplexMergeUnionsFlags()
    {

        Object[] spec1 = new Object[] { "v1", null, "v2" }; // paths "0", "2"
        Object[] spec2 = new Object[] { null, "v3", "v4" }; // paths "1", "2"
        
        Trie<String> t1 = dir -> new TrieUtil.CursorFromSpec<>(spec1, dir, FLAG1);
        Trie<String> t2 = dir -> new TrieUtil.CursorFromSpec<>(spec2, dir, FLAG2);
        
        Trie<String> merged = Trie.merge(Arrays.asList(t1, t2), new Trie.CollectionMergeResolver<String>() {
            @Override public String resolve(java.util.Collection<String> contents) { return contents.iterator().next(); }
            @Override public String resolve(String v1, String v2) { return v1; }
        });
        
        Cursor<String> c = merged.cursor(Direction.FORWARD);
        // Root
        assertEquals("Root should have unioned flags", FLAG1 | FLAG2, c.encodedPosition() & Cursor.FLAGS_MASK);
        
        // Path "0" (only t1)
        c.advance();
        assertEquals("Path 0 should have FLAG1 and content bit", FLAG1 | Cursor.MAY_HAVE_CONTENT_BIT, c.encodedPosition() & Cursor.FLAGS_MASK);
        
        // Path "1" (only t2)
        c.advance();
        assertEquals("Path 1 should have FLAG2 and content bit", FLAG2 | Cursor.MAY_HAVE_CONTENT_BIT, c.encodedPosition() & Cursor.FLAGS_MASK);
        
        // Path "2" (both)
        c.advance();
        assertEquals("Path 2 should have unioned flags and content bit", FLAG1 | FLAG2 | Cursor.MAY_HAVE_CONTENT_BIT, c.encodedPosition() & Cursor.FLAGS_MASK);
    }

    @Test
    public void testIntersectionPreservesFlags()
    {

        Object[] spec = new Object[] { "v1", "v2" }; // paths "0", "1"
        Trie<String> t = dir -> new TrieUtil.CursorFromSpec<>(spec, dir, FLAG1);
        TrieSet set = TrieSet.branch(TrieUtil.VERSION, TrieUtil.directComparable("0"));
        
        Trie<String> intersected = t.intersect(set);
        Cursor<String> c = intersected.cursor(Direction.FORWARD);
        
        // Root
        assertEquals("Intersected root should have source flags", FLAG1, c.encodedPosition() & Cursor.FLAGS_MASK);
        
        // Path "0"
        c.advance();
        assertEquals("Intersected path 0 should have source flags and content bit", FLAG1 | Cursor.MAY_HAVE_CONTENT_BIT, c.encodedPosition() & Cursor.FLAGS_MASK);
    }

    @Test
    public void testMappingMergeCursorUnionsFlags()
    {

        long pos = Cursor.rootPosition(Direction.FORWARD);
        Cursor<Integer> c1 = new MockCursor<>(pos | FLAG1);
        Cursor<Integer> c2 = new MockCursor<>(pos | FLAG2);

        MergeCursor.PlainMapping<Integer, Integer, Integer> merge = new MergeCursor.PlainMapping<>((x, y) -> x, c1, c2);
        
        long expected = pos | FLAG1 | FLAG2;
        assertEquals("MappingMergeCursor should union flags from both sources", expected, merge.encodedPosition());
    }

    @Test
    public void testRangeIntersectionCursorPreservesFlags()
    {

        long pos = Cursor.rootPosition(Direction.FORWARD);
        RangeCursor<TrieSetCursor.RangeState> source = new MockTrieSetCursor(pos | FLAG1, TrieSetCursor.RangeState.CONTAINED);
        TrieSetCursor set = new MockTrieSetCursor(pos | FLAG2, TrieSetCursor.RangeState.CONTAINED);

        RangeIntersectionCursor.RangeBySet<TrieSetCursor.RangeState> intersect = new RangeIntersectionCursor.RangeBySet<>(source, set);
        
        // RangeIntersectionCursor favors 'src' flags when matching
        assertEquals("RangeIntersectionCursor should preserve source flags when matching", pos | FLAG1, intersect.encodedPosition());
    }

    @Test
    public void testRangesCursorContentBit()
    {

        // Range [abc, def]
        ByteComparable abc = TrieUtil.directComparable("abc");
        ByteComparable def = TrieUtil.directComparable("def");
        RangesCursor cursor = RangesCursor.create(Direction.FORWARD, TrieUtil.VERSION, true, true, abc, def);
        
        // Root - NOT_CONTAINED state, no content bit
        assertEquals(TrieSetCursor.RangeState.NOT_CONTAINED, cursor.nonNullState());
        assertTrue("Root should NOT have content bit (NOT_CONTAINED)", (cursor.encodedPosition() & Cursor.MAY_HAVE_CONTENT_BIT) == 0);
        
        // "a" (prefix of abc) - still NOT_CONTAINED
        cursor.advance(); 
        assertEquals(TrieSetCursor.RangeState.NOT_CONTAINED, cursor.nonNullState());
        assertTrue("'a' should NOT have content bit (NOT_CONTAINED)", (cursor.encodedPosition() & Cursor.MAY_HAVE_CONTENT_BIT) == 0);

        // "ab" - still NOT_CONTAINED
        cursor.advance();
        assertEquals(TrieSetCursor.RangeState.NOT_CONTAINED, cursor.nonNullState());
        assertTrue("'ab' should NOT have content bit (NOT_CONTAINED)", (cursor.encodedPosition() & Cursor.MAY_HAVE_CONTENT_BIT) == 0);
        
        // "abc" (boundary) - START state, has content
        cursor.advance();
        assertEquals(TrieSetCursor.RangeState.START, cursor.nonNullState());
        assertTrue("'abc' should have content bit (START boundary)", (cursor.encodedPosition() & Cursor.MAY_HAVE_CONTENT_BIT) != 0);
        
        // "d" (prefix of def, internal to range [abc, def]) - CONTAINED, no content
        cursor.advance();
        assertEquals(TrieSetCursor.RangeState.CONTAINED, cursor.nonNullState());
        assertTrue("'d' should NOT have content bit (CONTAINED)", (cursor.encodedPosition() & Cursor.MAY_HAVE_CONTENT_BIT) == 0);
    }

    @Test
    public void testFlagUnionUtility() {

        long p1 = 0x1234567800000000L | 0x1L; // Depth etc, plus bit 0
        long p2 = 0x1234567800000000L | 0x2L; // Same pos, plus bit 1

        long unioned = Cursor.unionFlags(p1, p2, Cursor.FLAGS_MASK);
        assertEquals("Flags should be unioned", 0x1234567800000003L, unioned);

        long masked = Cursor.unionFlags(p1, p2, 0x1L);
        assertEquals("Only bit 0 should be unioned if mask is 0x1", 0x1234567800000001L, masked);
    }

    @Test
    public void testFlagIntersectionUtility()
    {
        long p1 = 0x1234567800000000L | FLAG1 | FLAG2; // Has FLAG1 and FLAG2
        long p2 = 0x1234567800000000L | FLAG2 | FLAG3; // Has FLAG2 and FLAG3
        
        long intersected = Cursor.intersectionFlags(p1, p2, Cursor.FLAGS_MASK);
        // Should keep only FLAG2 (common to both)
        assertEquals("Flags should be intersected", 0x1234567800000000L | FLAG2, intersected);
        
        long masked = Cursor.intersectionFlags(p1, p2, FLAG1 | FLAG2);
        // Should only intersect FLAG1 and FLAG2, FLAG3 ignored
        assertEquals("Only specified flags should be intersected", 0x1234567800000000L | FLAG2, masked);
    }

    @Test
    public void testDepthAdjustedCursorPreservesFlags()
    {

        long pos = Cursor.encode(1, 0x41, Direction.FORWARD) | FLAG1;
        Cursor<Integer> source = new MockCursor<>(pos);
        long attachmentPoint = Cursor.encode(5, 0x42, Direction.FORWARD) | FLAG2;
        
        DepthAdjustedCursor<Integer, Cursor<Integer>> adjusted = new DepthAdjustedCursor.Plain<>(source, attachmentPoint);
        
        // Depth 1 -> should add depth correction. flags from source should be preserved.
        long expected = pos + Cursor.depthCorrectionValue(attachmentPoint);
        assertEquals("DepthAdjustedCursor should preserve source flags at depth > 0", expected, adjusted.encodedPosition());
        
        // Root (depth 0)
        source = new MockCursor<>(Cursor.rootPosition(Direction.FORWARD) | FLAG3);
        adjusted = new DepthAdjustedCursor.Plain<>(source, attachmentPoint);
        // Should NOT union with attachmentPoint flags, only preserve source flags
        long expectedRoot = (attachmentPoint & ~Cursor.FLAGS_MASK) | (source.encodedPosition() & (Cursor.ON_RETURN_PATH_BIT | Cursor.FLAGS_MASK));
        assertEquals("DepthAdjustedCursor should preserve source flags at root", expectedRoot, adjusted.encodedPosition());
    }

    @Test
    public void testUnionFlagsMatchingPositions()
    {
        long pos = Cursor.encode(5, 0x41, Direction.FORWARD);
        long p1 = pos | FLAG1;
        long p2 = pos | FLAG2;

        long unioned = Cursor.unionFlagsMatchingPositions(p1, p2);
        assertEquals("Flags should be unioned", pos | FLAG1 | FLAG2, unioned);
    }

    @Test(expected = AssertionError.class)
    public void testUnionFlagsMatchingPositionsAssertion()
    {
        long p1 = Cursor.encode(5, 0x41, Direction.FORWARD);
        long p2 = Cursor.encode(5, 0x42, Direction.FORWARD);

        // This should trigger the assertion because positions don't match
        Cursor.unionFlagsMatchingPositions(p1, p2);
    }

    @Test
    public void testCollectionMergeCursorPositionOptimization()
    {
        // Test that CollectionMergeCursor correctly unions flags from multiple sources at the same position
        long pos = Cursor.rootPosition(Direction.FORWARD);
        List<Cursor<Integer>> inputs = Arrays.asList(
            new MockCursor<>(pos | FLAG1),
            new MockCursor<>(pos | FLAG2)
        );

        CollectionMergeCursor<Integer, Cursor<Integer>> merge = new CollectionMergeCursor.Plain<>(
            Trie.throwingResolver(), Direction.FORWARD, inputs, Cursor::tailCursor
        );

        // CollectionMergeCursor.collectAndCachePosition() should have unioned the flags
        assertEquals("CollectionMergeCursor should union flags from all sources at start", 
                     pos | FLAG1 | FLAG2, merge.encodedPosition());
    }

    @Test
    public void testPrefixedCursorPreservesFlags()
    {

        long pos = Cursor.rootPosition(Direction.FORWARD) | FLAG1;
        Cursor<Integer> source = new MockCursor<>(pos);
        ByteComparable prefix = TrieUtil.directComparable("abc");
        
        PrefixedCursor.Plain<Integer> prefixed = new PrefixedCursor.Plain<>(prefix, source);
        
        // Advance to source root
        prefixed.advance(); // "a"
        prefixed.advance(); // "b"
        prefixed.advance(); // "c"
        
        // Now it should be at source root. PrefixedCursor uses DepthAdjustedCursor internally when prefix is done.
        assertEquals("PrefixedCursor should preserve FLAG1 from source", FLAG1, prefixed.encodedPosition() & FLAG1);
    }

    @Test
    public void testRangeApplyCursorPreservesFlags()
    {

        long pos = Cursor.rootPosition(Direction.FORWARD) | FLAG1;
        Cursor<Integer> data = new MockCursor<>(pos);
        RangeCursor<TrieSetCursor.RangeState> range = new MockTrieSetCursor(Cursor.rootPosition(Direction.FORWARD) | FLAG2, TrieSetCursor.RangeState.CONTAINED);
        
        RangeApplyCursor<Integer, TrieSetCursor.RangeState> applied = new RangeApplyCursor<>((s, v) -> v, range, data);
        
        // RangeApplyCursor delegates encodedPosition to data
        assertEquals("RangeApplyCursor should preserve data flags", pos, applied.encodedPosition());
    }

    @Test
    public void testDeepMergeUnionsFlags()
    {

        long pos = Cursor.rootPosition(Direction.FORWARD);
        Cursor<Integer> c1 = new MockCursor<>(pos | FLAG1);
        Cursor<Integer> c2 = new MockCursor<>(pos | FLAG2);
        Cursor<Integer> c3 = new MockCursor<>(pos | FLAG3);

        MergeCursor.Plain<Integer> m1 = new MergeCursor.Plain<>(Trie.throwingResolver(), c1, c2);
        MergeCursor.Plain<Integer> m2 = new MergeCursor.Plain<>(Trie.throwingResolver(), m1, c3);
        
        long expected = pos | FLAG1 | FLAG2 | FLAG3;
        assertEquals("Deep MergeCursor should union flags from all ancestors", expected, m2.encodedPosition());

    }

    @Test
    public void testCollectionMergeCursorDeletionAwareUnionsFlags()
    {
        long pos = Cursor.rootPosition(Direction.FORWARD);
        List<DeletionAwareCursor<Integer, TrieSetCursor.RangeState>> inputs = Arrays.asList(
            new MockDeletionAwareCursor<>(pos | Cursor.MAY_HAVE_CONTENT_BIT),
            new MockDeletionAwareCursor<>(pos | Cursor.MAY_HAVE_DELETION_BRANCH_BIT)
        );

        CollectionMergeCursor.DeletionAware<Integer, TrieSetCursor.RangeState> merge =
            new CollectionMergeCursor.DeletionAware<>(
                contents -> contents.iterator().next(),
                contents -> contents.iterator().next(),
                (d, v) -> v,
                true,
                Direction.FORWARD,
                inputs,
                (c, dir) -> c
            );

        long expected = pos | Cursor.MAY_HAVE_CONTENT_BIT | Cursor.MAY_HAVE_DELETION_BRANCH_BIT;
        assertEquals("CollectionMergeCursor.DeletionAware should union both content and deletion branch flags",
                     expected, merge.encodedPosition());
    }

    @Test
    public void testSingletonCursorDeletionBranchFlags()
    {
        ByteSource src = ByteSource.of("abc", TrieUtil.VERSION);
        SingletonCursor.DeletionBranch<Integer, TrieSetCursor.RangeState> cursor =
            new SingletonCursor.DeletionBranch<>(Direction.FORWARD, src, TrieUtil.VERSION, null);

        // Initially NOT atEnd
        long startPos = cursor.encodedPosition();
        assertTrue("Initially it should NOT have MAY_HAVE_DELETION_BRANCH_BIT set",
                   (startPos & Cursor.MAY_HAVE_DELETION_BRANCH_BIT) == 0);

        // Advance to the end
        cursor.advance();
        cursor.advance();
        cursor.advance();
        cursor.advance(); // now at end
        
        long endPos = cursor.encodedPosition();
        assertTrue("At end, it should have MAY_HAVE_DELETION_BRANCH_BIT set",
                   (endPos & Cursor.MAY_HAVE_DELETION_BRANCH_BIT) != 0);
    }

    private static class MockDeletionAwareCursor<T, D extends RangeState<D>>
    extends MockCursor<T> implements DeletionAwareCursor<T, D>
    {
        MockDeletionAwareCursor(long position) { super(position); }
        @Override public RangeCursor<D> deletionBranchCursor(Direction direction) { return null; }
        @Override public DeletionAwareCursor<T, D> tailCursor(Direction direction) { return this; }
    }

    private static class MockCursor<T> implements Cursor<T>
    {
        long position;

        MockCursor(long position) { this.position = position; }

        @Override public long encodedPosition() { return position; }
        @Override public T content() { return null; }
        @Override public ByteComparable.Version byteComparableVersion() { return TrieUtil.VERSION; }
        @Override public long advance() { return position; }
        @Override public long skipTo(long p) { return position; }
        @Override public Cursor<T> tailCursor(Direction d) { return this; }
    }

    private static class MockTrieSetCursor extends MockCursor<TrieSetCursor.RangeState> implements TrieSetCursor
    {
        RangeState state;

        MockTrieSetCursor(long position, RangeState state)
        {
            super(position);
            this.state = state;
        }

        @Override public RangeState nonNullState() { return state == null ? RangeState.NOT_CONTAINED : state; }
        @Override public RangeState state() { return state; }
        @Override public TrieSetCursor tailCursor(Direction d) { return this; }
    }
}
