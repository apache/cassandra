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

package org.apache.cassandra.io.tries;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.function.LongSupplier;
import java.util.function.LongToIntFunction;
import java.util.stream.IntStream;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import org.agrona.collections.IntArrayList;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.TailOverridingRebufferer;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

@SuppressWarnings({"unchecked", "RedundantSuppression"})
public class WalkerTest extends AbstractTrieTestBase
{
    @Test
    public void testWalker() throws IOException
    {
        DataOutputBuffer buf = new DataOutputBufferPaged();
        IncrementalTrieWriter<Integer> builder = makeTrie(buf);
        long rootPos = builder.complete();

        Rebufferer source = new ByteBufRebufferer(buf.asNewBuffer());

        Walker<?> it = new Walker<>(source, rootPos);

        DataOutputBuffer dumpBuf = new DataOutputBuffer();
        it.dumpTrie(new PrintStream(dumpBuf), (buf1, payloadPos, payloadFlags) -> String.format("%d/%d", payloadPos, payloadFlags));
        logger.info("Trie dump: \n{}", new String(dumpBuf.getData()));
        logger.info("Trie toString: {}", it);

        it.goMax(rootPos);
        assertEquals(12, it.payloadFlags());
        assertEquals(TrieNode.PAYLOAD_ONLY.ordinal, it.nodeTypeOrdinal());
        assertEquals(1, it.nodeSize());
        assertFalse(it.hasChildren());

        it.goMin(rootPos);
        assertEquals(1, it.payloadFlags());
        assertEquals(TrieNode.PAYLOAD_ONLY.ordinal, it.nodeTypeOrdinal());
        assertEquals(1, it.nodeSize());
        assertFalse(it.hasChildren());

        assertEquals(-1, it.follow(source("151")));
        assertEquals(2, it.payloadFlags());

        assertEquals('3', it.follow(source("135")));

        assertEquals('3', it.followWithGreater(source("135")));
        it.goMin(it.greaterBranch);
        assertEquals(2, it.payloadFlags());

        assertEquals('3', it.followWithLesser(source("135")));
        it.goMax(it.lesserBranch);
        assertEquals(1, it.payloadFlags());

        assertEquals(3, (Object) it.prefix(source("155"), (walker, payloadPosition, payloadFlags) -> payloadFlags));
        assertNull(it.prefix(source("516"), (walker, payloadPosition, payloadFlags) -> payloadFlags));
        assertEquals(5, (Object) it.prefix(source("5151"), (walker, payloadPosition, payloadFlags) -> payloadFlags));
        assertEquals(1, (Object) it.prefix(source("1151"), (walker, payloadPosition, payloadFlags) -> payloadFlags));

        assertEquals(3, (Object) it.prefixAndNeighbours(source("155"), (walker, payloadPosition, payloadFlags) -> payloadFlags));
        assertNull(it.prefixAndNeighbours(source("516"), (walker, payloadPosition, payloadFlags) -> payloadFlags));
        assertEquals(5, (Object) it.prefixAndNeighbours(source("5151"), (walker, payloadPosition, payloadFlags) -> payloadFlags));
        assertEquals(1, (Object) it.prefixAndNeighbours(source("1151"), (walker, payloadPosition, payloadFlags) -> payloadFlags));

        assertEquals(3, (Object) it.prefixAndNeighbours(source("1555"), (walker, payloadPosition, payloadFlags) -> payloadFlags));
        it.goMax(it.lesserBranch);
        assertEquals(2, it.payloadFlags());
        it.goMin(it.greaterBranch);
        assertEquals(4, it.payloadFlags());
    }

    @Test
    public void testIteratorWithoutBounds() throws IOException
    {
        DataOutputBuffer buf = new DataOutputBufferPaged();
        IncrementalTrieWriter<Integer> builder = makeTrie(buf);
        long rootPos = builder.complete();

        int[] expected = IntStream.range(1, 13).toArray();
        checkIterates(buf.asNewBuffer(), rootPos, expected);

        checkIterates(buf.asNewBuffer(), rootPos, null, null, ValueIterator.LeftBoundTreatment.GREATER, expected);
        checkIterates(buf.asNewBuffer(), rootPos, null, null, ValueIterator.LeftBoundTreatment.ADMIT_EXACT, expected);
        checkIterates(buf.asNewBuffer(), rootPos, null, null, ValueIterator.LeftBoundTreatment.ADMIT_PREFIXES, expected);
    }

    @Test
    public void testIteratorWithBoundsGreater() throws IOException
    {
        DataOutputBuffer buf = new DataOutputBufferPaged();
        IncrementalTrieWriter<Integer> builder = makeTrie(buf);
        long rootPos = builder.complete();

        checkIterates(buf.asNewBuffer(), rootPos, "151", "515", ValueIterator.LeftBoundTreatment.GREATER, 3, 4, 5);
        checkIterates(buf.asNewBuffer(), rootPos, "15151", "51515", ValueIterator.LeftBoundTreatment.GREATER, 3, 4, 5);
        checkIterates(buf.asNewBuffer(), rootPos, "705", "73", ValueIterator.LeftBoundTreatment.GREATER, 9, 10, 11);
        checkIterates(buf.asNewBuffer(), rootPos, "7051", "735", ValueIterator.LeftBoundTreatment.GREATER, 10, 11);
        checkIterates(buf.asNewBuffer(), rootPos, "70", "737", ValueIterator.LeftBoundTreatment.GREATER, 9, 10, 11, 12);
        checkIterates(buf.asNewBuffer(), rootPos, "7054", "735", ValueIterator.LeftBoundTreatment.GREATER, 10, 11);

        checkIterates(buf.asNewBuffer(), rootPos, null, "515", ValueIterator.LeftBoundTreatment.GREATER, 1, 2, 3, 4, 5);
        checkIterates(buf.asNewBuffer(), rootPos, null, "51515", ValueIterator.LeftBoundTreatment.GREATER, 1, 2, 3, 4, 5);
        checkIterates(buf.asNewBuffer(), rootPos, "151", null, ValueIterator.LeftBoundTreatment.GREATER, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
        checkIterates(buf.asNewBuffer(), rootPos, "15151", null, ValueIterator.LeftBoundTreatment.GREATER, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);

        checkIterates(buf.asNewBuffer(), rootPos, "151", "155", ValueIterator.LeftBoundTreatment.GREATER, 3);
        checkIterates(buf.asNewBuffer(), rootPos, "15", "151", ValueIterator.LeftBoundTreatment.GREATER, 2);
        checkIterates(buf.asNewBuffer(), rootPos, "1511", "1512", ValueIterator.LeftBoundTreatment.GREATER);
        checkIterates(buf.asNewBuffer(), rootPos, "155", "155", ValueIterator.LeftBoundTreatment.GREATER);

        checkIterates(buf.asNewBuffer(), rootPos, "155", "156", ValueIterator.LeftBoundTreatment.GREATER);
        checkIterates(buf.asNewBuffer(), rootPos, "154", "156", ValueIterator.LeftBoundTreatment.GREATER, 3);
    }

    @Test
    public void testIteratorWithBoundsExact() throws IOException
    {
        DataOutputBuffer buf = new DataOutputBufferPaged();
        IncrementalTrieWriter<Integer> builder = makeTrie(buf);
        long rootPos = builder.complete();

        checkIterates(buf.asNewBuffer(), rootPos, "151", "515", ValueIterator.LeftBoundTreatment.ADMIT_EXACT, 2, 3, 4, 5);
        checkIterates(buf.asNewBuffer(), rootPos, "15151", "51515", ValueIterator.LeftBoundTreatment.ADMIT_EXACT, 3, 4, 5);
        checkIterates(buf.asNewBuffer(), rootPos, "705", "73", ValueIterator.LeftBoundTreatment.ADMIT_EXACT, 9, 10, 11);
        checkIterates(buf.asNewBuffer(), rootPos, "7051", "735", ValueIterator.LeftBoundTreatment.ADMIT_EXACT, 9, 10, 11);
        checkIterates(buf.asNewBuffer(), rootPos, "70", "737", ValueIterator.LeftBoundTreatment.ADMIT_EXACT, 8, 9, 10, 11, 12);
        checkIterates(buf.asNewBuffer(), rootPos, "7054", "735", ValueIterator.LeftBoundTreatment.ADMIT_EXACT, 10, 11);

        checkIterates(buf.asNewBuffer(), rootPos, null, "515", ValueIterator.LeftBoundTreatment.ADMIT_EXACT, 1, 2, 3, 4, 5);
        checkIterates(buf.asNewBuffer(), rootPos, null, "51515", ValueIterator.LeftBoundTreatment.ADMIT_EXACT, 1, 2, 3, 4, 5);
        checkIterates(buf.asNewBuffer(), rootPos, "151", null, ValueIterator.LeftBoundTreatment.ADMIT_EXACT, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
        checkIterates(buf.asNewBuffer(), rootPos, "15151", null, ValueIterator.LeftBoundTreatment.ADMIT_EXACT, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);

        checkIterates(buf.asNewBuffer(), rootPos, "151", "155", ValueIterator.LeftBoundTreatment.ADMIT_EXACT, 2, 3);
        checkIterates(buf.asNewBuffer(), rootPos, "15", "151", ValueIterator.LeftBoundTreatment.ADMIT_EXACT, 2);
        checkIterates(buf.asNewBuffer(), rootPos, "1511", "1512", ValueIterator.LeftBoundTreatment.ADMIT_EXACT);
        checkIterates(buf.asNewBuffer(), rootPos, "155", "155", ValueIterator.LeftBoundTreatment.ADMIT_EXACT, 3);

        checkIterates(buf.asNewBuffer(), rootPos, "155", "156", ValueIterator.LeftBoundTreatment.ADMIT_EXACT, 3);
        checkIterates(buf.asNewBuffer(), rootPos, "154", "156", ValueIterator.LeftBoundTreatment.ADMIT_EXACT, 3);
    }

    @Test
    public void testIteratorWithBoundsAndAdmitPrefix() throws IOException
    {
        DataOutputBuffer buf = new DataOutputBufferPaged();
        IncrementalTrieWriter<Integer> builder = makeTrie(buf);
        long rootPos = builder.complete();

        checkIterates(buf.asNewBuffer(), rootPos, "151", "515", ValueIterator.LeftBoundTreatment.ADMIT_PREFIXES, 2, 3, 4, 5);
        checkIterates(buf.asNewBuffer(), rootPos, "15151", "51515", ValueIterator.LeftBoundTreatment.ADMIT_PREFIXES, 2, 3, 4, 5);
        checkIterates(buf.asNewBuffer(), rootPos, "705", "73", ValueIterator.LeftBoundTreatment.ADMIT_PREFIXES, 8, 9, 10, 11);
        checkIterates(buf.asNewBuffer(), rootPos, "7051", "735", ValueIterator.LeftBoundTreatment.ADMIT_PREFIXES, 9, 10, 11);
        checkIterates(buf.asNewBuffer(), rootPos, "70", "737", ValueIterator.LeftBoundTreatment.ADMIT_PREFIXES, 8, 9, 10, 11, 12);
        // Note: 7054 has 70 as prefix, but we don't include 70 because a clearly smaller non-prefix entry 7051
        // exists between the bound and the prefix
        checkIterates(buf.asNewBuffer(), rootPos, "7054", "735", ValueIterator.LeftBoundTreatment.ADMIT_PREFIXES, 10, 11);


        checkIterates(buf.asNewBuffer(), rootPos, null, "515", ValueIterator.LeftBoundTreatment.ADMIT_PREFIXES, 1, 2, 3, 4, 5);
        checkIterates(buf.asNewBuffer(), rootPos, null, "51515", ValueIterator.LeftBoundTreatment.ADMIT_PREFIXES, 1, 2, 3, 4, 5);
        checkIterates(buf.asNewBuffer(), rootPos, "151", null, ValueIterator.LeftBoundTreatment.ADMIT_PREFIXES, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
        checkIterates(buf.asNewBuffer(), rootPos, "15151", null, ValueIterator.LeftBoundTreatment.ADMIT_PREFIXES, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
        checkIterates(buf.asNewBuffer(), rootPos, "7054", null, ValueIterator.LeftBoundTreatment.ADMIT_PREFIXES, 10, 11, 12);

        checkIterates(buf.asNewBuffer(), rootPos, "151", "155", ValueIterator.LeftBoundTreatment.ADMIT_PREFIXES, 2, 3);
        checkIterates(buf.asNewBuffer(), rootPos, "15", "151", ValueIterator.LeftBoundTreatment.ADMIT_PREFIXES, 2);
        checkIterates(buf.asNewBuffer(), rootPos, "1511", "1512", ValueIterator.LeftBoundTreatment.ADMIT_PREFIXES, 2);
        checkIterates(buf.asNewBuffer(), rootPos, "155", "155", ValueIterator.LeftBoundTreatment.ADMIT_PREFIXES, 3);

        checkIterates(buf.asNewBuffer(), rootPos, "155", "156", ValueIterator.LeftBoundTreatment.ADMIT_PREFIXES, 3);
        checkIterates(buf.asNewBuffer(), rootPos, "154", "156", ValueIterator.LeftBoundTreatment.ADMIT_PREFIXES, 3);
    }

    private void checkIterates(ByteBuffer buffer, long rootPos, String from, String to, ValueIterator.LeftBoundTreatment admitPrefix, int... expected)
    {
        Rebufferer source = new ByteBufRebufferer(buffer);
        ValueIterator<?> it = new ValueIterator<>(source, rootPos, source(from), source(to), admitPrefix);
        checkReturns(from + "-->" + to, it::nextPayloadedNode, pos -> getPayloadFlags(buffer, (int) pos), expected);

        if (admitPrefix != ValueIterator.LeftBoundTreatment.ADMIT_PREFIXES && from != null)
        {
            it = new ValueIterator<>(source, rootPos, null, source(to), admitPrefix, true);
            it.skipTo(source(from), admitPrefix);
            checkReturns(from + "-->" + to, it::nextPayloadedNode, pos -> getPayloadFlags(buffer, (int) pos), expected);

            it = new ValueIterator<>(source, rootPos, source(from), source(to), admitPrefix, true);
            it.skipTo(source(from), admitPrefix);
            checkReturns(from + "-->" + to, it::nextPayloadedNode, pos -> getPayloadFlags(buffer, (int) pos), expected);

            it = new ValueIterator<>(source, rootPos, null, source(to), admitPrefix, true);
            it.skipTo(source(from), admitPrefix);
            it.skipTo(source(from), admitPrefix);
            checkReturns(from + "-->" + to, it::nextPayloadedNode, pos -> getPayloadFlags(buffer, (int) pos), expected);
        }

        if (to != null)
        {
            // `to` is always inclusive, if we have a match for it check skipping to it works
            Walker<?> w = new Walker<>(source, rootPos);
            if (w.follow(source(to)) == ByteSource.END_OF_STREAM && w.payloadFlags() != 0
                && (admitPrefix != ValueIterator.LeftBoundTreatment.GREATER || !to.equals(from))) // skipping with ADMIT_EXACT may accept match after left bound is GREATER. Needs fix/error msg?
            {
                it = new ValueIterator<>(source, rootPos, source(from), source(to), admitPrefix, true);
                it.skipTo(source(to), ValueIterator.LeftBoundTreatment.ADMIT_EXACT);
                int[] exp = expected.length > 0 ? new int[] {expected[expected.length - 1]} : new int[0];
                checkReturns(from + "-->" + to + " skip " + to, it::nextPayloadedNode, pos -> getPayloadFlags(buffer, (int) pos), exp);
            }
        }

        if (admitPrefix != ValueIterator.LeftBoundTreatment.ADMIT_EXACT)
        {
            ReverseValueIterator<?> rit = new ReverseValueIterator<>(source, rootPos, source(from), source(to), admitPrefix == ValueIterator.LeftBoundTreatment.ADMIT_PREFIXES);
            reverse(expected);
            checkReturns(from + "<--" + to, rit::nextPayloadedNode, pos -> getPayloadFlags(buffer, (int) pos), expected);
            reverse(expected);  // return array in its original form if reused
        }
    }



    private void checkIterates(ByteBuffer buffer, long rootPos, int... expected)
    {
        Rebufferer source = new ByteBufRebufferer(buffer);
        ValueIterator<?> it = new ValueIterator<>(source, rootPos);
        checkReturns("Forward", it::nextPayloadedNode, pos -> getPayloadFlags(buffer, (int) pos), expected);

        ReverseValueIterator<?> rit = new ReverseValueIterator<>(source, rootPos);
        reverse(expected);
        checkReturns("Reverse", rit::nextPayloadedNode, pos -> getPayloadFlags(buffer, (int) pos), expected);
        reverse(expected);  // return array in its original form if reused
    }

    private void reverse(int[] expected)
    {
        final int size = expected.length;
        for (int i = 0; i < size / 2; ++i)
        {
            int t = expected[i];
            expected[i] = expected[size - 1 - i];
            expected[size - i - 1] = t;
        }
    }

    private int getPayloadFlags(ByteBuffer buffer, int pos)
    {
        return TrieNode.at(buffer, pos).payloadFlags(buffer, pos);
    }

    private void checkReturns(String testCase, LongSupplier supplier, LongToIntFunction mapper, int... expected)
    {
        IntArrayList list = new IntArrayList();
        while (true)
        {
            long pos = supplier.getAsLong();
            if (pos == -1)
                break;
            list.add(mapper.applyAsInt(pos));
        }
        assertArrayEquals(testCase + ": " + list + " != " + Arrays.toString(expected), expected, list.toIntArray());
    }


    @Test
    public void testSkipToGreater() throws IOException
    {
        DataOutputBuffer buf = new DataOutputBufferPaged();
        IncrementalTrieWriter<Integer> builder = makeTrie(buf);
        long rootPos = builder.complete();

        checkSkipsTo(buf.asNewBuffer(), rootPos, ValueIterator.LeftBoundTreatment.GREATER,
                     null,
                     new String[] { null, "151", "515", "999" },
                     new int[] { 1, 3, 6 });
        checkSkipsTo(buf.asNewBuffer(), rootPos, ValueIterator.LeftBoundTreatment.GREATER,
                     null,
                     new String[] { "151", "15151", "515", "555", "999" },
                     new int[] { 3, 4, 6, 7 });
        checkSkipsTo(buf.asNewBuffer(), rootPos, ValueIterator.LeftBoundTreatment.GREATER,
                     null,
                     new String[] { "69", "705", "7100", "73" },
                     new int[] { 8, 9, 10, 12 });
        checkSkipsTo(buf.asNewBuffer(), rootPos, ValueIterator.LeftBoundTreatment.GREATER,
                     null,
                     new String[] { "70", "7051", "717", "737" },
                     new int[] { 9, 10, 11 });
        checkSkipsTo(buf.asNewBuffer(), rootPos, ValueIterator.LeftBoundTreatment.GREATER,
                     "7051",
                     new String[] { "5555" },
                     new int[] { 7, 8, 9 });
        checkSkipsTo(buf.asNewBuffer(), rootPos, ValueIterator.LeftBoundTreatment.GREATER,
                     "7051",
                     new String[] { "5555", "7051" },
                     new int[] { 7 });
        checkSkipsTo(buf.asNewBuffer(), rootPos, ValueIterator.LeftBoundTreatment.GREATER,
                     "7050",
                     new String[] { "5555", "7051" },
                     new int[] { 7 });
        checkSkipsTo(buf.asNewBuffer(), rootPos, ValueIterator.LeftBoundTreatment.GREATER,
                     "7051",
                     new String[] { "5555", "7059" },
                     new int[] { 7 });
        checkSkipsTo(buf.asNewBuffer(), rootPos, ValueIterator.LeftBoundTreatment.GREATER,
                     "7051",
                     new String[] { "5555", "709" },
                     new int[] { 7 });
        checkSkipsTo(buf.asNewBuffer(), rootPos, ValueIterator.LeftBoundTreatment.GREATER,
                     "7051",
                     new String[] { "5555", "79" },
                     new int[] { 7 });
        checkSkipsTo(buf.asNewBuffer(), rootPos, ValueIterator.LeftBoundTreatment.GREATER,
                     "7051",
                     new String[] { "5555", "9" },
                     new int[] { 7 });
        checkSkipsTo(buf.asNewBuffer(), rootPos, ValueIterator.LeftBoundTreatment.GREATER,
                     "7051",
                     new String[] { "5555", null, "7051" },
                     new int[] { 7, 8 });
        checkSkipsTo(buf.asNewBuffer(), rootPos, ValueIterator.LeftBoundTreatment.GREATER,
                     "7051",
                     new String[] { "5555", null, "7059" },
                     new int[] { 7, 8 });
        checkSkipsTo(buf.asNewBuffer(), rootPos, ValueIterator.LeftBoundTreatment.GREATER,
                     "7051",
                     new String[] { "5555", null, "709" },
                     new int[] { 7, 8 });
        checkSkipsTo(buf.asNewBuffer(), rootPos, ValueIterator.LeftBoundTreatment.GREATER,
                     "7051",
                     new String[] { "5555", null, "79" },
                     new int[] { 7, 8 });
        checkSkipsTo(buf.asNewBuffer(), rootPos, ValueIterator.LeftBoundTreatment.GREATER,
                     "7051",
                     new String[] { "5555", null, "9" },
                     new int[] { 7, 8 });
    }

    @Test
    public void testSkipToExact() throws IOException
    {
        DataOutputBuffer buf = new DataOutputBufferPaged();
        IncrementalTrieWriter<Integer> builder = makeTrie(buf);
        long rootPos = builder.complete();

        checkSkipsTo(buf.asNewBuffer(), rootPos, ValueIterator.LeftBoundTreatment.ADMIT_EXACT,
                     null,
                     new String[] { null, "151", "515", "999" },
                     new int[] { 1, 2, 5 });
        checkSkipsTo(buf.asNewBuffer(), rootPos, ValueIterator.LeftBoundTreatment.ADMIT_EXACT,
                     null,
                     new String[] { "151", "15151", "515", "555", "999" },
                     new int[] { 2, 3, 5, 7 });
        checkSkipsTo(buf.asNewBuffer(), rootPos, ValueIterator.LeftBoundTreatment.ADMIT_EXACT,
                     null,
                     new String[] { "69", "705", "7100", "73" },
                     new int[] { 8, 9, 10, 11, 12 });
        checkSkipsTo(buf.asNewBuffer(), rootPos, ValueIterator.LeftBoundTreatment.ADMIT_EXACT,
                     null,
                     new String[] { "70", "7051", "717", "737" },
                     new int[] { 8, 9, 10, 12 });
        checkSkipsTo(buf.asNewBuffer(), rootPos, ValueIterator.LeftBoundTreatment.ADMIT_EXACT,
                     "7051",
                     new String[] { "5555" },
                     new int[] { 7, 8, 9 });
        checkSkipsTo(buf.asNewBuffer(), rootPos, ValueIterator.LeftBoundTreatment.ADMIT_EXACT,
                     "7051",
                     new String[] { "5555", "7051" },
                     new int[] { 7, 9 });
        checkSkipsTo(buf.asNewBuffer(), rootPos, ValueIterator.LeftBoundTreatment.ADMIT_EXACT,
                     "7050",
                     new String[] { "5555", "7051" },
                     new int[] { 7 });
        checkSkipsTo(buf.asNewBuffer(), rootPos, ValueIterator.LeftBoundTreatment.ADMIT_EXACT,
                     "7051",
                     new String[] { "5555", "7059" },
                     new int[] { 7 });
        checkSkipsTo(buf.asNewBuffer(), rootPos, ValueIterator.LeftBoundTreatment.ADMIT_EXACT,
                     "7051",
                     new String[] { "5555", "709" },
                     new int[] { 7 });
        checkSkipsTo(buf.asNewBuffer(), rootPos, ValueIterator.LeftBoundTreatment.ADMIT_EXACT,
                     "7051",
                     new String[] { "5555", "79" },
                     new int[] { 7 });
        checkSkipsTo(buf.asNewBuffer(), rootPos, ValueIterator.LeftBoundTreatment.ADMIT_EXACT,
                     "7051",
                     new String[] { "5555", "9" },
                     new int[] { 7 });
        checkSkipsTo(buf.asNewBuffer(), rootPos, ValueIterator.LeftBoundTreatment.ADMIT_EXACT,
                     "7051",
                     new String[] { "5555", null, "7051" },
                     new int[] { 7, 8, 9 });
        checkSkipsTo(buf.asNewBuffer(), rootPos, ValueIterator.LeftBoundTreatment.ADMIT_EXACT,
                     "7051",
                     new String[] { "5555", null, "7059" },
                     new int[] { 7, 8 });
        checkSkipsTo(buf.asNewBuffer(), rootPos, ValueIterator.LeftBoundTreatment.ADMIT_EXACT,
                     "7051",
                     new String[] { "5555", null, "709" },
                     new int[] { 7, 8 });
        checkSkipsTo(buf.asNewBuffer(), rootPos, ValueIterator.LeftBoundTreatment.ADMIT_EXACT,
                     "7051",
                     new String[] { "5555", null, "79" },
                     new int[] { 7, 8 });
        checkSkipsTo(buf.asNewBuffer(), rootPos, ValueIterator.LeftBoundTreatment.ADMIT_EXACT,
                     "7051",
                     new String[] { "5555", null, "9" },
                     new int[] { 7, 8 });
    }

    private void checkSkipsTo(ByteBuffer buffer, long rootPos, ValueIterator.LeftBoundTreatment leftBoundTreatment, String rightBound, String[] skips, int[] values)
    {
        checkSkipsTo(buffer, rootPos, leftBoundTreatment, null, rightBound, skips, values);
        String leftBound = skips[0];
        skips[0] = null;
        if (leftBound != null)
            checkSkipsTo(buffer, rootPos, leftBoundTreatment, leftBound, rightBound, skips, values);
    }

    private void checkSkipsTo(ByteBuffer buffer, long rootPos, ValueIterator.LeftBoundTreatment admitPrefix, String from, String to, String[] skips, int[] expected)
    {
        Rebufferer source = new ByteBufRebufferer(buffer);
        ValueIterator<?> it = new ValueIterator<>(source, rootPos, source(from), source(to), admitPrefix, true);
        int i = 0;
        IntArrayList list = new IntArrayList();
        while (true)
        {
            String skip = i < skips.length ? skips[i++] : null;
            if (skip != null)
                it.skipTo(source(skip), admitPrefix);

            long pos = it.nextPayloadedNode();
            if (pos == -1)
                break;
            list.add(getPayloadFlags(buffer, (int) pos));
        }
        assertArrayEquals(String.format("skipTo left %s right %s skips %s : %s != %s", from, to, Arrays.toString(skips), list, Arrays.toString(expected)), expected, list.toIntArray());
    }

    @Test
    public void testPartialTail() throws IOException
    {
        DataOutputBuffer buf = new DataOutputBufferPaged();
        IncrementalTrieWriter<Integer> builder = makeTrie(buf);
        IncrementalTrieWriter.PartialTail ptail = builder.makePartialRoot();
        long rootPos = builder.complete();
        try (Rebufferer source = new ByteBufRebufferer(buf.asNewBuffer());
             Rebufferer partialSource = new TailOverridingRebufferer(new ByteBufRebufferer(buf.asNewBuffer()), ptail.cutoff(), ptail.tail());
             ValueIterator<?> it = new ValueIterator<>(new ByteBufRebufferer(buf.asNewBuffer()),
                                                       rootPos,
                                                       source("151"),
                                                       source("515"),
                                                       ValueIterator.LeftBoundTreatment.ADMIT_PREFIXES);
             ValueIterator<?> tailIt = new ValueIterator<>(new TailOverridingRebufferer(new ByteBufRebufferer(buf.asNewBuffer()), ptail.cutoff(), ptail.tail()),
                                                           ptail.root(),
                                                           source("151"),
                                                           source("515"),
                                                           ValueIterator.LeftBoundTreatment.ADMIT_PREFIXES))
        {
            while (true)
            {
                long i1 = it.nextPayloadedNode();
                long i2 = tailIt.nextPayloadedNode();
                if (i1 == -1 || i2 == -1)
                    break;

                Rebufferer.BufferHolder bh1 = source.rebuffer(i1);
                Rebufferer.BufferHolder bh2 = partialSource.rebuffer(i2);

                int f1 = TrieNode.at(bh1.buffer(), (int) (i1 - bh1.offset())).payloadFlags(bh1.buffer(), (int) (i1 - bh1.offset()));
                int f2 = TrieNode.at(bh2.buffer(), (int) (i2 - bh2.offset())).payloadFlags(bh2.buffer(), (int) (i2 - bh2.offset()));
                assertEquals(f1, f2);

                bh2.release();
                bh1.release();
            }
        }
    }

    @Test
    public void testBigTrie() throws IOException
    {
        DataOutputBuffer buf = new AbstractTrieTestBase.DataOutputBufferPaged();
        IncrementalTrieWriter<Integer> builder = newTrieWriter(serializer, buf);
        payloadSize = 0;
        makeBigTrie(builder);
        builder.reset();
        payloadSize = 200;
        makeBigTrie(builder);

        long rootPos = builder.complete();
        Rebufferer source = new ByteBufRebufferer(buf.asNewBuffer());
        ValueIterator<?> it = new ValueIterator<>(source, rootPos);

        while (true)
        {
            long i1 = it.nextPayloadedNode();
            if (i1 == -1)
                break;

            TrieNode node = TrieNode.at(buf.asNewBuffer(), (int) i1);
            assertNotEquals(0, node.payloadFlags(buf.asNewBuffer(), (int) i1));
        }
    }


    private IncrementalTrieWriter<Integer> makeTrie(DataOutputBuffer out) throws IOException
    {
        IncrementalTrieWriter<Integer> builder = newTrieWriter(serializer, out);
        dump = true;
        builder.add(source("115"), 1);
        builder.add(source("151"), 2);
        builder.add(source("155"), 3);
        builder.add(source("511"), 4);
        builder.add(source("515"), 5);
        builder.add(source("551"), 6);
        builder.add(source("555555555555555555555555555555555555555555555555555555555555555555"), 7);

        builder.add(source("70"), 8);
        builder.add(source("7051"), 9);
        builder.add(source("717"), 10);
        builder.add(source("73"), 11);
        builder.add(source("737"), 12);
        return builder;
    }

    private void makeBigTrie(IncrementalTrieWriter<Integer> builder) throws IOException
    {
        dump = false;
        for (int shift = 0; shift < 8; shift++)
            for (long i = 1; i < 80; i++)
                builder.add(longSource(i, shift * 8, 100), (int) (i % 7) + 1);
    }

    private ByteComparable longSource(long l, int shift, int size)
    {
        String s = StringUtils.leftPad(toBase(l), 8, '0');
        s = StringUtils.rightPad(s, 8 + shift, '0');
        s = StringUtils.leftPad(s, size, '0');
        return source(s);
    }
}
