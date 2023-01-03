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
import java.util.Arrays;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.io.util.DataOutputBuffer;
import org.mockito.Mockito;
import org.quicktheories.api.Pair;
import org.quicktheories.generators.Generate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.longs;

@SuppressWarnings("unchecked")
public class TrieNodeTest
{
    private final SerializationNode<Integer> sn = Mockito.mock(SerializationNode.class);
    private final DataOutputBuffer out = new DataOutputBuffer();

    @After
    public void after()
    {
        reset(sn);
        out.clear();
    }

    @Test
    public void testTypeFor0Children()
    {
        when(sn.childCount()).thenReturn(0);
        assertSame(TrieNode.Types.PAYLOAD_ONLY, TrieNode.typeFor(sn, 0));
    }

    @Test
    public void testTypeFor1ChildNoPayload()
    {
        when(sn.childCount()).thenReturn(1);
        when(sn.payload()).thenReturn(null);

        qt().forAll(Generate.pick(Arrays.asList(
                Pair.of(Ranges.BITS_4, TrieNode.Types.SINGLE_NOPAYLOAD_4),
                Pair.of(Ranges.BITS_8, TrieNode.Types.SINGLE_8),
                Pair.of(Ranges.BITS_12, TrieNode.Types.SINGLE_NOPAYLOAD_12),
                Pair.of(Ranges.BITS_16, TrieNode.Types.SINGLE_16),
                Pair.of(Ranges.BITS_24, TrieNode.Types.DENSE_24),
                Pair.of(Ranges.BITS_32, TrieNode.Types.DENSE_32),
                Pair.of(Ranges.BITS_40, TrieNode.Types.DENSE_40),
                Pair.of(Ranges.BITS_GT40, TrieNode.Types.LONG_DENSE)
        )).flatMap(p -> longs().between(p._1.min, p._1.max).map(v -> Pair.of(v, p._2)))).check(p -> {
            when(sn.maxPositionDelta(0)).thenReturn(-p._1);
            return p._2 == TrieNode.typeFor(sn, 0);
        });
    }

    @Test
    public void testTypeFor1ChildAndPayload()
    {
        when(sn.childCount()).thenReturn(1);
        when(sn.payload()).thenReturn(1);

        qt().forAll(Generate.pick(Arrays.asList(
                Pair.of(Ranges.BITS_4, TrieNode.Types.SINGLE_8),
                Pair.of(Ranges.BITS_8, TrieNode.Types.SINGLE_8),
                Pair.of(Ranges.BITS_12, TrieNode.Types.SINGLE_16),
                Pair.of(Ranges.BITS_16, TrieNode.Types.SINGLE_16),
                Pair.of(Ranges.BITS_24, TrieNode.Types.DENSE_24),
                Pair.of(Ranges.BITS_32, TrieNode.Types.DENSE_32),
                Pair.of(Ranges.BITS_40, TrieNode.Types.DENSE_40),
                Pair.of(Ranges.BITS_GT40, TrieNode.Types.LONG_DENSE)
        )).flatMap(p -> longs().between(p._1.min, p._1.max).map(v -> Pair.of(v, p._2)))).check(p -> {
            when(sn.maxPositionDelta(0)).thenReturn(-p._1);
            return p._2 == TrieNode.typeFor(sn, 0);
        });
    }

    @Test
    public void testTypeForMoreChildrenAndNoPayload()
    {
        when(sn.childCount()).thenReturn(2);
        when(sn.payload()).thenReturn(null);

        qt().forAll(Generate.pick(Arrays.asList(
                Pair.of(Ranges.BITS_4, TrieNode.Types.DENSE_12),
                Pair.of(Ranges.BITS_8, TrieNode.Types.DENSE_12),
                Pair.of(Ranges.BITS_12, TrieNode.Types.DENSE_12),
                Pair.of(Ranges.BITS_16, TrieNode.Types.DENSE_16),
                Pair.of(Ranges.BITS_24, TrieNode.Types.DENSE_24),
                Pair.of(Ranges.BITS_32, TrieNode.Types.DENSE_32),
                Pair.of(Ranges.BITS_40, TrieNode.Types.DENSE_40),
                Pair.of(Ranges.BITS_GT40, TrieNode.Types.LONG_DENSE)
        )).flatMap(p -> longs().between(p._1.min, p._1.max).map(v -> Pair.of(v, p._2)))).check(p -> {
            when(sn.maxPositionDelta(0)).thenReturn(-p._1);
            return p._2 == TrieNode.typeFor(sn, 0);
        });
    }

    @Test
    public void testPayloadOnlyNode() throws IOException
    {
        TrieNode.Types.PAYLOAD_ONLY.serialize(out, null, 1 | 4, 0);
        out.flush();

        TrieNode node = TrieNode.at(out.asNewBuffer(), 0);
        assertEquals(TrieNode.Types.PAYLOAD_ONLY, node);
        assertEquals(1 | 4, node.payloadFlags(out.asNewBuffer(), 0));
        assertEquals(1, node.payloadPosition(out.asNewBuffer(), 0));
        assertEquals(1, node.sizeofNode(null));
        assertEquals(-1, node.search(null, 0, 0));
        assertEquals(Integer.MAX_VALUE, node.transitionByte(null, 0, 0));
        assertEquals(123, node.lesserTransition(null, 0, 0, 0, 123));
        assertEquals(123, node.greaterTransition(null, 0, 0, 0, 123));
        assertEquals(-1, node.lastTransition(null, 0, 0));
        assertEquals(-1, node.transition(null, 0, 0, 0));
        assertEquals(0, node.transitionRange(null, 0));
        assertEquals(0, node.transitionDelta(null, 0, 0));
    }

    private void prepareSingleNode(long delta)
    {
        when(sn.childCount()).thenReturn(1);
        when(sn.transition(0)).thenReturn(123);
        when(sn.serializedPositionDelta(0, 0)).thenReturn(delta);
    }

    private void singleNodeAssertions(TrieNode node, int payloadFlags, int size, long pos)
    {
        assertEquals(payloadFlags, node.payloadFlags(out.asNewBuffer(), 0));
        assertEquals(size, node.sizeofNode(null));
        assertEquals(0, node.search(out.asNewBuffer(), 0, 123));
        assertEquals(-1, node.search(out.asNewBuffer(), 0, 122));
        assertEquals(-2, node.search(out.asNewBuffer(), 0, 124));
        assertEquals(123, node.transitionByte(out.asNewBuffer(), 0, 0));
        assertEquals(Integer.MAX_VALUE, node.transitionByte(out.asNewBuffer(), 0, 1));
        assertEquals(100, node.lesserTransition(out.asNewBuffer(), 0, 100 - pos, -2, 123));
        assertEquals(234, node.greaterTransition(null, 0, 0, 0, 234));
        assertEquals(234, node.greaterTransition(out.asNewBuffer(), 0, 100 - pos, -2, 234));
        assertEquals(100, node.greaterTransition(out.asNewBuffer(), 0, 100 - pos, -1, 234));
        assertEquals(234, node.greaterTransition(out.asNewBuffer(), 0, 100 - pos, 0, 234));
        assertEquals(100, node.lastTransition(out.asNewBuffer(), 0, 100 - pos));
        assertEquals(100, node.transition(out.asNewBuffer(), 0, 100 - pos, 0));
        assertEquals(1, node.transitionRange(out.asNewBuffer(), 0));
        assertEquals(pos, node.transitionDelta(out.asNewBuffer(), 0, 0));
    }

    @Test
    public void testSingle16Node() throws IOException
    {
        prepareSingleNode(-43210L);
        TrieNode.Types.SINGLE_16.serialize(out, sn, 1 | 4, 0);
        TrieNode node = TrieNode.at(out.asNewBuffer(), 0);
        assertEquals(TrieNode.Types.SINGLE_16, node);
        singleNodeAssertions(node, 1 | 4, 4, -43210);
    }

    @Test
    public void testSingleNoPayload4Node() throws IOException
    {
        prepareSingleNode(-7L);
        TrieNode.Types.SINGLE_NOPAYLOAD_4.serialize(out, sn, 0, 0);
        TrieNode node = TrieNode.at(out.asNewBuffer(), 0);
        assertEquals(TrieNode.Types.SINGLE_NOPAYLOAD_4, node);
        singleNodeAssertions(node, 0, 2, -7);
    }

    @Test
    public void testSingleNoPayload12Node() throws IOException
    {
        prepareSingleNode(-1234L);
        TrieNode.Types.SINGLE_NOPAYLOAD_12.serialize(out, sn, 0, 0);
        TrieNode node = TrieNode.at(out.asNewBuffer(), 0);
        assertEquals(TrieNode.Types.SINGLE_NOPAYLOAD_12, node);
        singleNodeAssertions(node, 0, 3, -1234L);
    }

    private void prepareSparseNode(long delta) throws IOException
    {
        out.write(new byte[6]);
        when(sn.childCount()).thenReturn(3);
        when(sn.transition(0)).thenReturn(10);
        when(sn.transition(1)).thenReturn(20);
        when(sn.transition(2)).thenReturn(30);
        when(sn.serializedPositionDelta(0, 6)).thenReturn(delta);
        when(sn.serializedPositionDelta(1, 6)).thenReturn(delta + 2);
        when(sn.serializedPositionDelta(2, 6)).thenReturn(delta + 4);
    }

    private void sparseOrDenseNodeAssertions(TrieNode node, int payloadFlags, int size, long pos)
    {
        assertEquals(size, node.sizeofNode(sn));
        assertEquals(payloadFlags, node.payloadFlags(out.asNewBuffer(), 6));
        assertEquals(3, node.transitionRange(out.asNewBuffer(), 6));
        assertEquals(6 + size, node.payloadPosition(out.asNewBuffer(), 6));

        assertEquals(Integer.MAX_VALUE, node.transitionByte(out.asNewBuffer(), 6, 3));

        assertEquals(10, node.lesserTransition(out.asNewBuffer(), 6, 10 - pos, 1, 123));
        assertEquals(14, node.lesserTransition(out.asNewBuffer(), 6, 10 - pos, -4, 123));

        assertEquals(14, node.greaterTransition(out.asNewBuffer(), 6, 10 - pos, 1, 234));
        assertEquals(234, node.greaterTransition(out.asNewBuffer(), 6, 10 - pos, 2, 234));
        assertEquals(10, node.greaterTransition(out.asNewBuffer(), 6, 10 - pos, -1, 234));

        assertEquals(14, node.lastTransition(out.asNewBuffer(), 6, 10 - pos));

        assertEquals(pos, node.transitionDelta(out.asNewBuffer(), 6, 0));
        assertEquals(pos + 2, node.transitionDelta(out.asNewBuffer(), 6, 1));
        assertEquals(pos + 4, node.transitionDelta(out.asNewBuffer(), 6, 2));

        assertEquals(10, node.transition(out.asNewBuffer(), 6, 10 - pos, 0));
        assertEquals(12, node.transition(out.asNewBuffer(), 6, 10 - pos, 1));
        assertEquals(14, node.transition(out.asNewBuffer(), 6, 10 - pos, 2));
    }

    private void sparseNodeAssertions(TrieNode node, int payloadFlags, int size, long pos)
    {
        sparseOrDenseNodeAssertions(node, payloadFlags, size, pos);
        assertEquals(-1, node.search(out.asNewBuffer(), 6, 5));
        assertEquals(0, node.search(out.asNewBuffer(), 6, 10));
        assertEquals(-2, node.search(out.asNewBuffer(), 6, 15));
        assertEquals(-4, node.search(out.asNewBuffer(), 6, 35));

        assertEquals(10, node.transitionByte(out.asNewBuffer(), 6, 0));
        assertEquals(20, node.transitionByte(out.asNewBuffer(), 6, 1));
        assertEquals(30, node.transitionByte(out.asNewBuffer(), 6, 2));
    }

    @Test
    public void testSparse16Node() throws IOException
    {
        prepareSparseNode(-43210L);
        TrieNode.Types.SPARSE_16.serialize(out, sn, 1 | 4, 6);
        TrieNode node = TrieNode.at(out.asNewBuffer(), 6);
        assertEquals(TrieNode.Types.SPARSE_16, node);
        sparseNodeAssertions(node, 1 | 4, 11, -43210L);
    }

    @Test
    public void testSparse12Node() throws IOException
    {
        prepareSparseNode(-1234L);
        TrieNode.Types.SPARSE_12.serialize(out, sn, 1 | 4, 6);
        TrieNode node = TrieNode.at(out.asNewBuffer(), 6);
        assertEquals(TrieNode.Types.SPARSE_12, node);
        sparseNodeAssertions(node, 1 | 4, 10, -1234L);
    }

    private void prepareDenseNode(long delta) throws IOException
    {
        out.write(new byte[6]);
        when(sn.childCount()).thenReturn(3);
        when(sn.transition(0)).thenReturn(11);
        when(sn.transition(1)).thenReturn(12);
        when(sn.transition(2)).thenReturn(13);
        when(sn.serializedPositionDelta(0, 6)).thenReturn(delta);
        when(sn.serializedPositionDelta(1, 6)).thenReturn(delta + 2);
        when(sn.serializedPositionDelta(2, 6)).thenReturn(delta + 4);
    }

    private void denseNodeAssertions(TrieNode node, int payload, int size, long pos)
    {
        sparseOrDenseNodeAssertions(node, payload, size, pos);
        assertEquals(-1, node.search(out.asNewBuffer(), 6, 10));
        assertEquals(0, node.search(out.asNewBuffer(), 6, 11));
        assertEquals(-4, node.search(out.asNewBuffer(), 6, 14));

        assertEquals(11, node.transitionByte(out.asNewBuffer(), 6, 0));
        assertEquals(12, node.transitionByte(out.asNewBuffer(), 6, 1));
        assertEquals(13, node.transitionByte(out.asNewBuffer(), 6, 2));
    }

    @Test
    public void testDense16Node() throws IOException
    {
        prepareDenseNode(-43210L);
        TrieNode.Types.DENSE_16.serialize(out, sn, 1 | 4, 6);
        TrieNode node = TrieNode.at(out.asNewBuffer(), 6);
        assertEquals(TrieNode.Types.DENSE_16, node);
        denseNodeAssertions(node, 1 | 4, 9, -43210L);
    }

    @Test
    public void testDense12Node() throws IOException
    {
        prepareDenseNode(-1234L);
        TrieNode.Types.DENSE_12.serialize(out, sn, 1 | 4, 6);
        TrieNode node = TrieNode.at(out.asNewBuffer(), 6);
        assertEquals(TrieNode.Types.DENSE_12, node);
        denseNodeAssertions(node, 1 | 4, 8, -1234L);
    }

    @Test
    public void testLongDenseNode() throws IOException
    {
        prepareDenseNode(-0x7ffffffffffffffL);
        TrieNode.Types.LONG_DENSE.serialize(out, sn, 1 | 4, 6);
        TrieNode node = TrieNode.at(out.asNewBuffer(), 6);
        assertEquals(TrieNode.Types.LONG_DENSE, node);
        denseNodeAssertions(node, 1 | 4, 27, -0x7ffffffffffffffL);
    }

    private enum Ranges
    {
        BITS_4(0x1L, 0xfL),
        BITS_8(0x10L, 0xffL),
        BITS_12(0x100L, 0xfffL),
        BITS_16(0x1000L, 0xffffL),
        BITS_24(0x100000L, 0xffffffL),
        BITS_32(0x10000000L, 0xffffffffL),
        BITS_40(0x1000000000L, 0xffffffffffL),
        BITS_GT40(0x100000000000L, 0x7fffffffffffffffL);

        private final long min, max;

        Ranges(long min, long max)
        {
            this.min = min;
            this.max = max;
        }
    }
}