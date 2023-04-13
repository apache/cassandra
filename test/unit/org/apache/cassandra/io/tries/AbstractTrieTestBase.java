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
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.BiFunction;

import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.PageAware;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

@RunWith(Parameterized.class)
abstract public class AbstractTrieTestBase
{
    @Parameterized.Parameter(0)
    public TestClass writerClass;

    enum TestClass
    {
        SIMPLE(IncrementalTrieWriterSimple::new),
        PAGE_AWARE(IncrementalTrieWriterPageAware::new),
        PAGE_AWARE_DEEP_ON_STACK((serializer, dest) -> new IncrementalDeepTrieWriterPageAware<>(serializer, dest, 256)),
        PAGE_AWARE_DEEP_ON_HEAP((serializer, dest) -> new IncrementalDeepTrieWriterPageAware<>(serializer, dest, 0)),
        PAGE_AWARE_DEEP_MIXED((serializer, dest) -> new IncrementalDeepTrieWriterPageAware<>(serializer, dest, 2));

        final BiFunction<TrieSerializer<Integer, DataOutputPlus>, DataOutputPlus, IncrementalTrieWriter<Integer>> constructor;
        TestClass(BiFunction<TrieSerializer<Integer, DataOutputPlus>, DataOutputPlus, IncrementalTrieWriter<Integer>> constructor)
        {
            this.constructor = constructor;
        }
    }

    @Parameterized.Parameters(name = "{index}: trie writer class={0}")
    public static Collection<Object[]> data()
    {
        return Arrays.asList(new Object[]{ TestClass.SIMPLE },
                             new Object[]{ TestClass.PAGE_AWARE },
                             new Object[]{ TestClass.PAGE_AWARE_DEEP_ON_STACK },
                             new Object[]{ TestClass.PAGE_AWARE_DEEP_ON_HEAP },
                             new Object[]{ TestClass.PAGE_AWARE_DEEP_MIXED });
    }

    protected final static Logger logger = LoggerFactory.getLogger(TrieBuilderTest.class);
    protected final static int BASE = 80;

    protected boolean dump = false;
    protected int payloadSize = 0;

    @Before
    public void beforeTest()
    {
        dump = false;
        payloadSize = 0;
    }

    IncrementalTrieWriter<Integer> newTrieWriter(TrieSerializer<Integer, DataOutputPlus> serializer, DataOutputPlus out)
    {
        return writerClass.constructor.apply(serializer, out);
    }

    protected final TrieSerializer<Integer, DataOutputPlus> serializer = new TrieSerializer<Integer, DataOutputPlus>()
    {
        @Override
        public int sizeofNode(SerializationNode<Integer> node, long nodePosition)
        {
            return TrieNode.typeFor(node, nodePosition).sizeofNode(node) + payloadSize;
        }

        @Override
        public void write(DataOutputPlus dataOutput, SerializationNode<Integer> node, long nodePosition) throws IOException
        {
            if (dump)
                logger.info("Writing at {} type {} size {}: {}", Long.toHexString(nodePosition), TrieNode.typeFor(node, nodePosition), TrieNode.typeFor(node, nodePosition).sizeofNode(node), node);
            // Our payload value is an integer of four bits.
            // We use the payload bits in the trie node header to fully store it.
            TrieNode.typeFor(node, nodePosition).serialize(dataOutput, node, node.payload() != null ? node.payload() : 0, nodePosition);
            // and we also add some padding if a test needs it
            dataOutput.write(new byte[payloadSize]);
        }
    };


    protected int valueFor(long found)
    {
        return Long.bitCount(found + 1) & 0xF;
    }

    protected ByteComparable source(String s)
    {
        if (s == null)
            return null;
        ByteBuffer buf = ByteBuffer.allocate(s.length());
        for (int i = 0; i < s.length(); ++i)
            buf.put((byte) s.charAt(i));
        buf.rewind();
        return ByteComparable.fixedLength(buf);
    }

    protected String toBase(long v)
    {
        return BigInteger.valueOf(v).toString(BASE);
    }

    // In-memory buffer with added paging parameters, to make sure the code below does the proper layout
    protected static class DataOutputBufferPaged extends DataOutputBuffer
    {
        @Override
        public int maxBytesInPage()
        {
            return PageAware.PAGE_SIZE;
        }

        @Override
        public void padToPageBoundary() throws IOException
        {
            PageAware.pad(this);
        }

        @Override
        public int bytesLeftInPage()
        {
            long position = position();
            long bytesLeft = PageAware.pageLimit(position) - position;
            return (int) bytesLeft;
        }

        @Override
        public long paddedPosition()
        {
            return PageAware.padded(position());
        }
    }

    protected static class ByteBufRebufferer implements Rebufferer, Rebufferer.BufferHolder
    {
        final ByteBuffer buffer;

        ByteBufRebufferer(ByteBuffer buffer)
        {
            this.buffer = buffer;
        }

        @Override
        public ChannelProxy channel()
        {
            return null;
        }

        @Override
        public ByteBuffer buffer()
        {
            return buffer;
        }

        @Override
        public long fileLength()
        {
            return buffer.remaining();
        }

        @Override
        public double getCrcCheckChance()
        {
            return 0;
        }

        @Override
        public BufferHolder rebuffer(long position)
        {
            return this;
        }

        @Override
        public long offset()
        {
            return 0;
        }

        @Override
        public void release()
        {
            // nothing
        }

        @Override
        public void close()
        {
            // nothing
        }

        @Override
        public void closeReader()
        {
            // nothing
        }
    }
}
