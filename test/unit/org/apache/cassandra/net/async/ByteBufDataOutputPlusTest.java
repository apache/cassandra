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

package org.apache.cassandra.net.async;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.io.util.SafeMemory;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;

public class ByteBufDataOutputPlusTest
{
    private static final String KEYSPACE1 = "NettyPipilineTest";
    private static final String STANDARD1 = "Standard1";
    private static final int columnCount = 128;

    private ByteBuf buf;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, STANDARD1, columnCount, AsciiType.instance, BytesType.instance));
        CompactionManager.instance.disableAutoCompaction();
    }

    @After
    public void tearDown()
    {
        if (buf != null)
            buf.release();
    }

    @Test
    public void compareBufferSizes() throws IOException
    {
        final int currentFrameSize = getMessage().message.serializedSize(MessagingService.current_version);

        ByteBuffer buffer = ByteBuffer.allocateDirect(currentFrameSize); //bufferedOut.nioBuffer(0, bufferedOut.writableBytes());
        getMessage().message.serialize(new DataOutputBuffer(buffer), MessagingService.current_version);
        Assert.assertFalse(buffer.hasRemaining());
        Assert.assertEquals(buffer.capacity(), buffer.position());

        ByteBuf bbosOut = PooledByteBufAllocator.DEFAULT.ioBuffer(currentFrameSize, currentFrameSize);
        try
        {
            getMessage().message.serialize(new ByteBufDataOutputPlus(bbosOut), MessagingService.current_version);

            Assert.assertFalse(bbosOut.isWritable());
            Assert.assertEquals(bbosOut.capacity(), bbosOut.writerIndex());

            Assert.assertEquals(buffer.position(), bbosOut.writerIndex());
            for (int i = 0; i < currentFrameSize; i++)
            {
                Assert.assertEquals(buffer.get(i), bbosOut.getByte(i));
            }
        }
        finally
        {
            bbosOut.release();
        }
    }

    private QueuedMessage getMessage()
    {
        ColumnFamilyStore cfs1 = Keyspace.open(KEYSPACE1).getColumnFamilyStore(STANDARD1);
        ByteBuffer buf = ByteBuffer.allocate(1 << 10);
        RowUpdateBuilder rowUpdateBuilder = new RowUpdateBuilder(cfs1.metadata.get(), 0, "k")
                                            .clustering("bytes");
        for (int i = 0; i < columnCount; i++)
            rowUpdateBuilder.add("val" + i, buf);

        Mutation mutation = rowUpdateBuilder.build();
        return new QueuedMessage(mutation.createMessage(), 42);
    }

    @Test
    public void compareDOS() throws IOException
    {
        buf = PooledByteBufAllocator.DEFAULT.ioBuffer(1024, 1024);
        ByteBuffer buffer = ByteBuffer.allocateDirect(1024);

        ByteBufDataOutputPlus byteBufDataOutputPlus = new ByteBufDataOutputPlus(buf);
        DataOutputBuffer dataOutputBuffer = new DataOutputBuffer(buffer);

        write(byteBufDataOutputPlus);
        write(dataOutputBuffer);

        Assert.assertEquals(buffer.position(), buf.writerIndex());
        for (int i = 0; i < buffer.position(); i++)
        {
            Assert.assertEquals(buffer.get(i), buf.getByte(i));
        }
    }

    private void write(DataOutputPlus out) throws IOException
    {
        ByteBuffer b = ByteBuffer.allocate(8);
        b.putLong(29811134237462734L);
        out.write(b);
        b = ByteBuffer.allocateDirect(8);
        b.putDouble(92367.4253647890626);
        out.write(b);

        out.writeInt(29319236);

        byte[] array = new byte[17];
        for (int i = 0; i < array.length; i++)
            array[i] = (byte)i;
        out.write(array, 0 , array.length);

        out.write(42);
        out.writeUTF("This is a great string!!");
        out.writeByte(-100);
        out.writeUnsignedVInt(3247634L);
        out.writeVInt(12313695L);
        out.writeBoolean(true);
        out.writeShort(4371);
        out.writeChar('j');
        out.writeLong(472348263487234L);
        out.writeFloat(34534.12623F);
        out.writeDouble(0.2384253D);
        out.writeBytes("Write my bytes");
        out.writeChars("These are some swell chars");

        Memory memory = new SafeMemory(8);
        memory.setLong(0, -21365123651231L);
        out.write(memory, 0, memory.size());
        memory.close();
    }

    @Test (expected = UnsupportedOperationException.class)
    public void applyToChannel() throws IOException
    {
        ByteBufDataOutputPlus out = new ByteBufDataOutputPlus(Unpooled.wrappedBuffer(new byte[0]));
        out.applyToChannel(null);
    }
}
