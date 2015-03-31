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
package org.apache.cassandra.io.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.utils.ByteBufferUtil;

public class DataOutputTest
{
    @Test
    public void testDataOutputStreamPlus() throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStreamPlus write = new DataOutputStreamPlus(bos);
        DataInput canon = testWrite(write);
        DataInput test = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
        testRead(test, canon);
    }

    @Test
    public void testDataOutputChannelAndChannel() throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStreamPlus write = new DataOutputStreamAndChannel(Channels.newChannel(bos));
        DataInput canon = testWrite(write);
        DataInput test = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
        testRead(test, canon);
    }

    @Test
    public void testDataOutputBuffer() throws IOException
    {
        DataOutputBuffer write = new DataOutputBuffer();
        DataInput canon = testWrite(write);
        DataInput test = new DataInputStream(new ByteArrayInputStream(write.toByteArray()));
        testRead(test, canon);
    }

    @Test
    public void testDataOutputDirectByteBuffer() throws IOException
    {
        ByteBuffer buf = wrap(new byte[345], true);
        DataOutputByteBuffer write = new DataOutputByteBuffer(buf.duplicate());
        DataInput canon = testWrite(write);
        DataInput test = new DataInputStream(new ByteArrayInputStream(ByteBufferUtil.getArray(buf)));
        testRead(test, canon);
    }

    @Test
    public void testDataOutputHeapByteBuffer() throws IOException
    {
        ByteBuffer buf = wrap(new byte[345], false);
        DataOutputByteBuffer write = new DataOutputByteBuffer(buf.duplicate());
        DataInput canon = testWrite(write);
        DataInput test = new DataInputStream(new ByteArrayInputStream(ByteBufferUtil.getArray(buf)));
        testRead(test, canon);
    }

    @Test
    public void testSafeMemoryWriter() throws IOException
    {
        SafeMemoryWriter write = new SafeMemoryWriter(10);
        DataInput canon = testWrite(write);
        byte[] bytes = new byte[345];
        write.currentBuffer().getBytes(0, bytes, 0, 345);
        DataInput test = new DataInputStream(new ByteArrayInputStream(bytes));
        testRead(test, canon);
    }

    @Test
    public void testFileOutputStream() throws IOException
    {
        File file = FileUtils.createTempFile("dataoutput", "test");
        try
        {
            DataOutputStreamAndChannel write = new DataOutputStreamAndChannel(new FileOutputStream(file));
            DataInput canon = testWrite(write);
            write.close();
            DataInputStream test = new DataInputStream(new FileInputStream(file));
            testRead(test, canon);
            test.close();
        }
        finally
        {
            Assert.assertTrue(file.delete());
        }
    }

    @Test
    public void testRandomAccessFile() throws IOException
    {
        File file = FileUtils.createTempFile("dataoutput", "test");
        try
        {
            final RandomAccessFile raf = new RandomAccessFile(file, "rw");
            DataOutputStreamAndChannel write = new DataOutputStreamAndChannel(Channels.newOutputStream(raf.getChannel()), raf.getChannel());
            DataInput canon = testWrite(write);
            write.close();
            DataInputStream test = new DataInputStream(new FileInputStream(file));
            testRead(test, canon);
            test.close();
        }
        finally
        {
            Assert.assertTrue(file.delete());
        }
    }

    @Test
    public void testSequentialWriter() throws IOException
    {
        File file = FileUtils.createTempFile("dataoutput", "test");
        final SequentialWriter writer = new SequentialWriter(file, 32, false);
        DataOutputStreamAndChannel write = new DataOutputStreamAndChannel(writer, writer);
        DataInput canon = testWrite(write);
        write.flush();
        write.close();
        DataInputStream test = new DataInputStream(new FileInputStream(file));
        testRead(test, canon);
        test.close();
        Assert.assertTrue(file.delete());
    }

    private DataInput testWrite(DataOutputPlus test) throws IOException
    {
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        final DataOutput canon = new DataOutputStream(bos);
        Random rnd = ThreadLocalRandom.current();

        int size = 50;
        byte[] bytes = new byte[size];
        rnd.nextBytes(bytes);
        ByteBufferUtil.writeWithLength(bytes, test);
        ByteBufferUtil.writeWithLength(bytes, canon);

        bytes = new byte[size];
        rnd.nextBytes(bytes);
        ByteBufferUtil.writeWithLength(wrap(bytes, false), test);
        ByteBufferUtil.writeWithLength(bytes, canon);

        bytes = new byte[size];
        rnd.nextBytes(bytes);
        ByteBufferUtil.writeWithLength(wrap(bytes, true), test);
        ByteBufferUtil.writeWithLength(bytes, canon);

        bytes = new byte[size];
        rnd.nextBytes(bytes);
        ByteBufferUtil.writeWithShortLength(bytes, test);
        ByteBufferUtil.writeWithShortLength(bytes, canon);

        bytes = new byte[size];
        rnd.nextBytes(bytes);
        ByteBufferUtil.writeWithShortLength(wrap(bytes, false), test);
        ByteBufferUtil.writeWithShortLength(bytes, canon);

        bytes = new byte[size];
        rnd.nextBytes(bytes);
        ByteBufferUtil.writeWithShortLength(wrap(bytes, true), test);
        ByteBufferUtil.writeWithShortLength(bytes, canon);
        // 318

        {
            long v = rnd.nextLong();
            test.writeLong(v);
            canon.writeLong(v);
        }
        {
            int v = rnd.nextInt();
            test.writeInt(v);
            canon.writeInt(v);
        }
        {
            short v = (short) rnd.nextInt();
            test.writeShort(v);
            canon.writeShort(v);
        }
        {
            byte v = (byte) rnd.nextInt();
            test.write(v);
            canon.write(v);
        }
        {
            double v = rnd.nextDouble();
            test.writeDouble(v);
            canon.writeDouble(v);
        }
        {
            float v = (float) rnd.nextDouble();
            test.writeFloat(v);
            canon.writeFloat(v);
        }

        // 27
        return new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
    }

    private void testRead(DataInput test, DataInput canon) throws IOException
    {
        Assert.assertEquals(ByteBufferUtil.readWithLength(canon), ByteBufferUtil.readWithLength(test));
        Assert.assertEquals(ByteBufferUtil.readWithLength(canon), ByteBufferUtil.readWithLength(test));
        Assert.assertEquals(ByteBufferUtil.readWithLength(canon), ByteBufferUtil.readWithLength(test));
        Assert.assertEquals(ByteBufferUtil.readWithShortLength(canon), ByteBufferUtil.readWithShortLength(test));
        Assert.assertEquals(ByteBufferUtil.readWithShortLength(canon), ByteBufferUtil.readWithShortLength(test));
        Assert.assertEquals(ByteBufferUtil.readWithShortLength(canon), ByteBufferUtil.readWithShortLength(test));
        assert test.readLong() == canon.readLong();
        assert test.readInt() == canon.readInt();
        assert test.readShort() == canon.readShort();
        assert test.readByte() == canon.readByte();
        assert test.readDouble() == canon.readDouble();
        assert test.readFloat() == canon.readFloat();
        try
        {
            test.readInt();
            assert false;
        }
        catch (EOFException ignore)
        {
            // it worked
        }
    }

    private static ByteBuffer wrap(byte[] bytes, boolean direct)
    {
        ByteBuffer buf = direct ? ByteBuffer.allocateDirect(bytes.length + 20) : ByteBuffer.allocate(bytes.length + 20);
        buf.position(10);
        buf.limit(bytes.length + 10);
        buf.duplicate().put(bytes);
        return buf;
    }
}
