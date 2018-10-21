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
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.ByteBufferUtil;

public class DataOutputTest
{
    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testWrappedDataOutputStreamPlus() throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStreamPlus write = new WrappedDataOutputStreamPlus(bos);
        DataInput canon = testWrite(write);
        DataInput test = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
        testRead(test, canon);
    }

    @Test
    public void testWrappedDataOutputChannelAndChannel() throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStreamPlus write = new WrappedDataOutputStreamPlus(bos);
        DataInput canon = testWrite(write);
        DataInput test = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
        testRead(test, canon);
    }

    @Test
    public void testBufferedDataOutputStreamPlusAndChannel() throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStreamPlus write = new BufferedDataOutputStreamPlus(Channels.newChannel(bos));
        DataInput canon = testWrite(write);
        write.close();
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
    public void testDataOutputBufferZeroReallocate() throws IOException
    {
        try (DataOutputBufferSpy write = new DataOutputBufferSpy())
        {
            for (int ii = 0; ii < 1000000; ii++)
            {
                write.superReallocate(0);
            }
        }
    }

    @Test
    public void testDataOutputDirectByteBuffer() throws IOException
    {
        ByteBuffer buf = wrap(new byte[345], true);
        BufferedDataOutputStreamPlus write = new BufferedDataOutputStreamPlus(null, buf.duplicate());
        DataInput canon = testWrite(write);
        DataInput test = new DataInputStream(new ByteArrayInputStream(ByteBufferUtil.getArray(buf)));
        testRead(test, canon);
    }

    @Test
    public void testDataOutputHeapByteBuffer() throws IOException
    {
        ByteBuffer buf = wrap(new byte[345], false);
        BufferedDataOutputStreamPlus write = new BufferedDataOutputStreamPlus(null, buf.duplicate());
        DataInput canon = testWrite(write);
        DataInput test = new DataInputStream(new ByteArrayInputStream(ByteBufferUtil.getArray(buf)));
        testRead(test, canon);
    }

    private static class DataOutputBufferSpy extends DataOutputBuffer
    {
        Deque<Long> sizes = new ArrayDeque<>();

        DataOutputBufferSpy()
        {
            sizes.offer(128L);
        }

        void publicFlush() throws IOException
        {
            //Going to allow it to double instead of specifying a count
            doFlush(1);
        }

        void superReallocate(int count) throws IOException
        {
            super.expandToFit(count);
        }

        @Override
        protected void expandToFit(long count)
        {
            if (count <= 0)
                return;
            Long lastSize = sizes.peekLast();
            long newSize = calculateNewSize(count);
            sizes.offer(newSize);
            if (newSize > DataOutputBuffer.MAX_ARRAY_SIZE)
                throw new RuntimeException();
            if (newSize < 0)
                throw new AssertionError();
            if (lastSize != null && newSize <= lastSize)
                throw new AssertionError();
        }

        @Override
        protected long capacity()
        {
            return sizes.peekLast().intValue();
        }
    }

    //Check for overflow at the max size, without actually allocating all the memory
    @Test
    public void testDataOutputBufferMaxSizeFake() throws IOException
    {
        try (DataOutputBufferSpy write = new DataOutputBufferSpy())
        {
            boolean threw = false;
            try
            {
                while (true)
                    write.publicFlush();
            }
            catch (RuntimeException e) {
                if (e.getClass() == RuntimeException.class)
                    threw = true;
            }
            Assert.assertTrue(threw);
            Assert.assertTrue(write.sizes.peekLast() >= DataOutputBuffer.MAX_ARRAY_SIZE);
        }
    }

    @Test
    public void testDataOutputBufferMaxSize() throws IOException
    {
        //Need a lot of heap to run this test for real.
        //Tested everything else as much as possible since we can't do it all the time
        if (Runtime.getRuntime().maxMemory() < 5033164800L)
            return;

        try (DataOutputBuffer write = new DataOutputBuffer())
        {
            //Doesn't throw up to DataOuptutBuffer.MAX_ARRAY_SIZE which is the array size limit in Java
            for (int ii = 0; ii < DataOutputBuffer.MAX_ARRAY_SIZE / 8; ii++)
                write.writeLong(0);
            write.write(new byte[7]);

            //Should fail due to validation
            checkThrowsRuntimeException(validateReallocationCallable( write, DataOutputBuffer.MAX_ARRAY_SIZE + 1));
            //Check that it does throw
            checkThrowsRuntimeException(new Callable<Object>()
            {
                public Object call() throws Exception
                {
                    write.write(42);
                    return null;
                }
            });
        }
    }

    //Can't test it for real without tons of heap so test as much validation as possible
    @Test
    public void testDataOutputBufferBigReallocation() throws Exception
    {
        //Check saturating cast behavior
        Assert.assertEquals(DataOutputBuffer.MAX_ARRAY_SIZE, DataOutputBuffer.saturatedArraySizeCast(DataOutputBuffer.MAX_ARRAY_SIZE + 1L));
        Assert.assertEquals(DataOutputBuffer.MAX_ARRAY_SIZE, DataOutputBuffer.saturatedArraySizeCast(DataOutputBuffer.MAX_ARRAY_SIZE));
        Assert.assertEquals(DataOutputBuffer.MAX_ARRAY_SIZE - 1, DataOutputBuffer.saturatedArraySizeCast(DataOutputBuffer.MAX_ARRAY_SIZE - 1));
        Assert.assertEquals(0, DataOutputBuffer.saturatedArraySizeCast(0));
        Assert.assertEquals(1, DataOutputBuffer.saturatedArraySizeCast(1));
        checkThrowsIAE(saturatedArraySizeCastCallable(-1));

        //Check checked cast behavior
        checkThrowsIAE(checkedArraySizeCastCallable(DataOutputBuffer.MAX_ARRAY_SIZE + 1L));
        Assert.assertEquals(DataOutputBuffer.MAX_ARRAY_SIZE, DataOutputBuffer.checkedArraySizeCast(DataOutputBuffer.MAX_ARRAY_SIZE));
        Assert.assertEquals(DataOutputBuffer.MAX_ARRAY_SIZE - 1, DataOutputBuffer.checkedArraySizeCast(DataOutputBuffer.MAX_ARRAY_SIZE - 1));
        Assert.assertEquals(0, DataOutputBuffer.checkedArraySizeCast(0));
        Assert.assertEquals(1, DataOutputBuffer.checkedArraySizeCast(1));
        checkThrowsIAE(checkedArraySizeCastCallable(-1));


        try (DataOutputBuffer write = new DataOutputBuffer())
        {
            //Checked validation performed by DOB
            Assert.assertEquals(DataOutputBuffer.MAX_ARRAY_SIZE, write.validateReallocation(DataOutputBuffer.MAX_ARRAY_SIZE + 1L));
            Assert.assertEquals(DataOutputBuffer.MAX_ARRAY_SIZE, write.validateReallocation(DataOutputBuffer.MAX_ARRAY_SIZE));
            Assert.assertEquals(DataOutputBuffer.MAX_ARRAY_SIZE - 1, write.validateReallocation(DataOutputBuffer.MAX_ARRAY_SIZE - 1));
            checkThrowsRuntimeException(validateReallocationCallable( write, 0));
            checkThrowsRuntimeException(validateReallocationCallable( write, 1));
            checkThrowsIAE(validateReallocationCallable( write, -1));
        }
    }

    Callable<Object> saturatedArraySizeCastCallable(final long value)
    {
        return new Callable<Object>()
        {
            @Override
            public Object call() throws Exception
            {
                return DataOutputBuffer.saturatedArraySizeCast(value);
            }
        };
    }

    Callable<Object> checkedArraySizeCastCallable(final long value)
    {
        return new Callable<Object>()
        {
            @Override
            public Object call() throws Exception
            {
                return DataOutputBuffer.checkedArraySizeCast(value);
            }
        };
    }

    Callable<Object> validateReallocationCallable(final DataOutputBuffer write, final long value)
    {
        return new Callable<Object>()
        {
            @Override
            public Object call() throws Exception
            {
                return write.validateReallocation(value);
            }
        };
    }

    private static void checkThrowsIAE(Callable<Object> c)
    {
        checkThrowsException(c, IllegalArgumentException.class);
    }

    private static void checkThrowsRuntimeException(Callable<Object> c)
    {
        checkThrowsException(c, RuntimeException.class);
    }

    private static void checkThrowsException(Callable<Object> c, Class<?> exceptionClass)
    {
        boolean threw = false;
        try
        {
            c.call();
        }
        catch (Throwable t)
        {
            if (t.getClass() == exceptionClass)
                threw = true;
        }
        Assert.assertTrue(threw);
    }

    @Test
    public void testSafeMemoryWriter() throws IOException
    {
        try (SafeMemoryWriter write = new SafeMemoryWriter(10))
        {
            DataInput canon = testWrite(write);
            byte[] bytes = new byte[345];
            write.currentBuffer().getBytes(0, bytes, 0, 345);
            DataInput test = new DataInputStream(new ByteArrayInputStream(bytes));
            testRead(test, canon);
        }
    }

    @Test
    public void testWrappedFileOutputStream() throws IOException
    {
        File file = FileUtils.createTempFile("dataoutput", "test");
        try
        {
            DataOutputStreamPlus write = new WrappedDataOutputStreamPlus(new FileOutputStream(file));
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
    public void testFileOutputStream() throws IOException
    {
        File file = FileUtils.createTempFile("dataoutput", "test");
        try
        {
            DataOutputStreamPlus write = new BufferedDataOutputStreamPlus(new FileOutputStream(file));
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
            @SuppressWarnings("resource")
            final RandomAccessFile raf = new RandomAccessFile(file, "rw");
            DataOutputStreamPlus write = new BufferedDataOutputStreamPlus(raf.getChannel());
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
        SequentialWriterOption option = SequentialWriterOption.newBuilder().bufferSize(32).finishOnClose(true).build();
        final SequentialWriter writer = new SequentialWriter(file, option);
        DataOutputStreamPlus write = new WrappedDataOutputStreamPlus(writer);
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
