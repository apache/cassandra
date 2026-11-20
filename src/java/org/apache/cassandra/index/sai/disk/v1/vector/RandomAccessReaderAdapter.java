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

package org.apache.cassandra.index.sai.disk.v1.vector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;

import com.google.common.primitives.Ints;

import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.Rebufferer.BufferHolder;

import io.github.jbellis.jvector.disk.ReaderSupplier;

public class RandomAccessReaderAdapter extends RandomAccessReader implements io.github.jbellis.jvector.disk.RandomAccessReader
{
    static ReaderSupplier createSupplier(FileHandle fileHandle)
    {
        return () -> new RandomAccessReaderAdapter(fileHandle);
    }

    RandomAccessReaderAdapter(FileHandle fileHandle)
    {
        super(fileHandle.instantiateRebufferer(null));
    }

    /**
     * Read floats into a float[], starting at the current position.
     *
     * @param dest the array to read into
     * @param offset the offset in the array at which to start writing floats
     * @param count the number of floats to read
     *
     * Will change the buffer position.
     */
    @Override
    public void read(float[] dest, int offset, int count) throws IOException
    {
        if (count == 0)
            return;

        BufferHolder bh = getBufferHolder();
        long position = getPosition();

        FloatBuffer floatBuffer;
        if (getBufferHolderOffset() == 0 && position % Float.BYTES == 0)
        {
            // this is a separate code path because buffer() and asFloatBuffer() both allocate
            // new and relatively expensive xBuffer objects, so we want to avoid doing that
            // twice, where possible
            floatBuffer = bh.floatBuffer();
            floatBuffer.position(Ints.checkedCast(position / Float.BYTES));
        }
        else
        {
            // offset is non-zero, and probably not aligned to Float.BYTES, so
            // set the position before converting to FloatBuffer.
            ByteBuffer bb = bh.buffer();
            bb.position(Ints.checkedCast(position - getBufferHolderOffset()));
            floatBuffer = bb.asFloatBuffer();
        }

        if (count > floatBuffer.remaining())
        {
            // slow path -- desired slice is across region boundaries
            ByteBuffer bb = ByteBuffer.allocate(Float.BYTES * count);
            readFully(bb);
            floatBuffer = bb.asFloatBuffer();
        }

        floatBuffer.get(dest, offset, count);
        seek(position + (long) Float.BYTES * count);
    }

    @Override
    public void readFully(long[] dest) throws IOException
    {
        for (int i = 0; i < dest.length; i++)
            dest[i] = readLong();
    }

    /**
     * Read ints into an int[], starting at the current position.
     *
     * @param dest the array to read into
     * @param offset the offset in the array at which to start writing ints
     * @param count the number of ints to read
     *
     * Will change the buffer position.
     */
    @Override
    public void read(int[] dest, int offset, int count) throws IOException
    {
        if (count == 0)
            return;

        BufferHolder bh = getBufferHolder();
        long position = getPosition();

        IntBuffer intBuffer;
        if (getBufferHolderOffset() == 0 && position % Integer.BYTES == 0)
        {
            // this is a separate code path because buffer() and asIntBuffer() both allocate
            // new and relatively expensive xBuffer objects, so we want to avoid doing that
            // twice, where possible
            intBuffer = bh.intBuffer();
            intBuffer.position(Ints.checkedCast(position / Integer.BYTES));
        }
        else
        {
            // offset is non-zero, and probably not aligned to Integer.BYTES, so
            // set the position before converting to IntBuffer.
            ByteBuffer bb = bh.buffer();
            bb.position(Ints.checkedCast(position - getBufferHolderOffset()));
            intBuffer = bb.asIntBuffer();
        }

        if (count > intBuffer.remaining())
        {
            // slow path -- desired slice is across region boundaries
            ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES * count);
            readFully(bb);
            intBuffer = bb.asIntBuffer();
        }

        intBuffer.get(dest, offset, count);
        seek(position + (long) Integer.BYTES * count);
    }
}
