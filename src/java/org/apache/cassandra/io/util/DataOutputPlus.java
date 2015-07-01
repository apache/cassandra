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

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import org.apache.cassandra.utils.vint.VIntCoding;

import com.google.common.base.Function;

/**
 * Extension to DataOutput that provides for writing ByteBuffer and Memory, potentially with an efficient
 * implementation that is zero copy or at least has reduced bounds checking overhead.
 */
public interface DataOutputPlus extends DataOutput
{
    // write the buffer without modifying its position
    void write(ByteBuffer buffer) throws IOException;

    void write(Memory memory, long offset, long length) throws IOException;

    /**
     * Safe way to operate against the underlying channel. Impossible to stash a reference to the channel
     * and forget to flush
     */
    <R> R applyToChannel(Function<WritableByteChannel, R> c) throws IOException;

    default void writeVInt(long i) throws IOException
    {
        VIntCoding.writeVInt(i, this);
    }

    /**
     * Think hard before opting for an unsigned encoding. Is this going to bite someone because some day
     * they might need to pass in a sentinel value using negative numbers? Is the risk worth it
     * to save a few bytes?
     *
     * Signed, not a fan of unsigned values in protocols and formats
     */
    default void writeUnsignedVInt(long i) throws IOException
    {
        VIntCoding.writeUnsignedVInt(i, this);
    }


    public static class ForwardingDataOutput implements DataOutput
    {
        protected final DataOutput out;

        public ForwardingDataOutput(DataOutput out)
        {
            this.out = out;
        }

        public void write(byte[] b) throws IOException
        {
            out.write(b);
        }

        public void write(byte[] b, int off, int len) throws IOException
        {
            out.write(b, off, len);
        }

        public void write(int b) throws IOException
        {
            out.write(b);
        }

        public void writeBoolean(boolean v) throws IOException
        {
            out.writeBoolean(v);
        }

        public void writeByte(int v) throws IOException
        {
            out.writeByte(v);
        }

        public void writeBytes(String s) throws IOException
        {
            out.writeBytes(s);
        }

        public void writeChar(int v) throws IOException
        {
            out.writeChar(v);
        }

        public void writeChars(String s) throws IOException
        {
            out.writeChars(s);
        }

        public void writeDouble(double v) throws IOException
        {
            out.writeDouble(v);
        }

        public void writeFloat(float v) throws IOException
        {
            out.writeFloat(v);
        }

        public void writeInt(int v) throws IOException
        {
            out.writeInt(v);
        }

        public void writeLong(long v) throws IOException
        {
            out.writeLong(v);
        }

        public void writeShort(int v) throws IOException
        {
            out.writeShort(v);
        }

        public void writeUTF(String s) throws IOException
        {
            out.writeUTF(s);
        }

    }

    public static class DataOutputPlusAdapter extends ForwardingDataOutput implements DataOutputPlus
    {

        public DataOutputPlusAdapter(DataOutput out)
        {
            super(out);
        }

        public void write(ByteBuffer buffer) throws IOException
        {
            if (buffer.hasArray())
                out.write(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
            else
                throw new UnsupportedOperationException("IMPLEMENT ME");
        }

        public void write(Memory memory, long offset, long length) throws IOException
        {
            throw new UnsupportedOperationException("IMPLEMENT ME");
        }

        public <R> R applyToChannel(Function<WritableByteChannel, R> c) throws IOException
        {
            throw new UnsupportedOperationException("IMPLEMENT ME");
        }
    }
}
