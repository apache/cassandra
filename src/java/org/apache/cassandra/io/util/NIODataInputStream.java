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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;

import com.google.common.base.Preconditions;

/**
 * Rough equivalent of BufferedInputStream and DataInputStream wrapping the input stream of a File or Socket
 * Created to work around the fact that when BIS + DIS delegate to NIO for socket IO they will allocate large
 * thread local direct byte buffers when a large array is used to read.
 *
 * There may also be some performance improvement due to using a DBB as the underlying buffer for IO and the removal
 * of some indirection and delegation when it comes to reading out individual values, but that is not the goal.
 *
 * Closing NIODataInputStream will invoke close on the ReadableByteChannel provided at construction.
 *
 * NIODataInputStream is not thread safe.
 */
public class NIODataInputStream extends RebufferingInputStream
{
    protected final ReadableByteChannel channel;

    private static ByteBuffer makeBuffer(int bufferSize)
    {
        ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize);
        buffer.position(0);
        buffer.limit(0);

        return buffer;
    }

    public NIODataInputStream(ReadableByteChannel channel, int bufferSize)
    {
        super(makeBuffer(bufferSize));

        Preconditions.checkNotNull(channel);
        this.channel = channel;
    }

    @Override
    protected void reBuffer() throws IOException
    {
        Preconditions.checkState(buffer.remaining() == 0);
        buffer.clear();

        while ((channel.read(buffer)) == 0) {}

        buffer.flip();
    }

    @Override
    public void close() throws IOException
    {
        channel.close();
        super.close();
        FileUtils.clean(buffer);
        buffer = null;
    }

    @Override
    public int available() throws IOException
    {
        if (channel instanceof SeekableByteChannel)
        {
            SeekableByteChannel sbc = (SeekableByteChannel) channel;
            long remainder = Math.max(0, sbc.size() - sbc.position());
            return (remainder > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int)(remainder + buffer.remaining());
        }
        return buffer.remaining();
    }
}
