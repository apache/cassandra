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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Abstract base class for DataOutputStreams that accept writes from ByteBuffer or Memory and also provide
 * access to the underlying WritableByteChannel associated with their output stream.
 *
 * If no channel is provided by derived classes then a wrapper channel is provided.
 */
public abstract class DataOutputStreamPlus extends OutputStream implements DataOutputPlus
{
    //Dummy wrapper channel for derived implementations that don't have a channel
    protected final WritableByteChannel channel;

    protected DataOutputStreamPlus()
    {
        this.channel = newDefaultChannel();
    }

    protected DataOutputStreamPlus(WritableByteChannel channel)
    {
        this.channel = channel;
    }

    private static int MAX_BUFFER_SIZE =
            Integer.getInteger(Config.PROPERTY_PREFIX + "data_output_stream_plus_temp_buffer_size", 8192);

    /*
     * Factored out into separate method to create more flexibility around inlining
     */
    protected static byte[] retrieveTemporaryBuffer(int minSize)
    {
        byte[] bytes = tempBuffer.get();
        if (bytes.length < Math.min(minSize, MAX_BUFFER_SIZE))
        {
            // increase in powers of 2, to avoid wasted repeat allocations
            bytes = new byte[Math.min(MAX_BUFFER_SIZE, 2 * Integer.highestOneBit(minSize))];
            tempBuffer.set(bytes);
        }
        return bytes;
    }

    private static final FastThreadLocal<byte[]> tempBuffer = new FastThreadLocal<byte[]>()
    {
        @Override
        public byte[] initialValue()
        {
            return new byte[16];
        }
    };

    // Derived classes can override and *construct* a real channel, if it is not possible to provide one to the constructor
    protected WritableByteChannel newDefaultChannel()
    {
        return new WritableByteChannel()
        {

            @Override
            public boolean isOpen()
            {
                return true;
            }

            @Override
            public void close()
            {
            }

            @Override
            public int write(ByteBuffer src) throws IOException
            {
                int toWrite = src.remaining();

                if (src.hasArray())
                {
                    DataOutputStreamPlus.this.write(src.array(), src.arrayOffset() + src.position(), src.remaining());
                    src.position(src.limit());
                    return toWrite;
                }

                if (toWrite < 16)
                {
                    int offset = src.position();
                    for (int i = 0 ; i < toWrite ; i++)
                        DataOutputStreamPlus.this.write(src.get(i + offset));
                    src.position(src.limit());
                    return toWrite;
                }

                byte[] buf = retrieveTemporaryBuffer(toWrite);

                int totalWritten = 0;
                while (totalWritten < toWrite)
                {
                    int toWriteThisTime = Math.min(buf.length, toWrite - totalWritten);

                    ByteBufferUtil.arrayCopy(src, src.position() + totalWritten, buf, 0, toWriteThisTime);

                    DataOutputStreamPlus.this.write(buf, 0, toWriteThisTime);

                    totalWritten += toWriteThisTime;
                }

                src.position(src.limit());
                return totalWritten;
            }

        };
    }

}
