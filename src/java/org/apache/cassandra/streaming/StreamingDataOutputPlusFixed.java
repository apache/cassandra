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

package org.apache.cassandra.streaming;

import java.io.EOFException;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.cassandra.io.util.DataOutputBufferFixed;

public class StreamingDataOutputPlusFixed extends DataOutputBufferFixed implements StreamingDataOutputPlus
{
    public StreamingDataOutputPlusFixed(ByteBuffer buffer)
    {
        super(buffer);
    }

    @Override
    public int writeToChannel(Write write, RateLimiter limiter) throws IOException
    {
        int position = buffer.position();
        write.write(size -> buffer);
        return buffer.position() - position;
    }

    @Override
    public long writeFileToChannel(FileChannel file, RateLimiter limiter) throws IOException
    {
        return writeFileToChannel(file, limiter, 0L, file.size());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Bounded by the fixed buffer: a range longer than what is left of it throws {@link BufferOverflowException},
     * as any other overflow of a {@link DataOutputBufferFixed} does, rather than writing a prefix of the range.
     */
    @Override
    public long writeFileToChannel(FileChannel file, RateLimiter limiter, long position, long length) throws IOException
    {
        try
        {
            if (length > buffer.remaining())
                throw new BufferOverflowException();

            long count = 0;
            while (count < length)
            {
                // Only ever read up to the end of the requested range: the file is longer than the range for a
                // partial zero-copy stream, and reading past it would corrupt whatever is sent next.
                int savedLimit = buffer.limit();
                buffer.limit(buffer.position() + (int) (length - count));
                long read;
                try
                {
                    read = file.read(buffer, position + count);
                }
                finally
                {
                    buffer.limit(savedLimit);
                }
                // There is always room left for the rest of the range, so this can only be end of file: the range
                // is not all there, and the peer is expecting all of it.
                if (read < 0)
                    throw new EOFException(String.format("Reached end of file at position %d with %d of the %d bytes " +
                                                         "of the range left to write",
                                                         position + count, length - count, length));
                count += read;
            }
            return count;
        }
        finally
        {
            file.close();
        }
    }
}
