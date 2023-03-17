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

import org.apache.cassandra.utils.Shared;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

/**
 * Extension to DataOutput that provides for writing ByteBuffer and Memory, potentially with an efficient
 * implementation that is zero copy or at least has reduced bounds checking overhead.
 */
@Shared(scope = SIMULATION)
public interface DataOutputPlus extends DataOutput
{
    // write the buffer without modifying its position
    void write(ByteBuffer buffer) throws IOException;

    default void write(ReadableMemory memory, long offset, long length) throws IOException
    {
        for (ByteBuffer buffer : memory.asByteBuffers(offset, length))
            write(buffer);
    }

    default void writeVInt(long i) throws IOException
    {
        VIntCoding.writeVInt(i, this);
    }

    /**
     * This is more efficient for storing unsigned values, both in storage and CPU burden.
     *
     * Note that it is still possible to store negative values, they just take up more space.
     * So this method doesn't forbid e.g. negative sentinel values in future, if they need to be snuck in.
     * A protocol version bump can then be introduced to improve efficiency.
     */
    default void writeUnsignedVInt(long i) throws IOException
    {
        VIntCoding.writeUnsignedVInt(i, this);
    }

    /**
     * An efficient way to write the type {@code bytes} of a long
     *
     * @param register - the long value to be written
     * @param bytes - the number of bytes the register occupies. Valid values are between 1 and 8 inclusive.
     * @throws IOException
     */
    default void writeBytes(long register, int bytes) throws IOException
    {
        switch (bytes)
        {
            case 0:
                break;
            case 1:
                writeByte((int)(register >>> 56));
                break;
            case 2:
                writeShort((int)(register >> 48));
                break;
            case 3:
                writeShort((int)(register >> 48));
                writeByte((int)(register >> 40));
                break;
            case 4:
                writeInt((int)(register >> 32));
                break;
            case 5:
                writeInt((int)(register >> 32));
                writeByte((int)(register >> 24));
                break;
            case 6:
                writeInt((int)(register >> 32));
                writeShort((int)(register >> 16));
                break;
            case 7:
                writeInt((int)(register >> 32));
                writeShort((int)(register >> 16));
                writeByte((int)(register >> 8));
                break;
            case 8:
                writeLong(register);
                break;
            default:
                throw new IllegalArgumentException();
        }

    }

    /**
     * Returns the current position of the underlying target like a file-pointer
     * or the position withing a buffer. Not every implementation may support this
     * functionality. Whether or not this functionality is supported can be checked
     * via the {@link #hasPosition()}.
     *
     * @throws UnsupportedOperationException if the implementation does not support
     *                                       position
     */
    default long position()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * If the implementation supports providing a position, this method returns
     * {@code true}, otherwise {@code false}.
     */
    default boolean hasPosition()
    {
        return false;
    }
}
