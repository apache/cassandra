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

import org.apache.cassandra.utils.Shared;
import org.apache.cassandra.utils.vint.VIntCoding;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.google.common.primitives.Ints;

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

    /** @deprecated See CASSANDRA-18099 */
    @Deprecated(since = "5.0")
    default void writeVInt(int i)
    {
        throw new UnsupportedOperationException("Use writeVInt32/readVInt32");
    }

    default void writeVInt32(int i) throws IOException
    {
        VIntCoding.writeVInt32(i, this);
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

    /** @deprecated See CASSANDRA-18099 */
    @Deprecated(since = "5.0")
    default void writeUnsignedVInt(int i)
    {
        throw new UnsupportedOperationException("Use writeUnsignedVInt32/readUnsignedVInt32");
    }

    default void writeUnsignedVInt32(int i) throws IOException
    {
        VIntCoding.writeUnsignedVInt32(i, this);
    }

    /**
     * An efficient way to write the type {@code bytes} of a long
     *
     * @param register - the long value to be written
     * @param bytes - the number of bytes the register occupies. Valid values are between 1 and 8 inclusive.
     * @throws IOException
     */
    default void writeMostSignificantBytes(long register, int bytes) throws IOException
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

    // The methods below support page-aware layout for writing. These would only be implemented if position() is
    // also supported.

    /**
     * Returns the number of bytes that a page can take at maximum.
     */
    default int maxBytesInPage()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Pad this with zeroes until the next page boundary. If the destination position
     * is already at a page boundary, do not do anything.
     */
    default void padToPageBoundary() throws IOException
    {
        long position = position();
        long bytesLeft = PageAware.padded(position) - position;
        write(PageAware.EmptyPage.EMPTY_PAGE, 0, Ints.checkedCast(bytesLeft));
    }

    /**
     * Returns how many bytes are left in the page.
     */
    default int bytesLeftInPage()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the next padded position. This is either the current position (if already padded), or the start of next
     * page.
     */
    default long paddedPosition()
    {
        throw new UnsupportedOperationException();
    }
}