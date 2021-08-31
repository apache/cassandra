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

import org.apache.cassandra.utils.vint.VIntCoding;

/**
 * Extension to DataOutput that provides for writing ByteBuffer and Memory, potentially with an efficient
 * implementation that is zero copy or at least has reduced bounds checking overhead.
 */
public interface DataOutputPlus extends DataOutput
{
    // write the buffer without modifying its position
    void write(ByteBuffer buffer) throws IOException;

    default void write(Memory memory, long offset, long length) throws IOException
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
     * Pad this page with 0s to move on to the next.
     * @throws IOException
     */
    default void padToPageBoundary() throws IOException
    {
        throw new UnsupportedOperationException();
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
