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

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.cassandra.utils.Shared;
import org.apache.cassandra.utils.vint.VIntCoding;
import org.apache.cassandra.utils.vint.VIntCoding.VIntOutOfRangeException;

import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

/**
 * Extension to DataInput that provides support for reading varints
 */
@Shared(scope = SIMULATION)
public interface DataInputPlus extends DataInput
{
    /**
     * Read a 64-bit integer back.
     *
     * This method assumes it was originally written using
     * {@link DataOutputPlus#writeVInt(long)} or similar that zigzag encodes the vint.
     */
    default long readVInt() throws IOException
    {
        return VIntCoding.readVInt(this);
    }

    /**
     * Read up to a 32-bit integer back.
     *
     * This method assumes the integer was originally written using
     * {@link DataOutputPlus#writeVInt32(int)} or similar that zigzag encodes the vint.
     *
     * @throws VIntOutOfRangeException If the vint doesn't fit into a 32-bit integer
     */
    default int readVInt32() throws IOException
    {
        return VIntCoding.readVInt32(this);
    }

    /**
     * Think hard before opting for an unsigned encoding. Is this going to bite someone because some day
     * they might need to pass in a sentinel value using negative numbers? Is the risk worth it
     * to save a few bytes?
     *
     * Signed, not a fan of unsigned values in protocols and formats
     */
    default long readUnsignedVInt() throws IOException
    {
        return VIntCoding.readUnsignedVInt(this);
    }

    /**
     * Read up to a 32-bit integer back.
     *
     * This method assumes the original integer was written using {@link DataOutputPlus#writeUnsignedVInt32(int)}
     * or similar that doesn't zigzag encodes the vint.
     *
     * @throws VIntOutOfRangeException If the vint doesn't fit into a 32-bit integer
     */
    default int readUnsignedVInt32() throws IOException
    {
        return VIntCoding.readUnsignedVInt32(this);
    }

    /**
     * Always skips the requested number of bytes, unless EOF is reached
     *
     * @param n number of bytes to skip
     * @return number of bytes skipped
     */
    public int skipBytes(int n) throws IOException;

    public default void skipBytesFully(int n) throws IOException
    {
        int skipped = skipBytes(n);
        if (skipped != n)
            throw new EOFException("EOF after " + skipped + " bytes out of " + n);
    }

    /**
     * Wrapper around an InputStream that provides no buffering but can decode varints
     */
    abstract class DataInputStreamPlus extends InputStream implements DataInputPlus
    {
    }
}
