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
package org.apache.cassandra.hints;

import java.io.IOException;
import java.util.zip.CRC32;

import org.apache.cassandra.io.util.AbstractDataInput;

/**
 * An {@link AbstractDataInput} wrapper that calctulates the CRC in place.
 *
 * Useful for {@link org.apache.cassandra.hints.HintsReader}, for example, where we must verify the CRC, yet don't want
 * to allocate an extra byte array just that purpose.
 *
 * In addition to calculating the CRC, allows to enforce a maximim known size. This is needed
 * so that {@link org.apache.cassandra.db.Mutation.MutationSerializer} doesn't blow up the heap when deserializing a
 * corrupted sequence by reading a huge corrupted length of bytes via
 * via {@link org.apache.cassandra.utils.ByteBufferUtil#readWithLength(java.io.DataInput)}.
 */
public final class ChecksummedDataInput extends AbstractDataInput
{
    private final CRC32 crc;
    private final AbstractDataInput source;
    private int limit;

    private ChecksummedDataInput(AbstractDataInput source)
    {
        this.source = source;

        crc = new CRC32();
        limit = Integer.MAX_VALUE;
    }

    public static ChecksummedDataInput wrap(AbstractDataInput source)
    {
        return new ChecksummedDataInput(source);
    }

    public void resetCrc()
    {
        crc.reset();
    }

    public void resetLimit()
    {
        limit = Integer.MAX_VALUE;
    }

    public void limit(int newLimit)
    {
        limit = newLimit;
    }

    public int bytesRemaining()
    {
        return limit;
    }

    public int getCrc()
    {
        return (int) crc.getValue();
    }

    public void seek(long position) throws IOException
    {
        source.seek(position);
    }

    public long getPosition()
    {
        return source.getPosition();
    }

    public long getPositionLimit()
    {
        return source.getPositionLimit();
    }

    public int read() throws IOException
    {
        int b = source.read();
        crc.update(b);
        limit--;
        return b;
    }

    @Override
    public int read(byte[] buff, int offset, int length) throws IOException
    {
        if (length > limit)
            throw new IOException("Digest mismatch exception");

        int copied = source.read(buff, offset, length);
        crc.update(buff, offset, copied);
        limit -= copied;
        return copied;
    }
}
