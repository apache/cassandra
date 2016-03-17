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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.io.util.RandomAccessReader;

/**
 * A {@link RandomAccessReader} wrapper that calctulates the CRC in place.
 *
 * Useful for {@link org.apache.cassandra.hints.HintsReader}, for example, where we must verify the CRC, yet don't want
 * to allocate an extra byte array just that purpose. The CRC can be embedded in the input stream and checked via checkCrc().
 *
 * In addition to calculating the CRC, it allows to enforce a maximim known size. This is needed
 * so that {@link org.apache.cassandra.db.Mutation.MutationSerializer} doesn't blow up the heap when deserializing a
 * corrupted sequence by reading a huge corrupted length of bytes via
 * via {@link org.apache.cassandra.utils.ByteBufferUtil#readWithLength(java.io.DataInput)}.
 */
public class ChecksummedDataInput extends RandomAccessReader.RandomAccessReaderWithOwnChannel
{
    private final CRC32 crc;
    private int crcPosition;
    private boolean crcUpdateDisabled;

    private long limit;
    private DataPosition limitMark;

    protected ChecksummedDataInput(Builder builder)
    {
        super(builder);

        crc = new CRC32();
        crcPosition = 0;
        crcUpdateDisabled = false;

        resetLimit();
    }

    @SuppressWarnings("resource")   // channel owned by RandomAccessReaderWithOwnChannel
    public static ChecksummedDataInput open(File file)
    {
        return new Builder(new ChannelProxy(file)).build();
    }

    protected void releaseBuffer()
    {
        super.releaseBuffer();
    }

    public void resetCrc()
    {
        crc.reset();
        crcPosition = buffer.position();
    }

    public void limit(long newLimit)
    {
        limit = newLimit;
        limitMark = mark();
    }

    public void resetLimit()
    {
        limit = Long.MAX_VALUE;
        limitMark = null;
    }

    public void checkLimit(int length) throws IOException
    {
        if (limitMark == null)
            return;

        if ((bytesPastLimit() + length) > limit)
            throw new IOException("Digest mismatch exception");
    }

    public long bytesPastLimit()
    {
        assert limitMark != null;
        return bytesPastMark(limitMark);
    }

    public boolean checkCrc() throws IOException
    {
        try
        {
            updateCrc();

            // we must diable crc updates in case we rebuffer
            // when called source.readInt()
            crcUpdateDisabled = true;
            return ((int) crc.getValue()) == readInt();
        }
        finally
        {
            crcPosition = buffer.position();
            crcUpdateDisabled = false;
        }
    }

    @Override
    public void readFully(byte[] b) throws IOException
    {
        checkLimit(b.length);
        super.readFully(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException
    {
        checkLimit(len);
        return super.read(b, off, len);
    }

    @Override
    public void reBuffer()
    {
        updateCrc();
        super.reBuffer();
        crcPosition = buffer.position();
    }

    private void updateCrc()
    {
        if (crcPosition == buffer.position() || crcUpdateDisabled)
            return;

        assert crcPosition >= 0 && crcPosition < buffer.position();

        ByteBuffer unprocessed = buffer.duplicate();
        unprocessed.position(crcPosition)
                   .limit(buffer.position());

        crc.update(unprocessed);
    }

    public static class Builder extends RandomAccessReader.Builder
    {
        public Builder(ChannelProxy channel)
        {
            super(channel);
        }

        public ChecksummedDataInput build()
        {
            return new ChecksummedDataInput(this);
        }
    }
}
