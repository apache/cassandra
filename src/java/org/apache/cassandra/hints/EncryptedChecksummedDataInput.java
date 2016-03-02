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
import java.nio.ByteBuffer;
import javax.crypto.Cipher;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.security.EncryptionUtils;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.util.ChannelProxy;

public class EncryptedChecksummedDataInput extends ChecksummedDataInput
{
    private static final ThreadLocal<ByteBuffer> reusableBuffers = new ThreadLocal<ByteBuffer>()
    {
        protected ByteBuffer initialValue()
        {
            return ByteBuffer.allocate(0);
        }
    };

    private final Cipher cipher;
    private final ICompressor compressor;

    private final EncryptionUtils.ChannelProxyReadChannel readChannel;

    protected EncryptedChecksummedDataInput(ChannelProxy channel, Cipher cipher, ICompressor compressor, long filePosition)
    {
        super(channel);
        this.cipher = cipher;
        this.compressor = compressor;
        readChannel = new EncryptionUtils.ChannelProxyReadChannel(channel, filePosition);
        assert cipher != null;
        assert compressor != null;
    }

    /**
     * Since an entire block of compressed data is read off of disk, not just a hint at a time,
     * we don't report EOF until the decompressed data has also been read completely
     */
    public boolean isEOF()
    {
        return getSourcePosition() == channel.size() && buffer.remaining() == 0;
    }

    public long getSourcePosition()
    {
        return readChannel.getCurrentPosition();
    }

    @Override
    protected void readBuffer()
    {
        try
        {
            ByteBuffer byteBuffer = reusableBuffers.get();
            ByteBuffer decrypted = EncryptionUtils.decrypt(readChannel, byteBuffer, true, cipher);
            buffer = EncryptionUtils.uncompress(decrypted, buffer, true, compressor);

            if (decrypted.capacity() > byteBuffer.capacity())
                reusableBuffers.set(decrypted);
        }
        catch (IOException ioe)
        {
            throw new FSReadError(ioe, getPath());
        }
    }

    @SuppressWarnings("resource")
    public static ChecksummedDataInput upgradeInput(ChecksummedDataInput input, Cipher cipher, ICompressor compressor)
    {
        long position = input.getPosition();
        input.close();

        return new EncryptedChecksummedDataInput(new ChannelProxy(input.getPath()), cipher, compressor, position);
    }

    @VisibleForTesting
    Cipher getCipher()
    {
        return cipher;
    }

    @VisibleForTesting
    ICompressor getCompressor()
    {
        return compressor;
    }
}
