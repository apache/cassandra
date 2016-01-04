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

    protected EncryptedChecksummedDataInput(Builder builder)
    {
        super(builder);
        cipher = builder.cipher;
        compressor = builder.compressor;
        readChannel = new EncryptionUtils.ChannelProxyReadChannel(channel, builder.position);
        assert cipher != null;
        assert compressor != null;
    }

    /**
     * Since an entire block of compressed data is read off of disk, not just a hint at a time,
     * we don't report EOF until the decompressed data has also been read completely
     */
    public boolean isEOF()
    {
        return readChannel.getCurrentPosition() == channel.size() && buffer.remaining() == 0;
    }

    protected void reBufferStandard()
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

    public static class Builder extends CompressedChecksummedDataInputBuilder
    {
        Cipher cipher;

        public Builder(ChannelProxy channel)
        {
            super(channel);
        }

        public Builder withCipher(Cipher cipher)
        {
            this.cipher = cipher;
            return this;
        }

        public ChecksummedDataInput build()
        {
            assert position >= 0;
            assert compressor != null;
            assert cipher != null;
            return new EncryptedChecksummedDataInput(this);
        }
    }

    public static ChecksummedDataInput upgradeInput(ChecksummedDataInput input, Cipher cipher, ICompressor compressor)
    {
        long position = input.getPosition();
        input.close();

        Builder builder = new Builder(new ChannelProxy(input.getPath()));
        builder.withPosition(position);
        builder.withCompressor(compressor);
        builder.withCipher(cipher);
        return builder.build();
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
