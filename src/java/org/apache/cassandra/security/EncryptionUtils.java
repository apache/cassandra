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
package org.apache.cassandra.security;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.ShortBufferException;

import com.google.common.base.Preconditions;

import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.db.commitlog.EncryptedSegment;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Encryption and decryption functions specific to the commit log.
 * See comments in {@link EncryptedSegment} for details on the binary format.
 * The normal, and expected, invocation pattern is to compress then encrypt the data on the encryption pass,
 * then decrypt and uncompress the data on the decrypt pass.
 */
public class EncryptionUtils
{
    public static final int COMPRESSED_BLOCK_HEADER_SIZE = 4;
    public static final int ENCRYPTED_BLOCK_HEADER_SIZE = 8;

    private static final FastThreadLocal<ByteBuffer> reusableBuffers = new FastThreadLocal<ByteBuffer>()
    {
        protected ByteBuffer initialValue()
        {
            return ByteBuffer.allocate(ENCRYPTED_BLOCK_HEADER_SIZE);
        }
    };

    /**
     * Compress the raw data, as well as manage sizing of the {@code outputBuffer}; if the buffer is not big enough,
     * deallocate current, and allocate a large enough buffer.
     * Write the two header lengths (plain text length, compressed length) to the beginning of the buffer as we want those
     * values encapsulated in the encrypted block, as well.
     *
     * @return the byte buffer that was actaully written to; it may be the {@code outputBuffer} if it had enough capacity,
     * or it may be a new, larger instance. Callers should capture the return buffer (if calling multiple times).
     */
    public static ByteBuffer compress(ByteBuffer inputBuffer, ByteBuffer outputBuffer, boolean allowBufferResize, ICompressor compressor) throws IOException
    {
        int inputLength = inputBuffer.remaining();
        final int compressedLength = compressor.initialCompressedBufferLength(inputLength);
        outputBuffer = ByteBufferUtil.ensureCapacity(outputBuffer, compressedLength + COMPRESSED_BLOCK_HEADER_SIZE, allowBufferResize);

        outputBuffer.putInt(inputLength);
        compressor.compress(inputBuffer, outputBuffer);
        outputBuffer.flip();

        return outputBuffer;
    }

    /**
     * Encrypt the input data, and writes out to the same input buffer; if the buffer is not big enough,
     * deallocate current, and allocate a large enough buffer.
     * Writes the cipher text and headers out to the channel, as well.
     *
     * Note: channel is a parameter as we cannot write header info to the output buffer as we assume the input and output
     * buffers can be the same buffer (and writing the headers to a shared buffer will corrupt any input data). Hence,
     * we write out the headers directly to the channel, and then the cipher text (once encrypted).
     */
    public static ByteBuffer encryptAndWrite(ByteBuffer inputBuffer, WritableByteChannel channel, boolean allowBufferResize, Cipher cipher) throws IOException
    {
        final int plainTextLength = inputBuffer.remaining();
        final int encryptLength = cipher.getOutputSize(plainTextLength);
        ByteBuffer outputBuffer = inputBuffer.duplicate();
        outputBuffer = ByteBufferUtil.ensureCapacity(outputBuffer, encryptLength, allowBufferResize);

        // it's unfortunate that we need to allocate a small buffer here just for the headers, but if we reuse the input buffer
        // for the output, then we would overwrite the first n bytes of the real data with the header data.
        ByteBuffer intBuf = ByteBuffer.allocate(ENCRYPTED_BLOCK_HEADER_SIZE);
        intBuf.putInt(0, encryptLength);
        intBuf.putInt(4, plainTextLength);
        channel.write(intBuf);

        try
        {
            cipher.doFinal(inputBuffer, outputBuffer);
        }
        catch (ShortBufferException | IllegalBlockSizeException | BadPaddingException e)
        {
            throw new IOException("failed to encrypt commit log block", e);
        }

        outputBuffer.position(0).limit(encryptLength);
        channel.write(outputBuffer);
        outputBuffer.position(0).limit(encryptLength);

        return outputBuffer;
    }

    @SuppressWarnings("resource")
    public static ByteBuffer encrypt(ByteBuffer inputBuffer, ByteBuffer outputBuffer, boolean allowBufferResize, Cipher cipher) throws IOException
    {
        Preconditions.checkNotNull(outputBuffer, "output buffer may not be null");
        return encryptAndWrite(inputBuffer, new ChannelAdapter(outputBuffer), allowBufferResize, cipher);
    }

    /**
     * Decrypt the input data, as well as manage sizing of the {@code outputBuffer}; if the buffer is not big enough,
     * deallocate current, and allocate a large enough buffer.
     *
     * @return the byte buffer that was actaully written to; it may be the {@code outputBuffer} if it had enough capacity,
     * or it may be a new, larger instance. Callers should capture the return buffer (if calling multiple times).
     */
    public static ByteBuffer decrypt(ReadableByteChannel channel, ByteBuffer outputBuffer, boolean allowBufferResize, Cipher cipher) throws IOException
    {
        ByteBuffer metadataBuffer = reusableBuffers.get();
        if (metadataBuffer.capacity() < ENCRYPTED_BLOCK_HEADER_SIZE)
        {
            metadataBuffer = ByteBufferUtil.ensureCapacity(metadataBuffer, ENCRYPTED_BLOCK_HEADER_SIZE, true);
            reusableBuffers.set(metadataBuffer);
        }

        metadataBuffer.position(0).limit(ENCRYPTED_BLOCK_HEADER_SIZE);
        channel.read(metadataBuffer);
        if (metadataBuffer.remaining() < ENCRYPTED_BLOCK_HEADER_SIZE)
            throw new IllegalStateException("could not read encrypted blocked metadata header");
        int encryptedLength = metadataBuffer.getInt();
        // this is the length of the compressed data
        int plainTextLength = metadataBuffer.getInt();

        outputBuffer = ByteBufferUtil.ensureCapacity(outputBuffer, Math.max(plainTextLength, encryptedLength), allowBufferResize);
        outputBuffer.position(0).limit(encryptedLength);
        channel.read(outputBuffer);

        ByteBuffer dupe = outputBuffer.duplicate();
        dupe.clear();

        try
        {
            cipher.doFinal(outputBuffer, dupe);
        }
        catch (ShortBufferException | IllegalBlockSizeException | BadPaddingException e)
        {
            throw new IOException("failed to decrypt commit log block", e);
        }

        dupe.position(0).limit(plainTextLength);
        return dupe;
    }

    // path used when decrypting commit log files
    @SuppressWarnings("resource")
    public static ByteBuffer decrypt(FileDataInput fileDataInput, ByteBuffer outputBuffer, boolean allowBufferResize, Cipher cipher) throws IOException
    {
        return decrypt(new DataInputReadChannel(fileDataInput), outputBuffer, allowBufferResize, cipher);
    }

    /**
     * Uncompress the input data, as well as manage sizing of the {@code outputBuffer}; if the buffer is not big enough,
     * deallocate current, and allocate a large enough buffer.
     *
     * @return the byte buffer that was actaully written to; it may be the {@code outputBuffer} if it had enough capacity,
     * or it may be a new, larger instance. Callers should capture the return buffer (if calling multiple times).
     */
    public static ByteBuffer uncompress(ByteBuffer inputBuffer, ByteBuffer outputBuffer, boolean allowBufferResize, ICompressor compressor) throws IOException
    {
        int outputLength = inputBuffer.getInt();
        outputBuffer = ByteBufferUtil.ensureCapacity(outputBuffer, outputLength, allowBufferResize);
        compressor.uncompress(inputBuffer, outputBuffer);
        outputBuffer.position(0).limit(outputLength);

        return outputBuffer;
    }

    public static int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, ICompressor compressor) throws IOException
    {
        int outputLength = readInt(input, inputOffset);
        inputOffset += 4;
        inputLength -= 4;

        if (output.length - outputOffset < outputLength)
        {
            String msg = String.format("buffer to uncompress into is not large enough; buf size = %d, buf offset = %d, target size = %s",
                                       output.length, outputOffset, outputLength);
            throw new IllegalStateException(msg);
        }

        return compressor.uncompress(input, inputOffset, inputLength, output, outputOffset);
    }

    private static int readInt(byte[] input, int inputOffset)
    {
        return  (input[inputOffset + 3] & 0xFF)
                | ((input[inputOffset + 2] & 0xFF) << 8)
                | ((input[inputOffset + 1] & 0xFF) << 16)
                | ((input[inputOffset] & 0xFF) << 24);
    }

    /**
     * A simple {@link java.nio.channels.Channel} adapter for ByteBuffers.
     */
    private static final class ChannelAdapter implements WritableByteChannel
    {
        private final ByteBuffer buffer;

        private ChannelAdapter(ByteBuffer buffer)
        {
            this.buffer = buffer;
        }

        public int write(ByteBuffer src)
        {
            int count = src.remaining();
            buffer.put(src);
            return count;
        }

        public boolean isOpen()
        {
            return true;
        }

        public void close()
        {
            // nop
        }
    }

    private static class DataInputReadChannel implements ReadableByteChannel
    {
        private final FileDataInput fileDataInput;

        private DataInputReadChannel(FileDataInput dataInput)
        {
            this.fileDataInput = dataInput;
        }

        public int read(ByteBuffer dst) throws IOException
        {
            int readLength = dst.remaining();
            // we should only be performing encrypt/decrypt operations with on-heap buffers, so calling BB.array() should be legit here
            fileDataInput.readFully(dst.array(), dst.position(), readLength);
            return readLength;
        }

        public boolean isOpen()
        {
            try
            {
                return fileDataInput.isEOF();
            }
            catch (IOException e)
            {
                return true;
            }
        }

        public void close()
        {
            // nop
        }
    }

    public static class ChannelProxyReadChannel implements ReadableByteChannel
    {
        private final ChannelProxy channelProxy;
        private volatile long currentPosition;

        public ChannelProxyReadChannel(ChannelProxy channelProxy, long currentPosition)
        {
            this.channelProxy = channelProxy;
            this.currentPosition = currentPosition;
        }

        public int read(ByteBuffer dst) throws IOException
        {
            int bytesRead = channelProxy.read(dst, currentPosition);
            dst.flip();
            currentPosition += bytesRead;
            return bytesRead;
        }

        public long getCurrentPosition()
        {
            return currentPosition;
        }

        public boolean isOpen()
        {
            return channelProxy.isCleanedUp();
        }

        public void close()
        {
            // nop
        }

        public void setPosition(long sourcePosition)
        {
            this.currentPosition = sourcePosition;
        }
    }
}
