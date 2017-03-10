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

package org.apache.cassandra.transport.frame.compress;

import java.io.IOException;
import java.util.EnumSet;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Frame;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.transport.frame.FrameBodyTransformer;

public abstract class CompressingTransformer implements FrameBodyTransformer
{
    private static final CompressingTransformer LZ4 = new LZ4();
    private static final CompressingTransformer SNAPPY = new Snappy();

    private static final EnumSet<Frame.Header.Flag> headerFlags = EnumSet.of(Frame.Header.Flag.COMPRESSED);

    public static final CompressingTransformer getTransformer(Compressor compressor)
    {
        if (compressor instanceof LZ4Compressor)
            return LZ4;

        if (compressor instanceof SnappyCompressor)
        {
            if (SnappyCompressor.INSTANCE == null)
                throw new ProtocolException("This instance does not support Snappy compression");

            return SNAPPY;
        }

        throw new ProtocolException("Unsupported compression implementation: " + compressor.getClass().getCanonicalName());
    }

    CompressingTransformer() {}

    public EnumSet<Frame.Header.Flag> getOutboundHeaderFlags()
    {
        return headerFlags;
    }

    public ByteBuf transformInbound(ByteBuf inputBuf, EnumSet<Frame.Header.Flag> flags) throws IOException
    {
        return transformInbound(inputBuf);
    }

    abstract ByteBuf transformInbound(ByteBuf inputBuf) throws IOException;

    // Simple LZ4 encoding prefixes the compressed bytes with the
    // length of the uncompressed bytes. This length is explicitly big-endian
    // as the native protocol is entirely big-endian, so it feels like putting
    // little-endian here would be a annoying trap for client writer
    private static class LZ4 extends CompressingTransformer
    {
        public ByteBuf transformOutbound(ByteBuf inputBuf) throws IOException
        {
            byte[] input = CBUtil.readRawBytes(inputBuf);
            int maxCompressedLength = LZ4Compressor.INSTANCE.maxCompressedLength(input.length);
            ByteBuf outputBuf = CBUtil.allocator.heapBuffer(Integer.BYTES + maxCompressedLength);
            byte[] output = outputBuf.array();
            int outputOffset = outputBuf.arrayOffset();
            output[outputOffset]     = (byte) (input.length >>> 24);
            output[outputOffset + 1] = (byte) (input.length >>> 16);
            output[outputOffset + 2] = (byte) (input.length >>>  8);
            output[outputOffset + 3] = (byte) (input.length);
            try
            {
                int written = LZ4Compressor.INSTANCE.compress(input, 0, input.length, output, Integer.BYTES + outputOffset);
                outputBuf.writerIndex(Integer.BYTES + written);
                return outputBuf;
            }
            catch (IOException e)
            {
                outputBuf.release();
                throw e;
            }
        }

        ByteBuf transformInbound(ByteBuf inputBuf) throws IOException
        {
            byte[] input = CBUtil.readRawBytes(inputBuf);
            int uncompressedLength = ((input[0] & 0xFF) << 24)
                                   | ((input[1] & 0xFF) << 16)
                                   | ((input[2] & 0xFF) << 8)
                                   | ((input[3] & 0xFF));
            ByteBuf outputBuf = CBUtil.allocator.heapBuffer(uncompressedLength);
            try
            {
                outputBuf.writeBytes(LZ4Compressor.INSTANCE.decompress(input,
                                                                       Integer.BYTES,
                                                                       input.length - Integer.BYTES,
                                                                       uncompressedLength));
                return outputBuf;
            }
            catch (IOException e)
            {
                outputBuf.release();
                throw e;
            }
        }
    }

    // Simple Snappy encoding simply writes the compressed bytes, without the preceding length
    private static class Snappy extends CompressingTransformer
    {
        public ByteBuf transformOutbound(ByteBuf inputBuf) throws IOException
        {
            byte[] input = CBUtil.readRawBytes(inputBuf);
            int uncompressedLength = input.length;
            int maxCompressedLength = SnappyCompressor.INSTANCE.maxCompressedLength(uncompressedLength);
            ByteBuf outputBuf = CBUtil.allocator.heapBuffer(maxCompressedLength);
            try
            {
                int written = SnappyCompressor.INSTANCE.compress(input,
                                                                 0,
                                                                 uncompressedLength,
                                                                 outputBuf.array(),
                                                                 outputBuf.arrayOffset());
                outputBuf.writerIndex(written);
                return outputBuf;
            }
            catch (IOException e)
            {
                outputBuf.release();
                throw e;
            }
        }

        ByteBuf transformInbound(ByteBuf inputBuf) throws IOException
        {
            byte[] input = CBUtil.readRawBytes(inputBuf);
            int uncompressedLength = org.xerial.snappy.Snappy.uncompressedLength(input);
            ByteBuf outputBuf = CBUtil.allocator.heapBuffer(uncompressedLength);
            try
            {
                outputBuf.writeBytes(SnappyCompressor.INSTANCE.decompress(input, 0, input.length, uncompressedLength));
                return outputBuf;
            }
            catch (IOException e)
            {
                outputBuf.release();
                throw e;
            }
        }
    }
}
