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
package org.apache.cassandra.transport;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffers;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyError;

import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;

public interface FrameCompressor
{
    public Frame compress(Frame frame) throws IOException;
    public Frame decompress(Frame frame) throws IOException;

    /*
     * TODO: We can probably do more efficient, like by avoiding copy.
     * Also, we don't reuse ICompressor because the API doesn't expose enough.
     */
    public static class SnappyCompressor implements FrameCompressor
    {
        public static final SnappyCompressor instance;
        static
        {
            SnappyCompressor i;
            try
            {
                i = new SnappyCompressor();
            }
            catch (Exception e)
            {
                i = null;
            }
            catch (NoClassDefFoundError e)
            {
                i = null;
            }
            catch (SnappyError e)
            {
                i = null;
            }
            catch (UnsatisfiedLinkError e)
            {
                i = null;
            }
            instance = i;
        }

        private SnappyCompressor()
        {
            // this would throw java.lang.NoClassDefFoundError if Snappy class
            // wasn't found at runtime which should be processed by the calling method
            Snappy.getNativeLibraryVersion();
        }

        public Frame compress(Frame frame) throws IOException
        {
            byte[] input = CBUtil.readRawBytes(frame.body);
            byte[] output = new byte[Snappy.maxCompressedLength(input.length)];

            int written = Snappy.compress(input, 0, input.length, output, 0);
            return frame.with(ChannelBuffers.wrappedBuffer(output, 0, written));
        }

        public Frame decompress(Frame frame) throws IOException
        {
            byte[] input = CBUtil.readRawBytes(frame.body);

            if (!Snappy.isValidCompressedBuffer(input, 0, input.length))
                throw new ProtocolException("Provided frame does not appear to be Snappy compressed");

            byte[] output = new byte[Snappy.uncompressedLength(input)];
            int size = Snappy.uncompress(input, 0, input.length, output, 0);
            return frame.with(ChannelBuffers.wrappedBuffer(output, 0, size));
        }
    }

    /*
     * This is very close to the ICompressor implementation, and in particular
     * it also layout the uncompressed size at the beginning of the message to
     * make uncompression faster, but contrarly to the ICompressor, that length
     * is written in big-endian. The native protocol is entirely big-endian, so
     * it feels like putting little-endian here would be a annoying trap for
     * client writer.
     */
    public static class LZ4Compressor implements FrameCompressor
    {
        public static final LZ4Compressor instance = new LZ4Compressor();

        private static final int INTEGER_BYTES = 4;
        private final net.jpountz.lz4.LZ4Compressor compressor;
        private final net.jpountz.lz4.LZ4Decompressor decompressor;

        private LZ4Compressor()
        {
            final LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
            compressor = lz4Factory.fastCompressor();
            decompressor = lz4Factory.decompressor();
        }

        public Frame compress(Frame frame) throws IOException
        {
            byte[] input = CBUtil.readRawBytes(frame.body);

            int maxCompressedLength = compressor.maxCompressedLength(input.length);
            byte[] output = new byte[INTEGER_BYTES + maxCompressedLength];

            output[0] = (byte) (input.length >>> 24);
            output[1] = (byte) (input.length >>> 16);
            output[2] = (byte) (input.length >>>  8);
            output[3] = (byte) (input.length);

            try
            {
                int written = compressor.compress(input, 0, input.length, output, INTEGER_BYTES, maxCompressedLength);
                return frame.with(ChannelBuffers.wrappedBuffer(output, 0, INTEGER_BYTES + written));
            }
            catch (LZ4Exception e)
            {
                throw new IOException(e);
            }
        }

        public Frame decompress(Frame frame) throws IOException
        {
            byte[] input = CBUtil.readRawBytes(frame.body);

            int uncompressedLength = ((input[0] & 0xFF) << 24)
                                   | ((input[1] & 0xFF) << 16)
                                   | ((input[2] & 0xFF) <<  8)
                                   | ((input[3] & 0xFF));

            byte[] output = new byte[uncompressedLength];

            try
            {
                int read = decompressor.decompress(input, INTEGER_BYTES, output, 0, uncompressedLength);
                if (read != input.length - INTEGER_BYTES)
                    throw new IOException("Compressed lengths mismatch");

                return frame.with(ChannelBuffers.wrappedBuffer(output));
            }
            catch (LZ4Exception e)
            {
                throw new IOException(e);
            }
        }
    }
}
