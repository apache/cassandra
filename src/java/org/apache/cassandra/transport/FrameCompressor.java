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

import io.netty.buffer.ByteBuf;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyError;

import net.jpountz.lz4.LZ4Factory;

import org.apache.cassandra.utils.JVMStabilityInspector;

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
                JVMStabilityInspector.inspectThrowable(e);
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
            ByteBuf output = CBUtil.allocator.heapBuffer(Snappy.maxCompressedLength(input.length));

            try
            {
                int written = Snappy.compress(input, 0, input.length, output.array(), output.arrayOffset());
                output.writerIndex(written);
            }
            catch (final Throwable e)
            {
                output.release();
                throw e;
            }
            finally
            {
                //release the old frame
                frame.release();
            }

            return frame.with(output);
        }

        public Frame decompress(Frame frame) throws IOException
        {
            byte[] input = CBUtil.readRawBytes(frame.body);

            if (!Snappy.isValidCompressedBuffer(input, 0, input.length))
                throw new ProtocolException("Provided frame does not appear to be Snappy compressed");

            ByteBuf output = CBUtil.allocator.heapBuffer(Snappy.uncompressedLength(input));

            try
            {
                int size = Snappy.uncompress(input, 0, input.length, output.array(), output.arrayOffset());
                output.writerIndex(size);
            }
            catch (final Throwable e)
            {
                output.release();
                throw e;
            }
            finally
            {
                //release the old frame
                frame.release();
            }

            return frame.with(output);
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
            ByteBuf outputBuf = CBUtil.allocator.heapBuffer(INTEGER_BYTES + maxCompressedLength);

            byte[] output = outputBuf.array();
            int outputOffset = outputBuf.arrayOffset();

            output[outputOffset + 0] = (byte) (input.length >>> 24);
            output[outputOffset + 1] = (byte) (input.length >>> 16);
            output[outputOffset + 2] = (byte) (input.length >>>  8);
            output[outputOffset + 3] = (byte) (input.length);

            try
            {
                int written = compressor.compress(input, 0, input.length, output, outputOffset + INTEGER_BYTES, maxCompressedLength);
                outputBuf.writerIndex(INTEGER_BYTES + written);

                return frame.with(outputBuf);
            }
            catch (final Throwable e)
            {
                outputBuf.release();
                throw e;
            }
            finally
            {
                //release the old frame
                frame.release();
            }
        }

        public Frame decompress(Frame frame) throws IOException
        {
            byte[] input = CBUtil.readRawBytes(frame.body);

            int uncompressedLength = ((input[0] & 0xFF) << 24)
                                   | ((input[1] & 0xFF) << 16)
                                   | ((input[2] & 0xFF) <<  8)
                                   | ((input[3] & 0xFF));

            ByteBuf output = CBUtil.allocator.heapBuffer(uncompressedLength);

            try
            {
                int read = decompressor.decompress(input, INTEGER_BYTES, output.array(), output.arrayOffset(), uncompressedLength);
                if (read != input.length - INTEGER_BYTES)
                    throw new IOException("Compressed lengths mismatch");

                output.writerIndex(uncompressedLength);

                return frame.with(output);
            }
            catch (final Throwable e)
            {
                output.release();
                throw e;
            }
            finally
            {
                //release the old frame
                frame.release();
            }
        }
    }
}
