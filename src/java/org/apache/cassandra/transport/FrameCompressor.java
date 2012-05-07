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
            catch (NoClassDefFoundError e)
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
            byte[] input = new byte[frame.body.readableBytes()];
            byte[] output = new byte[Snappy.maxCompressedLength(input.length)];

            frame.body.readBytes(input);
            int written = Snappy.compress(input, 0, input.length, output, 0);
            return frame.with(ChannelBuffers.wrappedBuffer(output, 0, written));
        }

        public Frame decompress(Frame frame) throws IOException
        {
            byte[] input = new byte[frame.body.readableBytes()];
            frame.body.readBytes(input);

            if (!Snappy.isValidCompressedBuffer(input, 0, input.length))
                throw new ProtocolException("Provided frame does not appear to be Snappy compressed");

            byte[] output = new byte[Snappy.uncompressedLength(input)];
            int size = Snappy.uncompress(input, 0, input.length, output, 0);
            return frame.with(ChannelBuffers.wrappedBuffer(output, 0, size));
        }
    }
}
