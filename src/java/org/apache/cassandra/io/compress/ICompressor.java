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
package org.apache.cassandra.io.compress;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;

public interface ICompressor
{
    public int initialCompressedBufferLength(int chunkLength);

    public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException;

    /**
     * Compression for ByteBuffers
     */
    public int compress(ByteBuffer input, WrappedByteBuffer output) throws IOException;

    /**
     * Decompression for DirectByteBuffers
     */
    public int uncompress(ByteBuffer input, ByteBuffer output) throws IOException;

    /**
     * Notifies user if this compressor will wants/requires a direct byte buffers to
     * decompress direct byteBuffers
     */
    public boolean useDirectOutputByteBuffers();

    public Set<String> supportedOptions();

    /**
     * A simple wrapped Bytebuffer.
     * Not all implementations allow us to know the maximum size after
     * compression. This makes it hard to size the output buffer for compression
     * (and we want to reuse the buffer).  Instead we use this wrapped ByteBuffer
     * so that compress(...) can have the liberty to resize the underlying array if
     * necessary.
     */
    public static class WrappedByteBuffer
    {
        public ByteBuffer buffer;

        public WrappedByteBuffer(ByteBuffer buffer)
        {
            this.buffer = buffer;
        }
    }
}
