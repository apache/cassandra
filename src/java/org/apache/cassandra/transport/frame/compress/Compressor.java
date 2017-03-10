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

/**
 * Analogous to {@link org.apache.cassandra.io.compress.ICompressor}, but different enough that
 * it's worth specializing:
 * <ul>
 *   <li>disk IO is mostly oriented around ByteBuffers, whereas with Frames raw byte arrays are
 *   primarily used </li>
 *   <li>our LZ4 compression format is opionated about the endianness of the preceding length
 *   bytes, big for protocol, little for disk</li>
 *   <li>ICompressor doesn't make it easy to pre-allocate the output buffer/array</li>
 * </ul>
 *
 * In future it may be worth revisiting to unify the interfaces.
 */
public interface Compressor
{
    /**
     * @param length the decompressed length being compressed
     * @return the maximum length output possible for an input of the provided length
     */
    int maxCompressedLength(int length);

    /**
     * @param src the input bytes to be compressed
     * @param srcOffset the offset to start compressing src from
     * @param length the total number of bytes from srcOffset to pass to the compressor implementation
     * @param dest the output buffer to write the compressed bytes to
     * @param destOffset the offset into the dest buffer to start writing the compressed bytes
     * @return the length of resulting compressed bytes written into the dest buffer
     * @throws IOException if the compression implementation failed while compressing the input bytes
     */
    int compress(byte[] src, int srcOffset, int length, byte[] dest, int destOffset) throws IOException;

    /**
     * @param src the compressed bytes to be decompressed
     * @param expectedDecompressedLength the expected length the input bytes will decompress to
     * @return a byte[] containing the resuling decompressed bytes
     * @throws IOException thrown if the compression implementation failed to decompress the provided input bytes
     */
    byte[] decompress(byte[] src, int srcOffset, int length, int expectedDecompressedLength) throws IOException;
}
