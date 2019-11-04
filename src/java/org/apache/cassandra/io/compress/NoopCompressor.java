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
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * A Compressor which doesn't actually compress any data. This is useful for either non-compressible data
 * (typically already compressed or encrypted) that you still want block checksums for or for fast writing.
 * Some relevant tickets:
 * <p>
 *     <ul>
 *         <li>CASSANDRA-12682: Non compressed SSTables can silently corrupt data</li>
 *         <li>CASSANDRA-9264: Non compressed SSTables are written without checksums</li>
 *     </ul>
 * </p>
 */
public class NoopCompressor implements ICompressor
{
    public static NoopCompressor create(Map<String, String> ignored)
    {
        return new NoopCompressor();
    }

    public int initialCompressedBufferLength(int chunkLength)
    {
        return chunkLength;
    }

    public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException
    {
        System.arraycopy(input, inputOffset, output, outputOffset, inputLength);
        return inputLength;
    }

    public void compress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        output.put(input);
    }

    public void uncompress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        output.put(input);
    }

    public BufferType preferredBufferType()
    {
        return BufferType.ON_HEAP;
    }

    public boolean supports(BufferType bufferType)
    {
        return true;
    }

    public Set<String> supportedOptions()
    {
        return Collections.emptySet();
    }
}
