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

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;

public class LZ4Compressor implements Compressor
{
    public static final LZ4Compressor INSTANCE = new LZ4Compressor();

    private final net.jpountz.lz4.LZ4Compressor compressor;
    private final LZ4SafeDecompressor decompressor;

    private LZ4Compressor()
    {
        final LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
        compressor = lz4Factory.fastCompressor();
        decompressor = lz4Factory.safeDecompressor();
    }

    public int maxCompressedLength(int length)
    {
        return compressor.maxCompressedLength(length);
    }

    public int compress(byte[] src, int srcOffset, int length, byte[] dest, int destOffset) throws IOException
    {
        try
        {
            return compressor.compress(src, srcOffset, length, dest, destOffset);
        }
        catch (Throwable t)
        {
            throw new IOException("Error caught during LZ4 compression", t);
        }
    }

    public byte[] decompress(byte[] src, int offset, int length, int expectedDecompressedLength) throws IOException
    {
        try
        {
            byte[] decompressed = new byte[expectedDecompressedLength];
            decompressor.decompress(src, offset, length, decompressed, 0, expectedDecompressedLength);
            return decompressed;
        }
        catch (Throwable t)
        {
            throw new IOException("Error caught during LZ4 decompression", t);
        }
    }
}
