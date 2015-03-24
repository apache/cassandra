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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;

import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;

public class LZ4Compressor implements ICompressor
{

    private static final int INTEGER_BYTES = 4;

    @VisibleForTesting
    public static final LZ4Compressor instance = new LZ4Compressor();

    public static LZ4Compressor create(Map<String, String> args)
    {
        return instance;
    }

    private final net.jpountz.lz4.LZ4Compressor compressor;
    private final net.jpountz.lz4.LZ4Decompressor decompressor;

    private LZ4Compressor()
    {
        final LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
        compressor = lz4Factory.fastCompressor();
        decompressor = lz4Factory.decompressor();
    }

    public int initialCompressedBufferLength(int chunkLength)
    {
        return INTEGER_BYTES + compressor.maxCompressedLength(chunkLength);
    }

    public int compress(byte[] input, int inputOffset, int inputLength, WrappedArray output, int outputOffset) throws IOException
    {
        final byte[] dest = output.buffer;
        dest[outputOffset] = (byte) inputLength;
        dest[outputOffset + 1] = (byte) (inputLength >>> 8);
        dest[outputOffset + 2] = (byte) (inputLength >>> 16);
        dest[outputOffset + 3] = (byte) (inputLength >>> 24);
        final int maxCompressedLength = compressor.maxCompressedLength(inputLength);
        try
        {
            return INTEGER_BYTES + compressor.compress(input, inputOffset, inputLength,
                                                       dest, outputOffset + INTEGER_BYTES, maxCompressedLength);
        }
        catch (LZ4Exception e)
        {
            throw new IOException(e);
        }
    }

    public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException
    {
        final int decompressedLength =
                (input[inputOffset] & 0xFF)
                | ((input[inputOffset + 1] & 0xFF) << 8)
                | ((input[inputOffset + 2] & 0xFF) << 16)
                | ((input[inputOffset + 3] & 0xFF) << 24);
        final int compressedLength;
        try
        {
            compressedLength = decompressor.decompress(input, inputOffset + INTEGER_BYTES,
                                                       output, outputOffset, decompressedLength);
        }
        catch (LZ4Exception e)
        {
            throw new IOException(e);
        }

        if (compressedLength != inputLength - INTEGER_BYTES)
        {
            throw new IOException("Compressed lengths mismatch");
        }

        return decompressedLength;
    }

    public Set<String> supportedOptions()
    {
        return new HashSet<String>(Arrays.asList(CompressionParameters.CRC_CHECK_CHANCE));
    }
}
