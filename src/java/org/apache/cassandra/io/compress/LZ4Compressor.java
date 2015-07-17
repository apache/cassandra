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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import org.apache.cassandra.schema.CompressionParams;

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
    private final net.jpountz.lz4.LZ4FastDecompressor decompressor;

    private LZ4Compressor()
    {
        final LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
        compressor = lz4Factory.fastCompressor();
        decompressor = lz4Factory.fastDecompressor();
    }

    public int initialCompressedBufferLength(int chunkLength)
    {
        return INTEGER_BYTES + compressor.maxCompressedLength(chunkLength);
    }

    public void compress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        int len = input.remaining();
        output.put((byte) len);
        output.put((byte) (len >>> 8));
        output.put((byte) (len >>> 16));
        output.put((byte) (len >>> 24));

        try
        {
            compressor.compress(input, output);
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

    public void uncompress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        final int decompressedLength = (input.get() & 0xFF)
                | ((input.get() & 0xFF) << 8)
                | ((input.get() & 0xFF) << 16)
                | ((input.get() & 0xFF) << 24);

        try
        {
            int compressedLength = decompressor.decompress(input, input.position(), output, output.position(), decompressedLength);
            input.position(input.position() + compressedLength);
            output.position(output.position() + decompressedLength);
        }
        catch (LZ4Exception e)
        {
            throw new IOException(e);
        }

        if (input.remaining() > 0)
        {
            throw new IOException("Compressed lengths mismatch - "+input.remaining()+" bytes remain");
        }
    }

    public Set<String> supportedOptions()
    {
        return new HashSet<>(Arrays.asList(CompressionParams.CRC_CHECK_CHANCE));
    }

    public BufferType preferredBufferType()
    {
        return BufferType.OFF_HEAP;
    }

    public boolean supports(BufferType bufferType)
    {
        return true;
    }
}
