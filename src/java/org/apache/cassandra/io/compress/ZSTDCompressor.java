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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.github.luben.zstd.Zstd;

public class ZSTDCompressor implements ICompressor
{
    public static final int FAST_COMPRESSION = 1; // fastest compression time
    public static final int DEFAULT_LEVEL = 3;
    public static final int BEST_COMPRESSION = 22;// very good compression ratio
    private static final ZSTDCompressor instance = new ZSTDCompressor();
    private static final String COMPRESSION_LEVEL_OPTION_NAME = "compression_level";
    @VisibleForTesting
    protected static final int compressionLevel = DEFAULT_LEVEL;

    public static ZSTDCompressor create(Map<String, String> compressionOptions)
    {
        validateCompressionLevel(parseCompressionLevelOption(compressionOptions));
        return instance;
    }

    private static void validateCompressionLevel(int compressionLevel)
    {
        if (compressionLevel < FAST_COMPRESSION || compressionLevel > BEST_COMPRESSION)
        {
            throw new IllegalArgumentException(
                "ZSTD compression_level " + Integer.toString(compressionLevel) + " invalid ",
                null
            );
        }
    }

    private static int parseCompressionLevelOption(Map<String,String> compressionOptions)
    {
        return Integer.parseInt(compressionOptions.getOrDefault(COMPRESSION_LEVEL_OPTION_NAME,
                                                                Integer.toString(DEFAULT_LEVEL)));
    }

    @Override
    public int initialCompressedBufferLength(int chunkLength)
    {
        return (int)Zstd.compressBound(chunkLength);
    }

    @Override
    public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset)
    throws IOException
    {
        long decompressSize = Zstd.decompressByteArray(output,
                                                       outputOffset,
                                                       output.length - outputOffset,
                                                       input,
                                                       inputOffset,
                                                       inputLength);
        if (Zstd.isError(decompressSize))
        {
            throw new IOException("ZSTD uncompress failed with error reason " + Zstd.getErrorName(decompressSize));
        }

        return (int) decompressSize;
    }

    @Override
    public void uncompress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        Zstd.decompress(output, input);
    }

    @Override
    public void compress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        if (!input.isDirect())
        {
            throw new IllegalArgumentException("input must be a direct buffer");
        }

        if (!output.isDirect())
        {
            throw new IllegalArgumentException("output must be a direct buffer");
        }
        long compressedSize = Zstd.compressDirectByteBuffer(output,
                                                            output.position(),
                                                            output.limit() - output.position(),
                                                            input,
                                                            input.position(),
                                                            input.limit() - input.position(),
                                                            compressionLevel);
        if (Zstd.isError(compressedSize))
        {
            throw new IOException("ZSTD compress failed with error reason " + Zstd.getErrorName(compressedSize));
        }
        input.position(input.limit());
        output.position((int) (compressedSize + output.position()));
    }

    @Override
    public BufferType preferredBufferType()
    {
        return BufferType.OFF_HEAP;
    }

    @Override
    public boolean supports(BufferType bufferType)
    {
        return bufferType == BufferType.OFF_HEAP;
    }

    @Override
    public Set<String> supportedOptions()
    {
        return new HashSet(Collections.singletonList(COMPRESSION_LEVEL_OPTION_NAME));
    }
}
