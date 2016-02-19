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
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.Pair;

public class LZ4Compressor implements ICompressor
{
    private static final Logger logger = LoggerFactory.getLogger(LZ4Compressor.class);

    public static final String LZ4_FAST_COMPRESSOR = "fast";
    public static final String LZ4_HIGH_COMPRESSOR = "high";
    private static final Set<String> VALID_COMPRESSOR_TYPES = new HashSet<>(Arrays.asList(LZ4_FAST_COMPRESSOR, LZ4_HIGH_COMPRESSOR));

    private static final int DEFAULT_HIGH_COMPRESSION_LEVEL = 9;
    private static final String DEFAULT_LZ4_COMPRESSOR_TYPE = LZ4_FAST_COMPRESSOR;

    public static final String LZ4_HIGH_COMPRESSION_LEVEL = "lz4_high_compressor_level";
    public static final String LZ4_COMPRESSOR_TYPE = "lz4_compressor_type";

    private static final int INTEGER_BYTES = 4;

    private static final ConcurrentHashMap<Pair<String, Integer>, LZ4Compressor> instances = new ConcurrentHashMap<>();

    public static LZ4Compressor create(Map<String, String> args) throws ConfigurationException
    {
        String compressorType = validateCompressorType(args.get(LZ4_COMPRESSOR_TYPE));
        Integer compressionLevel = validateCompressionLevel(args.get(LZ4_HIGH_COMPRESSION_LEVEL));

        Pair<String, Integer> compressorTypeAndLevel = Pair.create(compressorType, compressionLevel);
        LZ4Compressor instance = instances.get(compressorTypeAndLevel);
        if (instance == null)
        {
            if (compressorType.equals(LZ4_FAST_COMPRESSOR) && args.get(LZ4_HIGH_COMPRESSION_LEVEL) != null)
                logger.warn("'{}' parameter is ignored when '{}' is '{}'", LZ4_HIGH_COMPRESSION_LEVEL, LZ4_COMPRESSOR_TYPE, LZ4_FAST_COMPRESSOR);
            instance = new LZ4Compressor(compressorType, compressionLevel);
            LZ4Compressor instanceFromMap = instances.putIfAbsent(compressorTypeAndLevel, instance);
            if(instanceFromMap != null)
                instance = instanceFromMap;
        }
        return instance;
    }

    private final net.jpountz.lz4.LZ4Compressor compressor;
    private final net.jpountz.lz4.LZ4FastDecompressor decompressor;
    @VisibleForTesting
    final String compressorType;
    @VisibleForTesting
    final Integer compressionLevel;

    private LZ4Compressor(String type, Integer compressionLevel)
    {
        this.compressorType = type;
        this.compressionLevel = compressionLevel;
        final LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
        switch (type)
        {
            case LZ4_HIGH_COMPRESSOR:
            {
                compressor = lz4Factory.highCompressor(compressionLevel);
                break;
            }
            case LZ4_FAST_COMPRESSOR:
            default:
            {
                compressor = lz4Factory.fastCompressor();
            }
        }

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
        return new HashSet<>(Arrays.asList(LZ4_HIGH_COMPRESSION_LEVEL, LZ4_COMPRESSOR_TYPE));
    }

    public static String validateCompressorType(String compressorType) throws ConfigurationException
    {
        if (compressorType == null)
            return DEFAULT_LZ4_COMPRESSOR_TYPE;

        if (!VALID_COMPRESSOR_TYPES.contains(compressorType))
        {
            throw new ConfigurationException(String.format("Invalid compressor type '%s' specified for LZ4 parameter '%s'. "
                                                           + "Valid options are %s.", compressorType, LZ4_COMPRESSOR_TYPE,
                                                           VALID_COMPRESSOR_TYPES.toString()));
        }
        else
        {
            return compressorType;
        }
    }

    public static Integer validateCompressionLevel(String compressionLevel) throws ConfigurationException
    {
        if (compressionLevel == null)
            return DEFAULT_HIGH_COMPRESSION_LEVEL;

        ConfigurationException ex = new ConfigurationException("Invalid value [" + compressionLevel + "] for parameter '"
                                                                 + LZ4_HIGH_COMPRESSION_LEVEL + "'. Value must be between 1 and 17.");

        Integer level;
        try
        {
            level = Integer.parseInt(compressionLevel);
        }
        catch (NumberFormatException e)
        {
            throw ex;
        }

        if (level < 1 || level > 17)
        {
            throw ex;
        }

        return level;
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
