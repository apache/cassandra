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

import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.schema.CompressionParams;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class DeflateCompressor implements ICompressor
{
    public static final DeflateCompressor instance = new DeflateCompressor();

    private static final FastThreadLocal<byte[]> threadLocalScratchBuffer = new FastThreadLocal<byte[]>()
    {
        @Override
        protected byte[] initialValue()
        {
            return new byte[CompressionParams.DEFAULT_CHUNK_LENGTH];
        }
    };

    public static byte[] getThreadLocalScratchBuffer()
    {
        return threadLocalScratchBuffer.get();
    }

    private final FastThreadLocal<Deflater> deflater;
    private final FastThreadLocal<Inflater> inflater;

    public static DeflateCompressor create(Map<String, String> compressionOptions)
    {
        // no specific options supported so far
        return instance;
    }

    private DeflateCompressor()
    {
        deflater = new FastThreadLocal<Deflater>()
        {
            @Override
            protected Deflater initialValue()
            {
                return new Deflater();
            }
        };
        inflater = new FastThreadLocal<Inflater>()
        {
            @Override
            protected Inflater initialValue()
            {
                return new Inflater();
            }
        };
    }

    public Set<String> supportedOptions()
    {
        return Collections.emptySet();
    }

    public int initialCompressedBufferLength(int sourceLen)
    {
        // Taken from zlib deflateBound(). See http://www.zlib.net/zlib_tech.html.
        return sourceLen + (sourceLen >> 12) + (sourceLen >> 14) + (sourceLen >> 25) + 13;
    }

    public void compress(ByteBuffer input, ByteBuffer output)
    {
        if (input.hasArray() && output.hasArray())
        {
            int length = compressArray(input.array(), input.arrayOffset() + input.position(), input.remaining(),
                                       output.array(), output.arrayOffset() + output.position(), output.remaining());
            input.position(input.limit());
            output.position(output.position() + length);
        }
        else
            compressBuffer(input, output);
    }

    public int compressArray(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        Deflater def = deflater.get();
        def.reset();
        def.setInput(input, inputOffset, inputLength);
        def.finish();
        if (def.needsInput())
            return 0;

        int len = def.deflate(output, outputOffset, maxOutputLength);
        assert def.finished();
        return len;
    }

    public void compressBuffer(ByteBuffer input, ByteBuffer output)
    {
        Deflater def = deflater.get();
        def.reset();

        byte[] buffer = getThreadLocalScratchBuffer();
        // Use half the buffer for input, half for output.
        int chunkLen = buffer.length / 2;
        while (input.remaining() > chunkLen)
        {
            input.get(buffer, 0, chunkLen);
            def.setInput(buffer, 0, chunkLen);
            while (!def.needsInput())
            {
                int len = def.deflate(buffer, chunkLen, chunkLen);
                output.put(buffer, chunkLen, len);
            }
        }
        int inputLength = input.remaining();
        input.get(buffer, 0, inputLength);
        def.setInput(buffer, 0, inputLength);
        def.finish();
        while (!def.finished())
        {
            int len = def.deflate(buffer, chunkLen, chunkLen);
            output.put(buffer, chunkLen, len);
        }
    }


    public void uncompress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        if (input.hasArray() && output.hasArray())
        {
            int length = uncompress(input.array(), input.arrayOffset() + input.position(), input.remaining(),
                                    output.array(), output.arrayOffset() + output.position(), output.remaining());
            input.position(input.limit());
            output.position(output.position() + length);
        }
        else
            uncompressBuffer(input, output);
    }

    public void uncompressBuffer(ByteBuffer input, ByteBuffer output) throws IOException
    {
        try
        {
            Inflater inf = inflater.get();
            inf.reset();

            byte[] buffer = getThreadLocalScratchBuffer();
            // Use half the buffer for input, half for output.
            int chunkLen = buffer.length / 2;
            while (input.remaining() > chunkLen)
            {
                input.get(buffer, 0, chunkLen);
                inf.setInput(buffer, 0, chunkLen);
                while (!inf.needsInput())
                {
                    int len = inf.inflate(buffer, chunkLen, chunkLen);
                    output.put(buffer, chunkLen, len);
                }
            }
            int inputLength = input.remaining();
            input.get(buffer, 0, inputLength);
            inf.setInput(buffer, 0, inputLength);
            while (!inf.needsInput())
            {
                int len = inf.inflate(buffer, chunkLen, chunkLen);
                output.put(buffer, chunkLen, len);
            }
        }
        catch (DataFormatException e)
        {
            throw new IOException(e);
        }
    }

    public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException
    {
        return uncompress(input, inputOffset, inputLength, output, outputOffset, output.length - outputOffset);
    }

    public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength) throws IOException
    {
        Inflater inf = inflater.get();
        inf.reset();
        inf.setInput(input, inputOffset, inputLength);
        if (inf.needsInput())
            return 0;

        // We assume output is big enough
        try
        {
            return inf.inflate(output, outputOffset, maxOutputLength);
        }
        catch (DataFormatException e)
        {
            throw new IOException(e);
        }
    }

    public boolean supports(BufferType bufferType)
    {
        return true;
    }

    public BufferType preferredBufferType()
    {
        // Prefer array-backed buffers.
        return BufferType.ON_HEAP;
    }
}
