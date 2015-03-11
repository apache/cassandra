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

import org.apache.cassandra.utils.ByteBufferUtil;

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

    private final ThreadLocal<Deflater> deflater;
    private final ThreadLocal<Inflater> inflater;

    public static DeflateCompressor create(Map<String, String> compressionOptions)
    {
        // no specific options supported so far
        return instance;
    }

    private DeflateCompressor()
    {
        deflater = new ThreadLocal<Deflater>()
        {
            @Override
            protected Deflater initialValue()
            {
                return new Deflater();
            }
        };
        inflater = new ThreadLocal<Inflater>()
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

    public int initialCompressedBufferLength(int chunkLength)
    {
        return chunkLength;
    }

    public int compress(ByteBuffer src, ICompressor.WrappedByteBuffer dest)
    {
        assert dest.buffer.hasArray();

        Deflater def = deflater.get();
        def.reset();
        def.setInput(src.array(), src.position(), src.limit());
        def.finish();
        if (def.needsInput())
            return 0;

        int startPos = dest.buffer.position();
        while (true)
        {
            int arrayOffset = dest.buffer.arrayOffset();
            int len = def.deflate(dest.buffer.array(), arrayOffset + dest.buffer.position(), dest.buffer.remaining());
            dest.buffer.position(dest.buffer.position() + len);
            if (def.finished())
            {
                return dest.buffer.position() - startPos;
            }
            else
            {
                // We're not done, output was too small. Increase it and continue
                ByteBuffer newDest = ByteBuffer.allocate(dest.buffer.capacity()*4/3 + 1);
                dest.buffer.rewind();
                newDest.put(dest.buffer);
                dest.buffer = newDest;
            }
        }
    }

    public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException
    {
        Inflater inf = inflater.get();
        inf.reset();
        inf.setInput(input, inputOffset, inputLength);
        if (inf.needsInput())
            return 0;

        // We assume output is big enough
        try
        {
            return inf.inflate(output, outputOffset, output.length - outputOffset);
        }
        catch (DataFormatException e)
        {
            throw new IOException(e);
        }
    }

    public int uncompress(ByteBuffer input_, ByteBuffer output) throws IOException
    {
        if (!output.hasArray())
            throw new IllegalArgumentException("DeflateCompressor doesn't work with direct byte buffers");

        byte[] input = ByteBufferUtil.getArray(input_);
        return uncompress(input, 0, input.length, output.array(), output.arrayOffset() + output.position());
    }

    public boolean useDirectOutputByteBuffers()
    {
        return false;
    }
}
