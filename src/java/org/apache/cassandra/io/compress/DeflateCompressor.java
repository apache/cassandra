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

    public int compress(byte[] input, int inputOffset, int inputLength, ICompressor.WrappedArray output, int outputOffset)
    {
        Deflater def = deflater.get();
        def.reset();
        def.setInput(input, inputOffset, inputLength);
        def.finish();
        if (def.needsInput())
            return 0;

        int offs = outputOffset;
        while (true)
        {
            offs += def.deflate(output.buffer, offs, output.buffer.length - offs);
            if (def.finished())
            {
                return offs - outputOffset;
            }
            else
            {
                // We're not done, output was too small. Increase it and continue
                byte[] newBuffer = new byte[(output.buffer.length*4)/3 + 1];
                System.arraycopy(output.buffer, 0, newBuffer, 0, offs);
                output.buffer = newBuffer;
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
}
