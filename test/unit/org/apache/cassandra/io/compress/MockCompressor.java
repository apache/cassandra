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
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Mock compressor used to test the effect of different output sizes in compression.
 * Writes only the size of the given buffer, and on decompression expands to sequence of that many 0s.
 */
public class MockCompressor implements ICompressor
{
    final int extra;
    final double ratio;

    public static Map<String, String> paramsFor(double ratio, int extra)
    {
        return ImmutableMap.of("extra", "" + extra, "ratio", "" + ratio);
    }

    public static MockCompressor create(Map<String, String> opts)
    {
        return new MockCompressor(Integer.parseInt(opts.get("extra")),
                                  Double.parseDouble(opts.get("ratio")));
    }

    private MockCompressor(int extra, double ratio)
    {
        this.extra = extra;
        this.ratio = ratio;
    }

    public int initialCompressedBufferLength(int chunkLength)
    {
        return (int) Math.ceil(chunkLength / ratio + extra);
    }

    public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset)
    throws IOException
    {
        final ByteBuffer outputBuffer = ByteBuffer.wrap(output, outputOffset, output.length - outputOffset);
        uncompress(ByteBuffer.wrap(input, inputOffset, inputLength),
                   outputBuffer);
        return outputBuffer.position();
    }

    public void compress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        int inputLength = input.remaining();
        int outputLength = initialCompressedBufferLength(inputLength);
        // assume the input is all zeros, write its length and pad until the required size
        output.putInt(inputLength);
        for (int i = 4; i < outputLength; ++i)
            output.put((byte) i);
        input.position(input.limit());
    }

    public void uncompress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        int outputLength = input.getInt();
        ByteBufferUtil.writeZeroes(output, outputLength);
    }

    public BufferType preferredBufferType()
    {
        return BufferType.OFF_HEAP;
    }

    public boolean supports(BufferType bufferType)
    {
        return true;
    }

    public Set<String> supportedOptions()
    {
        return ImmutableSet.of("extra", "ratio");
    }

}
