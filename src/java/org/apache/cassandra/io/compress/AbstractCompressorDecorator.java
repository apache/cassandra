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
import java.util.Set;

public abstract class AbstractCompressorDecorator implements ICompressor
{
    protected ICompressor baseCompressor;
    public AbstractCompressorDecorator(ICompressor compressor)
    {
        this.baseCompressor = compressor;
    }

    @Override
    public int initialCompressedBufferLength(int chunkLength)
    {
        return baseCompressor.initialCompressedBufferLength(chunkLength);
    }

    @Override
    public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException
    {
        return baseCompressor.uncompress(input, inputOffset, inputLength, output, outputOffset);
    }

    @Override
    public void compress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        baseCompressor.compress(input, output);
    }

    @Override
    public void uncompress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        baseCompressor.uncompress(input, output);
    }

    @Override
    public BufferType preferredBufferType() {
        return baseCompressor.preferredBufferType();
    }

    @Override
    public boolean supports(BufferType bufferType) {
        return baseCompressor.supports(bufferType);
    }

    @Override
    public Set<String> supportedOptions() {
        return baseCompressor.supportedOptions();
    }

}
