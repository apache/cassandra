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

public class CompressorDecorator extends AbstractCompressorDecorator
{
    private ICompressor pluginCompressor;
    public CompressorDecorator(ICompressor baseCompressor, ICompressor pluginCompressor)
    {
	    super(baseCompressor);
	    this.pluginCompressor = pluginCompressor;
    }

    @Override
    public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException
    {
        try
        {
            return pluginCompressor.uncompress(input, inputOffset, inputLength, output, outputOffset);
        }
        catch(IOException e)
        {
            return super.uncompress(input, inputOffset, inputLength, output, outputOffset);
        }
    }

    @Override
    public void compress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        try
        {
            pluginCompressor.compress(input, output);
        }
        catch(IOException e)
        {
            super.compress(input, output);
        }
    }

    @Override
    public void uncompress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        try
        {
            pluginCompressor.uncompress(input, output);
        }
        catch(IOException e)
        {
            super.uncompress(input, output);
        }
    }

}
