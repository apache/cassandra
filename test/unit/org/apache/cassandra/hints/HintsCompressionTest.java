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

package org.apache.cassandra.hints;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.io.compress.DeflateCompressor;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.compress.SnappyCompressor;

public class HintsCompressionTest extends AlteredHints
{
    private Class<? extends ICompressor> compressorClass;

    ImmutableMap<String, Object> params()
    {
        ImmutableMap<String, Object> compressionParams = ImmutableMap.<String, Object>builder()
                                                                     .put(ParameterizedClass.CLASS_NAME, compressorClass.getSimpleName())
                                                                     .build();
        return ImmutableMap.<String, Object>builder()
                           .put(HintsDescriptor.COMPRESSION, compressionParams)
                           .build();
    }

    boolean looksLegit(HintsWriter writer)
    {
        if (!(writer instanceof CompressedHintsWriter))
            return false;
        CompressedHintsWriter compressedHintsWriter = (CompressedHintsWriter)writer;
        return compressedHintsWriter.getCompressor().getClass().isAssignableFrom(compressorClass);
    }

    boolean looksLegit(ChecksummedDataInput checksummedDataInput)
    {
        if (!(checksummedDataInput instanceof CompressedChecksummedDataInput))
            return false;
        CompressedChecksummedDataInput compressedChecksummedDataInput = (CompressedChecksummedDataInput)checksummedDataInput;
        return compressedChecksummedDataInput.getCompressor().getClass().isAssignableFrom(compressorClass);
    }

    @Test
    public void lz4Compressor() throws Exception
    {
        compressorClass = LZ4Compressor.class;
        multiFlushAndDeserializeTest();
    }

    @Test
    public void snappyCompressor() throws Exception
    {
        compressorClass = SnappyCompressor.class;
        multiFlushAndDeserializeTest();
    }

    @Test
    public void deflateCompressor() throws Exception
    {
        compressorClass = DeflateCompressor.class;
        multiFlushAndDeserializeTest();
    }
}
