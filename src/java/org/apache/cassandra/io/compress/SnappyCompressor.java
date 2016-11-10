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
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyError;

import org.apache.cassandra.utils.JVMStabilityInspector;

public class SnappyCompressor implements ICompressor
{
    public static final SnappyCompressor instance = new SnappyCompressor();

    private static Logger logger = LoggerFactory.getLogger(SnappyCompressor.class);
    static
    {
        if (!isAvailable())
            logger.warn("Cannot initialize native Snappy library. Compression on new sstables will be disabled.");
    }

    public static SnappyCompressor create(Map<String, String> compressionOptions)
    {
        // this would throw java.lang.NoClassDefFoundError if Snappy class
        // wasn't found at runtime which should be processed by calling method
        Snappy.getNativeLibraryVersion();

        // no specific options supported so far
        return instance;
    }

    public static boolean isAvailable()
    {
        try
        {
            create(Collections.<String, String>emptyMap());
            return true;
        }
        catch (Exception e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            return false;
        }
        catch (NoClassDefFoundError | SnappyError | UnsatisfiedLinkError e)
        {
            return false;
        }
    }

    public Set<String> supportedOptions()
    {
        return Collections.emptySet();
    }

    public int initialCompressedBufferLength(int chunkLength)
    {
        return Snappy.maxCompressedLength(chunkLength);
    }

    public void compress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        int dlimit = output.limit();
        Snappy.compress(input, output);

        // Snappy doesn't match the ICompressor contract w/regards to state it leaves dest ByteBuffer's counters in
        output.position(output.limit());
        output.limit(dlimit);
        input.position(input.limit());
    }

    public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException
    {
        return Snappy.rawUncompress(input, inputOffset, inputLength, output, outputOffset);
    }

    public void uncompress(ByteBuffer input, ByteBuffer output)
            throws IOException
    {
        int dlimit = output.limit();
        Snappy.uncompress(input, output);

        // Snappy doesn't match the ICompressor contract w/regards to state it leaves dest ByteBuffer's counters in
        output.position(output.limit());
        output.limit(dlimit);
        input.position(input.limit());
    }

    public BufferType preferredBufferType()
    {
        return BufferType.OFF_HEAP;
    }

    public boolean supports(BufferType bufferType)
    {
        // Snappy can't deal with different input and output buffer types.
        // To avoid possible problems, pretend it can't support array-backed at all.
        return bufferType == BufferType.OFF_HEAP;
    }
}
