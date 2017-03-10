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

package org.apache.cassandra.transport.frame.compress;

import java.io.IOException;

import org.apache.cassandra.utils.JVMStabilityInspector;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyError;

public class SnappyCompressor implements Compressor
{
    public static final SnappyCompressor INSTANCE;
    static
    {
        SnappyCompressor i;
        try
        {
            i = new SnappyCompressor();
        }
        catch (Exception e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            i = null;
        }
        catch (NoClassDefFoundError | SnappyError | UnsatisfiedLinkError e)
        {
            i = null;
        }
        INSTANCE = i;
    }

    private SnappyCompressor()
    {
        // this would throw java.lang.NoClassDefFoundError if Snappy class
        // wasn't found at runtime which should be processed by the calling method
        Snappy.getNativeLibraryVersion();
    }

    @Override
    public int maxCompressedLength(int length)
    {
        return Snappy.maxCompressedLength(length);
    }

    @Override
    public int compress(byte[] src, int srcOffset, int length, byte[] dest, int destOffset) throws IOException
    {
        return Snappy.compress(src, 0, src.length, dest, destOffset);
    }

    @Override
    public byte[] decompress(byte[] src, int offset, int length, int expectedDecompressedLength) throws IOException
    {
        if (!Snappy.isValidCompressedBuffer(src, 0, length))
            throw new IOException("Provided frame does not appear to be Snappy compressed");

        int uncompressedLength = Snappy.uncompressedLength(src);
        byte[] output = new byte[uncompressedLength];
        Snappy.uncompress(src, offset, length, output, 0);
        return output;
    }
}
