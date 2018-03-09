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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.CRC32;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.io.compress.ICompressor;

public class CompressedHintsWriter extends HintsWriter
{
    // compressed and uncompressed size is stored at the beginning of each compressed block
    static final int METADATA_SIZE = 8;

    private final ICompressor compressor;

    private volatile ByteBuffer compressionBuffer = null;

    public CompressedHintsWriter(File directory, HintsDescriptor descriptor, File file, FileChannel channel, int fd, CRC32 globalCRC)
    {
        super(directory, descriptor, file, channel, fd, globalCRC);
        compressor = descriptor.createCompressor();
        assert compressor != null;
    }

    protected void writeBuffer(ByteBuffer bb) throws IOException
    {
        int originalSize = bb.remaining();
        int estimatedSize = compressor.initialCompressedBufferLength(originalSize) + METADATA_SIZE;

        if (compressionBuffer == null || compressionBuffer.capacity() < estimatedSize)
        {
            compressionBuffer = compressor.preferredBufferType().allocate(estimatedSize);
        }
        compressionBuffer.clear();

        compressionBuffer.position(METADATA_SIZE);
        compressor.compress(bb, compressionBuffer);
        int compressedSize = compressionBuffer.position() - METADATA_SIZE;

        compressionBuffer.rewind();
        compressionBuffer.putInt(originalSize);
        compressionBuffer.putInt(compressedSize);
        compressionBuffer.rewind();
        compressionBuffer.limit(compressedSize + METADATA_SIZE);
        super.writeBuffer(compressionBuffer);
    }

    @VisibleForTesting
    ICompressor getCompressor()
    {
        return compressor;
    }
}
