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

package org.apache.cassandra.io.util;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.List;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.compress.ZstdCompressor;
import org.apache.cassandra.utils.CollectionSerializers;

import static org.apache.cassandra.io.compress.ZstdCompressor.DEFAULT_COMPRESSION_LEVEL;
import static org.apache.cassandra.io.util.CompressedFrameDataOutputPlus.DEFAULT_FRAME_SIZE;
import static org.apache.cassandra.io.util.CompressedFrameDataOutputPlus.SIZE_OF_HEADER;

public class CompressedFrameDataInputPlus extends RebufferingInputStream
{
    final ICompressor compressor;
    final Checksum checksum;
    final ReadableByteChannel channel;
    ByteBuffer compressed;

    protected CompressedFrameDataInputPlus(int frameSize, ReadableByteChannel channel, ICompressor compressor, Checksum checksum)
    {
        super(compressor.preferredBufferType().allocate(frameSize));
        this.compressor = compressor;
        this.checksum = checksum;
        this.channel = channel;
        this.compressed = compressor.preferredBufferType().allocate(frameSize);
        buffer.limit(0);
    }

    @Override
    protected void reBuffer() throws IOException
    {
        compressed.position(0);
        compressed.limit(SIZE_OF_HEADER);
        while (channel.read(compressed) >= 0 && compressed.hasRemaining());
        compressed.flip();
        long headerChecksum = compressed.getLong();
        int length = compressed.getShort();

        boolean decompress = length >= 0;
        if (!decompress)
            length = -1 - length;

        compressed.clear();
        compressed.limit(length);
        while (compressed.hasRemaining())
        {
            if (channel.read(compressed) < 0)
                throw new EOFException("Incomplete file: header stipulated " + length + " bytes but found only " + compressed.position());
        }
        compressed.flip();
        this.checksum.update(compressed);
        compressed.flip();
        long dataChecksum = checksum.getValue();
        if (headerChecksum != dataChecksum)
            throw new IOException("Invalid checksum: " + headerChecksum + " != " + dataChecksum);

        buffer.clear();
        if (decompress) compressor.uncompress(compressed, buffer);
        else buffer.put(compressed);
        buffer.flip();
    }

    public static <T> T read(File file, IVersionedSerializer<T> serializer) throws IOException
    {
        try (CompressedFrameDataInputPlus in = new CompressedFrameDataInputPlus(DEFAULT_FRAME_SIZE, file.newReadChannel(), ZstdCompressor.getOrCreate(DEFAULT_COMPRESSION_LEVEL), new CRC32C()))
        {
            int version = in.readUnsignedVInt32();
            return serializer.deserialize(in, version);
        }
    }

    public static <T> T readOne(File file, UnversionedSerializer<T> serializer) throws IOException
    {
        try (CompressedFrameDataInputPlus in = new CompressedFrameDataInputPlus(DEFAULT_FRAME_SIZE, file.newReadChannel(), ZstdCompressor.getOrCreate(DEFAULT_COMPRESSION_LEVEL), new CRC32C()))
        {
            int version = in.readUnsignedVInt32();
            if (version != 0)
                throw new IOException("Expected version 0 for unversioned serializer");
            return serializer.deserialize(in);
        }
    }

    public static <T> List<T> readList(File file, UnversionedSerializer<T> serializer) throws IOException
    {
        try (CompressedFrameDataInputPlus in = new CompressedFrameDataInputPlus(DEFAULT_FRAME_SIZE, file.newReadChannel(), ZstdCompressor.getOrCreate(DEFAULT_COMPRESSION_LEVEL), new CRC32C()))
        {
            int version = in.readUnsignedVInt32();
            if (version != 0)
                throw new IOException("Expected version 0 for unversioned serializer");
            return CollectionSerializers.deserializeList(in, serializer);
        }
    }
}
