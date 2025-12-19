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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

import com.google.common.primitives.Shorts;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.compress.ZstdCompressor;
import org.apache.cassandra.utils.CollectionSerializers;
import org.apache.cassandra.utils.memory.MemoryUtil;

import static org.apache.cassandra.io.compress.ZstdCompressor.DEFAULT_COMPRESSION_LEVEL;

public class CompressedFrameDataOutputPlus extends BufferedDataOutputStreamPlus
{
    static final int SIZE_OF_HEADER = 10;
    static final int DEFAULT_FRAME_SIZE = 16 << 10;

    private final ICompressor compressor;
    private final Checksum checksum;
    private ByteBuffer compressed;
    protected CompressedFrameDataOutputPlus(int frameSize, WritableByteChannel out, ICompressor compressor, Checksum checksum)
    {
        super(out, compressor.preferredBufferType().allocate(frameSize));
        this.compressor = compressor;
        this.compressed = compressor.preferredBufferType().allocate(frameSize + SIZE_OF_HEADER);
        this.checksum = checksum;
        if (frameSize > Short.MAX_VALUE)
            throw new IllegalArgumentException("Frame size too large");
    }

    @Override
    protected void doFlush(int count) throws IOException
    {
        buffer.flip();
        compressed.clear();
        compressed.position(SIZE_OF_HEADER);
        compressor.compress(buffer, compressed);
        compressed.flip();
        int limit = compressed.limit();
        int length = limit - SIZE_OF_HEADER;
        if (length > buffer.limit())
        {
            length = -(1 + buffer.limit());
            compressed.clear();
            compressed.position(SIZE_OF_HEADER);
            buffer.position(0);
            compressed.put(buffer);
            compressed.flip();
        }
        compressed.putShort(SIZE_OF_HEADER - 2, Shorts.checkedCast(length));
        compressed.position(SIZE_OF_HEADER);
        checksum.update(compressed);
        compressed.putLong(0, checksum.getValue());
        compressed.position(0);
        while (compressed.hasRemaining())
            channel.write(compressed);
    }

    @Override
    public void close() throws IOException
    {
        super.close();
        MemoryUtil.clean(compressed);
        compressed = null;
    }

    public static <T> void write(File file, T value, IVersionedSerializer<T> serializer, int version) throws IOException
    {
        try (CompressedFrameDataOutputPlus out = new CompressedFrameDataOutputPlus(DEFAULT_FRAME_SIZE, file.newReadWriteChannel(), ZstdCompressor.getOrCreate(DEFAULT_COMPRESSION_LEVEL), new CRC32C()))
        {
            out.writeUnsignedVInt32(version);
            serializer.serialize(value, version);
        }
    }

    public static <T> void writeOne(File file, T value, UnversionedSerializer<T> serializer) throws IOException
    {
        try (CompressedFrameDataOutputPlus out = new CompressedFrameDataOutputPlus(DEFAULT_FRAME_SIZE, file.newReadWriteChannel(), ZstdCompressor.getOrCreate(DEFAULT_COMPRESSION_LEVEL), new CRC32C()))
        {
            out.writeUnsignedVInt32(0);
            serializer.serialize(value);
        }
    }

    public static <T> void writeList(File file, List<T> value, UnversionedSerializer<T> serializer) throws IOException
    {
        try (CompressedFrameDataOutputPlus out = new CompressedFrameDataOutputPlus(DEFAULT_FRAME_SIZE, file.newReadWriteChannel(), ZstdCompressor.getOrCreate(DEFAULT_COMPRESSION_LEVEL), new CRC32C()))
        {
            out.writeUnsignedVInt32(0);
            CollectionSerializers.serializeList(value, out, serializer);
        }
    }
}
