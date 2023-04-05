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
package org.apache.cassandra.db.streaming;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.compress.CompressionMetadata.Chunk;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * Container that carries compression parameters and chunks to decompress data from stream.
 */
public abstract class CompressionInfo
{
    public static final IVersionedSerializer<CompressionInfo> serializer = new CompressionInfoSerializer();

    /**
     * Returns the compression parameters.
     *
     * @return the compression parameters.
     */
    public abstract CompressionParams parameters();

    /**
     * Returns the offset and length of the file chunks.
     *
     * @return the offset and length of the file chunks.
     */
    public abstract CompressionMetadata.Chunk[] chunks();

    /**
     * Computes the size of the file to transfer.
     *
     * @return the size of the file in bytes
     */
    public long getTotalSize()
    {
        long size = 0;
        for (CompressionMetadata.Chunk chunk : chunks())
        {
            size += chunk.length + 4; // 4 bytes for CRC
        }
        return size;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof CompressionInfo))
            return false;

        CompressionInfo that = (CompressionInfo) o;

        return Objects.equals(parameters(), that.parameters())
               && Arrays.equals(chunks(), that.chunks());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(parameters(), chunks());
    }

    /**
     * Create a {@code CompressionInfo} instance which is fully initialized.
     *
     * @param chunks the file chunks
     * @param parameters the compression parameters
     */
    public static CompressionInfo newInstance(CompressionMetadata.Chunk[] chunks, CompressionParams parameters)
    {
        assert chunks != null && parameters != null;

        return new CompressionInfo()
        {
            @Override
            public Chunk[] chunks()
            {
                return chunks;
            }

            @Override
            public CompressionParams parameters()
            {
                return parameters;
            }
        };
    }

    /**
     * Create a {@code CompressionInfo} that will computes the file chunks only upon request.
     *
     * <p>The instance returned by that method will only computes the file chunks when the {@code chunks},
     * {@code equals} or {@code hashcode} methods are called for the first time. This is done to reduce the GC
     * pressure. See CASSANDRA-10680 for more details</p>.
     *
     * @param metadata the compression metadata
     * @param sections the file sections
     * @return a {@code CompressionInfo} that will computes the file chunks only upon request.
     */
    static CompressionInfo newLazyInstance(CompressionMetadata metadata, List<SSTableReader.PartitionPositionBounds> sections)
    {
        if (metadata == null)
        {
            return null;
        }

        return new CompressionInfo()
        {
            private volatile Chunk[] chunks;

            @Override
            public synchronized Chunk[] chunks()
            {
                if (chunks == null)
                    chunks = metadata.getChunksForSections(sections);

                return chunks;
            }

            @Override
            public CompressionParams parameters()
            {
                return metadata.parameters;
            }

            @Override
            public long getTotalSize()
            {
                // If the chunks have not been loaded yet we avoid to compute them.
                if (chunks == null)
                    return metadata.getTotalSizeForSections(sections);

                return super.getTotalSize();
            }
        };
    }

    static class CompressionInfoSerializer implements IVersionedSerializer<CompressionInfo>
    {
        public void serialize(CompressionInfo info, DataOutputPlus out, int version) throws IOException
        {
            if (info == null)
            {
                out.writeInt(-1);
                return;
            }

            Chunk[] chunks = info.chunks();
            int chunkCount = chunks.length;
            out.writeInt(chunkCount);
            for (int i = 0; i < chunkCount; i++)
                CompressionMetadata.Chunk.serializer.serialize(chunks[i], out, version);
            // compression params
            CompressionParams.serializer.serialize(info.parameters(), out, version);
        }

        public CompressionInfo deserialize(DataInputPlus in, int version) throws IOException
        {
            // chunks
            int chunkCount = in.readInt();
            if (chunkCount < 0)
                return null;

            CompressionMetadata.Chunk[] chunks = new CompressionMetadata.Chunk[chunkCount];
            for (int i = 0; i < chunkCount; i++)
                chunks[i] = CompressionMetadata.Chunk.serializer.deserialize(in, version);

            // compression params
            CompressionParams parameters = CompressionParams.serializer.deserialize(in, version);
            return CompressionInfo.newInstance(chunks, parameters);
        }

        public long serializedSize(CompressionInfo info, int version)
        {
            if (info == null)
                return TypeSizes.sizeof(-1);

            // chunks
            Chunk[] chunks = info.chunks();
            int chunkCount = chunks.length;
            long size = TypeSizes.sizeof(chunkCount);
            for (int i = 0; i < chunkCount; i++)
                size += CompressionMetadata.Chunk.serializer.serializedSize(chunks[i], version);
            // compression params
            size += CompressionParams.serializer.serializedSize(info.parameters(), version);
            return size;
        }
    }
}
