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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.ByteBufferUtil;

public class CassandraStreamHeader
{
    /** SSTable version */
    public final Version version;

    /** SSTable format **/
    public final SSTableFormat.Type format;
    public final long estimatedKeys;
    public final List<SSTableReader.PartitionPositionBounds> sections;
    /**
     * Compression info for SSTable to send. Can be null if SSTable is not compressed.
     * On sender, this field is always null to avoid holding large number of Chunks.
     * Use compressionMetadata instead.
     */
    private final CompressionMetadata compressionMetadata;
    public volatile CompressionInfo compressionInfo;
    public final int sstableLevel;
    public final SerializationHeader.Component header;

    /* flag indicating whether this is a partial or full sstable transfer */
    public final boolean fullStream;
    /* first token of the sstable required for faster streaming */
    public final DecoratedKey firstKey;
    public final TableId tableId;
    public final List<ComponentInfo> components;

    /* cached size value */
    private transient final long size;

    public CassandraStreamHeader(Version version, SSTableFormat.Type format, long estimatedKeys,
                                 List<SSTableReader.PartitionPositionBounds> sections, CompressionMetadata compressionMetadata,
                                 CompressionInfo compressionInfo, int sstableLevel, SerializationHeader.Component header,
                                 List<ComponentInfo> components,
                                 boolean fullStream, DecoratedKey firstKey, TableId tableId)
    {
        this.version = version;
        this.format = format;
        this.estimatedKeys = estimatedKeys;
        this.sections = sections;
        this.compressionMetadata = compressionMetadata;
        this.compressionInfo = compressionInfo;
        this.sstableLevel = sstableLevel;
        this.header = header;
        this.fullStream = fullStream;
        this.components = components;
        this.firstKey = firstKey;
        this.tableId = tableId;
        this.size = calculateSize();
    }

    private CassandraStreamHeader(Version version, SSTableFormat.Type format, long estimatedKeys,
                                  List<SSTableReader.PartitionPositionBounds> sections, CompressionMetadata compressionMetadata,
                                  CompressionInfo compressionInfo, int sstableLevel, SerializationHeader.Component header, TableId tableId)
    {
        this(version, format, estimatedKeys, sections, compressionMetadata, compressionInfo, sstableLevel, header, null, false, null, tableId);
    }

    public CassandraStreamHeader(Version version, SSTableFormat.Type format, long estimatedKeys,
                                 List<SSTableReader.PartitionPositionBounds> sections, CompressionMetadata compressionMetadata,
                                 int sstableLevel, SerializationHeader.Component header, TableId tableId)
    {
        this(version, format, estimatedKeys, sections, compressionMetadata, null, sstableLevel, header, tableId);
    }

    public CassandraStreamHeader(Version version, SSTableFormat.Type format, long estimatedKeys,
                                 List<SSTableReader.PartitionPositionBounds> sections, CompressionMetadata compressionMetadata,
                                 int sstableLevel, SerializationHeader.Component header, List<ComponentInfo> components,
                                 boolean fullStream, DecoratedKey firstKey, TableId tableId)
    {
        this(version, format, estimatedKeys, sections, compressionMetadata, null, sstableLevel, header,
             components, fullStream, firstKey, tableId);
    }

    public CassandraStreamHeader(Version version, SSTableFormat.Type format, long estimatedKeys,
                                 List<SSTableReader.PartitionPositionBounds> sections, CompressionInfo compressionInfo,
                                 int sstableLevel, SerializationHeader.Component header, List<ComponentInfo> components,
                                 boolean fullStream, DecoratedKey firstKey, TableId tableId)
    {
        this(version, format, estimatedKeys, sections, null, compressionInfo, sstableLevel, header,
             components, fullStream, firstKey, tableId);
    }

    public boolean isCompressed()
    {
        return compressionInfo != null;
    }

    /**
     * @return total file size to transfer in bytes
     */
    public long size()
    {
        return size;
    }

    private long calculateSize()
    {
        long transferSize = 0;

        if (fullStream)
        {
            for (ComponentInfo info : components)
                transferSize += info.length;
        }
        else
        {
            if (compressionInfo != null)
            {
                // calculate total length of transferring chunks
                for (CompressionMetadata.Chunk chunk : compressionInfo.chunks)
                    transferSize += chunk.length + 4; // 4 bytes for CRC
            }
            else
            {
                for (SSTableReader.PartitionPositionBounds section : sections)
                    transferSize += section.upperPosition - section.lowerPosition;
            }
        }
        return transferSize;
    }

    public synchronized void calculateCompressionInfo()
    {
        if (compressionMetadata != null && compressionInfo == null)
        {
            compressionInfo = CompressionInfo.fromCompressionMetadata(compressionMetadata, sections);
        }
    }

    @Override
    public String toString()
    {
        return "CassandraStreamHeader{" +
               "version=" + version +
               ", format=" + format +
               ", estimatedKeys=" + estimatedKeys +
               ", sections=" + sections +
               ", compressionInfo=" + compressionInfo +
               ", sstableLevel=" + sstableLevel +
               ", header=" + header +
               ", fullStream=" + fullStream +
               ", firstKey=" + firstKey +
               ", tableId=" + tableId +
               ", components=" + components +
               '}';
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CassandraStreamHeader that = (CassandraStreamHeader) o;
        return estimatedKeys == that.estimatedKeys &&
               sstableLevel == that.sstableLevel &&
               fullStream == that.fullStream &&
               Objects.equals(version, that.version) &&
               format == that.format &&
               Objects.equals(sections, that.sections) &&
               Objects.equals(compressionInfo, that.compressionInfo) &&
               Objects.equals(header, that.header) &&
               Objects.equals(components, that.components) &&
               Objects.equals(firstKey, that.firstKey) &&
               Objects.equals(tableId, that.tableId);
    }

    public int hashCode()
    {
        return Objects.hash(version, format, estimatedKeys, sections, compressionInfo, sstableLevel, header, components,
                            fullStream, firstKey, tableId);
    }

    public static final IVersionedSerializer<CassandraStreamHeader> serializer = new CassandraStreamHeaderSerializer();

    public static class CassandraStreamHeaderSerializer implements IVersionedSerializer<CassandraStreamHeader>
    {
        public void serialize(CassandraStreamHeader header, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(header.version.toString());
            out.writeUTF(header.format.name);

            out.writeLong(header.estimatedKeys);
            out.writeInt(header.sections.size());
            for (SSTableReader.PartitionPositionBounds section : header.sections)
            {
                out.writeLong(section.lowerPosition);
                out.writeLong(section.upperPosition);
            }
            header.calculateCompressionInfo();
            CompressionInfo.serializer.serialize(header.compressionInfo, out, version);
            out.writeInt(header.sstableLevel);

            SerializationHeader.serializer.serialize(header.version, header.header, out);

            header.tableId.serialize(out);
            out.writeBoolean(header.fullStream);

            if (header.fullStream)
            {
                out.writeInt(header.components.size());
                for (ComponentInfo info : header.components)
                    ComponentInfo.serializer.serialize(info, out, version);

                ByteBufferUtil.writeWithVIntLength(header.firstKey.getKey(), out);
            }
        }

        public CassandraStreamHeader deserialize(DataInputPlus in, int version) throws IOException
        {
            return deserialize(in, version, tableId -> {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(tableId);
                if (cfs != null)
                    return cfs.getPartitioner();

                return null;
            });
        }

        @VisibleForTesting
        public CassandraStreamHeader deserialize(DataInputPlus in, int version, Function<TableId, IPartitioner> partitionerMapper) throws IOException
        {
            Version sstableVersion = SSTableFormat.Type.current().info.getVersion(in.readUTF());
            SSTableFormat.Type format = SSTableFormat.Type.validate(in.readUTF());

            long estimatedKeys = in.readLong();
            int count = in.readInt();
            List<SSTableReader.PartitionPositionBounds> sections = new ArrayList<>(count);
            for (int k = 0; k < count; k++)
                sections.add(new SSTableReader.PartitionPositionBounds(in.readLong(), in.readLong()));
            CompressionInfo compressionInfo = CompressionInfo.serializer.deserialize(in, version);
            int sstableLevel = in.readInt();

            SerializationHeader.Component header =  SerializationHeader.serializer.deserialize(sstableVersion, in);

            TableId tableId = TableId.deserialize(in);
            boolean fullStream = in.readBoolean();
            List<ComponentInfo> components = null;
            DecoratedKey firstKey = null;

            if (fullStream)
            {
                int ncomp = in.readInt();
                components = new ArrayList<>(ncomp);

                for (int i=0; i < ncomp; i++)
                    components.add(ComponentInfo.serializer.deserialize(in, version));

                ByteBuffer keyBuf = ByteBufferUtil.readWithVIntLength(in);

                IPartitioner partitioner = partitionerMapper.apply(tableId);
                if (partitioner == null)
                    throw new IllegalArgumentException(String.format("Could not determine partitioner for tableId {}", tableId));
                firstKey = partitioner.decorateKey(keyBuf);
            }

            return new CassandraStreamHeader(sstableVersion, format, estimatedKeys, sections, compressionInfo,
                                             sstableLevel, header, components, fullStream, firstKey, tableId);
        }

        public long serializedSize(CassandraStreamHeader header, int version)
        {
            long size = 0;
            size += TypeSizes.sizeof(header.version.toString());
            size += TypeSizes.sizeof(header.format.name);
            size += TypeSizes.sizeof(header.estimatedKeys);

            size += TypeSizes.sizeof(header.sections.size());
            for (SSTableReader.PartitionPositionBounds section : header.sections)
            {
                size += TypeSizes.sizeof(section.lowerPosition);
                size += TypeSizes.sizeof(section.upperPosition);
            }
            size += CompressionInfo.serializer.serializedSize(header.compressionInfo, version);
            size += TypeSizes.sizeof(header.sstableLevel);

            size += SerializationHeader.serializer.serializedSize(header.version, header.header);

            size += header.tableId.serializedSize();
            size += TypeSizes.sizeof(header.fullStream);

            if (header.fullStream)
            {
                size += TypeSizes.sizeof(header.components.size());
                for (ComponentInfo info : header.components)
                    size += ComponentInfo.serializer.serializedSize(info, version);

                size += ByteBufferUtil.serializedSizeWithVIntLength(header.firstKey.getKey());
            }

            return size;
        }
    };
}
