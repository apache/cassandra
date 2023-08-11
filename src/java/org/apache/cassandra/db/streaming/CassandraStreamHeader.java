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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.ByteBufferUtil;

import static com.google.common.base.Preconditions.checkNotNull;

public class CassandraStreamHeader
{
    /** SSTable version */
    public final Version version;

    public final long estimatedKeys;
    public final List<SSTableReader.PartitionPositionBounds> sections;
    public final CompressionInfo compressionInfo;
    public final int sstableLevel;
    public final SerializationHeader.Component serializationHeader;

    /* flag indicating whether this is a partial or entire sstable transfer */
    public final boolean isEntireSSTable;
    /* first token of the sstable required for faster streaming */
    public final DecoratedKey firstKey;
    public final TableId tableId;
    public final ComponentManifest componentManifest;

    /* cached size value */
    private final long size;

    private CassandraStreamHeader(Builder builder)
    {
        version = builder.version;
        estimatedKeys = builder.estimatedKeys;
        sections = builder.sections;
        compressionInfo = builder.compressionInfo;
        sstableLevel = builder.sstableLevel;
        serializationHeader = builder.serializationHeader;
        tableId = builder.tableId;
        isEntireSSTable = builder.isEntireSSTable;
        componentManifest = builder.componentManifest;
        firstKey = builder.firstKey;
        size = calculateSize();
    }

    public static Builder builder()
    {
        return new Builder();
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

    @VisibleForTesting
    public long calculateSize()
    {
        if (isEntireSSTable)
            return componentManifest.totalSize();

        if (compressionInfo != null)
            return compressionInfo.getTotalSize();

        long transferSize = 0;
        for (SSTableReader.PartitionPositionBounds section : sections)
            transferSize += section.upperPosition - section.lowerPosition;
        return transferSize;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CassandraStreamHeader that = (CassandraStreamHeader) o;
        return estimatedKeys == that.estimatedKeys &&
               sstableLevel == that.sstableLevel &&
               isEntireSSTable == that.isEntireSSTable &&
               Objects.equals(version, that.version) &&
               Objects.equals(sections, that.sections) &&
               Objects.equals(compressionInfo, that.compressionInfo) &&
               Objects.equals(serializationHeader, that.serializationHeader) &&
               Objects.equals(componentManifest, that.componentManifest) &&
               Objects.equals(firstKey, that.firstKey) &&
               Objects.equals(tableId, that.tableId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(version, estimatedKeys, sections, compressionInfo, sstableLevel, serializationHeader, componentManifest,
                            isEntireSSTable, firstKey, tableId);
    }

    @Override
    public String toString()
    {
        return "CassandraStreamHeader{" +
               "version=" + version +
               ", format=" + version.format.name() +
               ", estimatedKeys=" + estimatedKeys +
               ", sections=" + sections +
               ", sstableLevel=" + sstableLevel +
               ", header=" + serializationHeader +
               ", isEntireSSTable=" + isEntireSSTable +
               ", firstKey=" + firstKey +
               ", tableId=" + tableId +
               '}';
    }

    public static final IVersionedSerializer<CassandraStreamHeader> serializer = new CassandraStreamHeaderSerializer();

    public static class CassandraStreamHeaderSerializer implements IVersionedSerializer<CassandraStreamHeader>
    {
        public void serialize(CassandraStreamHeader header, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(header.version.toString());
            out.writeUTF(header.version.format.name());

            out.writeLong(header.estimatedKeys);
            out.writeInt(header.sections.size());
            for (SSTableReader.PartitionPositionBounds section : header.sections)
            {
                out.writeLong(section.lowerPosition);
                out.writeLong(section.upperPosition);
            }
            CompressionInfo.serializer.serialize(header.compressionInfo, out, version);
            out.writeInt(header.sstableLevel);

            SerializationHeader.serializer.serialize(header.version, header.serializationHeader, out);

            header.tableId.serialize(out);
            out.writeBoolean(header.isEntireSSTable);

            if (header.isEntireSSTable)
            {
                ComponentManifest.serializers.get(header.version.format.name()).serialize(header.componentManifest, out, version);
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
            String sstableVersionString = in.readUTF();
            String formatName = in.readUTF();
            SSTableFormat<?, ?> format = Objects.requireNonNull(DatabaseDescriptor.getSSTableFormats().get(formatName),
                                                                String.format("Unknown SSTable format '%s'", formatName));
            Version sstableVersion = format.getVersion(sstableVersionString);

            long estimatedKeys = in.readLong();
            int count = in.readInt();
            List<SSTableReader.PartitionPositionBounds> sections = new ArrayList<>(count);
            for (int k = 0; k < count; k++)
                sections.add(new SSTableReader.PartitionPositionBounds(in.readLong(), in.readLong()));
            CompressionInfo compressionInfo = CompressionInfo.serializer.deserialize(in, version);
            int sstableLevel = in.readInt();

            SerializationHeader.Component header =  SerializationHeader.serializer.deserialize(sstableVersion, in);

            TableId tableId = TableId.deserialize(in);
            boolean isEntireSSTable = in.readBoolean();
            ComponentManifest manifest = null;
            DecoratedKey firstKey = null;

            if (isEntireSSTable)
            {
                manifest = ComponentManifest.serializers.get(format.name()).deserialize(in, version);
                ByteBuffer keyBuf = ByteBufferUtil.readWithVIntLength(in);
                IPartitioner partitioner = partitionerMapper.apply(tableId);
                if (partitioner == null)
                    throw new IllegalArgumentException(String.format("Could not determine partitioner for tableId %s", tableId));
                firstKey = partitioner.decorateKey(keyBuf);
            }

            return builder().withSSTableVersion(sstableVersion)
                            .withSSTableLevel(sstableLevel)
                            .withEstimatedKeys(estimatedKeys)
                            .withSections(sections)
                            .withCompressionInfo(compressionInfo)
                            .withSerializationHeader(header)
                            .withComponentManifest(manifest)
                            .isEntireSSTable(isEntireSSTable)
                            .withFirstKey(firstKey)
                            .withTableId(tableId)
                            .build();
        }

        public long serializedSize(CassandraStreamHeader header, int version)
        {
            long size = 0;
            size += TypeSizes.sizeof(header.version.toString());
            size += TypeSizes.sizeof(header.version.format.name());
            size += TypeSizes.sizeof(header.estimatedKeys);

            size += TypeSizes.sizeof(header.sections.size());
            for (SSTableReader.PartitionPositionBounds section : header.sections)
            {
                size += TypeSizes.sizeof(section.lowerPosition);
                size += TypeSizes.sizeof(section.upperPosition);
            }

            size += CompressionInfo.serializer.serializedSize(header.compressionInfo, version);
            size += TypeSizes.sizeof(header.sstableLevel);

            size += SerializationHeader.serializer.serializedSize(header.version, header.serializationHeader);

            size += header.tableId.serializedSize();
            size += TypeSizes.sizeof(header.isEntireSSTable);

            if (header.isEntireSSTable)
            {
                size += ComponentManifest.serializers.get(header.version.format.name()).serializedSize(header.componentManifest, version);
                size += ByteBufferUtil.serializedSizeWithVIntLength(header.firstKey.getKey());
            }
            return size;
        }
    }

    public static final class Builder
    {
        private Version version;
        private long estimatedKeys;
        private List<SSTableReader.PartitionPositionBounds> sections;
        private CompressionInfo compressionInfo;
        private int sstableLevel;
        private SerializationHeader.Component serializationHeader;
        private ComponentManifest componentManifest;
        private boolean isEntireSSTable;
        private DecoratedKey firstKey;
        private TableId tableId;

        public Builder withSSTableVersion(Version version)
        {
            this.version = version;
            return this;
        }

        public Builder withSSTableLevel(int sstableLevel)
        {
            this.sstableLevel = sstableLevel;
            return this;
        }

        public Builder withEstimatedKeys(long estimatedKeys)
        {
            this.estimatedKeys = estimatedKeys;
            return this;
        }

        public Builder withSections(List<SSTableReader.PartitionPositionBounds> sections)
        {
            this.sections = sections;
            return this;
        }

        public Builder withCompressionInfo(CompressionInfo compressionInfo)
        {
            this.compressionInfo = compressionInfo;
            return this;
        }

        public Builder withSerializationHeader(SerializationHeader.Component header)
        {
            this.serializationHeader = header;
            return this;
        }

        public Builder withTableId(TableId tableId)
        {
            this.tableId = tableId;
            return this;
        }

        public Builder isEntireSSTable(boolean isEntireSSTable)
        {
            this.isEntireSSTable = isEntireSSTable;
            return this;
        }

        public Builder withComponentManifest(ComponentManifest componentManifest)
        {
            this.componentManifest = componentManifest;
            return this;
        }

        public Builder withFirstKey(DecoratedKey firstKey)
        {
            this.firstKey = firstKey;
            return this;
        }

        public CassandraStreamHeader build()
        {
            checkNotNull(version);
            checkNotNull(sections);
            checkNotNull(serializationHeader);
            checkNotNull(tableId);

            if (isEntireSSTable)
            {
                checkNotNull(componentManifest);
                checkNotNull(firstKey);
            }

            return new CassandraStreamHeader(this);
        }
    }
}
