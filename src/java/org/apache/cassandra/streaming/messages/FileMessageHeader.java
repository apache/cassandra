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
package org.apache.cassandra.streaming.messages;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.streaming.compress.CompressionInfo;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDSerializer;

/**
 * StreamingFileHeader is appended before sending actual data to describe what it's sending.
 */
public class FileMessageHeader
{
    public static FileMessageHeaderSerializer serializer = new FileMessageHeaderSerializer();

    public final UUID cfId;
    public final int sequenceNumber;
    /** SSTable version */
    public final Version version;

    /** SSTable format **/
    public final SSTableFormat.Type format;
    public final long estimatedKeys;
    public final List<Pair<Long, Long>> sections;
    /**
     * Compression info for SSTable to send. Can be null if SSTable is not compressed.
     * On sender, this field is always null to avoid holding large number of Chunks.
     * Use compressionMetadata instead.
     */
    public final CompressionInfo compressionInfo;
    private final CompressionMetadata compressionMetadata;
    public final long repairedAt;
    public final int sstableLevel;
    public final SerializationHeader.Component header;

    /* cached size value */
    private transient final long size;

    public FileMessageHeader(UUID cfId,
                             int sequenceNumber,
                             Version version,
                             SSTableFormat.Type format,
                             long estimatedKeys,
                             List<Pair<Long, Long>> sections,
                             CompressionInfo compressionInfo,
                             long repairedAt,
                             int sstableLevel,
                             SerializationHeader.Component header)
    {
        this.cfId = cfId;
        this.sequenceNumber = sequenceNumber;
        this.version = version;
        this.format = format;
        this.estimatedKeys = estimatedKeys;
        this.sections = sections;
        this.compressionInfo = compressionInfo;
        this.compressionMetadata = null;
        this.repairedAt = repairedAt;
        this.sstableLevel = sstableLevel;
        this.header = header;
        this.size = calculateSize();
    }

    public FileMessageHeader(UUID cfId,
                             int sequenceNumber,
                             Version version,
                             SSTableFormat.Type format,
                             long estimatedKeys,
                             List<Pair<Long, Long>> sections,
                             CompressionMetadata compressionMetadata,
                             long repairedAt,
                             int sstableLevel,
                             SerializationHeader.Component header)
    {
        this.cfId = cfId;
        this.sequenceNumber = sequenceNumber;
        this.version = version;
        this.format = format;
        this.estimatedKeys = estimatedKeys;
        this.sections = sections;
        this.compressionInfo = null;
        this.compressionMetadata = compressionMetadata;
        this.repairedAt = repairedAt;
        this.sstableLevel = sstableLevel;
        this.header = header;
        this.size = calculateSize();
    }

    public boolean isCompressed()
    {
        return compressionInfo != null || compressionMetadata != null;
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
        if (compressionInfo != null)
        {
            // calculate total length of transferring chunks
            for (CompressionMetadata.Chunk chunk : compressionInfo.chunks)
                transferSize += chunk.length + 4; // 4 bytes for CRC
        }
        else if (compressionMetadata != null)
        {
            transferSize = compressionMetadata.getTotalSizeForSections(sections);
        }
        else
        {
            for (Pair<Long, Long> section : sections)
                transferSize += section.right - section.left;
        }
        return transferSize;
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("Header (");
        sb.append("cfId: ").append(cfId);
        sb.append(", #").append(sequenceNumber);
        sb.append(", version: ").append(version);
        sb.append(", format: ").append(format);
        sb.append(", estimated keys: ").append(estimatedKeys);
        sb.append(", transfer size: ").append(size());
        sb.append(", compressed?: ").append(isCompressed());
        sb.append(", repairedAt: ").append(repairedAt);
        sb.append(", level: ").append(sstableLevel);
        sb.append(')');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileMessageHeader that = (FileMessageHeader) o;
        return sequenceNumber == that.sequenceNumber && cfId.equals(that.cfId);
    }

    @Override
    public int hashCode()
    {
        int result = cfId.hashCode();
        result = 31 * result + sequenceNumber;
        return result;
    }

    static class FileMessageHeaderSerializer
    {
        public CompressionInfo serialize(FileMessageHeader header, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(header.cfId, out, version);
            out.writeInt(header.sequenceNumber);
            out.writeUTF(header.version.toString());

            //We can't stream to a node that doesn't understand a new sstable format
            if (version < StreamMessage.VERSION_22 && header.format != SSTableFormat.Type.LEGACY && header.format != SSTableFormat.Type.BIG)
                throw new UnsupportedOperationException("Can't stream non-legacy sstables to nodes < 2.2");

            if (version >= StreamMessage.VERSION_22)
                out.writeUTF(header.format.name);

            out.writeLong(header.estimatedKeys);
            out.writeInt(header.sections.size());
            for (Pair<Long, Long> section : header.sections)
            {
                out.writeLong(section.left);
                out.writeLong(section.right);
            }
            // construct CompressionInfo here to avoid holding large number of Chunks on heap.
            CompressionInfo compressionInfo = null;
            if (header.compressionMetadata != null)
                compressionInfo = new CompressionInfo(header.compressionMetadata.getChunksForSections(header.sections), header.compressionMetadata.parameters);
            CompressionInfo.serializer.serialize(compressionInfo, out, version);
            out.writeLong(header.repairedAt);
            out.writeInt(header.sstableLevel);

            if (version >= StreamMessage.VERSION_30 && header.version.storeRows())
                SerializationHeader.serializer.serialize(header.version, header.header, out);
            return compressionInfo;
        }

        public FileMessageHeader deserialize(DataInputPlus in, int version) throws IOException
        {
            UUID cfId = UUIDSerializer.serializer.deserialize(in, MessagingService.current_version);
            int sequenceNumber = in.readInt();
            Version sstableVersion = SSTableFormat.Type.current().info.getVersion(in.readUTF());

            SSTableFormat.Type format = SSTableFormat.Type.LEGACY;
            if (version >= StreamMessage.VERSION_22)
                format = SSTableFormat.Type.validate(in.readUTF());

            long estimatedKeys = in.readLong();
            int count = in.readInt();
            List<Pair<Long, Long>> sections = new ArrayList<>(count);
            for (int k = 0; k < count; k++)
                sections.add(Pair.create(in.readLong(), in.readLong()));
            CompressionInfo compressionInfo = CompressionInfo.serializer.deserialize(in, MessagingService.current_version);
            long repairedAt = in.readLong();
            int sstableLevel = in.readInt();
            SerializationHeader.Component header = version >= StreamMessage.VERSION_30 && sstableVersion.storeRows()
                                                 ? SerializationHeader.serializer.deserialize(sstableVersion, in)
                                                 : null;

            return new FileMessageHeader(cfId, sequenceNumber, sstableVersion, format, estimatedKeys, sections, compressionInfo, repairedAt, sstableLevel, header);
        }

        public long serializedSize(FileMessageHeader header, int version)
        {
            long size = UUIDSerializer.serializer.serializedSize(header.cfId, version);
            size += TypeSizes.sizeof(header.sequenceNumber);
            size += TypeSizes.sizeof(header.version.toString());

            if (version >= StreamMessage.VERSION_22)
                size += TypeSizes.sizeof(header.format.name);

            size += TypeSizes.sizeof(header.estimatedKeys);

            size += TypeSizes.sizeof(header.sections.size());
            for (Pair<Long, Long> section : header.sections)
            {
                size += TypeSizes.sizeof(section.left);
                size += TypeSizes.sizeof(section.right);
            }
            size += CompressionInfo.serializer.serializedSize(header.compressionInfo, version);
            size += TypeSizes.sizeof(header.sstableLevel);

            if (version >= StreamMessage.VERSION_30)
                size += SerializationHeader.serializer.serializedSize(header.version, header.header);

            return size;
        }
    }
}
