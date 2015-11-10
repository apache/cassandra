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

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.util.DataOutputPlus;
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
    public final String version;
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

    public FileMessageHeader(UUID cfId,
                             int sequenceNumber,
                             String version,
                             long estimatedKeys,
                             List<Pair<Long, Long>> sections,
                             CompressionInfo compressionInfo,
                             long repairedAt)
    {
        this.cfId = cfId;
        this.sequenceNumber = sequenceNumber;
        this.version = version;
        this.estimatedKeys = estimatedKeys;
        this.sections = sections;
        this.compressionInfo = compressionInfo;
        this.compressionMetadata = null;
        this.repairedAt = repairedAt;
    }

    public FileMessageHeader(UUID cfId,
                             int sequenceNumber,
                             String version,
                             long estimatedKeys,
                             List<Pair<Long, Long>> sections,
                             CompressionMetadata compressionMetadata,
                             long repairedAt)
    {
        this.cfId = cfId;
        this.sequenceNumber = sequenceNumber;
        this.version = version;
        this.estimatedKeys = estimatedKeys;
        this.sections = sections;
        this.compressionInfo = null;
        this.compressionMetadata = compressionMetadata;
        this.repairedAt = repairedAt;
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
        long size = 0;
        if (compressionInfo != null)
        {
            // calculate total length of transferring chunks
            for (CompressionMetadata.Chunk chunk : compressionInfo.chunks)
                size += chunk.length + 4; // 4 bytes for CRC
        }
        else if (compressionMetadata != null)
        {
            size = compressionMetadata.getTotalSizeForSections(sections);
        }
        else
        {
            for (Pair<Long, Long> section : sections)
                size += section.right - section.left;
        }
        return size;
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("Header (");
        sb.append("cfId: ").append(cfId);
        sb.append(", #").append(sequenceNumber);
        sb.append(", version: ").append(version);
        sb.append(", estimated keys: ").append(estimatedKeys);
        sb.append(", transfer size: ").append(size());
        sb.append(", compressed?: ").append(isCompressed());
        sb.append(", repairedAt: ").append(repairedAt);
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
            out.writeUTF(header.version);
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
            return compressionInfo;
        }

        public FileMessageHeader deserialize(DataInput in, int version) throws IOException
        {
            UUID cfId = UUIDSerializer.serializer.deserialize(in, MessagingService.current_version);
            int sequenceNumber = in.readInt();
            String sstableVersion = in.readUTF();
            long estimatedKeys = in.readLong();
            int count = in.readInt();
            List<Pair<Long, Long>> sections = new ArrayList<>(count);
            for (int k = 0; k < count; k++)
                sections.add(Pair.create(in.readLong(), in.readLong()));
            CompressionInfo compressionInfo = CompressionInfo.serializer.deserialize(in, MessagingService.current_version);
            long repairedAt = in.readLong();
            return new FileMessageHeader(cfId, sequenceNumber, sstableVersion, estimatedKeys, sections, compressionInfo, repairedAt);
        }

        public long serializedSize(FileMessageHeader header, int version)
        {
            long size = UUIDSerializer.serializer.serializedSize(header.cfId, version);
            size += TypeSizes.NATIVE.sizeof(header.sequenceNumber);
            size += TypeSizes.NATIVE.sizeof(header.version);
            size += TypeSizes.NATIVE.sizeof(header.estimatedKeys);

            size += TypeSizes.NATIVE.sizeof(header.sections.size());
            for (Pair<Long, Long> section : header.sections)
            {
                size += TypeSizes.NATIVE.sizeof(section.left);
                size += TypeSizes.NATIVE.sizeof(section.right);
            }
            size += CompressionInfo.serializer.serializedSize(header.compressionInfo, version);
            return size;
        }
    }
}