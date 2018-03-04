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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.Pair;

public class CassandraStreamHeader
{
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
    private final CompressionMetadata compressionMetadata;
    public volatile CompressionInfo compressionInfo;
    public final int sstableLevel;
    public final SerializationHeader.Component header;

    /* cached size value */
    private transient final long size;

    private CassandraStreamHeader(Version version, SSTableFormat.Type format, long estimatedKeys, List<Pair<Long, Long>> sections, CompressionMetadata compressionMetadata, CompressionInfo compressionInfo, int sstableLevel, SerializationHeader.Component header)
    {
        this.version = version;
        this.format = format;
        this.estimatedKeys = estimatedKeys;
        this.sections = sections;
        this.compressionMetadata = compressionMetadata;
        this.compressionInfo = compressionInfo;
        this.sstableLevel = sstableLevel;
        this.header = header;

        this.size = calculateSize();
    }

    public CassandraStreamHeader(Version version, SSTableFormat.Type format, long estimatedKeys, List<Pair<Long, Long>> sections, CompressionMetadata compressionMetadata, int sstableLevel, SerializationHeader.Component header)
    {
        this(version, format, estimatedKeys, sections, compressionMetadata, null, sstableLevel, header);
    }

    public CassandraStreamHeader(Version version, SSTableFormat.Type format, long estimatedKeys, List<Pair<Long, Long>> sections, CompressionInfo compressionInfo, int sstableLevel, SerializationHeader.Component header)
    {
        this(version, format, estimatedKeys, sections, null, compressionInfo, sstableLevel, header);
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
        if (compressionInfo != null)
        {
            // calculate total length of transferring chunks
            for (CompressionMetadata.Chunk chunk : compressionInfo.chunks)
                transferSize += chunk.length + 4; // 4 bytes for CRC
        }
        else
        {
            for (Pair<Long, Long> section : sections)
                transferSize += section.right - section.left;
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
               '}';
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CassandraStreamHeader that = (CassandraStreamHeader) o;
        return estimatedKeys == that.estimatedKeys &&
               sstableLevel == that.sstableLevel &&
               Objects.equals(version, that.version) &&
               format == that.format &&
               Objects.equals(sections, that.sections) &&
               Objects.equals(compressionInfo, that.compressionInfo) &&
               Objects.equals(header, that.header);
    }

    public int hashCode()
    {
        return Objects.hash(version, format, estimatedKeys, sections, compressionInfo, sstableLevel, header);
    }


    public static final IVersionedSerializer<CassandraStreamHeader> serializer = new IVersionedSerializer<CassandraStreamHeader>()
    {
        public void serialize(CassandraStreamHeader header, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(header.version.toString());
            out.writeUTF(header.format.name);

            out.writeLong(header.estimatedKeys);
            out.writeInt(header.sections.size());
            for (Pair<Long, Long> section : header.sections)
            {
                out.writeLong(section.left);
                out.writeLong(section.right);
            }
            header.calculateCompressionInfo();
            CompressionInfo.serializer.serialize(header.compressionInfo, out, version);
            out.writeInt(header.sstableLevel);
            SerializationHeader.serializer.serialize(header.version, header.header, out);
        }

        public CassandraStreamHeader deserialize(DataInputPlus in, int version) throws IOException
        {
            Version sstableVersion = SSTableFormat.Type.current().info.getVersion(in.readUTF());
            SSTableFormat.Type format = SSTableFormat.Type.validate(in.readUTF());

            long estimatedKeys = in.readLong();
            int count = in.readInt();
            List<Pair<Long, Long>> sections = new ArrayList<>(count);
            for (int k = 0; k < count; k++)
                sections.add(Pair.create(in.readLong(), in.readLong()));
            CompressionInfo compressionInfo = CompressionInfo.serializer.deserialize(in, version);
            int sstableLevel = in.readInt();
            SerializationHeader.Component header =  SerializationHeader.serializer.deserialize(sstableVersion, in);

            return new CassandraStreamHeader(sstableVersion, format, estimatedKeys, sections, compressionInfo, sstableLevel, header);
        }

        public long serializedSize(CassandraStreamHeader header, int version)
        {
            long size = 0;
            size += TypeSizes.sizeof(header.version.toString());
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

            size += SerializationHeader.serializer.serializedSize(header.version, header.header);

            return size;
        }
    };
}
