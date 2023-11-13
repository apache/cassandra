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
package org.apache.cassandra.streaming;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.IPartitionerDependentSerializer;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.CollectionSerializers;

/**
 * Summary of streaming.
 */
public class StreamSummary implements Serializable
{
    public static final IPartitionerDependentSerializer<StreamSummary> serializer = new StreamSummarySerializer();

    public final TableId tableId;
    public final List<Range<Token>> ranges;

    /**
     * Number of files to transfer. Can be 0 if nothing to transfer for some streaming request.
     */
    public final int files;
    public final long totalSize;

    public StreamSummary(TableId tableId, List<Range<Token>> ranges, int files, long totalSize)
    {
        this.tableId = tableId;
        this.ranges = ranges;
        this.files = files;
        this.totalSize = totalSize;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StreamSummary summary = (StreamSummary) o;
        return files == summary.files && totalSize == summary.totalSize && tableId.equals(summary.tableId) && ranges.equals(summary.ranges);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(tableId, ranges, files, totalSize);
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("StreamSummary{");
        sb.append("path=").append(tableId);
        sb.append(", ranges=").append(ranges);
        sb.append(", files=").append(files);
        sb.append(", totalSize=").append(totalSize);
        sb.append('}');
        return sb.toString();
    }

    public static class StreamSummarySerializer implements IPartitionerDependentSerializer<StreamSummary>
    {
        public void serialize(StreamSummary summary, DataOutputPlus out, int version) throws IOException
        {
            summary.tableId.serialize(out);
            out.writeInt(summary.files);
            out.writeLong(summary.totalSize);
            if (version >= MessagingService.VERSION_50)
                CollectionSerializers.serializeCollection(summary.ranges, out, version, Range.rangeSerializer);
        }

        public StreamSummary deserialize(DataInputPlus in, IPartitioner p, int version) throws IOException
        {
            TableId tableId = TableId.deserialize(in);
            int files = in.readInt();
            long totalSize = in.readLong();
            List<Range<Token>> ranges = ImmutableList.of();
            if (version >= MessagingService.VERSION_50)
                ranges = CollectionSerializers.deserializeList(in, p, version, Range.rangeSerializer);
            return new StreamSummary(tableId, ranges, files, totalSize);
        }

        public long serializedSize(StreamSummary summary, int version)
        {
            long size = summary.tableId.serializedSize();
            size += TypeSizes.sizeof(summary.files);
            size += TypeSizes.sizeof(summary.totalSize);
            if (version >= MessagingService.VERSION_50)
                size += CollectionSerializers.serializedCollectionSize(summary.ranges, version, Range.rangeSerializer);
            return size;
        }
    }
}
