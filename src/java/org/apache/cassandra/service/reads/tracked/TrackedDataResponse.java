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

package org.apache.cassandra.service.reads.tracked;

import org.apache.cassandra.db.IReadResponse;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadKind;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterators;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Preconditions;

public class TrackedDataResponse implements IReadResponse
{
    private final int serializationVersion;
    private final List<ByteBuffer> data;

    public TrackedDataResponse(int serializationVersion, ByteBuffer data)
    {
        this(serializationVersion, Collections.singletonList(data));
    }

    private TrackedDataResponse(int serializationVersion, List<ByteBuffer> data)
    {
        Preconditions.checkArgument(!data.isEmpty());
        this.serializationVersion = serializationVersion;
        this.data = data;
    }

    public TrackedDataResponse merge(TrackedDataResponse that)
    {
        return merge(this, that);
    }

    public static TrackedDataResponse merge(TrackedDataResponse l, TrackedDataResponse r)
    {
        Preconditions.checkArgument(l.serializationVersion == r.serializationVersion);
        List<ByteBuffer> newData = new ArrayList<>(l.data.size() + r.data.size());
        newData.addAll(l.data);
        newData.addAll(r.data);
        return new TrackedDataResponse(l.serializationVersion, newData);
    }

    public static TrackedDataResponse merge(List<TrackedDataResponse> responses)
    {
        Preconditions.checkArgument(!responses.isEmpty());

        int version = responses.get(0).serializationVersion;
        int size = responses.get(0).data.size();

        for (int i=1,mi=responses.size(); i<mi; i++)
        {
            Preconditions.checkState(responses.get(i).serializationVersion == version);
            size += responses.get(i).data.size();
        }

        List<ByteBuffer> newData = new ArrayList<>(size);
        for (int i=0,mi=responses.size(); i<mi; i++)
            newData.addAll(responses.get(i).data);

        return new TrackedDataResponse(version, newData);
    }

    public static TrackedDataResponse create(PartitionIterator iter, ColumnFilter selection)
    {
        try (DataOutputBuffer buffer = new DataOutputBuffer())
        {
            PartitionIterators.Serializer.serialize(iter, selection, buffer, MessagingService.current_version);
            return new TrackedDataResponse(MessagingService.current_version, buffer.buffer(false));
        }
        catch (IOException e)
        {
            // We're serializing in memory so this shouldn't happen
            throw new RuntimeException(e);
        }
    }

    private static PartitionIterator makeIterator(int serializationVersion, ByteBuffer data, ReadCommand command)
    {
        try (DataInputBuffer in = new DataInputBuffer(data, true))
        {
            return PartitionIterators.Serializer.deserialize(command.metadata(), command.columnFilter(), in, serializationVersion);
        }
        catch (IOException e)
        {
            // We're deserializing in memory so this shouldn't happen
            throw new RuntimeException(e);
        }
    }

    public PartitionIterator makeIteratorUnlimited(ReadCommand command)
    {
        if (data.size() == 1)
            return makeIterator(serializationVersion, data.get(0), command);

        List<PartitionIterator> iterators = new ArrayList<>(data.size());
        for (ByteBuffer buffer : data)
            iterators.add(makeIterator(serializationVersion, buffer, command));
        return PartitionIterators.mergeNonOverlapping(iterators);
    }

    public PartitionIterator makeIterator(ReadCommand command)
    {
        DataLimits.Counter counter = command.limits().newCounter(command.nowInSec(),
                                                                 true,
                                                                 command.selectsFullPartition(),
                                                                 command.metadata().enforceStrictLiveness());
        return counter.applyTo(makeIteratorUnlimited(command));
    }

    public static final IVersionedSerializer<TrackedDataResponse> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(TrackedDataResponse response, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(response.serializationVersion);
            out.writeInt(response.data.size());
            for (ByteBuffer buffer : response.data)
                ByteBufferUtil.writeWithVIntLength(buffer, out);
        }

        @Override
        public TrackedDataResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            int serializationVersion = in.readInt();
            int size = in.readInt();
            List<ByteBuffer> data = new ArrayList<>(size);
            for (int i = 0; i < size; i++)
                data.add(ByteBufferUtil.readWithVIntLength(in));
            return new TrackedDataResponse(serializationVersion, data);
        }

        @Override
        public long serializedSize(TrackedDataResponse response, int version)
        {
            long size = TypeSizes.sizeof(response.serializationVersion) + TypeSizes.sizeof(response.data.size());
            for (ByteBuffer buffer : response.data)
                size += ByteBufferUtil.serializedSizeWithVIntLength(buffer);
            return size;
        }
    };

    @Override
    public ReadKind kind()
    {
        return ReadKind.TRACKED_DATA;
    }
}
