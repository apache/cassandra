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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.IReadResponse;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadKind;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterators;
import org.apache.cassandra.io.EmbeddedAsymmetricVersionedSerializer;
import org.apache.cassandra.io.IVersionedAsymmetricSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.replication.Version;
import org.apache.cassandra.replication.VersionedSerializer;
import org.apache.cassandra.utils.ArraySerializers;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CollectionSerializers;

public class TrackedDataResponse implements IReadResponse
{
    private final int[] versions;
    private final List<ByteBuffer> data;

    public TrackedDataResponse(int version, ByteBuffer data)
    {
        this(new int[] { version }, Collections.singletonList(data));
    }

    private TrackedDataResponse(int[] versions, List<ByteBuffer> data)
    {
        Preconditions.checkArgument(!data.isEmpty());
        Preconditions.checkArgument(versions.length == data.size());
        this.versions = versions;
        this.data = data;
    }

    public TrackedDataResponse merge(TrackedDataResponse that)
    {
        return merge(this, that);
    }

    public static TrackedDataResponse merge(TrackedDataResponse l, TrackedDataResponse r)
    {
        int[] newVersions = new int[l.versions.length + r.versions.length];
        List<ByteBuffer> newData = new ArrayList<>(l.data.size() + r.data.size());
        System.arraycopy(l.versions, 0, newVersions, 0, l.versions.length);
        System.arraycopy(r.versions, 0, newVersions, l.versions.length, r.versions.length);
        newData.addAll(l.data);
        newData.addAll(r.data);
        return new TrackedDataResponse(newVersions, newData);
    }

    public static TrackedDataResponse merge(List<TrackedDataResponse> responses)
    {
        Preconditions.checkArgument(!responses.isEmpty());

        int size = 0;
        for (TrackedDataResponse response : responses)
            size += response.data.size();

        int[] newVersions = new int[size];
        List<ByteBuffer> newData = new ArrayList<>(size);

        int offset = 0;
        for (TrackedDataResponse response : responses)
        {
            System.arraycopy(response.versions, 0, newVersions, offset, response.versions.length);
            offset += response.versions.length;
            newData.addAll(response.data);
        }

        return new TrackedDataResponse(newVersions, newData);
    }

    public static TrackedDataResponse create(PartitionIterator iter, ColumnFilter selection)
    {
        try (DataOutputBuffer buffer = new DataOutputBuffer())
        {
            int version = Version.CLUSTER_SAFE_VERSION.messagingVersion();
            PartitionIterators.Serializer.serialize(iter, selection, buffer, version);
            return new TrackedDataResponse(version, buffer.buffer(false));
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
            return makeIterator(versions[0], data.get(0), command);
        List<PartitionIterator> iterators = new ArrayList<>(data.size());
        for (int i = 0; i < data.size(); i++)
            iterators.add(makeIterator(versions[i], data.get(i), command));
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

    private static final VersionedSerializer<TrackedDataResponse> serializer = new VersionedSerializer<>()
    {
        @Override
        public void serialize(TrackedDataResponse response, DataOutputPlus out, Version version) throws IOException
        {
            ArraySerializers.serializeVIntArray(response.versions, out);
            CollectionSerializers.serializeList(response.data, out, ByteBufferUtil.byteBufferSerializer);
        }

        @Override
        public TrackedDataResponse deserialize(DataInputPlus in, Version version) throws IOException
        {
            int[] versions = ArraySerializers.deserializeVIntArray(in);
            List<ByteBuffer> data = CollectionSerializers.deserializeList(in, ByteBufferUtil.byteBufferSerializer);
            return new TrackedDataResponse(versions, data);
        }

        @Override
        public long serializedSize(TrackedDataResponse response, Version version)
        {
            long size = ArraySerializers.serializedVIntArraySize(response.versions);
            size += CollectionSerializers.serializedListSize(response.data, ByteBufferUtil.byteBufferSerializer);
            return size;
        }
    };

    public static final IVersionedAsymmetricSerializer<TrackedDataResponse, TrackedDataResponse> embedded =
        EmbeddedAsymmetricVersionedSerializer.mtEmbedded(serializer);

    @Override
    public ReadKind kind()
    {
        return ReadKind.TRACKED_DATA;
    }
}
