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
import java.util.List;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.ShortMutationId;
import org.apache.cassandra.service.reads.IReadResponse;
import org.apache.cassandra.utils.ByteBufferUtil;

public class TrackedReadResponse implements IReadResponse
{
    public final MutationSummary summary;

    public TrackedReadResponse(MutationSummary summary)
    {
        this.summary = summary;
    }

    public boolean isDataResponse()
    {
        return false;
    }

    public Data asDataResponse()
    {
        throw new IllegalArgumentException("Not a data response");
    }

    public static abstract class Data extends TrackedReadResponse
    {
        private final ByteBuffer initialData;
        private final ByteBuffer secondaryData;

        public Data(ByteBuffer initialData, ByteBuffer secondaryData, MutationSummary summary)
        {
            super(summary);
            this.initialData = initialData;
            this.secondaryData = secondaryData;
        }

        @Override
        public boolean isDataResponse()
        {
            return true;
        }

        @Override
        public Data asDataResponse()
        {
            return this;
        }

        abstract DeserializationHelper.Flag flag();

        abstract int dataSerializationVersion();

        @Override
        public UnfilteredPartitionIterator makeIterator(ReadCommand command)
        {
            try (DataInputBuffer initialIn = new DataInputBuffer(initialData, true);
                 DataInputBuffer secondaryIn = new DataInputBuffer(secondaryData, true))
            {
                List<UnfilteredPartitionIterator> iterators = new ArrayList<>(2);
                iterators.add(makeIterator(initialIn, command));
                iterators.add(makeIterator(secondaryIn, command));
                return UnfilteredPartitionIterators.merge(iterators, UnfilteredPartitionIterators.MergeListener.NOOP);
            }
            catch (IOException e)
            {
                // We're deserializing in memory so this shouldn't happen
                throw new RuntimeException(e);
            }
        }

        private UnfilteredPartitionIterator makeIterator(DataInputBuffer in, ReadCommand command) throws IOException
        {
            // Note that the command parameter shadows the 'command' field and this is intended because
            // the later can be null (for RemoteDataResponse as those are created in the serializers and
            // those don't have easy access to the command). This is also why we need the command as parameter here.
            return UnfilteredPartitionIterators.serializerForIntraNode().deserialize(in,
                                                                                     dataSerializationVersion(),
                                                                                     command.metadata(),
                                                                                     command.columnFilter(),
                                                                                     flag());
        }
    }

    @Override
    public UnfilteredPartitionIterator makeIterator(ReadCommand command)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toDebugString(ReadCommand command, DecoratedKey key)
    {
        throw new UnsupportedOperationException();
    }

    public static class LocalData extends Data
    {
        public LocalData(ByteBuffer initialData, ByteBuffer secondaryData, MutationSummary summary)
        {
            super(initialData, secondaryData, summary);
        }

        @Override
        DeserializationHelper.Flag flag()
        {
            return DeserializationHelper.Flag.LOCAL;
        }

        @Override
        int dataSerializationVersion()
        {
            return MessagingService.current_version;
        }
    }

    public static class RemoteData extends Data
    {
        private final int dataSerializationVersion;

        public RemoteData(int dataSerializationVersion, ByteBuffer initialData, ByteBuffer secondaryData, MutationSummary summary)
        {
            super(initialData, secondaryData, summary);
            this.dataSerializationVersion = dataSerializationVersion;
        }

        @Override
        DeserializationHelper.Flag flag()
        {
            return DeserializationHelper.Flag.FROM_REMOTE;
        }

        @Override
        int dataSerializationVersion()
        {
            return dataSerializationVersion;
        }
    }

    @Override
    public Kind kind()
    {
        return Kind.TRACKED;
    }

    public static TrackedReadResponse createDataResponse(UnfilteredPartitionIterator partitionIterator, ReadCommand command, MutationSummary initialSummary)
    {
        // Complete/materialize the initial read
        ByteBuffer initialData = IReadResponse.serializeData(partitionIterator, command.columnFilter());

        // Create another summary once initial data has been read fully. We do this to catch
        // any mutations that may have arrived during initial read execution.
        MutationSummary secondarySummary = command.createMutationSummary(true);

        // Compute any mutations that we could've missed during initial read execution.
        ArrayList<ShortMutationId> delta = new ArrayList<>();
        MutationSummary.difference(secondarySummary, initialSummary, delta);

        // Merge the potentially missed mutations together, and send the merged result
        // alongside initial data read. Some redundancy is possible, but it should be minimal to non-existant
        // most of the time.
        UnfilteredPartitionIterator journalIterator = command.queryJournal(delta);
        ByteBuffer secondaryData = IReadResponse.serializeData(journalIterator, command.columnFilter());

        return new LocalData(initialData, secondaryData, secondarySummary);
    }

    public static TrackedReadResponse createSummaryResponse(MutationSummary summary)
    {
        return new TrackedReadResponse(summary);
    }

    public static TrackedReadResponse fromResponse(IReadResponse response)
    {
        if (response.kind() != Kind.TRACKED)
            throw new IllegalArgumentException("Response kind must be " + Kind.TRACKED + ", got " + response.kind());
        return (TrackedReadResponse) response;
    }

    public static final IVersionedSerializer<TrackedReadResponse> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(TrackedReadResponse response, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(response.isDataResponse());
            MutationSummary.serializer.serialize(response.summary, out, version);

            if (response.isDataResponse())
            {
                ByteBufferUtil.writeWithVIntLength(response.asDataResponse().initialData, out);
                ByteBufferUtil.writeWithVIntLength(response.asDataResponse().secondaryData, out);
            }
        }

        @Override
        public TrackedReadResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            boolean dataResponse = in.readBoolean();
            MutationSummary summary = MutationSummary.serializer.deserialize(in, version);

            if (!dataResponse)
                return new TrackedReadResponse(summary);

            ByteBuffer initialData = ByteBufferUtil.readWithVIntLength(in);
            ByteBuffer secondaryData = ByteBufferUtil.readWithVIntLength(in);
            return new RemoteData(version, initialData, secondaryData, summary);
        }

        @Override
        public long serializedSize(TrackedReadResponse response, int version)
        {
            long size = TypeSizes.BOOL_SIZE; // is data response
            size += MutationSummary.serializer.serializedSize(response.summary, version);
            if (response.isDataResponse())
            {
                size += ByteBufferUtil.serializedSizeWithVIntLength(response.asDataResponse().initialData);
                size += ByteBufferUtil.serializedSizeWithVIntLength(response.asDataResponse().secondaryData);
            }
            return size;
        }
    };
}
