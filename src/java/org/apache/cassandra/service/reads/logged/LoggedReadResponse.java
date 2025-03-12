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

package org.apache.cassandra.service.reads.logged;

import java.io.IOException;
import java.nio.ByteBuffer;

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
import org.apache.cassandra.replication.MutationTracker.PendingRead;
import org.apache.cassandra.service.reads.IReadResponse;
import org.apache.cassandra.utils.ByteBufferUtil;

public class LoggedReadResponse implements IReadResponse
{
    public final MutationSummary summary;

    public LoggedReadResponse(MutationSummary summary)
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

    public static abstract class Data extends LoggedReadResponse
    {
        private final ByteBuffer data;

        public Data(ByteBuffer data, MutationSummary summary)
        {
            super(summary);
            this.data = data;
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
            try (DataInputBuffer in = new DataInputBuffer(data, true))
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
            catch (IOException e)
            {
                // We're deserializing in memory so this shouldn't happen
                throw new RuntimeException(e);
            }
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
        public LocalData(ByteBuffer data, MutationSummary summary)
        {
            super(data, summary);
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
        public RemoteData(int dataSerializationVersion, ByteBuffer data, MutationSummary summary)
        {
            super(data, summary);
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
        return Kind.LOGGED;
    }

    public static LoggedReadResponse createDataResponse(UnfilteredPartitionIterator partitionIterator, ReadCommand command, MutationSummary summary, PendingRead pendingRead)
    {
        partitionIterator = pendingRead.augmentResponseWithPendingWrites(partitionIterator, summary);
        return new LocalData(IReadResponse.serializeData(partitionIterator, command.columnFilter()), summary);
    }

    public static LoggedReadResponse createSummaryResponse(MutationSummary summary)
    {
        return new LoggedReadResponse(summary);
    }

    public static LoggedReadResponse fromResponse(IReadResponse response)
    {
        if (response.kind() != Kind.LOGGED)
            throw new IllegalArgumentException("Response kind must be " + Kind.LOGGED + ", got " + response.kind());
        return (LoggedReadResponse) response;
    }

    public static final IVersionedSerializer<LoggedReadResponse> serializer = new IVersionedSerializer<LoggedReadResponse>()
    {
        @Override
        public void serialize(LoggedReadResponse response, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(response.isDataResponse());
            MutationSummary.serializer.serialize(response.summary, out, version);
            if (response.isDataResponse())
                ByteBufferUtil.writeWithVIntLength(response.asDataResponse().data, out);
        }

        @Override
        public LoggedReadResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            boolean dataResponse = in.readBoolean();
            MutationSummary summary = MutationSummary.serializer.deserialize(in, version);

            if (!dataResponse)
                return new LoggedReadResponse(summary);

            ByteBuffer data = ByteBufferUtil.readWithVIntLength(in);
            return new RemoteData(version, data, summary);
        }

        @Override
        public long serializedSize(LoggedReadResponse response, int version)
        {
            long size = TypeSizes.BOOL_SIZE; // is data response
            size += MutationSummary.serializer.serializedSize(response.summary, version);
            if (response.isDataResponse())
                size += ByteBufferUtil.serializedSizeWithVIntLength(response.asDataResponse().data);

            return size;
        }
    };
}
