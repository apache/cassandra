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

import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.filter.ColumnFilter;
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

public class TrackedDataResponse
{
    private final int serializationVersion;
    private final ByteBuffer data;

    public TrackedDataResponse(int serializationVersion, ByteBuffer data)
    {
        this.serializationVersion = serializationVersion;
        this.data = data;
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

    public PartitionIterator makeIterator(ReadCommand command)
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

    public static final IVersionedSerializer<TrackedDataResponse> serializer = new IVersionedSerializer<TrackedDataResponse>()
    {
        @Override
        public void serialize(TrackedDataResponse response, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(response.serializationVersion);
            ByteBufferUtil.writeWithVIntLength(response.data, out);
        }

        @Override
        public TrackedDataResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            int serializationVersion = in.readInt();
            ByteBuffer data = ByteBufferUtil.readWithVIntLength(in);
            return new TrackedDataResponse(serializationVersion, data);
        }

        @Override
        public long serializedSize(TrackedDataResponse response, int version)
        {
            return TypeSizes.sizeof(response.serializationVersion)
                   + ByteBufferUtil.serializedSizeWithVIntLength(response.data);
        }
    };
}
