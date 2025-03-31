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

package org.apache.cassandra.service.reads;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.reads.tracked.TrackedReadResponse;

public interface IReadResponse
{
    enum Kind
    {
        UNTRACKED,
        TRACKED;

        public static final IVersionedSerializer<Kind> serializer = new IVersionedSerializer<>()
        {
            @Override
            public void serialize(Kind kind, DataOutputPlus out, int version) throws IOException
            {
                switch (kind)
                {
                    case UNTRACKED:
                        out.writeByte(0);
                        break;
                    case TRACKED:
                        out.writeByte(1);
                        break;
                    default:
                        throw new IllegalStateException("Unhandled kind: " + kind);
                }
            }

            @Override
            public Kind deserialize(DataInputPlus in, int version) throws IOException
            {
                int tag = in.readByte();
                switch (tag)
                {
                    case 0:
                        return UNTRACKED;
                    case 1:
                        return TRACKED;
                    default:
                        throw new IllegalStateException("Unhandled kind value: " + tag);
                }
            }

            @Override
            public long serializedSize(Kind t, int version)
            {
                return TypeSizes.BYTE_SIZE;
            }
        };
    }

    Kind kind();
    UnfilteredPartitionIterator makeIterator(ReadCommand command);
    String toDebugString(ReadCommand command, DecoratedKey key);

    static ByteBuffer serializeData(UnfilteredPartitionIterator iter, ColumnFilter selection)
    {
        try (DataOutputBuffer buffer = new DataOutputBuffer())
        {
            UnfilteredPartitionIterators.serializerForIntraNode().serialize(iter, selection, buffer, MessagingService.current_version);
            return buffer.buffer();
        }
        catch (IOException e)
        {
            // We're serializing in memory so this shouldn't happen
            throw new RuntimeException(e);
        }
    }

    IVersionedSerializer<IReadResponse> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(IReadResponse response, DataOutputPlus out, int version) throws IOException
        {
            if (version >= MessagingService.VERSION_52)
                Kind.serializer.serialize(response.kind(), out, version);
            else
                Preconditions.checkArgument(response.kind() == Kind.UNTRACKED);

            switch (response.kind())
            {
                case UNTRACKED:
                    ReadResponse.serializer.serialize((ReadResponse) response, out, version);
                    break;
                case TRACKED:
                    TrackedReadResponse.serializer.serialize((TrackedReadResponse) response, out, version);
                    break;
                default:
                    throw new IllegalStateException("Unhandled kind: " + response.kind());
            }
        }

        @Override
        public IReadResponse deserialize(DataInputPlus in, int version) throws IOException
        {

            Kind kind = version >= MessagingService.VERSION_52 ? Kind.serializer.deserialize(in, version) : Kind.UNTRACKED;
            switch (kind)
            {
                case UNTRACKED:
                    return ReadResponse.serializer.deserialize(in, version);
                case TRACKED:
                    return TrackedReadResponse.serializer.deserialize(in, version);
                default:
                    throw new IllegalStateException("Unhandled kind: " + kind);
            }
        }

        @Override
        public long serializedSize(IReadResponse response, int version)
        {
            long size = 0;
            if (version >= MessagingService.VERSION_52)
                size += Kind.serializer.serializedSize(response.kind(), version);
            else
                Preconditions.checkArgument(response.kind() == Kind.UNTRACKED);

            switch (response.kind())
            {
                case UNTRACKED:
                    return size + ReadResponse.serializer.serializedSize((ReadResponse) response, version);
                case TRACKED:
                    return size + TrackedReadResponse.serializer.serializedSize((TrackedReadResponse) response, version);
                default:
                    throw new IllegalStateException("Unhandled kind: " + response.kind());
            }
        }
    };
}
