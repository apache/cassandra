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

package org.apache.cassandra.db;

import java.io.IOException;

import com.google.common.base.Preconditions;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.reads.tracked.TrackedRead.DataRequest;
import org.apache.cassandra.service.reads.tracked.TrackedRead.SummaryRequest;

import static org.apache.cassandra.db.ReadKind.UNTRACKED;

/**
 * Interface for read command that allows it be serialized and embedded in another message. Used in Paxos
 * to provide a common base class to serialize for tracked and untracked reads. Tracked reads contain
 * additional information needed to execute the read beyond the read command itself so an additional interface
 * is needed.
 */
public interface EmbeddableSinglePartitionReadCommand
{
    ReadKind kind();

    default boolean isTracked()
    {
        return kind().isTracked();
    }

    TableMetadata metadata();

    DecoratedKey partitionKey();

    IVersionedSerializer<EmbeddableSinglePartitionReadCommand> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(EmbeddableSinglePartitionReadCommand command, DataOutputPlus out, int version) throws IOException
        {
            if (version >= MessagingService.VERSION_61)
                ReadKind.serializer.serialize(command.kind(), out);
            else
                Preconditions.checkArgument(command.kind() == UNTRACKED);

            switch (command.kind())
            {
                case UNTRACKED:
                    ReadCommand.serializer.serialize((ReadCommand) command, out, version);
                    break;
                case TRACKED_DATA:
                    DataRequest.embedded.serialize((DataRequest) command, out, version);
                    break;
                case TRACKED_SUMMARY:
                    SummaryRequest.embedded.serialize((SummaryRequest) command, out, version);
                    break;
                default:
                    throw new IllegalStateException("Unhandled kind: " + command.kind());
            }
        }

        @Override
        public EmbeddableSinglePartitionReadCommand deserialize(DataInputPlus in, int version) throws IOException
        {

            ReadKind kind = version >= MessagingService.VERSION_61 ? ReadKind.serializer.deserialize(in) : UNTRACKED;
            switch (kind)
            {
                case UNTRACKED:
                    return (SinglePartitionReadCommand)ReadCommand.serializer.deserialize(in, version);
                case TRACKED_DATA:
                    return DataRequest.embedded.deserialize(in, version);
                case TRACKED_SUMMARY:
                    return SummaryRequest.embedded.deserialize(in, version);
                default:
                    throw new IllegalStateException("Unhandled kind: " + kind);
            }
        }

        @Override
        public long serializedSize(EmbeddableSinglePartitionReadCommand command, int version)
        {
            long size = 0;
            if (version >= MessagingService.VERSION_61)
                size += ReadKind.serializer.serializedSize(command.kind());
            else
                Preconditions.checkArgument(command.kind() == UNTRACKED);

            switch (command.kind())
            {
                case UNTRACKED:
                    return size + ReadCommand.serializer.serializedSize((ReadCommand) command, version);
                case TRACKED_DATA:
                    return size + DataRequest.embedded.serializedSize((DataRequest) command, version);
                case TRACKED_SUMMARY:
                    return size + SummaryRequest.embedded.serializedSize((SummaryRequest) command, version);
                default:
                    throw new IllegalStateException("Unhandled kind: " + command.kind());
            }
        }
    };
}
