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

package org.apache.cassandra.service.paxos;

import java.io.IOException;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * Request to forward a consensus read operation to a replica coordinator.
 * This is used when the original coordinator is not a replica but needs to
 * execute a consensus read for a tracked keyspace that requires proper coordination.
 *
 * Consensus reads only ever contain a single read command.
 */
public class ConsensusReadForwardRequest
{
    public static final Serializer serializer = new Serializer();

    public final SinglePartitionReadCommand command;
    public final ConsistencyLevel consistencyLevel;

    public ConsensusReadForwardRequest(SinglePartitionReadCommand command, ConsistencyLevel consistencyLevel)
    {
        this.command = command;
        this.consistencyLevel = consistencyLevel;
    }

    public static class Serializer implements IVersionedSerializer<ConsensusReadForwardRequest>
    {
        @Override
        public void serialize(ConsensusReadForwardRequest forwardRequest, DataOutputPlus out, int version) throws IOException
        {
            ReadCommand.serializer.serialize(forwardRequest.command, out, version);
            out.writeByte(forwardRequest.consistencyLevel.code);
        }

        @Override
        public ConsensusReadForwardRequest deserialize(DataInputPlus in, int version) throws IOException
        {
            ReadCommand readCommand = ReadCommand.serializer.deserialize(in, version);
            if (!(readCommand instanceof SinglePartitionReadCommand))
                throw new IOException("Expected SinglePartitionReadCommand but got " + readCommand.getClass());
            ConsistencyLevel consistencyLevel = ConsistencyLevel.fromCode(in.readUnsignedByte());
            return new ConsensusReadForwardRequest((SinglePartitionReadCommand) readCommand, consistencyLevel);
        }

        @Override
        public long serializedSize(ConsensusReadForwardRequest forwardRequest, int version)
        {
            long size = ReadCommand.serializer.serializedSize(forwardRequest.command, version);
            size += 1; // consistencyLevel.code
            return size;
        }
    }
}