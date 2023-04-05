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

package org.apache.cassandra.service.paxos.cleanup;

import java.io.IOException;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.service.paxos.PaxosRepairHistory;

public class PaxosCleanupHistory
{
    final TableId tableId;
    final Ballot highBound;
    final PaxosRepairHistory history;

    public PaxosCleanupHistory(TableId tableId, Ballot highBound, PaxosRepairHistory history)
    {
        this.tableId = tableId;
        this.highBound = highBound;
        this.history = history;
    }

    public static final IVersionedSerializer<PaxosCleanupHistory> serializer = new IVersionedSerializer<PaxosCleanupHistory>()
    {
        public void serialize(PaxosCleanupHistory message, DataOutputPlus out, int version) throws IOException
        {
            message.tableId.serialize(out);
            message.highBound.serialize(out);
            PaxosRepairHistory.serializer.serialize(message.history, out, version);
        }

        public PaxosCleanupHistory deserialize(DataInputPlus in, int version) throws IOException
        {
            TableId tableId = TableId.deserialize(in);
            Ballot lowBound = Ballot.deserialize(in);
            PaxosRepairHistory history = PaxosRepairHistory.serializer.deserialize(in, version);
            return new PaxosCleanupHistory(tableId, lowBound, history);
        }

        public long serializedSize(PaxosCleanupHistory message, int version)
        {
            long size = message.tableId.serializedSize();
            size += Ballot.sizeInBytes();
            size += PaxosRepairHistory.serializer.serializedSize(message.history, version);
            return size;
        }
    };
}
