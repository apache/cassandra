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

package org.apache.cassandra.service.accord.serializers;

import java.io.IOException;

import accord.messages.Commit;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class CommitSerializer
{
    public static final IVersionedSerializer<Commit> request = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(Commit commit, DataOutputPlus out, int version) throws IOException
        {
            TopologySerializers.requestScope.serialize(commit.scope(), out, version);
            CommandSerializers.txnId.serialize(commit.txnId, out, version);
            CommandSerializers.txn.serialize(commit.txn, out, version);
            CommandSerializers.timestamp.serialize(commit.executeAt, out, version);
            CommandSerializers.deps.serialize(commit.deps, out, version);
            out.writeBoolean(commit.read);
        }

        @Override
        public Commit deserialize(DataInputPlus in, int version) throws IOException
        {
            return new Commit(TopologySerializers.requestScope.deserialize(in, version),
                              CommandSerializers.txnId.deserialize(in, version),
                              CommandSerializers.txn.deserialize(in, version),
                              CommandSerializers.timestamp.deserialize(in, version),
                              CommandSerializers.deps.deserialize(in, version),
                              in.readBoolean());
        }

        @Override
        public long serializedSize(Commit commit, int version)
        {
            long size = TopologySerializers.requestScope.serializedSize(commit.scope(), version);
            size += CommandSerializers.txnId.serializedSize(commit.txnId, version);
            size += CommandSerializers.txn.serializedSize(commit.txn, version);
            size += CommandSerializers.timestamp.serializedSize(commit.executeAt, version);
            size += CommandSerializers.deps.serializedSize(commit.deps, version);
            size += TypeSizes.sizeof(commit.read);
            return size;
        }
    };
}
