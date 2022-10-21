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
import accord.primitives.Keys;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class CommitSerializers
{
    public static final IVersionedSerializer<Commit> request = new TxnRequestSerializer<Commit>()
    {
        @Override
        public void serializeBody(Commit msg, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(msg.txnId, out, version);
            CommandSerializers.txn.serialize(msg.txn, out, version);
            CommandSerializers.deps.serialize(msg.deps, out, version);
            KeySerializers.key.serialize(msg.homeKey, out, version);
            CommandSerializers.timestamp.serialize(msg.executeAt, out, version);
            out.writeBoolean(msg.read);
        }

        @Override
        public Commit deserializeBody(DataInputPlus in, int version, Keys scope, long waitForEpoch) throws IOException
        {
            return new Commit(scope, waitForEpoch,
                              CommandSerializers.txnId.deserialize(in, version),
                              CommandSerializers.txn.deserialize(in, version),
                              CommandSerializers.deps.deserialize(in, version),
                              KeySerializers.key.deserialize(in, version),
                              CommandSerializers.timestamp.deserialize(in, version),
                              in.readBoolean());
        }

        @Override
        public long serializedBodySize(Commit msg, int version)
        {
            return CommandSerializers.txnId.serializedSize(msg.txnId, version)
                   + CommandSerializers.txn.serializedSize(msg.txn, version)
                   + CommandSerializers.deps.serializedSize(msg.deps, version)
                   + KeySerializers.key.serializedSize(msg.homeKey, version)
                   + CommandSerializers.timestamp.serializedSize(msg.executeAt, version)
                   + TypeSizes.BOOL_SIZE;
        }
    };

    public static final IVersionedSerializer<Commit.Invalidate> invalidate = new TxnRequestSerializer<Commit.Invalidate>()
    {
        @Override
        public void serializeBody(Commit.Invalidate invalidate, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(invalidate.txnId, out, version);
            KeySerializers.keys.serialize(invalidate.txnKeys, out, version);
        }

        @Override
        public Commit.Invalidate deserializeBody(DataInputPlus in, int version, Keys scope, long waitForEpoch) throws IOException
        {
            return Commit.Invalidate.SerializerSupport.create(scope, waitForEpoch,
                                                              CommandSerializers.txnId.deserialize(in, version),
                                                              KeySerializers.keys.deserialize(in, version));
        }

        @Override
        public long serializedBodySize(Commit.Invalidate invalidate, int version)
        {
            return CommandSerializers.txnId.serializedSize(invalidate.txnId, version)
                   + KeySerializers.keys.serializedSize(invalidate.txnKeys, version);
        }
    };
}
