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
import accord.primitives.Ballot;
import accord.primitives.FullRoute;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

import static org.apache.cassandra.utils.NullableSerializer.deserializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializedNullableSize;

public class CommitSerializers
{
    public static final IVersionedSerializer<Commit.Kind> kind = new EnumSerializer<>(Commit.Kind.class);

    public static final CommitSerializer request = new CommitSerializer();
    public static class CommitSerializer extends TxnRequestSerializer.WithUnsyncedSerializer<Commit>
    {
        @Override
        public void serializeBody(Commit msg, DataOutputPlus out, int version) throws IOException
        {
            kind.serialize(msg.kind, out, version);
            CommandSerializers.ballot.serialize(msg.ballot, out, version);
            CommandSerializers.timestamp.serialize(msg.executeAt, out, version);
            CommandSerializers.nullablePartialTxn.serialize(msg.partialTxn, out, version);
            if (msg.kind.withDeps == Commit.WithDeps.HasDeps)
                DepsSerializers.partialDeps.serialize(msg.scope, msg.partialDeps, out, version);
            serializeNullable(msg.route, out, version, KeySerializers.fullRoute);
        }

        @Override
        public Commit deserializeBody(DataInputPlus in, int version, TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch) throws IOException
        {
            Commit.Kind kind = CommitSerializers.kind.deserialize(in, version);
            Ballot ballot = CommandSerializers.ballot.deserialize(in, version);
            Timestamp executeAt = CommandSerializers.timestamp.deserialize(in, version);
            PartialTxn partialTxn = CommandSerializers.nullablePartialTxn.deserialize(in, version);
            PartialDeps partialDeps = null;
            if (kind.withDeps == Commit.WithDeps.HasDeps)
                partialDeps = DepsSerializers.partialDeps.deserialize(scope, in, version);
            FullRoute<?> route = deserializeNullable(in, version, KeySerializers.fullRoute);
            return Commit.SerializerSupport.create(txnId, scope, waitForEpoch, minEpoch, kind, ballot, executeAt, partialTxn, partialDeps, route);
        }

        @Override
        public long serializedBodySize(Commit msg, int version)
        {
            long size = kind.serializedSize(msg.kind, version)
                   + CommandSerializers.ballot.serializedSize(msg.ballot, version)
                   + CommandSerializers.timestamp.serializedSize(msg.executeAt, version)
                   + CommandSerializers.nullablePartialTxn.serializedSize(msg.partialTxn, version);

            if (msg.kind.withDeps == Commit.WithDeps.HasDeps)
                size += DepsSerializers.partialDeps.serializedSize(msg.scope, msg.partialDeps, version);

            size += serializedNullableSize(msg.route, version, KeySerializers.fullRoute);
            return size;
        }
    }

    public static final IVersionedSerializer<Commit.Invalidate> invalidate = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(Commit.Invalidate invalidate, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(invalidate.txnId, out, version);
            KeySerializers.participants.serialize(invalidate.scope, out, version);
            out.writeUnsignedVInt(invalidate.waitForEpoch);
            out.writeUnsignedVInt(invalidate.invalidateUntilEpoch - invalidate.waitForEpoch);
        }

        @Override
        public Commit.Invalidate deserialize(DataInputPlus in, int version) throws IOException
        {
            TxnId txnId = CommandSerializers.txnId.deserialize(in, version);
            Participants<?> scope = KeySerializers.participants.deserialize(in, version);
            long waitForEpoch = in.readUnsignedVInt();
            long invalidateUntilEpoch = in.readUnsignedVInt() + waitForEpoch;
            return Commit.Invalidate.SerializerSupport.create(txnId, scope, waitForEpoch, invalidateUntilEpoch);
        }

        @Override
        public long serializedSize(Commit.Invalidate invalidate, int version)
        {
            return CommandSerializers.txnId.serializedSize(invalidate.txnId, version)
                   + KeySerializers.participants.serializedSize(invalidate.scope, version)
                   + TypeSizes.sizeofUnsignedVInt(invalidate.waitForEpoch)
                   + TypeSizes.sizeofUnsignedVInt(invalidate.invalidateUntilEpoch - invalidate.waitForEpoch);
        }
    };
}
