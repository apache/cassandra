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
import accord.primitives.PartialRoute;
import accord.primitives.Routables;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

import static org.apache.cassandra.service.accord.serializers.NullableSerializer.deserializeNullable;
import static org.apache.cassandra.service.accord.serializers.NullableSerializer.serializeNullable;
import static org.apache.cassandra.service.accord.serializers.NullableSerializer.serializedSizeNullable;

public class CommitSerializers
{
    public static final IVersionedSerializer<Commit> request = new TxnRequestSerializer<Commit>()
    {
        @Override
        public void serializeBody(Commit msg, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.timestamp.serialize(msg.executeAt, out, version);
            serializeNullable(msg.partialTxn, out, version, CommandSerializers.partialTxn);
            DepsSerializer.partialDeps.serialize(msg.partialDeps, out, version);
            serializeNullable(msg.route, out, version, KeySerializers.fullRoute);
            serializeNullable(msg.read, out, version, ReadDataSerializers.request);
        }

        @Override
        public Commit deserializeBody(DataInputPlus in, int version, TxnId txnId, PartialRoute<?> scope, long waitForEpoch) throws IOException
        {
            return Commit.SerializerSupport.create(txnId, scope, waitForEpoch,
                                                   CommandSerializers.timestamp.deserialize(in, version),
                                                   deserializeNullable(in, version, CommandSerializers.partialTxn),
                                                   DepsSerializer.partialDeps.deserialize(in, version),
                                                   deserializeNullable(in, version, KeySerializers.fullRoute),
                                                   deserializeNullable(in, version, ReadDataSerializers.request)
            );
        }

        @Override
        public long serializedBodySize(Commit msg, int version)
        {
            return CommandSerializers.timestamp.serializedSize(msg.executeAt, version)
                   + serializedSizeNullable(msg.partialTxn, version, CommandSerializers.partialTxn)
                   + DepsSerializer.partialDeps.serializedSize(msg.partialDeps, version)
                   + serializedSizeNullable(msg.route, version, KeySerializers.fullRoute)
                   + serializedSizeNullable(msg.read, version, ReadDataSerializers.request);
        }
    };

    public static final IVersionedSerializer<Commit.Invalidate> invalidate = new IVersionedSerializer<Commit.Invalidate>()
    {
        @Override
        public void serialize(Commit.Invalidate invalidate, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(invalidate.txnId, out, version);
            KeySerializers.unseekables.serialize(invalidate.scope, out, version);
            out.writeUnsignedVInt(invalidate.waitForEpoch);
            out.writeUnsignedVInt(invalidate.invalidateUntilEpoch - invalidate.waitForEpoch);
        }

        @Override
        public Commit.Invalidate deserialize(DataInputPlus in, int version) throws IOException
        {
            TxnId txnId = CommandSerializers.txnId.deserialize(in, version);
            Unseekables<?, ?> scope = KeySerializers.unseekables.deserialize(in, version);
            long waitForEpoch = in.readUnsignedVInt();
            long invalidateUntilEpoch = in.readUnsignedVInt() + waitForEpoch;
            return Commit.Invalidate.SerializerSupport.create(txnId, scope, waitForEpoch, invalidateUntilEpoch);
        }

        @Override
        public long serializedSize(Commit.Invalidate invalidate, int version)
        {
            return CommandSerializers.txnId.serializedSize(invalidate.txnId, version)
                   + KeySerializers.unseekables.serializedSize(invalidate.scope, version)
                   + TypeSizes.sizeofUnsignedVInt(invalidate.waitForEpoch)
                   + TypeSizes.sizeofUnsignedVInt(invalidate.invalidateUntilEpoch - invalidate.waitForEpoch);
        }
    };
}
