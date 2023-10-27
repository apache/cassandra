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
import javax.annotation.Nullable;

import accord.messages.Commit;
import accord.messages.ReadData;
import accord.primitives.FullRoute;
import accord.primitives.PartialDeps;
import accord.primitives.PartialRoute;
import accord.primitives.PartialTxn;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.Invariants;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.CastingSerializer;

import static org.apache.cassandra.utils.NullableSerializer.deserializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializedNullableSize;

public class CommitSerializers
{
    private static final IVersionedSerializer<Commit.Kind> kind = new IVersionedSerializer<Commit.Kind>()
    {
        public void serialize(Commit.Kind kind, DataOutputPlus out, int version) throws IOException
        {
            Invariants.checkArgument(kind == Commit.Kind.Minimal || kind == Commit.Kind.Maximal);
            out.writeBoolean(kind == Commit.Kind.Maximal);

        }

        public Commit.Kind deserialize(DataInputPlus in, int version) throws IOException
        {
            return in.readBoolean() ? Commit.Kind.Maximal : Commit.Kind.Minimal;
        }

        public long serializedSize(Commit.Kind kind, int version)
        {
            return TypeSizes.BOOL_SIZE;
        }
    };

    public abstract static class CommitSerializer<C extends Commit, R extends ReadData> extends TxnRequestSerializer<C>
    {
        private final IVersionedSerializer<ReadData> read;

        public CommitSerializer(Class<R> klass, IVersionedSerializer<R> read)
        {
            this.read = new CastingSerializer<>(klass, read);
        }

        @Override
        public void serializeBody(C msg, DataOutputPlus out, int version) throws IOException
        {
            kind.serialize(msg.kind, out, version);
            CommandSerializers.timestamp.serialize(msg.executeAt, out, version);
            CommandSerializers.nullablePartialTxn.serialize(msg.partialTxn, out, version);
            DepsSerializer.partialDeps.serialize(msg.partialDeps, out, version);
            serializeNullable(msg.route, out, version, KeySerializers.fullRoute);
            serializeNullable(msg.readData, out, version, read);
        }

        protected abstract C deserializeCommit(TxnId txnId, PartialRoute<?> scope, long waitForEpoch, Commit.Kind kind, Timestamp executeAt,
                                               @Nullable PartialTxn partialTxn, PartialDeps partialDeps,
                                               @Nullable FullRoute<?> fullRoute, @Nullable ReadData read);

        @Override
        public C deserializeBody(DataInputPlus in, int version, TxnId txnId, PartialRoute<?> scope, long waitForEpoch) throws IOException
        {
            return deserializeCommit(txnId, scope, waitForEpoch,
                                     kind.deserialize(in, version),
                                     CommandSerializers.timestamp.deserialize(in, version),
                                     CommandSerializers.nullablePartialTxn.deserialize(in, version),
                                     DepsSerializer.partialDeps.deserialize(in, version),
                                     deserializeNullable(in, version, KeySerializers.fullRoute),
                                     deserializeNullable(in, version, read)
            );
        }

        @Override
        public long serializedBodySize(C msg, int version)
        {
            return kind.serializedSize(msg.kind, version)
                   + CommandSerializers.timestamp.serializedSize(msg.executeAt, version)
                   + CommandSerializers.nullablePartialTxn.serializedSize(msg.partialTxn, version)
                   + DepsSerializer.partialDeps.serializedSize(msg.partialDeps, version)
                   + serializedNullableSize(msg.route, version, KeySerializers.fullRoute)
                   + serializedNullableSize(msg.readData, version, read);
        }
    }

    public static final IVersionedSerializer<Commit> request = new CommitSerializer<Commit, ReadData>(ReadData.class, ReadDataSerializers.readData)
    {
        @Override
        protected Commit deserializeCommit(TxnId txnId, PartialRoute<?> scope, long waitForEpoch, Commit.Kind kind, Timestamp executeAt, @Nullable PartialTxn partialTxn, PartialDeps partialDeps, @Nullable FullRoute<?> fullRoute, @Nullable ReadData read)
        {
            return Commit.SerializerSupport.create(txnId, scope, waitForEpoch, kind, executeAt, partialTxn, partialDeps, fullRoute, read);
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
            Unseekables<?> scope = KeySerializers.unseekables.deserialize(in, version);
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
