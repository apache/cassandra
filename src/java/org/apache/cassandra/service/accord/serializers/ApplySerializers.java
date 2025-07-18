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

import accord.api.Result;
import accord.coordinate.ExecuteFlag.ExecuteFlags;
import accord.messages.Apply;
import accord.primitives.Ballot;
import accord.primitives.FullRoute;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.utils.Invariants;
import accord.utils.VIntCoding;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.serializers.CommandSerializers.ExecuteAtSerializer;

import static accord.primitives.Txn.Kind.Write;


public class ApplySerializers
{
    private static final UnversionedSerializer<Apply.Kind> kind = new UnversionedSerializer<>()
    {
        public void serialize(Apply.Kind kind, DataOutputPlus out) throws IOException
        {
            Invariants.requireArgument(kind == Apply.Kind.Maximal || kind == Apply.Kind.Minimal);
            out.writeBoolean(kind == Apply.Kind.Maximal);
        }

        public Apply.Kind deserialize(DataInputPlus in) throws IOException
        {
            return in.readBoolean() ? Apply.Kind.Maximal : Apply.Kind.Minimal;
        }

        public long serializedSize(Apply.Kind t)
        {
            return TypeSizes.BOOL_SIZE;
        }
    };

    public abstract static class ApplySerializer<A extends Apply> extends TxnRequestSerializer<A>
    {
        @Override
        public void serializeBody(A apply, DataOutputPlus out, Version version) throws IOException
        {
            CommandSerializers.ballot.serialize(apply.ballot, out);
            out.writeVInt(apply.minEpoch - apply.waitForEpoch);
            out.writeUnsignedVInt(apply.maxEpoch - apply.minEpoch);
            kind.serialize(apply.kind, out);
            ExecuteAtSerializer.serialize(apply.txnId, apply.executeAt, out);
            DepsSerializers.partialDeps.serialize(apply.deps(), out);
            CommandSerializers.nullablePartialTxn.serialize(apply.txn(), out, version);
            KeySerializers.nullableFullRoute.serialize(apply.fullRoute, out);
            if (apply.txnId.is(Write))
                CommandSerializers.writes.serialize(apply.writes(), out, version);
            out.writeUnsignedVInt32(apply.flags.bits());
        }

        protected abstract A deserializeApply(TxnId txnId, Ballot ballot, Route<?> scope, long minEpoch, long waitForEpoch, long maxEpoch, Apply.Kind kind,
                                              Timestamp executeAt, PartialDeps deps, PartialTxn txn, FullRoute<?> fullRoute, Writes writes, Result result, ExecuteFlags flags);

        @Override
        public A deserializeBody(DataInputPlus in, Version version, TxnId txnId, Route<?> scope, long waitForEpoch) throws IOException
        {
            Ballot ballot = CommandSerializers.ballot.deserialize(in);
            long minEpoch = waitForEpoch + in.readVInt();
            long maxEpoch = minEpoch + in.readUnsignedVInt();
            return deserializeApply(txnId, ballot, scope, minEpoch, waitForEpoch, maxEpoch,
                                    kind.deserialize(in),
                                    ExecuteAtSerializer.deserialize(txnId, in),
                                    DepsSerializers.partialDeps.deserialize(in),
                                    CommandSerializers.nullablePartialTxn.deserialize(in, version),
                                    KeySerializers.nullableFullRoute.deserialize(in),
                                    (txnId.is(Write) ? CommandSerializers.writes.deserialize(in, version) : null),
                                    ResultSerializers.APPLIED,
                                    ExecuteFlags.get(in.readUnsignedVInt32()));
        }

        @Override
        public long serializedBodySize(A apply, Version version)
        {
            return CommandSerializers.ballot.serializedSize(apply.ballot)
                   + TypeSizes.sizeofVInt(apply.minEpoch - apply.waitForEpoch)
                   + TypeSizes.sizeofUnsignedVInt(apply.maxEpoch - apply.minEpoch)
                   + kind.serializedSize(apply.kind)
                   + ExecuteAtSerializer.serializedSize(apply.txnId, apply.executeAt)
                   + DepsSerializers.partialDeps.serializedSize(apply.deps())
                   + CommandSerializers.nullablePartialTxn.serializedSize(apply.txn(), version)
                   + KeySerializers.nullableFullRoute.serializedSize(apply.fullRoute)
                   + (apply.txnId.is(Write) ? CommandSerializers.writes.serializedSize(apply.writes(), version) : 0)
                   + VIntCoding.sizeOfUnsignedVInt(apply.flags.bits())
            ;
        }
    }

    public static final IVersionedSerializer<Apply> request = new ApplySerializer<>()
    {
        @Override
        protected Apply deserializeApply(TxnId txnId, Ballot ballot, Route<?> scope, long minEpoch, long waitForEpoch, long maxEpoch, Apply.Kind kind,
                                         Timestamp executeAt, PartialDeps deps, PartialTxn txn, FullRoute<?> fullRoute, Writes writes, Result result, ExecuteFlags flags)
        {
            return Apply.SerializationSupport.create(txnId, ballot, scope, minEpoch, waitForEpoch, maxEpoch, kind, executeAt, deps, txn, fullRoute, writes, result, flags);
        }
    };

    public static final UnversionedSerializer<Apply.ApplyReply> reply = new UnversionedSerializer<>()
    {
        private final Apply.ApplyReply[] replies = Apply.ApplyReply.values();

        @Override
        public void serialize(Apply.ApplyReply t, DataOutputPlus out) throws IOException
        {
            out.writeByte(t.ordinal());
        }

        @Override
        public Apply.ApplyReply deserialize(DataInputPlus in) throws IOException
        {
            return replies[in.readByte()];
        }

        @Override
        public long serializedSize(Apply.ApplyReply t)
        {
            return 1;
        }
    };
}
