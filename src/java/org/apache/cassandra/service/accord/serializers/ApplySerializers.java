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
import accord.messages.Apply;
import accord.primitives.FullRoute;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.utils.Invariants;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;


public class ApplySerializers
{
    private static final IVersionedSerializer<Apply.Kind> kind = new IVersionedSerializer<>()
    {
        public void serialize(Apply.Kind kind, DataOutputPlus out, int version) throws IOException
        {
            Invariants.checkArgument(kind == Apply.Kind.Maximal || kind == Apply.Kind.Minimal);
            out.writeBoolean(kind == Apply.Kind.Maximal);
        }

        public Apply.Kind deserialize(DataInputPlus in, int version) throws IOException
        {
            return in.readBoolean() ? Apply.Kind.Maximal : Apply.Kind.Minimal;
        }

        public long serializedSize(Apply.Kind t, int version)
        {
            return TypeSizes.BOOL_SIZE;
        }
    };

    public abstract static class ApplySerializer<A extends Apply> extends TxnRequestSerializer<A>
    {
        @Override
        public void serializeBody(A apply, DataOutputPlus out, int version) throws IOException
        {
            out.writeVInt(apply.minEpoch - apply.waitForEpoch);
            kind.serialize(apply.kind, out, version);
            CommandSerializers.timestamp.serialize(apply.executeAt, out, version);
            DepsSerializers.partialDeps.serialize(apply.deps, out, version);
            CommandSerializers.nullablePartialTxn.serialize(apply.txn, out, version);
            KeySerializers.nullableFullRoute.serialize(apply.fullRoute, out, version);
            CommandSerializers.writes.serialize(apply.writes, out, version);
        }

        protected abstract A deserializeApply(TxnId txnId, Route<?> scope, long minEpoch, long waitForEpoch, Apply.Kind kind,
                                              Timestamp executeAt, PartialDeps deps, PartialTxn txn, FullRoute<?> fullRoute, Writes writes, Result result);

        @Override
        public A deserializeBody(DataInputPlus in, int version, TxnId txnId, Route<?> scope, long waitForEpoch) throws IOException
        {
            return deserializeApply(txnId, scope, waitForEpoch + in.readVInt(), waitForEpoch,
                                    kind.deserialize(in, version),
                                    CommandSerializers.timestamp.deserialize(in, version),
                                    DepsSerializers.partialDeps.deserialize(in, version),
                                    CommandSerializers.nullablePartialTxn.deserialize(in, version),
                                    KeySerializers.nullableFullRoute.deserialize(in, version),
                                    CommandSerializers.writes.deserialize(in, version),
                                    ResultSerializers.APPLIED);
        }

        @Override
        public long serializedBodySize(A apply, int version)
        {
            return TypeSizes.sizeofVInt(apply.minEpoch - apply.waitForEpoch)
                   + kind.serializedSize(apply.kind, version)
                   + CommandSerializers.timestamp.serializedSize(apply.executeAt, version)
                   + DepsSerializers.partialDeps.serializedSize(apply.deps, version)
                   + CommandSerializers.nullablePartialTxn.serializedSize(apply.txn, version)
                   + KeySerializers.nullableFullRoute.serializedSize(apply.fullRoute, version)
                   + CommandSerializers.writes.serializedSize(apply.writes, version);
        }
    }

    public static final IVersionedSerializer<Apply> request = new ApplySerializer<>()
    {
        @Override
        protected Apply deserializeApply(TxnId txnId, Route<?> scope, long minEpoch, long waitForEpoch, Apply.Kind kind,
                               Timestamp executeAt, PartialDeps deps, PartialTxn txn, FullRoute<?> fullRoute, Writes writes, Result result)
        {
            return Apply.SerializationSupport.create(txnId, scope, minEpoch, waitForEpoch, kind, executeAt, deps, txn, fullRoute, writes, result);
        }
    };

    public static final IVersionedSerializer<Apply.ApplyReply> reply = new IVersionedSerializer<>()
    {
        private final Apply.ApplyReply[] replies = Apply.ApplyReply.values();

        @Override
        public void serialize(Apply.ApplyReply t, DataOutputPlus out, int version) throws IOException
        {
            out.writeByte(t.ordinal());
        }

        @Override
        public Apply.ApplyReply deserialize(DataInputPlus in, int version) throws IOException
        {
            return replies[in.readByte()];
        }

        @Override
        public long serializedSize(Apply.ApplyReply t, int version)
        {
            return 1;
        }
    };
}
