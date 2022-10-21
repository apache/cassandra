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

import com.google.common.base.Preconditions;

import accord.api.Result;
import accord.local.Status;
import accord.messages.BeginInvalidate;
import accord.messages.BeginInvalidate.InvalidateNack;
import accord.messages.BeginInvalidate.InvalidateOk;
import accord.messages.BeginRecovery.RecoverReply;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.txn.Writes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

import static org.apache.cassandra.utils.NullableSerializer.deserializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializedSizeNullable;

public class BeginInvalidateSerializers
{
    public static final IVersionedSerializer<BeginInvalidate> request = new IVersionedSerializer<BeginInvalidate>()
    {
        @Override
        public void serialize(BeginInvalidate begin, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(begin.txnId, out, version);
            KeySerializers.key.serialize(begin.someKey, out, version);
            CommandSerializers.ballot.serialize(begin.ballot, out, version);
        }

        @Override
        public BeginInvalidate deserialize(DataInputPlus in, int version) throws IOException
        {
            return new BeginInvalidate(CommandSerializers.txnId.deserialize(in, version),
                                       KeySerializers.key.deserialize(in, version),
                                       CommandSerializers.ballot.deserialize(in, version));
        }

        @Override
        public long serializedSize(BeginInvalidate begin, int version)
        {
            return CommandSerializers.txnId.serializedSize(begin.txnId, version)
                   + KeySerializers.key.serializedSize(begin.someKey, version)
                   + CommandSerializers.ballot.serializedSize(begin.ballot, version);
        }
    };

    public static final IVersionedSerializer<RecoverReply> reply = new RecoverySerializers.RecoverReplySerializer<InvalidateOk, InvalidateNack>()
    {
        @Override
        void serializeNack(InvalidateNack recoverNack, DataOutputPlus out, int version) throws IOException
        {
            super.serializeNack(recoverNack, out, version);
            serializeNullable(CommandSerializers.txn, recoverNack.txn, out, version);
            serializeNullable(KeySerializers.key, recoverNack.homeKey, out, version);
        }

        @Override
        void serializeOk(InvalidateOk recoverOk, DataOutputPlus out, int version) throws IOException
        {
            super.serializeOk(recoverOk, out, version);
            serializeNullable(CommandSerializers.txn, recoverOk.txn, out, version);
            serializeNullable(KeySerializers.key, recoverOk.homeKey, out, version);
        }

        @Override
        InvalidateNack deserializeNack(Ballot supersededBy, DataInputPlus in, int version) throws IOException
        {
            return new InvalidateNack(supersededBy,
                                      deserializeNullable(CommandSerializers.txn, in, version),
                                      deserializeNullable(KeySerializers.key, in, version));
        }

        @Override
        InvalidateOk deserializeOk(TxnId txnId, Status status, Ballot accepted, Timestamp executeAt, Deps deps, Deps earlierCommittedWitness, Deps earlierAcceptedNoWitness, boolean rejectsFastPath, Writes writes, Result result, DataInputPlus in, int version) throws IOException
        {
            Preconditions.checkArgument(earlierCommittedWitness == null);
            Preconditions.checkArgument(earlierAcceptedNoWitness == null);
            Preconditions.checkArgument(!rejectsFastPath);
            return new InvalidateOk(txnId, status, accepted, executeAt, deps, writes, result,
                                    deserializeNullable(CommandSerializers.txn, in, version),
                                    deserializeNullable(KeySerializers.key, in, version));
        }

        @Override
        long serializedNackSize(InvalidateNack recoverNack, int version)
        {
            return super.serializedNackSize(recoverNack, version)
                   + serializedSizeNullable(CommandSerializers.txn, recoverNack.txn, version)
                   + serializedSizeNullable(KeySerializers.key, recoverNack.homeKey, version);
        }

        @Override
        long serializedOkSize(InvalidateOk recoverOk, int version)
        {
            return super.serializedOkSize(recoverOk, version)
                   + serializedSizeNullable(CommandSerializers.txn, recoverOk.txn, version)
                   + serializedSizeNullable(KeySerializers.key, recoverOk.homeKey, version);
        }
    };
}
