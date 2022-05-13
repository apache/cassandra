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
import accord.api.RoutingKey;
import accord.local.SaveStatus;
import accord.local.Status.Durability;
import accord.messages.CheckStatus;
import accord.messages.CheckStatus.CheckStatusNack;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.CheckStatus.CheckStatusReply;
import accord.primitives.Ballot;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Routables;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.primitives.Writes;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.txn.TxnData;

import static accord.messages.CheckStatus.SerializationSupport.createOk;
import static org.apache.cassandra.utils.NullableSerializer.deserializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializedSizeNullable;

public class CheckStatusSerializers
{
    public static final IVersionedSerializer<CheckStatus> request = new IVersionedSerializer<CheckStatus>()
    {
        final CheckStatus.IncludeInfo[] infos = CheckStatus.IncludeInfo.values();

        @Override
        public void serialize(CheckStatus check, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(check.txnId, out, version);
            KeySerializers.unseekables.serialize(check.query, out, version);
            out.writeUnsignedVInt(check.startEpoch);
            out.writeUnsignedVInt(check.endEpoch - check.startEpoch);
            out.writeByte(check.includeInfo.ordinal());
        }

        @Override
        public CheckStatus deserialize(DataInputPlus in, int version) throws IOException
        {
            TxnId txnId = CommandSerializers.txnId.deserialize(in, version);
            Unseekables<?, ?> query = KeySerializers.unseekables.deserialize(in, version);
            long startEpoch = in.readUnsignedVInt();
            long endEpoch = in.readUnsignedVInt() + startEpoch;
            CheckStatus.IncludeInfo info = infos[in.readByte()];
            return new CheckStatus(txnId, query, startEpoch, endEpoch, info);
        }

        @Override
        public long serializedSize(CheckStatus check, int version)
        {
            return CommandSerializers.txnId.serializedSize(check.txnId, version)
                   + KeySerializers.unseekables.serializedSize(check.query, version)
                   + TypeSizes.sizeofUnsignedVInt(check.startEpoch)
                   + TypeSizes.sizeofUnsignedVInt(check.endEpoch - check.startEpoch)
                   + TypeSizes.BYTE_SIZE;
        }
    };

    public static final IVersionedSerializer<CheckStatusReply> reply = new IVersionedSerializer<CheckStatusReply>()
    {
        private static final byte OK   = 0x00;
        private static final byte FULL = 0x01;
        private static final byte NACK = 0x02;

        @Override
        public void serialize(CheckStatusReply reply, DataOutputPlus out, int version) throws IOException
        {
            if (!reply.isOk())
            {
                out.write(NACK);
                return;
            }

            CheckStatusOk ok = (CheckStatusOk) reply;
            out.write(reply instanceof CheckStatusOkFull ? FULL : OK);
            CommandSerializers.saveStatus.serialize(ok.saveStatus, out, version);
            CommandSerializers.ballot.serialize(ok.promised, out, version);
            CommandSerializers.ballot.serialize(ok.accepted, out, version);
            serializeNullable(ok.executeAt, out, version, CommandSerializers.timestamp);
            out.writeBoolean(ok.isCoordinating);
            CommandSerializers.durability.serialize(ok.durability, out, version);
            serializeNullable(ok.route, out, version, KeySerializers.route);
            serializeNullable(ok.homeKey, out, version, KeySerializers.routingKey);

            if (!(reply instanceof CheckStatusOkFull))
                return;

            CheckStatusOkFull okFull = (CheckStatusOkFull) ok;
            serializeNullable(okFull.partialTxn, out, version, CommandSerializers.partialTxn);
            serializeNullable(okFull.committedDeps, out, version, DepsSerializer.partialDeps);
            serializeNullable(okFull.writes, out, version, CommandSerializers.writes);
            serializeNullable((TxnData) okFull.result, out, version, TxnData.serializer);
        }

        @Override
        public CheckStatusReply deserialize(DataInputPlus in, int version) throws IOException
        {
            byte kind = in.readByte();
            switch (kind)
            {
                default: throw new IOException("Unhandled CheckStatusReply kind: " + Integer.toHexString(Byte.toUnsignedInt(kind)));
                case NACK:
                    return CheckStatusNack.nack();
                case OK:
                case FULL:
                    SaveStatus status = CommandSerializers.saveStatus.deserialize(in, version);
                    Ballot promised = CommandSerializers.ballot.deserialize(in, version);
                    Ballot accepted = CommandSerializers.ballot.deserialize(in, version);
                    Timestamp executeAt = deserializeNullable(in, version, CommandSerializers.timestamp);
                    boolean isCoordinating = in.readBoolean();
                    Durability durability = CommandSerializers.durability.deserialize(in, version);
                    Route<?> route = deserializeNullable(in, version, KeySerializers.route);
                    RoutingKey homeKey = deserializeNullable(in, version, KeySerializers.routingKey);

                    if (kind == OK)
                        return createOk(status, promised, accepted, executeAt, isCoordinating, durability, route, homeKey);

                    PartialTxn partialTxn = deserializeNullable(in, version, CommandSerializers.partialTxn);
                    PartialDeps committedDeps = deserializeNullable(in, version, DepsSerializer.partialDeps);
                    Writes writes = deserializeNullable(in, version, CommandSerializers.writes);
                    Result result = deserializeNullable(in, version, TxnData.serializer);
                    return createOk(status, promised, accepted, executeAt, isCoordinating, durability, route, homeKey,
                                    partialTxn, committedDeps, writes, result);
            }
        }

        @Override
        public long serializedSize(CheckStatusReply reply, int version)
        {
            long size = TypeSizes.BYTE_SIZE;
            if (!reply.isOk())
                return size;

            CheckStatusOk ok = (CheckStatusOk) reply;
            size += CommandSerializers.saveStatus.serializedSize(ok.saveStatus, version);
            size += CommandSerializers.ballot.serializedSize(ok.promised, version);
            size += CommandSerializers.ballot.serializedSize(ok.accepted, version);
            size += serializedSizeNullable(ok.executeAt, version, CommandSerializers.timestamp);
            size += TypeSizes.BOOL_SIZE;
            size += CommandSerializers.durability.serializedSize(ok.durability, version);
            size += serializedSizeNullable(ok.homeKey, version, KeySerializers.routingKey);
            size += serializedSizeNullable(ok.route, version, KeySerializers.route);

            if (!(reply instanceof CheckStatusOkFull))
                return size;

            CheckStatusOkFull okFull = (CheckStatusOkFull) ok;
            size += serializedSizeNullable(okFull.partialTxn, version, CommandSerializers.partialTxn);
            size += serializedSizeNullable(okFull.committedDeps, version, DepsSerializer.partialDeps);
            size += serializedSizeNullable(okFull.writes, version, CommandSerializers.writes);
            size += serializedSizeNullable((TxnData) okFull.result, version, TxnData.serializer);
            return size;
        }
    };
}
