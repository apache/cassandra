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
import accord.coordinate.Infer;
import accord.local.SaveStatus;
import accord.local.Status.Durability;
import accord.local.Status.Known;
import accord.messages.CheckStatus;
import accord.messages.CheckStatus.CheckStatusNack;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.CheckStatus.CheckStatusReply;
import accord.messages.CheckStatus.FoundKnown;
import accord.messages.CheckStatus.FoundKnownMap;
import accord.primitives.Ballot;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.primitives.Writes;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

import static accord.messages.CheckStatus.SerializationSupport.createOk;

public class CheckStatusSerializers
{
    public static final IVersionedSerializer<FoundKnown> foundKnown = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(FoundKnown known, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.known.serialize(known, out, version);
            CommandSerializers.invalidIfNot.serialize(known.invalidIfNot, out, version);
            CommandSerializers.isPreempted.serialize(known.isPreempted, out, version);
        }

        @Override
        public FoundKnown deserialize(DataInputPlus in, int version) throws IOException
        {
            Known known = CommandSerializers.known.deserialize(in, version);
            Infer.InvalidIfNot invalidIfNot = CommandSerializers.invalidIfNot.deserialize(in, version);
            Infer.IsPreempted isPreempted = CommandSerializers.isPreempted.deserialize(in, version);
            return new FoundKnown(known, invalidIfNot, isPreempted);
        }

        @Override
        public long serializedSize(FoundKnown known, int version)
        {
            return CommandSerializers.known.serializedSize(known, version)
                 + CommandSerializers.invalidIfNot.serializedSize(known.invalidIfNot, version)
                 + CommandSerializers.isPreempted.serializedSize(known.isPreempted, version);
        }
    };

    public static final IVersionedSerializer<FoundKnownMap> foundKnownMap = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(FoundKnownMap knownMap, DataOutputPlus out, int version) throws IOException
        {
            int size = knownMap.size();
            out.writeUnsignedVInt32(size);
            for (int i = 0 ; i <= size ; ++i)
                KeySerializers.routingKey.serialize(knownMap.startAt(i), out, version);
            for (int i = 0 ; i < size ; ++i)
                foundKnown.serialize(knownMap.valueAt(i), out, version);
        }

        @Override
        public FoundKnownMap deserialize(DataInputPlus in, int version) throws IOException
        {
            int size = in.readUnsignedVInt32();
            RoutingKey[] starts = new RoutingKey[size + 1];
            for (int i = 0 ; i <= size ; ++i)
                starts[i] = KeySerializers.routingKey.deserialize(in, version);
            FoundKnown[] values = new FoundKnown[size];
            for (int i = 0 ; i < size ; ++i)
                values[i] = foundKnown.deserialize(in, version);
            return FoundKnownMap.SerializerSupport.create(true, starts, values);
        }

        @Override
        public long serializedSize(FoundKnownMap knownMap, int version)
        {
            int size = knownMap.size();
            long result = TypeSizes.sizeofUnsignedVInt(size);
            for (int i = 0 ; i <= size ; ++i)
                result += KeySerializers.routingKey.serializedSize(knownMap.startAt(i), version);
            for (int i = 0 ; i < size ; ++i)
                result += foundKnown.serializedSize(knownMap.valueAt(i), version);
            return result;
        }
    };

    public static final IVersionedSerializer<CheckStatus> request = new IVersionedSerializer<>()
    {
        final CheckStatus.IncludeInfo[] infos = CheckStatus.IncludeInfo.values();

        @Override
        public void serialize(CheckStatus check, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(check.txnId, out, version);
            KeySerializers.unseekables.serialize(check.query, out, version);
            out.writeUnsignedVInt(check.sourceEpoch);
            out.writeByte(check.includeInfo.ordinal());
        }

        @Override
        public CheckStatus deserialize(DataInputPlus in, int version) throws IOException
        {
            TxnId txnId = CommandSerializers.txnId.deserialize(in, version);
            Unseekables<?> query = KeySerializers.unseekables.deserialize(in, version);
            long sourceEpoch = in.readUnsignedVInt();
            CheckStatus.IncludeInfo info = infos[in.readByte()];
            return new CheckStatus(txnId, query, sourceEpoch, info);
        }

        @Override
        public long serializedSize(CheckStatus check, int version)
        {
            return CommandSerializers.txnId.serializedSize(check.txnId, version)
                   + KeySerializers.unseekables.serializedSize(check.query, version)
                   + TypeSizes.sizeofUnsignedVInt(check.sourceEpoch)
                   + TypeSizes.BYTE_SIZE;
        }
    };

    public static final IVersionedSerializer<CheckStatusReply> reply = new IVersionedSerializer<>()
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
            foundKnownMap.serialize(ok.map, out, version);
            CommandSerializers.saveStatus.serialize(ok.maxKnowledgeSaveStatus, out, version);
            CommandSerializers.saveStatus.serialize(ok.maxSaveStatus, out, version);
            CommandSerializers.ballot.serialize(ok.promised, out, version);
            CommandSerializers.ballot.serialize(ok.accepted, out, version);
            CommandSerializers.nullableTimestamp.serialize(ok.executeAt, out, version);
            out.writeBoolean(ok.isCoordinating);
            CommandSerializers.durability.serialize(ok.durability, out, version);
            KeySerializers.nullableRoute.serialize(ok.route, out, version);
            KeySerializers.nullableRoutingKey.serialize(ok.homeKey, out, version);

            if (!(reply instanceof CheckStatusOkFull))
                return;

            CheckStatusOkFull okFull = (CheckStatusOkFull) ok;
            CommandSerializers.nullablePartialTxn.serialize(okFull.partialTxn, out, version);
            DepsSerializer.nullablePartialDeps.serialize(okFull.committedDeps, out, version);
            CommandSerializers.nullableWrites.serialize(okFull.writes, out, version);
        }

        @Override
        public CheckStatusReply deserialize(DataInputPlus in, int version) throws IOException
        {
            byte kind = in.readByte();
            switch (kind)
            {
                default: throw new IOException("Unhandled CheckStatusReply kind: " + Integer.toHexString(Byte.toUnsignedInt(kind)));
                case NACK:
                    return CheckStatusNack.NotOwned;
                case OK:
                case FULL:
                    FoundKnownMap map = foundKnownMap.deserialize(in, version);
                    SaveStatus maxKnowledgeStatus = CommandSerializers.saveStatus.deserialize(in, version);
                    SaveStatus maxStatus = CommandSerializers.saveStatus.deserialize(in, version);
                    Ballot promised = CommandSerializers.ballot.deserialize(in, version);
                    Ballot accepted = CommandSerializers.ballot.deserialize(in, version);
                    Timestamp executeAt = CommandSerializers.nullableTimestamp.deserialize(in, version);
                    boolean isCoordinating = in.readBoolean();
                    Durability durability = CommandSerializers.durability.deserialize(in, version);
                    Route<?> route = KeySerializers.nullableRoute.deserialize(in, version);
                    RoutingKey homeKey = KeySerializers.nullableRoutingKey.deserialize(in, version);

                    if (kind == OK)
                        return createOk(map, maxKnowledgeStatus, maxStatus, promised, accepted, executeAt,
                                        isCoordinating, durability, route, homeKey);

                    PartialTxn partialTxn = CommandSerializers.nullablePartialTxn.deserialize(in, version);
                    PartialDeps committedDeps = DepsSerializer.nullablePartialDeps.deserialize(in, version);
                    Writes writes = CommandSerializers.nullableWrites.deserialize(in, version);

                    Result result = null;
                    if (maxKnowledgeStatus == SaveStatus.PreApplied || maxKnowledgeStatus == SaveStatus.Applied
                        || maxKnowledgeStatus == SaveStatus.TruncatedApply || maxKnowledgeStatus == SaveStatus.TruncatedApplyWithOutcome || maxKnowledgeStatus == SaveStatus.TruncatedApplyWithDeps)
                        result = CommandSerializers.APPLIED;

                    return createOk(map, maxKnowledgeStatus, maxStatus, promised, accepted, executeAt,
                                    isCoordinating, durability, route, homeKey, partialTxn, committedDeps, writes, result);
            }
        }

        @Override
        public long serializedSize(CheckStatusReply reply, int version)
        {
            long size = TypeSizes.BYTE_SIZE;
            if (!reply.isOk())
                return size;

            CheckStatusOk ok = (CheckStatusOk) reply;
            size += foundKnownMap.serializedSize(ok.map, version);
            size += CommandSerializers.saveStatus.serializedSize(ok.maxKnowledgeSaveStatus, version);
            size += CommandSerializers.saveStatus.serializedSize(ok.maxSaveStatus, version);
            size += CommandSerializers.ballot.serializedSize(ok.promised, version);
            size += CommandSerializers.ballot.serializedSize(ok.accepted, version);
            size += CommandSerializers.nullableTimestamp.serializedSize(ok.executeAt, version);
            size += TypeSizes.BOOL_SIZE;
            size += CommandSerializers.durability.serializedSize(ok.durability, version);
            size += KeySerializers.nullableRoutingKey.serializedSize(ok.homeKey, version);
            size += KeySerializers.nullableRoute.serializedSize(ok.route, version);

            if (!(reply instanceof CheckStatusOkFull))
                return size;

            CheckStatusOkFull okFull = (CheckStatusOkFull) ok;
            size += CommandSerializers.nullablePartialTxn.serializedSize(okFull.partialTxn, version);
            size += DepsSerializer.nullablePartialDeps.serializedSize(okFull.committedDeps, version);
            size += CommandSerializers.nullableWrites.serializedSize(okFull.writes, version);
            return size;
        }
    };
}
