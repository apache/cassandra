/*
 * Licensed to the Apache Software ation (ASF) under one
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
import accord.messages.CheckStatus;
import accord.messages.CheckStatus.CheckStatusNack;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.CheckStatus.CheckStatusReply;
import accord.primitives.Ballot;
import accord.primitives.Known;
import accord.primitives.KnownMap;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.SaveStatus;
import accord.primitives.Status.Durability;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

import static accord.messages.CheckStatus.SerializationSupport.createOk;
import static org.apache.cassandra.service.accord.serializers.CommandSerializers.known;

public class CheckStatusSerializers
{
    public static final IVersionedSerializer<KnownMap> knownMap = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(KnownMap knownMap, DataOutputPlus out, int version) throws IOException
        {
            int size = knownMap.size();
            out.writeUnsignedVInt32(size);
            for (int i = 0 ; i <= size ; ++i)
                KeySerializers.routingKey.serialize(knownMap.startAt(i), out, version);
            for (int i = 0 ; i < size ; ++i)
            {
                KnownMap.MinMax minMax = knownMap.valueAt(i);
                if (minMax == null)
                {
                    out.writeByte(0);
                    continue;
                }
                boolean equal = minMax.min.equals(minMax);
                out.writeByte(equal ? 1 : 2);
                known.serialize(minMax.min, out, version);
                if (!equal)
                    known.serialize(minMax, out, version);
            }
        }

        @Override
        public KnownMap deserialize(DataInputPlus in, int version) throws IOException
        {
            int size = in.readUnsignedVInt32();
            RoutingKey[] starts = new RoutingKey[size + 1];
            for (int i = 0 ; i <= size ; ++i)
                starts[i] = KeySerializers.routingKey.deserialize(in, version);
            KnownMap.MinMax[] values = new KnownMap.MinMax[size];
            for (int i = 0 ; i < size ; ++i)
            {
                int kind = in.readByte();
                if (kind == 0)
                    continue;
                Known min = known.deserialize(in, version);
                Known max = kind == 1 ? min : known.deserialize(in, version);
                values[i] = new KnownMap.MinMax(min, max);
            }
            return KnownMap.SerializerSupport.create(true, starts, values);
        }

        @Override
        public long serializedSize(KnownMap knownMap, int version)
        {
            int size = knownMap.size();
            long result = TypeSizes.sizeofUnsignedVInt(size);
            for (int i = 0 ; i <= size ; ++i)
                result += KeySerializers.routingKey.serializedSize(knownMap.startAt(i), version);
            for (int i = 0 ; i < size ; ++i)
            {
                KnownMap.MinMax minMax = knownMap.valueAt(i);
                result += TypeSizes.BYTE_SIZE;
                if (minMax == null)
                    continue;
                boolean equal = minMax.min.equals(minMax);
                result += known.serializedSize(minMax.min, version);
                if (!equal)
                    result += known.serializedSize(minMax, version);
            }
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
            KeySerializers.participants.serialize(check.query, out, version);
            out.writeUnsignedVInt(check.sourceEpoch);
            out.writeByte(check.includeInfo.ordinal());
        }

        @Override
        public CheckStatus deserialize(DataInputPlus in, int version) throws IOException
        {
            TxnId txnId = CommandSerializers.txnId.deserialize(in, version);
            Participants<?> query = KeySerializers.participants.deserialize(in, version);
            long sourceEpoch = in.readUnsignedVInt();
            CheckStatus.IncludeInfo info = infos[in.readByte()];
            return new CheckStatus(txnId, query, sourceEpoch, info);
        }

        @Override
        public long serializedSize(CheckStatus check, int version)
        {
            return CommandSerializers.txnId.serializedSize(check.txnId, version)
                   + KeySerializers.participants.serializedSize(check.query, version)
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
            knownMap.serialize(ok.map, out, version);
            CommandSerializers.saveStatus.serialize(ok.maxKnowledgeSaveStatus, out, version);
            CommandSerializers.saveStatus.serialize(ok.maxSaveStatus, out, version);
            CommandSerializers.ballot.serialize(ok.maxPromised, out, version);
            CommandSerializers.ballot.serialize(ok.maxAcceptedOrCommitted, out, version);
            CommandSerializers.ballot.serialize(ok.acceptedOrCommitted, out, version);
            CommandSerializers.nullableTimestamp.serialize(ok.executeAt, out, version);
            out.writeBoolean(ok.isCoordinating);
            CommandSerializers.durability.serialize(ok.durability, out, version);
            KeySerializers.nullableRoute.serialize(ok.route, out, version);
            KeySerializers.nullableRoutingKey.serialize(ok.homeKey, out, version);
            CommandSerializers.invalidIf.serialize(ok.invalidIf, out, version);

            if (!(reply instanceof CheckStatusOkFull))
                return;

            CheckStatusOkFull okFull = (CheckStatusOkFull) ok;
            CommandSerializers.nullablePartialTxn.serialize(okFull.partialTxn, out, version);
            DepsSerializers.nullablePartialDeps.serialize(okFull.stableDeps, out, version);
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
                    KnownMap map = knownMap.deserialize(in, version);
                    SaveStatus maxKnowledgeStatus = CommandSerializers.saveStatus.deserialize(in, version);
                    SaveStatus maxStatus = CommandSerializers.saveStatus.deserialize(in, version);
                    Ballot maxPromised = CommandSerializers.ballot.deserialize(in, version);
                    Ballot maxAcceptedOrCommitted = CommandSerializers.ballot.deserialize(in, version);
                    Ballot acceptedOrCommitted = CommandSerializers.ballot.deserialize(in, version);
                    Timestamp executeAt = CommandSerializers.nullableTimestamp.deserialize(in, version);
                    boolean isCoordinating = in.readBoolean();
                    Durability durability = CommandSerializers.durability.deserialize(in, version);
                    Route<?> route = KeySerializers.nullableRoute.deserialize(in, version);
                    RoutingKey homeKey = KeySerializers.nullableRoutingKey.deserialize(in, version);
                    Infer.InvalidIf invalidIf = CommandSerializers.invalidIf.deserialize(in, version);

                    if (kind == OK)
                        return createOk(map, maxKnowledgeStatus, maxStatus, maxPromised, maxAcceptedOrCommitted, acceptedOrCommitted, executeAt,
                                        isCoordinating, durability, route, homeKey, invalidIf);

                    PartialTxn partialTxn = CommandSerializers.nullablePartialTxn.deserialize(in, version);
                    PartialDeps committedDeps = DepsSerializers.nullablePartialDeps.deserialize(in, version);
                    Writes writes = CommandSerializers.nullableWrites.deserialize(in, version);

                    Result result = null;
                    if (maxKnowledgeStatus.known.outcome.isOrWasApply())
                        result = ResultSerializers.APPLIED;

                    return createOk(map, maxKnowledgeStatus, maxStatus, maxPromised, maxAcceptedOrCommitted, acceptedOrCommitted, executeAt,
                                    isCoordinating, durability, route, homeKey, invalidIf, partialTxn, committedDeps, writes, result);

            }
        }

        @Override
        public long serializedSize(CheckStatusReply reply, int version)
        {
            long size = TypeSizes.BYTE_SIZE;
            if (!reply.isOk())
                return size;

            CheckStatusOk ok = (CheckStatusOk) reply;
            size += knownMap.serializedSize(ok.map, version);
            size += CommandSerializers.saveStatus.serializedSize(ok.maxKnowledgeSaveStatus, version);
            size += CommandSerializers.saveStatus.serializedSize(ok.maxSaveStatus, version);
            size += CommandSerializers.ballot.serializedSize(ok.maxPromised, version);
            size += CommandSerializers.ballot.serializedSize(ok.maxAcceptedOrCommitted, version);
            size += CommandSerializers.ballot.serializedSize(ok.acceptedOrCommitted, version);
            size += CommandSerializers.nullableTimestamp.serializedSize(ok.executeAt, version);
            size += TypeSizes.BOOL_SIZE;
            size += CommandSerializers.durability.serializedSize(ok.durability, version);
            size += KeySerializers.nullableRoute.serializedSize(ok.route, version);
            size += KeySerializers.nullableRoutingKey.serializedSize(ok.homeKey, version);
            size += CommandSerializers.invalidIf.serializedSize(ok.invalidIf, version);

            if (!(reply instanceof CheckStatusOkFull))
                return size;

            CheckStatusOkFull okFull = (CheckStatusOkFull) ok;
            size += CommandSerializers.nullablePartialTxn.serializedSize(okFull.partialTxn, version);
            size += DepsSerializers.nullablePartialDeps.serializedSize(okFull.stableDeps, version);
            size += CommandSerializers.nullableWrites.serializedSize(okFull.writes, version);
            return size;
        }
    };
}
