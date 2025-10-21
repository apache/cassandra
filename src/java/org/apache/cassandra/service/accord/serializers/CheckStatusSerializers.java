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
import java.util.Objects;

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
import accord.primitives.KnownMap.MinAndMaxKnown;
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
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.serializers.CommandSerializers.ExecuteAtSerializer;

import static accord.messages.CheckStatus.SerializationSupport.createOk;
import static org.apache.cassandra.service.accord.serializers.CommandSerializers.known;

public class CheckStatusSerializers
{
    public static final UnversionedSerializer<KnownMap> knownMap = new UnversionedSerializer<>()
    {
        @Override
        public void serialize(KnownMap knownMap, DataOutputPlus out) throws IOException
        {
            int size = knownMap.size();
            out.writeUnsignedVInt32(size);
            for (int i = 0 ; i <= size ; ++i)
                KeySerializers.routingKey.serialize(knownMap.startAt(i), out);
            for (int i = 0 ; i < size ; ++i)
            {
                MinAndMaxKnown minAndMax = knownMap.valueAt(i);
                if (minAndMax == null)
                {
                    out.writeByte(0);
                    continue;
                }
                boolean equal = Objects.equals(minAndMax.minOwned, minAndMax.max);
                out.writeByte(equal ? 1 : 2);
                known.serialize(minAndMax.minOwned, out);
                if (!equal)
                    known.serialize(minAndMax.max, out);
            }
        }

        @Override
        public KnownMap deserialize(DataInputPlus in) throws IOException
        {
            int size = in.readUnsignedVInt32();
            RoutingKey[] starts = new RoutingKey[size + 1];
            for (int i = 0 ; i <= size ; ++i)
                starts[i] = KeySerializers.routingKey.deserialize(in);
            MinAndMaxKnown[] values = new MinAndMaxKnown[size];
            for (int i = 0 ; i < size ; ++i)
            {
                int kind = in.readByte();
                if (kind == 0)
                    continue;
                Known min = known.deserialize(in);
                Known max = kind == 1 ? min : known.deserialize(in);
                values[i] = new MinAndMaxKnown(min, max);
            }
            return KnownMap.SerializerSupport.create(true, starts, values);
        }

        @Override
        public long serializedSize(KnownMap knownMap)
        {
            int size = knownMap.size();
            long result = TypeSizes.sizeofUnsignedVInt(size);
            for (int i = 0 ; i <= size ; ++i)
                result += KeySerializers.routingKey.serializedSize(knownMap.startAt(i));
            for (int i = 0 ; i < size ; ++i)
            {
                MinAndMaxKnown minMax = knownMap.valueAt(i);
                result += TypeSizes.BYTE_SIZE;
                if (minMax == null)
                    continue;
                boolean equal = Objects.equals(minMax.minOwned, minMax.max);
                result += known.serializedSize(minMax.minOwned);
                if (!equal)
                    result += known.serializedSize(minMax.max);
            }
            return result;
        }
    };

    public static final UnversionedSerializer<CheckStatus> request = new UnversionedSerializer<>()
    {
        final CheckStatus.IncludeInfo[] infos = CheckStatus.IncludeInfo.values();

        @Override
        public void serialize(CheckStatus check, DataOutputPlus out) throws IOException
        {
            CommandSerializers.txnId.serialize(check.txnId, out);
            KeySerializers.participants.serialize(check.query, out);
            out.writeUnsignedVInt(check.sourceEpoch);
            out.writeByte(check.includeInfo.ordinal());
            CommandSerializers.ballot.serialize(check.bumpBallot, out);
        }

        @Override
        public CheckStatus deserialize(DataInputPlus in) throws IOException
        {
            TxnId txnId = CommandSerializers.txnId.deserialize(in);
            Participants<?> query = KeySerializers.participants.deserialize(in);
            long sourceEpoch = in.readUnsignedVInt();
            CheckStatus.IncludeInfo info = infos[in.readByte()];
            Ballot ballot = CommandSerializers.ballot.deserialize(in);
            return new CheckStatus(txnId, query, sourceEpoch, info, ballot);
        }

        @Override
        public long serializedSize(CheckStatus check)
        {
            return CommandSerializers.txnId.serializedSize(check.txnId)
                   + KeySerializers.participants.serializedSize(check.query)
                   + TypeSizes.sizeofUnsignedVInt(check.sourceEpoch)
                   + TypeSizes.BYTE_SIZE
                   + CommandSerializers.ballot.serializedSize(check.bumpBallot);
        }
    };

    public static final IVersionedSerializer<CheckStatusReply> reply = new IVersionedSerializer<>()
    {
        private static final byte OK   = 0x00;
        private static final byte FULL = 0x01;
        private static final byte NACK = 0x02;

        @Override
        public void serialize(CheckStatusReply reply, DataOutputPlus out, Version version) throws IOException
        {
            if (!reply.isOk())
            {
                out.write(NACK);
                return;
            }

            CheckStatusOk ok = (CheckStatusOk) reply;
            out.write(reply instanceof CheckStatusOkFull ? FULL : OK);
            knownMap.serialize(ok.map, out);
            CommandSerializers.saveStatus.serialize(ok.maxKnowledgeSaveStatus, out);
            CommandSerializers.saveStatus.serialize(ok.maxSaveStatus, out);
            CommandSerializers.ballot.serialize(ok.maxPromised, out);
            CommandSerializers.ballot.serialize(ok.maxAcceptedOrCommitted, out);
            CommandSerializers.ballot.serialize(ok.acceptedOrCommitted, out);
            ExecuteAtSerializer.serializeNullable(ok.executeAt, out);
            out.writeBoolean(ok.isCoordinating);
            CommandSerializers.durability.serialize(ok.durability, out);
            KeySerializers.nullableRoute.serialize(ok.route, out);
            KeySerializers.nullableRoutingKey.serialize(ok.homeKey, out);
            CommandSerializers.invalidIf.serialize(ok.invalidIf, out);

            if (!(reply instanceof CheckStatusOkFull))
                return;

            CheckStatusOkFull okFull = (CheckStatusOkFull) ok;
            CommandSerializers.nullablePartialTxn.serialize(okFull.partialTxn, out, version);
            DepsSerializers.nullablePartialDeps.serialize(okFull.stableDeps, out);
            CommandSerializers.nullableWrites.serialize(okFull.writes, out, version);
        }

        @Override
        public CheckStatusReply deserialize(DataInputPlus in, Version version) throws IOException
        {
            byte kind = in.readByte();
            switch (kind)
            {
                default: throw new IOException("Unhandled CheckStatusReply kind: " + Integer.toHexString(Byte.toUnsignedInt(kind)));
                case NACK:
                    return CheckStatusNack.NotOwned;
                case OK:
                case FULL:
                    KnownMap map = knownMap.deserialize(in);
                    SaveStatus maxKnowledgeStatus = CommandSerializers.saveStatus.deserialize(in);
                    SaveStatus maxStatus = CommandSerializers.saveStatus.deserialize(in);
                    Ballot maxPromised = CommandSerializers.ballot.deserialize(in);
                    Ballot maxAcceptedOrCommitted = CommandSerializers.ballot.deserialize(in);
                    Ballot acceptedOrCommitted = CommandSerializers.ballot.deserialize(in);
                    Timestamp executeAt = ExecuteAtSerializer.deserializeNullable(in);
                    boolean isCoordinating = in.readBoolean();
                    Durability durability = CommandSerializers.durability.deserialize(in);
                    Route<?> route = KeySerializers.nullableRoute.deserialize(in);
                    RoutingKey homeKey = KeySerializers.nullableRoutingKey.deserialize(in);
                    Infer.InvalidIf invalidIf = CommandSerializers.invalidIf.deserialize(in);

                    if (kind == OK)
                        return createOk(map, maxKnowledgeStatus, maxStatus, maxPromised, maxAcceptedOrCommitted, acceptedOrCommitted, executeAt,
                                        isCoordinating, durability, route, homeKey, invalidIf);

                    PartialTxn partialTxn = CommandSerializers.nullablePartialTxn.deserialize(in, version);
                    PartialDeps committedDeps = DepsSerializers.nullablePartialDeps.deserialize(in);
                    Writes writes = CommandSerializers.nullableWrites.deserialize(in, version);

                    Result result = null;
                    if (maxKnowledgeStatus.known.outcome().isOrWasApply())
                        result = ResultSerializers.APPLIED;

                    return createOk(map, maxKnowledgeStatus, maxStatus, maxPromised, maxAcceptedOrCommitted, acceptedOrCommitted, executeAt,
                                    isCoordinating, durability, route, homeKey, invalidIf, partialTxn, committedDeps, writes, result);

            }
        }

        @Override
        public long serializedSize(CheckStatusReply reply, Version version)
        {
            long size = TypeSizes.BYTE_SIZE;
            if (!reply.isOk())
                return size;

            CheckStatusOk ok = (CheckStatusOk) reply;
            size += knownMap.serializedSize(ok.map);
            size += CommandSerializers.saveStatus.serializedSize(ok.maxKnowledgeSaveStatus);
            size += CommandSerializers.saveStatus.serializedSize(ok.maxSaveStatus);
            size += CommandSerializers.ballot.serializedSize(ok.maxPromised);
            size += CommandSerializers.ballot.serializedSize(ok.maxAcceptedOrCommitted);
            size += CommandSerializers.ballot.serializedSize(ok.acceptedOrCommitted);
            size += ExecuteAtSerializer.serializedNullableSize(ok.executeAt);
            size += TypeSizes.BOOL_SIZE;
            size += CommandSerializers.durability.serializedSize(ok.durability);
            size += KeySerializers.nullableRoute.serializedSize(ok.route);
            size += KeySerializers.nullableRoutingKey.serializedSize(ok.homeKey);
            size += CommandSerializers.invalidIf.serializedSize(ok.invalidIf);

            if (!(reply instanceof CheckStatusOkFull))
                return size;

            CheckStatusOkFull okFull = (CheckStatusOkFull) ok;
            size += CommandSerializers.nullablePartialTxn.serializedSize(okFull.partialTxn, version);
            size += DepsSerializers.nullablePartialDeps.serializedSize(okFull.stableDeps);
            size += CommandSerializers.nullableWrites.serializedSize(okFull.writes, version);
            return size;
        }
    };
}
