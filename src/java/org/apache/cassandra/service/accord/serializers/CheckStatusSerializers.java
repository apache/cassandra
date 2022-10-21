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

import accord.messages.CheckStatus;
import accord.messages.CheckStatus.CheckStatusNack;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.CheckStatus.CheckStatusReply;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.db.AccordData;

import static org.apache.cassandra.service.accord.serializers.NullableSerializer.deserializeNullable;
import static org.apache.cassandra.service.accord.serializers.NullableSerializer.serializeNullable;
import static org.apache.cassandra.service.accord.serializers.NullableSerializer.serializedSizeNullable;

public class CheckStatusSerializers
{
    public static final IVersionedSerializer<CheckStatus> request = new IVersionedSerializer<CheckStatus>()
    {
        @Override
        public void serialize(CheckStatus check, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(check.txnId, out, version);
            KeySerializers.key.serialize(check.key, out, version);
            out.writeLong(check.epoch);
            out.writeInt(check.includeInfo.ordinal());
        }

        @Override
        public CheckStatus deserialize(DataInputPlus in, int version) throws IOException
        {
            return new CheckStatus(CommandSerializers.txnId.deserialize(in, version),
                                   KeySerializers.key.deserialize(in, version),
                                   in.readLong(),
                                   CheckStatus.IncludeInfo.values()[in.readInt()]);
        }

        @Override
        public long serializedSize(CheckStatus check, int version)
        {
            return CommandSerializers.txnId.serializedSize(check.txnId, version)
                   + KeySerializers.key.serializedSize(check.key, version)
                   + TypeSizes.LONG_SIZE
                   + TypeSizes.INT_SIZE;
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

            CheckStatusOk statusOk = (CheckStatusOk) reply;
            out.write(reply instanceof CheckStatusOkFull ? FULL : OK);
            CommandSerializers.status.serialize(statusOk.status, out, version);
            CommandSerializers.ballot.serialize(statusOk.promised, out, version);
            CommandSerializers.ballot.serialize(statusOk.accepted, out, version);
            out.writeBoolean(statusOk.isCoordinating);
            out.writeBoolean(statusOk.hasExecutedOnAllShards);

            if (!(reply instanceof CheckStatusOkFull))
                return;

            CheckStatusOkFull okFull = (CheckStatusOkFull) statusOk;
            serializeNullable(okFull.txn, out, version, CommandSerializers.txn);
            serializeNullable(okFull.homeKey, out, version, KeySerializers.key);
            serializeNullable(okFull.executeAt, out, version, CommandSerializers.timestamp);
            CommandSerializers.deps.serialize(okFull.deps, out, version);
            serializeNullable(okFull.writes, out, version, CommandSerializers.writes);
            serializeNullable((AccordData) okFull.result, out, version, AccordData.serializer);
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
                    return new CheckStatusOk(CommandSerializers.status.deserialize(in, version),
                                             CommandSerializers.ballot.deserialize(in, version),
                                             CommandSerializers.ballot.deserialize(in, version),
                                             in.readBoolean(),
                                             in.readBoolean());
                case FULL:
                    return new CheckStatusOkFull(CommandSerializers.status.deserialize(in, version),
                                                 CommandSerializers.ballot.deserialize(in, version),
                                                 CommandSerializers.ballot.deserialize(in, version),
                                                 in.readBoolean(),
                                                 in.readBoolean(),
                                                 deserializeNullable(in, version, CommandSerializers.txn),
                                                 deserializeNullable(in, version, KeySerializers.key),
                                                 deserializeNullable(in, version, CommandSerializers.timestamp),
                                                 CommandSerializers.deps.deserialize(in, version),
                                                 deserializeNullable(in, version, CommandSerializers.writes),
                                                 deserializeNullable(in, version, AccordData.serializer));
            }
        }

        @Override
        public long serializedSize(CheckStatusReply reply, int version)
        {
            long size = TypeSizes.BYTE_SIZE;
            if (!reply.isOk())
                return size;

            CheckStatusOk statusOk = (CheckStatusOk) reply;
            size += CommandSerializers.status.serializedSize(statusOk.status, version);
            size += CommandSerializers.ballot.serializedSize(statusOk.promised, version);
            size += CommandSerializers.ballot.serializedSize(statusOk.accepted, version);
            size += TypeSizes.BOOL_SIZE;
            size += TypeSizes.BOOL_SIZE;

            if (!(reply instanceof CheckStatusOkFull))
                return size;

            CheckStatusOkFull okFull = (CheckStatusOkFull) statusOk;
            size += serializedSizeNullable(okFull.txn, version, CommandSerializers.txn);
            size += serializedSizeNullable(okFull.homeKey, version, KeySerializers.key);
            size += serializedSizeNullable(okFull.executeAt, version, CommandSerializers.timestamp);
            size += CommandSerializers.deps.serializedSize(okFull.deps, version);
            size += serializedSizeNullable(okFull.writes, version, CommandSerializers.writes);
            size += serializedSizeNullable((AccordData) okFull.result, version, AccordData.serializer);

            return size;
        }
    };
}
