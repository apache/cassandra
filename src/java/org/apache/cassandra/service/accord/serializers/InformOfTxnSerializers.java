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

import accord.messages.InformOfTxn;
import accord.messages.InformOfTxn.InformOfTxnNack;
import accord.messages.InformOfTxn.InformOfTxnOk;
import accord.messages.InformOfTxn.InformOfTxnReply;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class InformOfTxnSerializers
{
    public static final IVersionedSerializer<InformOfTxn> request = new IVersionedSerializer<InformOfTxn>()
    {
        @Override
        public void serialize(InformOfTxn inform, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(inform.txnId, out, version);
            KeySerializers.key.serialize(inform.homeKey, out, version);
            CommandSerializers.txn.serialize(inform.txn, out, version);

        }

        @Override
        public InformOfTxn deserialize(DataInputPlus in, int version) throws IOException
        {
            return new InformOfTxn(CommandSerializers.txnId.deserialize(in, version),
                                   KeySerializers.key.deserialize(in, version),
                                   CommandSerializers.txn.deserialize(in, version));
        }

        @Override
        public long serializedSize(InformOfTxn inform, int version)
        {
            return CommandSerializers.txnId.serializedSize(inform.txnId, version)
                   + KeySerializers.key.serializedSize(inform.homeKey, version)
                   + CommandSerializers.txn.serializedSize(inform.txn, version);
        }
    };

    public static final IVersionedSerializer<InformOfTxnReply> reply = new IVersionedSerializer<InformOfTxnReply>()
    {
        @Override
        public void serialize(InformOfTxnReply reply, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(reply.isOk());
        }

        @Override
        public InformOfTxnReply deserialize(DataInputPlus in, int version) throws IOException
        {
            return in.readBoolean() ? InformOfTxnOk.ok() : InformOfTxnNack.nack();
        }

        @Override
        public long serializedSize(InformOfTxnReply reply, int version)
        {
            return TypeSizes.BOOL_SIZE;
        }
    };
}
