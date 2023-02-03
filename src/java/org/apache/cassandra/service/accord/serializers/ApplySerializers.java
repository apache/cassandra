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

import accord.messages.Apply;
import accord.primitives.PartialRoute;
import accord.primitives.TxnId;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.txn.TxnData;

public class ApplySerializers
{
    public static final IVersionedSerializer<Apply> request = new TxnRequestSerializer<Apply>()
    {
        @Override
        public void serializeBody(Apply apply, DataOutputPlus out, int version) throws IOException
        {
            out.writeUnsignedVInt(apply.untilEpoch);
            KeySerializers.seekables.serialize(apply.keys(), out, version);
            CommandSerializers.timestamp.serialize(apply.executeAt, out, version);
            DepsSerializer.partialDeps.serialize(apply.deps, out, version);
            CommandSerializers.writes.serialize(apply.writes, out, version);
            TxnData.serializer.serialize((TxnData) apply.result, out, version);
        }

        @Override
        public Apply deserializeBody(DataInputPlus in, int version, TxnId txnId, PartialRoute scope, long waitForEpoch) throws IOException
        {
            return Apply.SerializationSupport.create(txnId, scope, waitForEpoch, in.readUnsignedVInt(),
                                                     KeySerializers.seekables.deserialize(in, version),
                                                     CommandSerializers.timestamp.deserialize(in, version),
                                                     DepsSerializer.partialDeps.deserialize(in, version),
                                                     CommandSerializers.writes.deserialize(in, version),
                                                     TxnData.serializer.deserialize(in, version));
        }

        @Override
        public long serializedBodySize(Apply apply, int version)
        {
            return TypeSizes.sizeofUnsignedVInt(apply.untilEpoch)
                   + KeySerializers.seekables.serializedSize(apply.keys(), version)
                   + CommandSerializers.timestamp.serializedSize(apply.executeAt, version)
                   + DepsSerializer.partialDeps.serializedSize(apply.deps, version)
                   + CommandSerializers.writes.serializedSize(apply.writes, version)
                   + TxnData.serializer.serializedSize((TxnData) apply.result, version);
        }
    };

    public static final IVersionedSerializer<Apply.ApplyReply> reply = new IVersionedSerializer<Apply.ApplyReply>()
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
