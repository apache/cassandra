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
import accord.primitives.Keys;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.db.AccordData;

public class ApplySerializers
{
    public static final IVersionedSerializer<Apply> request = new TxnRequestSerializer<Apply>()
    {
        @Override
        public void serializeBody(Apply apply, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(apply.txnId, out, version);
            CommandSerializers.txn.serialize(apply.txn, out, version);
            KeySerializers.key.serialize(apply.homeKey, out, version);
            CommandSerializers.timestamp.serialize(apply.executeAt, out, version);
            CommandSerializers.deps.serialize(apply.deps, out, version);
            CommandSerializers.writes.serialize(apply.writes, out, version);
            AccordData.serializer.serialize((AccordData) apply.result, out, version);
        }

        @Override
        public Apply deserializeBody(DataInputPlus in, int version, Keys scope, long waitForEpoch) throws IOException
        {
            return Apply.SerializationSupport.create(scope, waitForEpoch,
                                                     CommandSerializers.txnId.deserialize(in, version),
                                                     CommandSerializers.txn.deserialize(in, version),
                                                     KeySerializers.key.deserialize(in, version),
                                                     CommandSerializers.timestamp.deserialize(in, version),
                                                     CommandSerializers.deps.deserialize(in, version),
                                                     CommandSerializers.writes.deserialize(in, version),
                                                     AccordData.serializer.deserialize(in, version));
        }

        @Override
        public long serializedBodySize(Apply apply, int version)
        {
            return CommandSerializers.txnId.serializedSize(apply.txnId, version)
                   + CommandSerializers.txn.serializedSize(apply.txn, version)
                   + KeySerializers.key.serializedSize(apply.homeKey, version)
                   + CommandSerializers.timestamp.serializedSize(apply.executeAt, version)
                   + CommandSerializers.deps.serializedSize(apply.deps, version)
                   + CommandSerializers.writes.serializedSize(apply.writes, version)
                   + AccordData.serializer.serializedSize((AccordData) apply.result, version);
        }
    };

    public static final IVersionedSerializer<Apply.ApplyOk> reply = new IVersionedSerializer<Apply.ApplyOk>()
    {
        @Override
        public void serialize(Apply.ApplyOk t, DataOutputPlus out, int version) throws IOException
        {

        }

        @Override
        public Apply.ApplyOk deserialize(DataInputPlus in, int version) throws IOException
        {
            return Apply.ApplyOk.INSTANCE;
        }

        @Override
        public long serializedSize(Apply.ApplyOk t, int version)
        {
            return 0;
        }
    };
}
