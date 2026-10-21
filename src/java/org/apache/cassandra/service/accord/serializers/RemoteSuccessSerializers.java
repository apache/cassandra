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

import accord.messages.RemoteSuccess;
import accord.primitives.TxnId;

import org.apache.cassandra.io.VersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.txn.TxnDataResult;

public class RemoteSuccessSerializers
{
    public static final VersionedSerializer<RemoteSuccess, Version> remoteSuccess = new VersionedSerializer<>()
    {
        @Override
        public void serialize(RemoteSuccess msg, DataOutputPlus out, Version version) throws IOException
        {
            CommandSerializers.txnId.serialize(msg.txnId, out);
            TxnDataResult.serializer.serialize((TxnDataResult) msg.result, out, version);
        }

        @Override
        public RemoteSuccess deserialize(DataInputPlus in, Version version) throws IOException
        {
            TxnId txnId = CommandSerializers.txnId.deserialize(in);
            TxnDataResult data = TxnDataResult.serializer.deserialize(in, version);
            return new RemoteSuccess(txnId, data);
        }

        @Override
        public long serializedSize(RemoteSuccess msg, Version version)
        {
            return CommandSerializers.txnId.serializedSize(msg.txnId)
                   + TxnDataResult.serializer.serializedSize((TxnDataResult) msg.result, version);
        }
    };
}
