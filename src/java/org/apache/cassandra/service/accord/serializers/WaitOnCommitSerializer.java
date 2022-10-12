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

import accord.messages.WaitOnCommit;
import accord.messages.WaitOnCommit.WaitOnCommitOk;
import accord.primitives.Keys;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class WaitOnCommitSerializer
{
    public static final IVersionedSerializer<WaitOnCommit> request = new TxnRequestSerializer<WaitOnCommit>()
    {
        @Override
        public void serializeBody(WaitOnCommit wait, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(wait.txnId, out, version);
        }

        @Override
        public WaitOnCommit deserializeBody(DataInputPlus in, int version, Keys scope, long waitForEpoch) throws IOException
        {
            return WaitOnCommit.SerializerSupport.create(scope, waitForEpoch, CommandSerializers.txnId.deserialize(in, version));
        }

        @Override
        public long serializedBodySize(WaitOnCommit wait, int version)
        {
            return CommandSerializers.txnId.serializedSize(wait.txnId, version);
        }
    };

    public static final IVersionedSerializer<WaitOnCommitOk> reply = new IVersionedSerializer<WaitOnCommitOk>()
    {
        @Override
        public void serialize(WaitOnCommitOk ok, DataOutputPlus out, int version) throws IOException
        {

        }

        @Override
        public WaitOnCommitOk deserialize(DataInputPlus in, int version) throws IOException
        {
            return WaitOnCommitOk.INSTANCE;
        }

        @Override
        public long serializedSize(WaitOnCommitOk ok, int version)
        {
            return 0;
        }
    };
}
