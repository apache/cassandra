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
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.db.AccordData;

public class ApplySerializer
{
    public static final IVersionedSerializer<Apply> request = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(Apply apply, DataOutputPlus out, int version) throws IOException
        {
            TopologySerializers.requestScope.serialize(apply.scope(), out, version);
            CommandSerializers.txnId.serialize(apply.txnId, out, version);
            CommandSerializers.txn.serialize(apply.txn, out, version);
            CommandSerializers.timestamp.serialize(apply.executeAt, out, version);
            CommandSerializers.deps.serialize(apply.deps, out, version);
            CommandSerializers.writes.serialize(apply.writes, out, version);
            AccordData.serializer.serialize((AccordData) apply.result, out, version);
        }

        @Override
        public Apply deserialize(DataInputPlus in, int version) throws IOException
        {
            return new Apply(TopologySerializers.requestScope.deserialize(in, version),
                             CommandSerializers.txnId.deserialize(in, version),
                             CommandSerializers.txn.deserialize(in, version),
                             CommandSerializers.timestamp.deserialize(in, version),
                             CommandSerializers.deps.deserialize(in, version),
                             CommandSerializers.writes.deserialize(in, version),
                             AccordData.serializer.deserialize(in, version));
        }

        @Override
        public long serializedSize(Apply apply, int version)
        {
            long size = TopologySerializers.requestScope.serializedSize(apply.scope(), version);
            size += CommandSerializers.txnId.serializedSize(apply.txnId, version);
            size += CommandSerializers.txn.serializedSize(apply.txn, version);
            size += CommandSerializers.timestamp.serializedSize(apply.executeAt, version);
            size += CommandSerializers.deps.serializedSize(apply.deps, version);
            size += CommandSerializers.writes.serializedSize(apply.writes, version);
            size += AccordData.serializer.serializedSize((AccordData) apply.result, version);
            return size;
        }
    };
}
