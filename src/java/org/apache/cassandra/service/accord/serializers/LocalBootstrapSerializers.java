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

import accord.local.Bootstrap.CreateBootstrapCompleteMarkerTransaction;
import accord.local.Bootstrap.MarkBootstrapComplete;
import accord.primitives.Ranges;
import accord.primitives.SyncPoint;
import accord.primitives.TxnId;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class LocalBootstrapSerializers
{
    /**
     * {@code data class MarkBootstrapComplete(int storeId, TxnId localSyncId, Ranges ranges)}
     */
    public static final IVersionedSerializer<MarkBootstrapComplete> markBootstrapComplete = new IVersionedSerializer<MarkBootstrapComplete>()
    {
        @Override
        public void serialize(MarkBootstrapComplete t, DataOutputPlus out, int version) throws IOException
        {
            out.writeUnsignedVInt32(t.storeId);
            CommandSerializers.txnId.serialize(t.localSyncId, out, version);
            KeySerializers.ranges.serialize(t.ranges, out, version);

        }

        @Override
        public MarkBootstrapComplete deserialize(DataInputPlus in, int version) throws IOException
        {
            int storeId = in.readUnsignedVInt32();
            TxnId txnId = CommandSerializers.txnId.deserialize(in, version);
            Ranges ranges = KeySerializers.ranges.deserialize(in, version);
            return new MarkBootstrapComplete(storeId, txnId, ranges);
        }

        @Override
        public long serializedSize(MarkBootstrapComplete t, int version)
        {
            long size = TypeSizes.sizeofUnsignedVInt(t.storeId);
            size += CommandSerializers.txnId.serializedSize(t.localSyncId, version);
            size += KeySerializers.ranges.serializedSize(t.ranges, version);
            return size;
        }
    };

    /**
     * {@code data class CreateBootstrapCompleteMarkerTransaction(int storeId, TxnId localSyncId, SyncPoint<Ranges> syncPoint, Ranges valid)}
     */
    public static final IVersionedSerializer<CreateBootstrapCompleteMarkerTransaction> createBootstrapCompleteMarkerTransaction = new IVersionedSerializer<CreateBootstrapCompleteMarkerTransaction>()
    {

        @Override
        public void serialize(CreateBootstrapCompleteMarkerTransaction t, DataOutputPlus out, int version) throws IOException
        {
            out.writeUnsignedVInt32(t.storeId);
            CommandSerializers.txnId.serialize(t.localSyncId, out, version);
            SetDurableSerializers.syncPoint.serialize(t.syncPoint, out, version);
            KeySerializers.ranges.serialize(t.valid, out, version);
        }

        @Override
        public CreateBootstrapCompleteMarkerTransaction deserialize(DataInputPlus in, int version) throws IOException
        {
            int storeId = in.readUnsignedVInt32();
            TxnId txnId = CommandSerializers.txnId.deserialize(in, version);
            SyncPoint<Ranges> syncPoint = SetDurableSerializers.syncPoint.deserialize(in, version);
            Ranges valid = KeySerializers.ranges.deserialize(in, version);
            return new CreateBootstrapCompleteMarkerTransaction(storeId, txnId, syncPoint, valid);
        }

        @Override
        public long serializedSize(CreateBootstrapCompleteMarkerTransaction t, int version)
        {
            long size = TypeSizes.sizeofUnsignedVInt(t.storeId);
            size += CommandSerializers.txnId.serializedSize(t.localSyncId, version);
            size += SetDurableSerializers.syncPoint.serializedSize(t.syncPoint, version);
            size += KeySerializers.ranges.serializedSize(t.valid, version);
            return size;
        }
    };
}
