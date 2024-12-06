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

package org.apache.cassandra.service.reads.tracked;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.replication.MutationTrackingService;

/**
 * Notifies the read coordinator that this node has received mutations
 * from the sending node
 */
public class ReadReconcileNotify
{
    private static final Logger logger = LoggerFactory.getLogger(ReadReconcileNotify.class);

    public final long reconciliationId;
    public final int syncId;

    public ReadReconcileNotify(long reconciliationId, int syncId)
    {
        this.reconciliationId = reconciliationId;
        this.syncId = syncId;
    }

    @Override
    public String toString()
    {
        return "ReadReconcileNotify{" +
               "reconciliationId=" + reconciliationId +
               ", syncId=" + syncId +
               '}';
    }

    public static final IVerbHandler<ReadReconcileNotify> verbHandler = new IVerbHandler<ReadReconcileNotify>()
    {
        @Override
        public void doVerb(Message<ReadReconcileNotify> message) throws IOException
        {
            ReadReconcileNotify notify = message.payload;
            logger.trace("Received read reconcile notify {} from {}", notify, message.from());
            MutationTrackingService.instance.reconciliations().acknowledgeSync(notify.reconciliationId, notify.syncId);
        }
    };

    public static final IVersionedSerializer<ReadReconcileNotify> serializer = new IVersionedSerializer<ReadReconcileNotify>()
    {
        @Override
        public void serialize(ReadReconcileNotify notify, DataOutputPlus out, int version) throws IOException
        {
            out.writeLong(notify.reconciliationId);
            out.writeInt(notify.syncId);
        }

        @Override
        public ReadReconcileNotify deserialize(DataInputPlus in, int version) throws IOException
        {
            return new ReadReconcileNotify(in.readLong(), in.readInt());
        }

        @Override
        public long serializedSize(ReadReconcileNotify t, int version)
        {
            return TypeSizes.sizeof(t.reconciliationId)
                   + TypeSizes.sizeof(t.syncId);
        }
    };
}
