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

import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.replication.MutationTrackingService;

/**
 * Notifies the reconcile coordinator (data node) that this node has received
 * all missing mutations from involved peers that were needed for the read.
 */
public class ReadReconcileAck
{
    private static final Logger logger = LoggerFactory.getLogger(ReadReconcileAck.class);

    public final TrackedRead.Id readId;

    public ReadReconcileAck(TrackedRead.Id readId)
    {
        this.readId = readId;
    }

    @Override
    public String toString()
    {
        return "ReadReconcileAck{readId=" + readId + '}';
    }

    public static final IVerbHandler<ReadReconcileAck> verbHandler = message ->
    {
        MutationTrackingService.ensureEnabled();
        ReadReconcileAck notify = message.payload;
        logger.trace("Received reconcile ack from {}, for {}", message.from(), notify.readId);
        ReadReconciliations.instance.acceptSyncAck(message.from(), notify.readId);
    };

    public static final UnversionedSerializer<ReadReconcileAck> serializer = new UnversionedSerializer<>()
    {
        @Override
        public void serialize(ReadReconcileAck notify, DataOutputPlus out) throws IOException
        {
            TrackedRead.Id.serializer.serialize(notify.readId, out);
        }

        @Override
        public ReadReconcileAck deserialize(DataInputPlus in) throws IOException
        {
            return new ReadReconcileAck(TrackedRead.Id.serializer.deserialize(in));
        }

        @Override
        public long serializedSize(ReadReconcileAck notify)
        {
            return TrackedRead.Id.serializer.serializedSize(notify.readId);
        }
    };
}
