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
package org.apache.cassandra.replication;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;

public final class PullMutationsRequest
{
    private static final Logger logger = LoggerFactory.getLogger(PullMutationsRequest.class);

    private final Offsets.Immutable offsets;
    private final ActiveLogReconciler.Priority priority;

    public PullMutationsRequest(Offsets.Immutable offsets, ActiveLogReconciler.Priority priority)
    {
        this.offsets = offsets;
        this.priority = priority;
    }

    public static final UnversionedSerializer<PullMutationsRequest> serializer = new UnversionedSerializer<>()
    {
        @Override
        public void serialize(PullMutationsRequest pull, DataOutputPlus out) throws IOException
        {
            Offsets.serializer.serialize(pull.offsets, out);
            out.writeByte(pull.priority.id);
        }

        @Override
        public PullMutationsRequest deserialize(DataInputPlus in) throws IOException
        {
            Offsets.Immutable offsets = Offsets.serializer.deserialize(in);
            ActiveLogReconciler.Priority priority = ActiveLogReconciler.Priority.fromId(in.readUnsignedByte());
            return new PullMutationsRequest(offsets, priority);
        }

        @Override
        public long serializedSize(PullMutationsRequest pull)
        {
            return Offsets.serializer.serializedSize(pull.offsets) + 1;
        }
    };

    public static IVerbHandler<PullMutationsRequest> verbHandler = message -> {
        MutationTrackingService.ensureEnabled();
        InetAddressAndPort forHost = message.from();
        Offsets offsets = message.payload.offsets;
        ActiveLogReconciler.Priority priority = message.payload.priority;
        logger.trace("Received pull mutations request from {} for {} with priority {}", forHost, offsets, priority);
        MutationTrackingService.instance().requestMissingMutations(offsets, forHost, priority);
    };

    @Override
    public String toString()
    {
        return "PullMutationsRequest{" +
               "offsets=" + offsets +
               ", priority=" + priority +
               '}';
    }
}
