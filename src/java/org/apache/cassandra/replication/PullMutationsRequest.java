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

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;

public final class PullMutationsRequest
{
    private static final Logger logger = LoggerFactory.getLogger(PullMutationsRequest.class);

    private final Offsets.Immutable offsets;

    public PullMutationsRequest(Offsets.Immutable offsets)
    {
        this.offsets = offsets;
    }

    public static IVersionedSerializer<PullMutationsRequest> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(PullMutationsRequest pull, DataOutputPlus out, int version) throws IOException
        {
            Offsets.serializer.serialize(pull.offsets, out, version);
        }

        @Override
        public PullMutationsRequest deserialize(DataInputPlus in, int version) throws IOException
        {
            return new PullMutationsRequest(Offsets.serializer.deserialize(in, version));
        }

        @Override
        public long serializedSize(PullMutationsRequest pull, int version)
        {
            return Offsets.serializer.serializedSize(pull.offsets, version);
        }
    };

    public static IVerbHandler<PullMutationsRequest> verbHandler = new IVerbHandler<>()
    {
        @Override
        public void doVerb(Message<PullMutationsRequest> message)
        {
            InetAddressAndPort forHost = message.from();
            Offsets offsets = message.payload.offsets;
            logger.trace("Received pull mutations request from {} for {}", forHost, offsets);
            MutationTrackingService.instance.requestMissingMutations(offsets, forHost);
        }
    };
}
