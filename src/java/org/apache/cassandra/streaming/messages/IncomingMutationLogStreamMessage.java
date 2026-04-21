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
package org.apache.cassandra.streaming.messages;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.streaming.LogStreamHeader;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.streaming.StreamReceiveException;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamingChannel;
import org.apache.cassandra.streaming.StreamingDataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Incoming mutation log stream message for receiving mutation logs during bootstrap.
 * 
 * This message handles deserialization and processing of mutation logs received from
 * nodes during the bootstrap process.
 */
public class IncomingMutationLogStreamMessage extends MutationLogStreamMessage
{
    private static final Logger logger = LoggerFactory.getLogger(IncomingMutationLogStreamMessage.class);

    public static final StreamMessage.Serializer<IncomingMutationLogStreamMessage> serializer = new IncomingMutationLogStreamMessageSerializer();

    public final StreamSession session;

    public IncomingMutationLogStreamMessage(LogStreamHeader header, StreamSession session)
    {
        super(header);
        this.session = session;
    }

    @Override
    public StreamSession getOrCreateAndAttachInboundSession(StreamingChannel channel, int messagingVersion)
    {
        session.attachInbound(channel);
        return session;
    }

    public static class IncomingMutationLogStreamMessageSerializer implements StreamMessage.Serializer<IncomingMutationLogStreamMessage>
    {
        public IncomingMutationLogStreamMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            LogStreamHeader header = LogStreamHeader.serializer.deserialize(in, version);
            StreamSession session = StreamManager.instance.findSession(header.sender, header.planId, header.sessionIndex, header.sendByFollower);
            if (session == null)
                throw new IllegalStateException(String.format("unknown stream session: %s - %d", header.planId, header.sessionIndex));

            try
            {
                while (in.readBoolean())
                {
                    int userVersion = in.readInt();
                    ByteBuffer buffer = ByteBufferUtil.readWithVIntLength(in);
                    Mutation mutation = Mutation.serializer.deserialize(buffer, userVersion);

                    if (logger.isTraceEnabled())
                        logger.trace("Received mutation {}: session={}, keyspace={}, token={}",
                                     mutation.id(),
                                     session.planId(),
                                     mutation.getKeyspaceName(),
                                     mutation.key().getToken());

                    mutation.apply();
                }

                MutationTrackingService.instance().recordFullyReconciledOffsets(header.reconciled);

                return new IncomingMutationLogStreamMessage(header, session);
            }
            catch (Throwable t)
            {
                if (t instanceof StreamReceiveException)
                    throw (StreamReceiveException) t;
                throw new StreamReceiveException(session, t);
            }
        }

        public void serialize(IncomingMutationLogStreamMessage message, StreamingDataOutputPlus out, int version, StreamSession session)
        {
            throw new UnsupportedOperationException("Not allowed to call serialize on an incoming stream");
        }

        public long serializedSize(IncomingMutationLogStreamMessage message, int version)
        {
            throw new UnsupportedOperationException("Not allowed to call serializedSize on an incoming stream");
        }
    }
}
