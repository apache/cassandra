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

package org.apache.cassandra.streaming;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.streaming.messages.KeepAliveMessage;
import org.apache.cassandra.streaming.messages.StreamMessage;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static org.apache.cassandra.streaming.StreamSession.createLogTag;

/**
 * The task that performs the actual deserialization.
 */
public class StreamDeserializingTask implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(StreamDeserializingTask.class);

    private final StreamingChannel channel;
    private final int messagingVersion;
    @VisibleForTesting
    protected StreamSession session;

    public StreamDeserializingTask(StreamSession session, StreamingChannel channel, int messagingVersion)
    {
        this.session = session;
        this.channel = channel;
        this.messagingVersion = messagingVersion;
    }

    @Override
    public void run()
    {
        StreamingDataInputPlus input = channel.in();
        try
        {
            StreamMessage message;
            while (null != (message = StreamMessage.deserialize(input, messagingVersion)))
            {
                // keep-alives don't necessarily need to be tied to a session (they could be arrive before or after
                // wrt session lifecycle, due to races), just log that we received the message and carry on
                if (message instanceof KeepAliveMessage)
                {
                    if (logger.isDebugEnabled())
                        logger.debug("{} Received {}", createLogTag(session, channel), message);
                    continue;
                }

                if (session == null)
                    session = deriveSession(message);

                if (logger.isDebugEnabled())
                    logger.debug("{} Received {}", createLogTag(session, channel), message);

                session.messageReceived(message);
            }
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            if (session != null)
            {
                session.onError(t);
            }
            else if (t instanceof StreamReceiveException)
            {
                ((StreamReceiveException)t).session.onError(t.getCause());
            }
            else
            {
                logger.error("{} stream operation from {} failed", createLogTag(session, channel), InetAddressAndPort.toString(channel.peer(), true), t);
            }
        }
        finally
        {
            channel.close();
            input.close();
        }
    }

    @VisibleForTesting
    public StreamSession deriveSession(StreamMessage message)
    {
        // StreamInitMessage starts a new channel here, but IncomingStreamMessage needs a session
        // to be established a priori
        StreamSession streamSession = message.getOrCreateAndAttachInboundSession(channel, messagingVersion);

        // Attach this channel to the session: this only happens upon receiving the first init message as a follower;
        // in all other cases, no new control channel will be added, as the proper control channel will be already attached.
        streamSession.attachInbound(channel);
        return streamSession;
    }
}
