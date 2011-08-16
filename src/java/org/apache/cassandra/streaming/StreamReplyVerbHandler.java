package org.apache.cassandra.streaming;

/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

import java.io.DataInputStream;
import java.io.IOError;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;

public class StreamReplyVerbHandler implements IVerbHandler
{
    private static Logger logger = LoggerFactory.getLogger(StreamReplyVerbHandler.class);

    public void doVerb(Message message, String id)
    {
        byte[] body = message.getMessageBody();
        FastByteArrayInputStream bufIn = new FastByteArrayInputStream(body);

        try
        {
            StreamReply reply = StreamReply.serializer.deserialize(new DataInputStream(bufIn), message.getVersion());
            logger.debug("Received StreamReply {}", reply);
            StreamOutSession session = StreamOutSession.get(message.getFrom(), reply.sessionId);
            if (session == null)
            {
                logger.debug("Received stream action " + reply.action + " for an unknown session from " + message.getFrom());
                return;
            }

            switch (reply.action)
            {
                case FILE_FINISHED:
                    session.validateCurrentFile(reply.file);
                    session.startNext();
                    break;
                case FILE_RETRY:
                    session.validateCurrentFile(reply.file);
                    logger.info("Need to re-stream file {} to {}", reply.file, message.getFrom());
                    session.retry();
                    break;
                case SESSION_FINISHED:
                    session.close();
                    break;
                default:
                    throw new RuntimeException("Cannot handle FileStatus.Action: " + reply.action);
            }
        }
        catch (IOException ex)
        {
            throw new IOError(ex);
        }
    }
}
