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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;

public class StreamReplyVerbHandler implements IVerbHandler<StreamReply>
{
    private static final Logger logger = LoggerFactory.getLogger(StreamReplyVerbHandler.class);

    public void doVerb(MessageIn<StreamReply> message, String id)
    {
        StreamReply reply = message.payload;
        logger.debug("Received StreamReply {}", reply);
        StreamOutSession session = StreamOutSession.get(reply.sessionId);
        if (session == null)
        {
            logger.debug("Received stream action " + reply.action + " for an unknown session from " + message.from);
            return;
        }

        switch (reply.action)
        {
            case FILE_FINISHED:
                logger.info("Successfully sent {} to {}", reply.file, message.from);
                session.validateCurrentFile(reply.file);
                session.startNext();
                break;
            case FILE_RETRY:
                session.validateCurrentFile(reply.file);
                logger.info("Need to re-stream file {} to {}", reply.file, message.from);
                session.retry();
                break;
            case SESSION_FINISHED:
                session.close(true);
                break;
            case SESSION_FAILURE:
                session.close(false);
                break;
            default:
                throw new RuntimeException("Cannot handle FileStatus.Action: " + reply.action);
        }
    }
}
