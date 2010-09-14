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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOError;
import java.io.IOException;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.StorageService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamStatusVerbHandler implements IVerbHandler
{
    private static Logger logger = LoggerFactory.getLogger(StreamStatusVerbHandler.class);

    public void doVerb(Message message)
    {
        byte[] body = message.getMessageBody();
        ByteArrayInputStream bufIn = new ByteArrayInputStream(body);

        try
        {
            FileStatus streamStatus = FileStatus.serializer().deserialize(new DataInputStream(bufIn));
            StreamOutSession session = StreamOutSession.get(message.getFrom(), streamStatus.getSessionId());

            switch (streamStatus.getAction())
            {
                case DELETE:
                    session.finishAndStartNext(streamStatus.getFile());
                    break;
                case STREAM:
                    logger.warn("Need to re-stream file {} to {}", streamStatus.getFile(), message.getFrom());
                    session.retry(streamStatus.getFile());
                    break;
                case EMPTY:
                    logger.error("Did not find matching ranges on {}", message.getFrom());
                    StreamInSession.get(message.getFrom(), streamStatus.getSessionId()).remove();
                    if (StorageService.instance.isBootstrapMode())
                        StorageService.instance.removeBootstrapSource(message.getFrom(), new String(message.getHeader(StreamOut.TABLE_NAME)));
                    break;
                default:
                    throw new RuntimeException("Cannot handle FileStatus.Action: " + streamStatus.getAction());
            }
        }
        catch (IOException ex)
        {
            throw new IOError(ex);
        }
    }
}
