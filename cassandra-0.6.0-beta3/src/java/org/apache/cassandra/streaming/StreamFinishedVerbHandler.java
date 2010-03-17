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

import org.apache.log4j.Logger;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.streaming.StreamOutManager;

public class StreamFinishedVerbHandler implements IVerbHandler
{
    private static Logger logger = Logger.getLogger(StreamFinishedVerbHandler.class);

    public void doVerb(Message message)
    {
        byte[] body = message.getMessageBody();
        ByteArrayInputStream bufIn = new ByteArrayInputStream(body);

        try
        {
            CompletedFileStatus streamStatus = CompletedFileStatus.serializer().deserialize(new DataInputStream(bufIn));

            switch (streamStatus.getAction())
            {
                case DELETE:
                    StreamOutManager.get(message.getFrom()).finishAndStartNext(streamStatus.getFile());
                    break;

                case STREAM:
                    if (logger.isDebugEnabled())
                        logger.debug("Need to re-stream file " + streamStatus.getFile());
                    StreamOutManager.get(message.getFrom()).startNext();
                    break;

                default:
                    break;
            }
        }
        catch (IOException ex)
        {
            throw new IOError(ex);
        }
    }
}
