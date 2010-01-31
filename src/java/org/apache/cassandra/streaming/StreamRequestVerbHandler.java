 /**
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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.IOError;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;

import org.apache.log4j.Logger;

 /**
 * This verb handler handles the StreamRequestMessage that is sent by
 * the node requesting range transfer.
*/
public class StreamRequestVerbHandler implements IVerbHandler
{
    private static Logger logger_ = Logger.getLogger(StreamRequestVerbHandler.class);
    
    public void doVerb(Message message)
    {
        if (logger_.isDebugEnabled())
            logger_.debug("Received a StreamRequestMessage from " + message.getFrom());
        
        byte[] body = message.getMessageBody();
        ByteArrayInputStream bufIn = new ByteArrayInputStream(body);
        try
        {
            StreamRequestMessage streamRequestMessage = StreamRequestMessage.serializer().deserialize(new DataInputStream(bufIn));
            StreamRequestMetadata[] streamRequestMetadata = streamRequestMessage.streamRequestMetadata_;

            for (StreamRequestMetadata srm : streamRequestMetadata)
            {
                if (logger_.isDebugEnabled())
                    logger_.debug(srm.toString());
                StreamOut.transferRanges(srm.target_, srm.ranges_, null);
            }
        }
        catch (IOException ex)
        {
            throw new IOError(ex);
        }
    }
}

