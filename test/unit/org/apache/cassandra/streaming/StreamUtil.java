/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.apache.cassandra.streaming;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.net.InetAddress;

import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;

public class StreamUtil
{

    /**
     * Takes an stream request message and creates an empty status response. Exists here because StreamRequestMessage
     * is package protected.
     */
    static public void finishStreamRequest(Message msg, InetAddress to) 
    {
        byte[] body = msg.getMessageBody();
        ByteArrayInputStream bufIn = new ByteArrayInputStream(body);

        try
        {
            StreamRequestMessage srm = StreamRequestMessage.serializer().deserialize(new DataInputStream(bufIn), MessagingService.version_);
            StreamInSession session = StreamInSession.get(to, srm.sessionId);
            session.closeIfFinished();
        }
        catch (Exception e)
        {
            System.err.println(e); 
            e.printStackTrace();
        }
    }
}
