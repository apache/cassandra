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

package org.apache.cassandra.db;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;

public class RowMutationVerbHandler implements IVerbHandler
{
    private static Logger logger_ = LoggerFactory.getLogger(RowMutationVerbHandler.class);

    public void doVerb(Message message, String id)
    {
        try
        {
            RowMutation rm = RowMutation.fromBytes(message.getMessageBody(), message.getVersion());
            if (logger_.isDebugEnabled())
              logger_.debug("Applying " + rm);

            // Check if there were any forwarding headers in this message
            byte[] forwardBytes = message.getHeader(RowMutation.FORWARD_HEADER);
            if (forwardBytes != null && message.getVersion() >= MessagingService.VERSION_11)
                forwardToLocalNodes(message, forwardBytes);

            rm.apply();

            WriteResponse response = new WriteResponse(rm.getTable(), rm.key(), true);
            Message responseMessage = WriteResponse.makeWriteResponseMessage(message, response);
            if (logger_.isDebugEnabled())
              logger_.debug(rm + " applied.  Sending response to " + id + "@" + message.getFrom());
            MessagingService.instance().sendReply(responseMessage, id, message.getFrom());
        }
        catch (IOException e)
        {
            logger_.error("Error in row mutation", e);
        }
    }

    /**
     * Older version (< 1.0) will not send this message at all, hence we don't
     * need to check the version of the data.
     */
    private void forwardToLocalNodes(Message message, byte[] forwardBytes) throws IOException
    {
        DataInputStream dis = new DataInputStream(new FastByteArrayInputStream(forwardBytes));
        int size = dis.readInt();

        // remove fwds from message to avoid infinite loop
        Message messageCopy = message.withHeaderRemoved(RowMutation.FORWARD_HEADER);
        for (int i = 0; i < size; i++)
        {
            // Send a message to each of the addresses on our Forward List
            InetAddress address = CompactEndpointSerializationHelper.deserialize(dis);
            String id = dis.readUTF();
            if (logger_.isDebugEnabled())
                logger_.debug("Forwarding message to " + address + " with= ID: " + id);
            // Let the response go back to the coordinator
            MessagingService.instance().sendOneWay(messageCopy, id, address);
        }
    }
}
