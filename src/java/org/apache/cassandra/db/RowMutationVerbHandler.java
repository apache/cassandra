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
package org.apache.cassandra.db;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.net.*;
import org.apache.cassandra.tracing.Tracing;

public class RowMutationVerbHandler implements IVerbHandler<RowMutation>
{
    private static final Logger logger = LoggerFactory.getLogger(RowMutationVerbHandler.class);

    public void doVerb(MessageIn<RowMutation> message, String id)
    {
        try
        {
            RowMutation rm = message.payload;
            logger.debug("Applying mutation");

            // Check if there were any forwarding headers in this message
            InetAddress replyTo = message.from;
            byte[] from = message.parameters.get(RowMutation.FORWARD_FROM);
            if (from == null)
            {
                byte[] forwardBytes = message.parameters.get(RowMutation.FORWARD_TO);
                if (forwardBytes != null && message.version >= MessagingService.VERSION_11)
                    forwardToLocalNodes(rm, message.verb, forwardBytes, message.from);
            }
            else
            {
                replyTo = InetAddress.getByAddress(from);
            }

            rm.apply();
            WriteResponse response = new WriteResponse();
            Tracing.trace("Enqueuing response to {}", replyTo);
            MessagingService.instance().sendReply(response.createMessage(), id, replyTo);
        }
        catch (IOException e)
        {
            logger.error("Error in row mutation", e);
        }
    }

    /**
     * Older version (< 1.0) will not send this message at all, hence we don't
     * need to check the version of the data.
     */
    private void forwardToLocalNodes(RowMutation rm, MessagingService.Verb verb, byte[] forwardBytes, InetAddress from) throws IOException
    {
        DataInputStream dis = new DataInputStream(new FastByteArrayInputStream(forwardBytes));
        int size = dis.readInt();

        // remove fwds from message to avoid infinite loop
        MessageOut<RowMutation> message = new MessageOut<RowMutation>(verb, rm, RowMutation.serializer).withParameter(RowMutation.FORWARD_FROM, from.getAddress());
        for (int i = 0; i < size; i++)
        {
            // Send a message to each of the addresses on our Forward List
            InetAddress address = CompactEndpointSerializationHelper.deserialize(dis);
            String id = dis.readUTF();
            logger.debug("Forwarding message to {}@{}", id, address);
            // Let the response go back to the coordinator
            MessagingService.instance().sendOneWay(message, id, address);
        }
    }
}
