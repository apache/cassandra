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

import java.io.*;

import java.net.InetAddress;
import java.nio.ByteBuffer;

import com.google.common.base.Charsets;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.net.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static com.google.common.base.Charsets.UTF_8;


public class RowMutationVerbHandler implements IVerbHandler
{
    private static Logger logger_ = LoggerFactory.getLogger(RowMutationVerbHandler.class);

    public void doVerb(Message message)
    {
        try
        {
            RowMutation rm = RowMutation.fromBytes(message.getMessageBody());
            if (logger_.isDebugEnabled())
              logger_.debug("Applying " + rm);

            /* Check if there were any hints in this message */
            byte[] hintedBytes = message.getHeader(RowMutation.HINT);
            if (hintedBytes != null)
            {
                assert hintedBytes.length > 0;
                DataInputStream dis = new DataInputStream(new ByteArrayInputStream(hintedBytes));
                while (dis.available() > 0)
                {
                    ByteBuffer addressBytes = FBUtilities.readShortByteArray(dis);
                    if (logger_.isDebugEnabled())
                        logger_.debug("Adding hint for " + InetAddress.getByName(ByteBufferUtil.string(addressBytes, Charsets.UTF_8)));
                    RowMutation hintedMutation = new RowMutation(Table.SYSTEM_TABLE, addressBytes);
                    hintedMutation.addHints(rm);
                    hintedMutation.apply();
                }
            }

            Table.open(rm.getTable()).apply(rm, true);

            WriteResponse response = new WriteResponse(rm.getTable(), rm.key(), true);
            Message responseMessage = WriteResponse.makeWriteResponseMessage(message, response);
            if (logger_.isDebugEnabled())
              logger_.debug(rm + " applied.  Sending response to " + message.getMessageId() + "@" + message.getFrom());
            MessagingService.instance.sendOneWay(responseMessage, message.getFrom());
        }
        catch (IOException e)
        {
            logger_.error("Error in row mutation", e);
        }
    }
}
