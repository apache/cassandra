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
import java.io.IOError;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;

public class TruncateVerbHandler implements IVerbHandler<Truncation>
{
    private static final Logger logger = LoggerFactory.getLogger(TruncateVerbHandler.class);

    public void doVerb(MessageIn<Truncation> message, String id)
    {
        try
        {
            Truncation t = message.payload;
            logger.debug("Applying {}", t);

            try
            {
                ColumnFamilyStore cfs = Table.open(t.keyspace).getColumnFamilyStore(t.columnFamily);
                cfs.truncate().get();
            }
            catch (Exception e)
            {
                logger.error("Error in truncation", e);
                respondError(t, message);
            }
            logger.debug("Truncate operation succeeded at this host");

            TruncateResponse response = new TruncateResponse(t.keyspace, t.columnFamily, true);
            logger.debug("{} applied.  Sending response to {}@{} ", new Object[]{ t, id, message.from });
            MessagingService.instance().sendReply(response.createMessage(), id, message.from);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    private static void respondError(Truncation t, MessageIn truncateRequestMessage) throws IOException
    {
        TruncateResponse response = new TruncateResponse(t.keyspace, t.columnFamily, false);
        MessagingService.instance().sendOneWay(response.createMessage(), truncateRequestMessage.from);
    }
}
