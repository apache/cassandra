/*
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
package org.apache.cassandra.service;

import org.apache.log4j.Logger;

import org.apache.cassandra.db.RangeCommand;
import org.apache.cassandra.db.RangeReply;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;

public class RangeVerbHandler implements IVerbHandler
{
    private static final Logger logger = Logger.getLogger(RangeVerbHandler.class);

    public void doVerb(Message message)
    {
        try
        {
            RangeCommand command = RangeCommand.read(message);
            Table table = Table.open(command.table);

            RangeReply rangeReply = table.getColumnFamilyStore(command.columnFamily).getKeyRange(command.startWith, command.stopAt, command.maxResults);
            Message response = rangeReply.getReply(message);
            if (logger.isDebugEnabled())
                logger.debug("Sending " + rangeReply + " to " + message.getMessageId() + "@" + message.getFrom());
            MessagingService.instance().sendOneWay(response, message.getFrom());
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}
