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

package org.apache.cassandra.service;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexScanVerbHandler implements IVerbHandler
{
    private static final Logger logger = LoggerFactory.getLogger(IndexScanVerbHandler.class);

    public void doVerb(Message message)
    {
        try
        {
            IndexScanCommand command = IndexScanCommand.read(message);
            ColumnFamilyStore cfs = Table.open(command.keyspace).getColumnFamilyStore(command.column_family);
            RangeSliceReply reply = new RangeSliceReply(cfs.scan(command.index_clause, QueryFilter.getFilter(command.predicate, cfs.getComparator())));
            Message response = reply.getReply(message);
            if (logger.isDebugEnabled())
                logger.debug("Sending " + reply+ " to " + message.getMessageId() + "@" + message.getFrom());
            MessagingService.instance.sendOneWay(response, message.getFrom());
        }
        catch (Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }
}
