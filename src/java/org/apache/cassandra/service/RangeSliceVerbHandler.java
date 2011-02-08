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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.RangeSliceCommand;
import org.apache.cassandra.db.RangeSliceReply;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;

public class RangeSliceVerbHandler implements IVerbHandler
{

    private static final Logger logger = LoggerFactory.getLogger(RangeSliceVerbHandler.class);

    public void doVerb(Message message, String id)
    {
        try
        {
            if (StorageService.instance.isBootstrapMode())
            {
                /* Don't service reads! */
                throw new RuntimeException("Cannot service reads while bootstrapping!");
            }
            RangeSliceCommand command = RangeSliceCommand.read(message);
            ColumnFamilyStore cfs = Table.open(command.keyspace).getColumnFamilyStore(command.column_family);
            RangeSliceReply reply = new RangeSliceReply(cfs.getRangeSlice(command.super_column,
                                                                          command.range,
                                                                          command.max_keys,
                                                                          QueryFilter.getFilter(command.predicate, cfs.getComparator())));
            Message response = reply.getReply(message);
            if (logger.isDebugEnabled())
                logger.debug("Sending " + reply+ " to " + id + "@" + message.getFrom());
            MessagingService.instance().sendReply(response, id, message.getFrom());
        }
        catch (Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }
}
