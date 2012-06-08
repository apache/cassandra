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
package org.apache.cassandra.service;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.RangeSliceCommand;
import org.apache.cassandra.db.RangeSliceReply;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.IFilter;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;

public class RangeSliceVerbHandler implements IVerbHandler<RangeSliceCommand>
{
    private static final Logger logger = LoggerFactory.getLogger(RangeSliceVerbHandler.class);

    static List<Row> executeLocally(RangeSliceCommand command) throws ExecutionException, InterruptedException
    {
        ColumnFamilyStore cfs = Table.open(command.keyspace).getColumnFamilyStore(command.column_family);
        if (cfs.indexManager.hasIndexFor(command.row_filter))
            return cfs.search(command.row_filter, command.range, command.maxResults, command.predicate, command.maxIsColumns);
        else
            return cfs.getRangeSlice(command.super_column, command.range, command.maxResults, command.predicate, command.row_filter, command.maxIsColumns, command.isPaging);
    }

    public void doVerb(MessageIn<RangeSliceCommand> message, String id)
    {
        try
        {
            if (StorageService.instance.isBootstrapMode())
            {
                /* Don't service reads! */
                throw new RuntimeException("Cannot service reads while bootstrapping!");
            }
            RangeSliceReply reply = new RangeSliceReply(executeLocally(message.payload));
            if (logger.isDebugEnabled())
                logger.debug("Sending " + reply+ " to " + id + "@" + message.from);
            MessagingService.instance().sendReply(reply.createMessage(), id, message.from);
        }
        catch (Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }
}
