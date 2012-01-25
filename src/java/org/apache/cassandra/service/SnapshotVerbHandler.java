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

import org.apache.cassandra.db.SnapshotCommand;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnapshotVerbHandler implements IVerbHandler
{
    private static final Logger logger = LoggerFactory.getLogger(SnapshotVerbHandler.class);
    public void doVerb(Message message, String id)
    {
        try
        {
            SnapshotCommand command = SnapshotCommand.read(message);
            if (command.clear_snapshot)
                Table.open(command.keyspace).clearSnapshot(command.snapshot_name);
            else
                Table.open(command.keyspace).getColumnFamilyStore(command.column_family).snapshot(command.snapshot_name);
            Message response = message.getReply(FBUtilities.getBroadcastAddress(), new byte[0], MessagingService.version_);
            if (logger.isDebugEnabled())
                logger.debug("Sending response to snapshot request {} to {} ", command.snapshot_name, message.getFrom());
            MessagingService.instance().sendReply(response, id, message.getFrom());
        }
        catch (Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }
}
