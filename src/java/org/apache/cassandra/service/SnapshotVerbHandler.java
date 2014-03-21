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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.SnapshotCommand;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

public class SnapshotVerbHandler implements IVerbHandler<SnapshotCommand>
{
    private static final Logger logger = LoggerFactory.getLogger(SnapshotVerbHandler.class);

    public void doVerb(MessageIn<SnapshotCommand> message, int id)
    {
        SnapshotCommand command = message.payload;
        if (command.clear_snapshot)
        {
            Keyspace.clearSnapshot(command.snapshot_name, command.keyspace);
        }
        else
            Keyspace.open(command.keyspace).getColumnFamilyStore(command.column_family).snapshot(command.snapshot_name);
        logger.debug("Enqueuing response to snapshot request {} to {}", command.snapshot_name, message.from);
        MessagingService.instance().sendReply(new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE), id, message.from);
    }
}
