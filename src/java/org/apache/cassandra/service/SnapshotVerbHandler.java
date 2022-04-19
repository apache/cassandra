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

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SnapshotCommand;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.DiagnosticSnapshotService;

import static org.apache.cassandra.net.ParamType.SNAPSHOT_RANGES;

public class SnapshotVerbHandler implements IVerbHandler<SnapshotCommand>
{
    public static final SnapshotVerbHandler instance = new SnapshotVerbHandler();
    private static final Logger logger = LoggerFactory.getLogger(SnapshotVerbHandler.class);

    public void doVerb(Message<SnapshotCommand> message)
    {
        SnapshotCommand command = message.payload;
        if (command.clear_snapshot)
        {
            StorageService.instance.clearSnapshot(command.snapshot_name, command.keyspace);
        }
        else if (DiagnosticSnapshotService.isDiagnosticSnapshotRequest(command))
        {
            List<Range<Token>> ranges = Collections.emptyList();
            if (message.header.params().containsKey(SNAPSHOT_RANGES))
                ranges = (List<Range<Token>>) message.header.params().get(SNAPSHOT_RANGES);
            DiagnosticSnapshotService.snapshot(command, ranges, message.from());
        }
        else
        {
            Keyspace.open(command.keyspace).getColumnFamilyStore(command.column_family).snapshot(command.snapshot_name);
        }

        logger.debug("Enqueuing response to snapshot request {} to {}", command.snapshot_name, message.from());
        MessagingService.instance().send(message.emptyResponse(), message.from());
    }
}
