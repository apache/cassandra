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

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SnapshotCommand;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;

public class SnapshotVerbHandler implements IVerbHandler<SnapshotCommand>
{
    public static final SnapshotVerbHandler instance = new SnapshotVerbHandler();
    public static final String REPAIRED_DATA_MISMATCH_SNAPSHOT_PREFIX = "RepairedDataMismatch-";
    private static final Executor REPAIRED_DATA_MISMATCH_SNAPSHOT_EXECUTOR = Executors.newSingleThreadExecutor();

    private static final Logger logger = LoggerFactory.getLogger(SnapshotVerbHandler.class);

    public void doVerb(Message<SnapshotCommand> message)
    {
        SnapshotCommand command = message.payload;
        if (command.clear_snapshot)
        {
            Keyspace.clearSnapshot(command.snapshot_name, command.keyspace);
        }
        else if (command.snapshot_name.startsWith(REPAIRED_DATA_MISMATCH_SNAPSHOT_PREFIX))
        {
            REPAIRED_DATA_MISMATCH_SNAPSHOT_EXECUTOR.execute(new RepairedDataSnapshotTask(command, message.from()));
        }
        else
        {
            Keyspace.open(command.keyspace).getColumnFamilyStore(command.column_family).snapshot(command.snapshot_name);
        }

        logger.debug("Enqueuing response to snapshot request {} to {}", command.snapshot_name, message.from());
        MessagingService.instance().send(message.emptyResponse(), message.from());
    }

    private static class RepairedDataSnapshotTask implements Runnable
    {
        final SnapshotCommand command;
        final InetAddressAndPort from;

        RepairedDataSnapshotTask(SnapshotCommand command, InetAddressAndPort from)
        {
            this.command = command;
            this.from = from;
        }

        public void run()
        {
            try
            {
                Keyspace ks = Keyspace.open(command.keyspace);
                if (ks == null)
                {
                    logger.info("Snapshot request received from {} for {}.{} but keyspace not found",
                                from,
                                command.keyspace,
                                command.column_family);
                    return;
                }

                ColumnFamilyStore cfs = ks.getColumnFamilyStore(command.column_family);
                if (cfs.snapshotExists(command.snapshot_name))
                {
                    logger.info("Received snapshot request from {} for {}.{} following repaired data mismatch, " +
                                "but snapshot with tag {} already exists",
                                from,
                                command.keyspace,
                                command.column_family,
                                command.snapshot_name);
                    return;
                }
                logger.info("Creating snapshot requested by {} of {}.{} following repaired data mismatch",
                            from,
                            command.keyspace,
                            command.column_family);
                cfs.snapshot(command.snapshot_name);
            }
            catch (IllegalArgumentException e)
            {
                logger.warn("Snapshot request received from {} for {}.{} but table not found",
                            from,
                            command.keyspace,
                            command.column_family);
            }
        }
    }
}
