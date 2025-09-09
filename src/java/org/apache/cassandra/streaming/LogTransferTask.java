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
package org.apache.cassandra.streaming;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.replication.MutationJournal;
import org.apache.cassandra.replication.ReconciledKeyspaceOffsets;
import org.apache.cassandra.replication.ReconciledLogSnapshot;
import org.apache.cassandra.streaming.messages.OutgoingMutationLogStreamMessage;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Task for tracking sending of mutation log streams.
 */
public class LogTransferTask extends LogStreamTask
{
    private static final Logger logger = LoggerFactory.getLogger(LogTransferTask.class);

    private volatile ScheduledFuture<?> timeoutFuture;
    private final ReconciledLogSnapshot reconciled;
    private final MutationJournal.Snapshot snapshot;

    public LogTransferTask(StreamSession session, InetAddressAndPort peer, ReconciledLogSnapshot reconciled, MutationJournal.Snapshot snapshot)
    {
        super(session, peer);
        this.reconciled = reconciled;
        this.snapshot = snapshot;
    }

    public ReconciledKeyspaceOffsets reconciled(String keyspace, Collection<Range<Token>> ranges)
    {
        ReconciledLogSnapshot subset = reconciled.select(Collections.singletonMap(keyspace, ranges));
        return subset.getKeyspace(keyspace);
    }

    public OutgoingMutationLogStreamMessage getMessage(StreamSession session)
    {
        LogStreamManifest manifest = getManifest();

        ReconciledLogSnapshot subset = reconciled.select(manifest.keyspaceRanges);

        LogStreamHeader header = new LogStreamHeader(manifest,
                                                      subset,
                                                      FBUtilities.getBroadcastAddressAndPort(),
                                                      session.planId(),
                                                      0,
                                                      session.isFollower());
        logger.trace("[Stream #{}] Creating outgoing mutation log stream message for peer {}", session.planId(), peer);
        return new OutgoingMutationLogStreamMessage(header, snapshot);
    }

    private synchronized void cancelTimeout()
    {
        if (timeoutFuture != null)
        {
            timeoutFuture.cancel(false);
            timeoutFuture = null;
        }
    }

    public synchronized void complete()
    {
        // Cancel timeout on successful completion
        cancelTimeout();
        // TODO: validate message header with expected ranges
        logger.trace("[Stream #{}] Log transfer task completed for peer {}", session.planId(), peer);
        if (markCompleted())
            session.taskCompleted(this);
    }

    public void scheduleTimeout()
    {
        timeoutFuture = ScheduledExecutors.nonPeriodicTasks.schedule(session::sessionTimeout, DatabaseDescriptor.getStreamTransferTaskTimeout().toMilliseconds(), TimeUnit.MILLISECONDS);
    }

    public void timeout()
    {
        session.sessionTimeout();
    }

    @Override
    public void abort()
    {
        cancelTimeout();
    }
}