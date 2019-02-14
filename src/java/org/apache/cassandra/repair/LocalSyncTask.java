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
package org.apache.cassandra.repair;

import java.net.InetAddress;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.StreamEvent;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;

/**
 * LocalSyncTask performs streaming between local(coordinator) node and remote replica.
 */
public class LocalSyncTask extends SyncTask implements StreamEventHandler
{
    private final TraceState state = Tracing.instance.get();

    private static final Logger logger = LoggerFactory.getLogger(LocalSyncTask.class);

    private final long repairedAt;

    private final boolean pullRepair;

    public LocalSyncTask(RepairJobDesc desc, InetAddress firstEndpoint, InetAddress secondEndpoint, List<Range<Token>> rangesToSync, long repairedAt, boolean pullRepair)
    {
        super(desc, firstEndpoint, secondEndpoint, rangesToSync);
        this.repairedAt = repairedAt;
        this.pullRepair = pullRepair;
    }

    /**
     * Starts sending/receiving our list of differences to/from the remote endpoint: creates a callback
     * that will be called out of band once the streams complete.
     */
    protected void startSync(List<Range<Token>> differences)
    {
        InetAddress local = FBUtilities.getBroadcastAddress();
        // We can take anyone of the node as source or destination, however if one is localhost, we put at source to avoid a forwarding
        InetAddress dst = secondEndpoint.equals(local) ? firstEndpoint : secondEndpoint;
        InetAddress preferred = SystemKeyspace.getPreferredIP(dst);

        String message = String.format("Performing streaming repair of %d ranges with %s", differences.size(), dst);
        logger.info("[repair #{}] {}", desc.sessionId, message);
        boolean isIncremental = false;
        if (desc.parentSessionId != null)
        {
            ActiveRepairService.ParentRepairSession prs = ActiveRepairService.instance.getParentRepairSession(desc.parentSessionId);
            isIncremental = prs.isIncremental;
        }
        Tracing.traceRepair(message);
        StreamPlan plan = new StreamPlan("Repair", repairedAt, 1, false, isIncremental, false).listeners(this)
                                            .flushBeforeTransfer(true)
                                            // request ranges from the remote node
                                            .requestRanges(dst, preferred, desc.keyspace, differences, desc.columnFamily);
        if (!pullRepair)
        {
            // send ranges to the remote node if we are not performing a pull repair
            plan.transferRanges(dst, preferred, desc.keyspace, differences, desc.columnFamily);
        }

        plan.execute();
    }

    public void handleStreamEvent(StreamEvent event)
    {
        if (state == null)
            return;
        switch (event.eventType)
        {
            case STREAM_PREPARED:
                StreamEvent.SessionPreparedEvent spe = (StreamEvent.SessionPreparedEvent) event;
                state.trace("Streaming session with {} prepared", spe.session.peer);
                break;
            case STREAM_COMPLETE:
                StreamEvent.SessionCompleteEvent sce = (StreamEvent.SessionCompleteEvent) event;
                state.trace("Streaming session with {} {}", sce.peer, sce.success ? "completed successfully" : "failed");
                break;
            case FILE_PROGRESS:
                ProgressInfo pi = ((StreamEvent.ProgressEvent) event).progress;
                state.trace("{}/{} ({}%) {} idx:{}{}",
                            new Object[] { FBUtilities.prettyPrintMemory(pi.currentBytes),
                                           FBUtilities.prettyPrintMemory(pi.totalBytes),
                                           pi.currentBytes * 100 / pi.totalBytes,
                                           pi.direction == ProgressInfo.Direction.OUT ? "sent to" : "received from",
                                           pi.sessionIndex,
                                           pi.peer });
        }
    }

    public void onSuccess(StreamState result)
    {
        String message = String.format("Sync complete using session %s between %s and %s on %s", desc.sessionId, firstEndpoint, secondEndpoint, desc.columnFamily);
        logger.info("[repair #{}] {}", desc.sessionId, message);
        Tracing.traceRepair(message);
        set(stat);
    }

    public void onFailure(Throwable t)
    {
        setException(t);
    }
}
