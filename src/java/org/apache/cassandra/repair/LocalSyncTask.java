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

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.StreamEvent;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTrees;

/**
 * LocalSyncTask performs streaming between local(coordinator) node and remote replica.
 */
public class LocalSyncTask extends SyncTask implements StreamEventHandler
{
    private final TraceState state = Tracing.instance.get();

    private static final Logger logger = LoggerFactory.getLogger(LocalSyncTask.class);

    private final UUID pendingRepair;

    @VisibleForTesting
    final boolean requestRanges;
    @VisibleForTesting
    final boolean transferRanges;

    public LocalSyncTask(RepairJobDesc desc, InetAddressAndPort local, InetAddressAndPort remote,
                         List<Range<Token>> diff, UUID pendingRepair,
                         boolean requestRanges, boolean transferRanges, PreviewKind previewKind)
    {
        super(desc, local, remote, diff, previewKind);
        Preconditions.checkArgument(requestRanges || transferRanges, "Nothing to do in a sync job");
        Preconditions.checkArgument(local.equals(FBUtilities.getBroadcastAddressAndPort()));

        this.pendingRepair = pendingRepair;
        this.requestRanges = requestRanges;
        this.transferRanges = transferRanges;
    }

    @VisibleForTesting
    StreamPlan createStreamPlan()
    {
        InetAddressAndPort remote =  nodePair.peer;

        StreamPlan plan = new StreamPlan(StreamOperation.REPAIR, 1, false, pendingRepair, previewKind)
                          .listeners(this)
                          .flushBeforeTransfer(pendingRepair == null);

        if (requestRanges)
        {
            // see comment on RangesAtEndpoint.toDummyList for why we synthesize replicas here
            plan.requestRanges(remote, desc.keyspace, RangesAtEndpoint.toDummyList(rangesToSync),
                               RangesAtEndpoint.toDummyList(Collections.emptyList()), desc.columnFamily);
        }

        if (transferRanges)
        {
            // send ranges to the remote node if we are not performing a pull repair
            // see comment on RangesAtEndpoint.toDummyList for why we synthesize replicas here
            plan.transferRanges(remote, desc.keyspace, RangesAtEndpoint.toDummyList(rangesToSync), desc.columnFamily);
        }

        return plan;
    }

    /**
     * Starts sending/receiving our list of differences to/from the remote endpoint: creates a callback
     * that will be called out of band once the streams complete.
     */
    @Override
    protected void startSync()
    {
        InetAddressAndPort remote = nodePair.peer;

        String message = String.format("Performing streaming repair of %d ranges with %s", rangesToSync.size(), remote);
        logger.info("{} {}", previewKind.logPrefix(desc.sessionId), message);
        Tracing.traceRepair(message);

        createStreamPlan().execute();
    }

    @Override
    public boolean isLocal()
    {
        return true;
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
        String message = String.format("Sync complete using session %s between %s and %s on %s", desc.sessionId, nodePair.coordinator, nodePair.peer, desc.columnFamily);
        logger.info("{} {}", previewKind.logPrefix(desc.sessionId), message);
        Tracing.traceRepair(message);
        set(stat.withSummaries(result.createSummaries()));
        finished();
    }

    public void onFailure(Throwable t)
    {
        setException(t);
        finished();
    }

    @Override
    public String toString()
    {
        return "LocalSyncTask{" +
               "requestRanges=" + requestRanges +
               ", transferRanges=" + transferRanges +
               ", rangesToSync=" + rangesToSync +
               ", nodePair=" + nodePair +
               '}';
    }
}
