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
import java.util.Collection;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.locator.RangesAtEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.repair.messages.SyncResponse;
import org.apache.cassandra.repair.state.SyncState;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.StreamEvent;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.utils.TimeUUID;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.net.Verb.SYNC_RSP;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;


/**
 * StreamingRepairTask performs data streaming between two remote replicas, neither of which is repair coordinator.
 * Task will send {@link SyncResponse} message back to coordinator upon streaming completion.
 */
public class StreamingRepairTask implements Runnable, StreamEventHandler
{
    private static final Logger logger = LoggerFactory.getLogger(StreamingRepairTask.class);

    private final SharedContext ctx;
    private final SyncState state;
    private final RepairJobDesc desc;
    private final boolean asymmetric;
    private final InetAddressAndPort initiator;
    private final InetAddressAndPort src;
    private final InetAddressAndPort dst;
    private final Collection<Range<Token>> ranges;
    private final TimeUUID pendingRepair;
    private final PreviewKind previewKind;

    public StreamingRepairTask(SharedContext ctx, SyncState state, RepairJobDesc desc, InetAddressAndPort initiator, InetAddressAndPort src, InetAddressAndPort dst, Collection<Range<Token>> ranges, TimeUUID pendingRepair, PreviewKind previewKind, boolean asymmetric)
    {
        this.ctx = ctx;
        this.state = state;
        this.desc = desc;
        this.initiator = initiator;
        this.src = src;
        this.dst = dst;
        this.ranges = ranges;
        this.asymmetric = asymmetric;
        this.pendingRepair = pendingRepair;
        this.previewKind = previewKind;
    }

    public void run()
    {
        logger.info("[streaming task #{}] Performing {}streaming repair of {} ranges with {}", desc.sessionId, asymmetric ? "asymmetric " : "", ranges.size(), dst);
        long start = approxTime.now();
        StreamPlan streamPlan = createStreamPlan(dst);
        logger.info("[streaming task #{}] Stream plan created in {}ms", desc.sessionId, MILLISECONDS.convert(approxTime.now() - start, NANOSECONDS));
        state.phase.start();
        ctx.streamExecutor().execute(streamPlan);
    }

    @VisibleForTesting
    StreamPlan createStreamPlan(InetAddressAndPort dest)
    {
        state.phase.planning();
        StreamPlan sp = new StreamPlan(StreamOperation.REPAIR, 1, false, pendingRepair, previewKind)
               .listeners(this)
               .flushBeforeTransfer(pendingRepair == null) // sstables are isolated at the beginning of an incremental repair session, so flushing isn't neccessary
               // see comment on RangesAtEndpoint.toDummyList for why we synthesize replicas here
               .requestRanges(dest, desc.keyspace, RangesAtEndpoint.toDummyList(ranges),
                       RangesAtEndpoint.toDummyList(Collections.emptyList()), desc.columnFamily); // request ranges from the remote node
        if (!asymmetric)
            // see comment on RangesAtEndpoint.toDummyList for why we synthesize replicas here
            sp.transferRanges(dest, desc.keyspace, RangesAtEndpoint.toDummyList(ranges), desc.columnFamily); // send ranges to the remote node
        return sp;
    }

    @Override
    public void handleStreamEvent(StreamEvent event)
    {
        // Nothing to do here, all we care about is the final success or failure and that's handled by
        // onSuccess and onFailure
    }

    /**
     * If we succeeded on both stream in and out, respond back to coordinator
     */
    @Override
    public void onSuccess(StreamState state)
    {
        logger.info("[repair #{}] streaming task succeed, returning response to {}", desc.sessionId, initiator);
        this.state.phase.success();
        RepairMessage.sendMessageWithRetries(ctx, new SyncResponse(desc, src, dst, true, state.createSummaries()), SYNC_RSP, initiator);
    }

    /**
     * If we failed on either stream in or out, respond fail to coordinator
     */
    @Override
    public void onFailure(Throwable t)
    {
        this.state.phase.fail(t);
        RepairMessage.sendMessageWithRetries(ctx, new SyncResponse(desc, src, dst, false, Collections.emptyList()), SYNC_RSP, initiator);
    }
}
