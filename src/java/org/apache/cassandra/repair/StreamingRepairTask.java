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
import java.util.UUID;
import java.util.Collections;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.messages.SyncComplete;
import org.apache.cassandra.repair.messages.SyncRequest;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.StreamEvent;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.streaming.StreamOperation;

/**
 * StreamingRepairTask performs data streaming between two remote replica which neither is not repair coordinator.
 * Task will send {@link SyncComplete} message back to coordinator upon streaming completion.
 */
public class StreamingRepairTask implements Runnable, StreamEventHandler
{
    private static final Logger logger = LoggerFactory.getLogger(StreamingRepairTask.class);

    private final RepairJobDesc desc;
    private final SyncRequest request;
    private final UUID pendingRepair;
    private final PreviewKind previewKind;

    public StreamingRepairTask(RepairJobDesc desc, SyncRequest request, UUID pendingRepair, PreviewKind previewKind)
    {
        this.desc = desc;
        this.request = request;
        this.pendingRepair = pendingRepair;
        this.previewKind = previewKind;
    }

    public void run()
    {
        InetAddress dest = request.dst;
        InetAddress preferred = SystemKeyspace.getPreferredIP(dest);
        logger.info("[streaming task #{}] Performing streaming repair of {} ranges with {}", desc.sessionId, request.ranges.size(), request.dst);
        createStreamPlan(dest, preferred).execute();
    }

    @VisibleForTesting
    StreamPlan createStreamPlan(InetAddress dest, InetAddress preferred)
    {
        return new StreamPlan(StreamOperation.REPAIR, 1, false, false, pendingRepair, previewKind)
               .listeners(this)
               .flushBeforeTransfer(pendingRepair == null) // sstables are isolated at the beginning of an incremental repair session, so flushing isn't neccessary
               .requestRanges(dest, preferred, desc.keyspace, request.ranges, desc.columnFamily) // request ranges from the remote node
               .transferRanges(dest, preferred, desc.keyspace, request.ranges, desc.columnFamily); // send ranges to the remote node
    }

    public void handleStreamEvent(StreamEvent event)
    {
        // Nothing to do here, all we care about is the final success or failure and that's handled by
        // onSuccess and onFailure
    }

    /**
     * If we succeeded on both stream in and out, reply back to coordinator
     */
    public void onSuccess(StreamState state)
    {
        logger.info("{} streaming task succeed, returning response to {}", previewKind.logPrefix(desc.sessionId), request.initiator);
        MessagingService.instance().sendOneWay(new SyncComplete(desc, request.src, request.dst, true, state.createSummaries()).createMessage(), request.initiator);
    }

    /**
     * If we failed on either stream in or out, reply fail to coordinator
     */
    public void onFailure(Throwable t)
    {
        MessagingService.instance().sendOneWay(new SyncComplete(desc, request.src, request.dst, false, Collections.emptyList()).createMessage(), request.initiator);
    }
}
