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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.messages.SyncComplete;
import org.apache.cassandra.repair.messages.SyncRequest;
import org.apache.cassandra.streaming.StreamEvent;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamState;

/**
 * StreamingRepairTask performs data streaming between two remote replica which neither is not repair coordinator.
 * Task will send {@link SyncComplete} message back to coordinator upon streaming completion.
 */
public class StreamingRepairTask implements Runnable, StreamEventHandler
{
    private static final Logger logger = LoggerFactory.getLogger(StreamingRepairTask.class);

    private final RepairJobDesc desc;
    private final SyncRequest request;
    private final long repairedAt;

    public StreamingRepairTask(RepairJobDesc desc, SyncRequest request, long repairedAt)
    {
        this.desc = desc;
        this.request = request;
        this.repairedAt = repairedAt;
    }

    public void run()
    {
        InetAddress dest = request.dst;
        InetAddress preferred = SystemKeyspace.getPreferredIP(dest);
        logger.info(String.format("[streaming task #%s] Performing streaming repair of %d ranges with %s", desc.sessionId, request.ranges.size(), request.dst));
        new StreamPlan("Repair", repairedAt, 1, false).listeners(this)
                                            .flushBeforeTransfer(true)
                                            // request ranges from the remote node
                                            .requestRanges(dest, preferred, desc.keyspace, request.ranges, desc.columnFamily)
                                            // send ranges to the remote node
                                            .transferRanges(dest, preferred, desc.keyspace, request.ranges, desc.columnFamily)
                                            .execute();
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
        logger.info(String.format("[repair #%s] streaming task succeed, returning response to %s", desc.sessionId, request.initiator));
        MessagingService.instance().sendOneWay(new SyncComplete(desc, request.src, request.dst, true).createMessage(), request.initiator);
    }

    /**
     * If we failed on either stream in or out, reply fail to coordinator
     */
    public void onFailure(Throwable t)
    {
        MessagingService.instance().sendOneWay(new SyncComplete(desc, request.src, request.dst, false).createMessage(), request.initiator);
    }
}
