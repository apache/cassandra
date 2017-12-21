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
import java.util.UUID;

import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
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

public class AsymmetricLocalSyncTask extends AsymmetricSyncTask implements StreamEventHandler
{
    private final UUID pendingRepair;
    private final TraceState state = Tracing.instance.get();

    public AsymmetricLocalSyncTask(RepairJobDesc desc, InetAddress fetchFrom, List<Range<Token>> rangesToFetch, UUID pendingRepair, PreviewKind previewKind)
    {
        super(desc, FBUtilities.getBroadcastAddress(), fetchFrom, rangesToFetch, previewKind);
        this.pendingRepair = pendingRepair;
    }

    public void startSync(List<Range<Token>> rangesToFetch)
    {
        InetAddress preferred = SystemKeyspace.getPreferredIP(fetchFrom);
        StreamPlan plan = new StreamPlan(StreamOperation.REPAIR,
                                         1, false,
                                         false,
                                         pendingRepair,
                                         previewKind)
                          .listeners(this)
                          .flushBeforeTransfer(pendingRepair == null)
                          // request ranges from the remote node
                          .requestRanges(fetchFrom, preferred, desc.keyspace, rangesToFetch, desc.columnFamily);
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
        String message = String.format("Sync complete using session %s between %s and %s on %s", desc.sessionId, fetchingNode, fetchFrom, desc.columnFamily);
        Tracing.traceRepair(message);
        set(stat);
        finished();
    }

    public void onFailure(Throwable t)
    {
        setException(t);
        finished();
    }
}
