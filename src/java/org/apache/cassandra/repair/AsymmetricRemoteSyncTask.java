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

import java.util.List;


import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RepairException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.messages.SyncRequest;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.SessionSummary;
import org.apache.cassandra.tracing.Tracing;

/**
 * AsymmetricRemoteSyncTask sends {@link SyncRequest} to target node to repair(stream)
 * data with other target replica.
 *
 * When AsymmetricRemoteSyncTask receives SyncComplete from the target, task completes.
 */
public class AsymmetricRemoteSyncTask extends SyncTask implements CompletableRemoteSyncTask
{
    public AsymmetricRemoteSyncTask(SharedContext ctx, RepairJobDesc desc, InetAddressAndPort to, InetAddressAndPort from, List<Range<Token>> differences, PreviewKind previewKind)
    {
        super(ctx, desc, to, from, differences, previewKind);
    }

    public void startSync()
    {
        InetAddressAndPort local = ctx.broadcastAddressAndPort();
        SyncRequest request = new SyncRequest(desc, local, nodePair.coordinator, nodePair.peer, rangesToSync, previewKind, true);
        String message = String.format("Forwarding streaming repair of %d ranges to %s (to be streamed with %s)", request.ranges.size(), request.src, request.dst);
        Tracing.traceRepair(message);
        sendRequest(request, request.src);
    }

    public void syncComplete(boolean success, List<SessionSummary> summaries)
    {
        if (success)
        {
            trySuccess(stat.withSummaries(summaries));
        }
        else
        {
            tryFailure(RepairException.warn(desc, previewKind, String.format("Sync failed between %s and %s", nodePair.coordinator, nodePair.peer)));
        }
    }

    @Override
    public String toString()
    {
        return "AsymmetricRemoteSyncTask{" +
               "rangesToSync=" + rangesToSync +
               ", nodePair=" + nodePair +
               '}';
    }
}
