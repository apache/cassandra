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
import org.apache.cassandra.streaming.StreamEvent;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.utils.FBUtilities;

/**
 * LocalSyncTask performs streaming between local(coordinator) node and remote replica.
 */
public class LocalSyncTask extends SyncTask implements StreamEventHandler
{
    private static final Logger logger = LoggerFactory.getLogger(LocalSyncTask.class);

    private final long repairedAt;

    public LocalSyncTask(RepairJobDesc desc, TreeResponse r1, TreeResponse r2, long repairedAt)
    {
        super(desc, r1, r2);
        this.repairedAt = repairedAt;
    }

    /**
     * Starts sending/receiving our list of differences to/from the remote endpoint: creates a callback
     * that will be called out of band once the streams complete.
     */
    protected void startSync(List<Range<Token>> differences)
    {
        InetAddress local = FBUtilities.getBroadcastAddress();
        // We can take anyone of the node as source or destination, however if one is localhost, we put at source to avoid a forwarding
        InetAddress dst = r2.endpoint.equals(local) ? r1.endpoint : r2.endpoint;
        InetAddress preferred = SystemKeyspace.getPreferredIP(dst);

        logger.info(String.format("[repair #%s] Performing streaming repair of %d ranges with %s", desc.sessionId, differences.size(), dst));
        new StreamPlan("Repair", repairedAt, 1, false).listeners(this)
                                            .flushBeforeTransfer(true)
                                            // request ranges from the remote node
                                            .requestRanges(dst, preferred, desc.keyspace, differences, desc.columnFamily)
                                            // send ranges to the remote node
                                            .transferRanges(dst, preferred, desc.keyspace, differences, desc.columnFamily)
                                            .execute();
    }

    public void handleStreamEvent(StreamEvent event) { /* noop */ }

    public void onSuccess(StreamState result)
    {
        logger.info(String.format("[repair #%s] Sync complete between %s and %s on %s", desc.sessionId, r1.endpoint, r2.endpoint, desc.columnFamily));
        set(stat);
    }

    public void onFailure(Throwable t)
    {
        setException(t);
    }
}
