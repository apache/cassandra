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

package org.apache.cassandra.service.paxos.v1;


import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.DecoratedKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.PrepareResponse;
import org.apache.cassandra.transport.Dispatcher;

public class PrepareCallback extends AbstractPaxosCallback<PrepareResponse>
{
    private static final Logger logger = LoggerFactory.getLogger(PrepareCallback.class);

    private boolean promised = true;
    private Commit mostRecentCommit;
    private Commit mostRecentInProgressCommit;

    private final Map<InetAddressAndPort, Commit> commitsByReplica = new ConcurrentHashMap<>();


    public PrepareCallback(DecoratedKey key, TableMetadata metadata, ReplicaPlan.ForPaxosWrite replicaPlan, Dispatcher.RequestTime requestTime)
    {
        super(replicaPlan.contacts().endpoints(), replicaPlan.requiredParticipants(), replicaPlan.consistencyLevel(), requestTime);
        // need to inject the right key in the empty commit so comparing with empty commits in the response works as expected
        mostRecentCommit = Commit.emptyCommit(key, metadata);
        mostRecentInProgressCommit = Commit.emptyCommit(key, metadata);
    }

    public boolean isPromised() { return promised; }
    public Commit getMostRecentCommit() { return mostRecentCommit; }
    public Commit getMostRecentInProgressCommit() { return mostRecentInProgressCommit; }

    public synchronized void onResponse(Message<PrepareResponse> msg)
    {
        Preconditions.checkNotNull(msg, "Got unexpected null message in onResponse callback.");
        PrepareResponse response = msg.payload;
        tracker.recordResponse(msg.from());
        logger.trace("Prepare response {} from {}", response, msg.from());

        // We don't want to flip the promise state if we've already succeeded or failed on this callback; if we've hit
        // a quorum promised subsequent failures shouldn't be considered in our calculation.
        if (latch.count() == 0)
            return;

        // We set the mostRecentInProgressCommit even if we're not promised as, in that case, the ballot of that commit
        // will be used to avoid generating a ballot that has not chance to win on retry (think clock skew).
        if (response.inProgressCommit.isAfter(mostRecentInProgressCommit))
            mostRecentInProgressCommit = response.inProgressCommit;

        // Immediately bail out if any participant declines
        if (!response.promised)
        {
            promised = false;
            while (latch.count() > 0)
                latch.decrement();
            return;
        }

        commitsByReplica.put(msg.from(), response.mostRecentCommit);
        if (response.mostRecentCommit.isAfter(mostRecentCommit))
            mostRecentCommit = response.mostRecentCommit;

        latch.decrement();
        if (tracker.isSuccessful())
            signal();
    }

    public Iterable<InetAddressAndPort> replicasMissingMostRecentCommit()
    {
        return Iterables.filter(commitsByReplica.keySet(), inetAddress -> (!commitsByReplica.get(inetAddress).ballot.equals(mostRecentCommit.ballot)));
    }
}
