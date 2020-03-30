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

import com.google.common.collect.Iterables;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.PrepareResponse;

public class PrepareCallback extends AbstractPaxosCallback<PrepareResponse>
{
    private static final Logger logger = LoggerFactory.getLogger(PrepareCallback.class);

    public boolean promised = true;
    public Commit mostRecentCommit;
    public Commit mostRecentInProgressCommit;

    private final Map<InetAddressAndPort, Commit> commitsByReplica = new ConcurrentHashMap<>();

    public PrepareCallback(DecoratedKey key, TableMetadata metadata, int targets, ConsistencyLevel consistency, long queryStartNanoTime)
    {
        super(targets, consistency, queryStartNanoTime);
        // need to inject the right key in the empty commit so comparing with empty commits in the response works as expected
        mostRecentCommit = Commit.emptyCommit(key, metadata);
        mostRecentInProgressCommit = Commit.emptyCommit(key, metadata);
    }

    public synchronized void onResponse(Message<PrepareResponse> message)
    {
        PrepareResponse response = message.payload;
        logger.trace("Prepare response {} from {}", response, message.from());

        // We set the mostRecentInProgressCommit even if we're not promised as, in that case, the ballot of that commit
        // will be used to avoid generating a ballot that has not chance to win on retry (think clock skew).
        if (response.inProgressCommit.isAfter(mostRecentInProgressCommit))
            mostRecentInProgressCommit = response.inProgressCommit;

        if (!response.promised)
        {
            promised = false;
            while (latch.count() > 0)
                latch.decrement();
            return;
        }

        commitsByReplica.put(message.from(), response.mostRecentCommit);
        if (response.mostRecentCommit.isAfter(mostRecentCommit))
            mostRecentCommit = response.mostRecentCommit;

        latch.decrement();
    }

    public Iterable<InetAddressAndPort> replicasMissingMostRecentCommit()
    {
        return Iterables.filter(commitsByReplica.keySet(), inetAddress -> (!commitsByReplica.get(inetAddress).ballot.equals(mostRecentCommit.ballot)));
    }
}
