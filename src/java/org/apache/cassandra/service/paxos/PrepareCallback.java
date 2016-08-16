package org.apache.cassandra.service.paxos;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.net.InetAddress;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.utils.UUIDGen;

public class PrepareCallback extends AbstractPaxosCallback<PrepareResponse>
{
    private static final Logger logger = LoggerFactory.getLogger(PrepareCallback.class);

    public boolean promised = true;
    public Commit mostRecentCommit;
    public Commit mostRecentInProgressCommit;
    public Commit mostRecentInProgressCommitWithUpdate;

    private final Map<InetAddress, Commit> commitsByReplica = new ConcurrentHashMap<InetAddress, Commit>();

    public PrepareCallback(DecoratedKey key, CFMetaData metadata, int targets, ConsistencyLevel consistency, long queryStartNanoTime)
    {
        super(targets, consistency, queryStartNanoTime);
        // need to inject the right key in the empty commit so comparing with empty commits in the reply works as expected
        mostRecentCommit = Commit.emptyCommit(key, metadata);
        mostRecentInProgressCommit = Commit.emptyCommit(key, metadata);
        mostRecentInProgressCommitWithUpdate = Commit.emptyCommit(key, metadata);
    }

    public synchronized void response(MessageIn<PrepareResponse> message)
    {
        PrepareResponse response = message.payload;
        logger.trace("Prepare response {} from {}", response, message.from);

        // In case of clock skew, another node could be proposing with ballot that are quite a bit
        // older than our own. In that case, we record the more recent commit we've received to make
        // sure we re-prepare on an older ballot.
        if (response.inProgressCommit.isAfter(mostRecentInProgressCommit))
            mostRecentInProgressCommit = response.inProgressCommit;

        if (!response.promised)
        {
            promised = false;
            while (latch.getCount() > 0)
                latch.countDown();
            return;
        }

        commitsByReplica.put(message.from, response.mostRecentCommit);
        if (response.mostRecentCommit.isAfter(mostRecentCommit))
            mostRecentCommit = response.mostRecentCommit;

        // If some response has an update, then we should replay the update with the highest ballot. So find
        // the the highest commit that actually have an update
        if (response.inProgressCommit.isAfter(mostRecentInProgressCommitWithUpdate) && !response.inProgressCommit.update.isEmpty())
            mostRecentInProgressCommitWithUpdate = response.inProgressCommit;

        latch.countDown();
    }

    public Iterable<InetAddress> replicasMissingMostRecentCommit(CFMetaData metadata, int nowInSec)
    {
        // In general, we need every replicas that have answered to the prepare (a quorum) to agree on the MRC (see
        // coment in StorageProxy.beginAndRepairPaxos(), but basically we need to make sure at least a quorum of nodes
        // have learn a commit before commit a new one otherwise that previous commit is not guaranteed to have reach a
        // quorum and further commit may proceed on incomplete information).
        // However, if that commit is too hold, it may have been expired from some of the replicas paxos table (we don't
        // keep the paxos state forever or that could grow unchecked), and we could end up in some infinite loop as
        // explained on CASSANDRA-12043. To avoid that, we ignore a MRC that is too old, i.e. older than the TTL we set
        // on paxos tables. For such old commit, we rely on hints and repair to ensure the commit has indeed be
        // propagated to all nodes.
        long paxosTtlSec = SystemKeyspace.paxosTtlSec(metadata);
        if (UUIDGen.unixTimestampInSec(mostRecentCommit.ballot) + paxosTtlSec < nowInSec)
            return Collections.emptySet();

        return Iterables.filter(commitsByReplica.keySet(), new Predicate<InetAddress>()
        {
            public boolean apply(InetAddress inetAddress)
            {
                return (!commitsByReplica.get(inetAddress).ballot.equals(mostRecentCommit.ballot));
            }
        });
    }
}
