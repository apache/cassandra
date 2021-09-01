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

package org.apache.cassandra.simulator.paxos;

import java.util.Arrays;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.simulator.cluster.ClusterActionListener.TopologyChangeValidator;

import static java.util.Arrays.stream;

public class PaxosTopologyChangeVerifier implements TopologyChangeValidator
{
    final Cluster cluster;
    final String keyspace;
    final String table;
    final Object id;

    int[] primaryKeys;
    int quorumRfBefore;
    int[][] replicasBefore;
    int[][] replicasAfter;
    Ballots.LatestBallots[][] ballotsBefore;
    Ballots.LatestBallots[][] ballotsAfter;

    public PaxosTopologyChangeVerifier(Cluster cluster, String keyspace, String table, Object id)
    {
        this.cluster = cluster;
        this.keyspace = keyspace;
        this.table = table;
        this.id = id;
    }

    @Override
    public void before(int[] primaryKeys, int[][] replicasForKeys, int quorumRf)
    {
        this.primaryKeys = primaryKeys;
        this.quorumRfBefore = quorumRf;
        this.replicasBefore = replicasForKeys;
        this.ballotsBefore = Ballots.read(cluster, keyspace, table, primaryKeys, replicasForKeys, true);
    }

    @Override
    public void after(int[][] replicasForKeys, int quorumRfAfter)
    {
        replicasAfter = replicasForKeys;
        int quorumBefore = quorumRfBefore / 2 + 1;
        int quorumAfter = quorumRfAfter / 2 + 1;
        Ballots.LatestBallots[][] allBefore = ballotsBefore;
        Ballots.LatestBallots[][] allAfter = Ballots.read(cluster, keyspace, table, primaryKeys, replicasAfter, true);
        for (int pki = 0; pki < primaryKeys.length; ++pki)
        {
            Ballots.LatestBallots[] before = allBefore[pki];
            Ballots.LatestBallots[] after = allAfter[pki];

            if (replicasBefore[pki].length != quorumRfBefore || replicasAfter[pki].length != quorumRfAfter)
                throw new AssertionError(String.format("Inconsistent ownership information: %s (expect %d) vs %s (expect %d)", Arrays.toString(replicasBefore[pki]), quorumRfBefore, Arrays.toString(replicasAfter[pki]), quorumRfAfter));

            if (before.length != quorumRfBefore || after.length != quorumRfAfter)
                throw new AssertionError("Inconsistent ownership/ballot information");

            {
                // if we had accepted to a quorum we should be committed to a quorum afterwards
                // note that we will not always witness something newer than the latest accepted proposal,
                // because if we don't witness it during repair, we will simply invalidate it with the low bound
                long acceptedBefore = stream(before).mapToLong(n -> n.accept).max().orElse(0L);
                int countBefore = (int) stream(before).filter(n -> n.accept == acceptedBefore).count();
                int countAfter = countBefore < quorumAfter
                                 ? (int) stream(after).filter(n -> n.any() >= acceptedBefore).count()
                                 : (int) stream(after).filter(n -> n.permanent() >= acceptedBefore).count();

                if (countBefore >= quorumBefore && countAfter < quorumAfter)
                {
                    throw new AssertionError(String.format("%d: %d accepted by %d before %s but only %s on %d after (expect at least %d)",
                                                           primaryKeys[pki], acceptedBefore, countBefore, this, countBefore >= quorumAfter ? "committed" : "accepted", countAfter, quorumAfter));
                }
            }
            {
                // we should always have at least a quorum of newer records than the most recently witnessed commit
                long committedBefore = stream(before).mapToLong(Ballots.LatestBallots::permanent).max().orElse(0L);
                int countAfter = (int) stream(after).filter(n -> n.permanent() >= committedBefore).count();
                if (countAfter < quorumAfter)
                {
                    throw new AssertionError(String.format("%d: %d committed before %s but only committed on %d after (expect at least %d)",
                                                           primaryKeys[pki], committedBefore, id, countAfter, quorumAfter));
                }
            }
        }

        // clear memory usage on success
        replicasAfter = replicasBefore = null;
        ballotsAfter = ballotsBefore = null;
    }

}
