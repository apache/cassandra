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

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.simulator.cluster.ClusterActionListener.RepairValidator;

import static java.util.Arrays.stream;

public class PaxosRepairValidator implements RepairValidator
{
    final Cluster cluster;
    final String keyspace;
    final String table;
    final Object id;

    boolean isPaxos;
    int quorumRf;
    Ballots.LatestBallots[][] ballotsBefore;
    int[] primaryKeys;
    int[][] replicasForKeys;

    public PaxosRepairValidator(Cluster cluster, String keyspace, String table, Object id)
    {
        this.cluster = cluster;
        this.keyspace = keyspace;
        this.table = table;
        this.id = id;
    }

    @Override
    public void before(int[] primaryKeys, int[][] replicasForKeys, int quorumRf)
    {
        this.quorumRf = quorumRf;
        this.primaryKeys = primaryKeys;
        this.replicasForKeys = replicasForKeys;
        this.ballotsBefore = Ballots.read(cluster, keyspace, table, primaryKeys, replicasForKeys, false);
    }

    @Override
    public void after()
    {
        if (ballotsBefore == null)
            return;

        int quorum = quorumRf / 2 + 1;
        Ballots.LatestBallots[][] ballotsAfter  = Ballots.read(cluster, keyspace, table, primaryKeys, replicasForKeys, true);
        for (int pki = 0; pki < primaryKeys.length ; ++pki)
        {
            Ballots.LatestBallots[] before = ballotsBefore[pki];
            Ballots.LatestBallots[] after  = ballotsAfter[pki];

            if (before.length != after.length || before.length != quorumRf)
                throw new AssertionError("Inconsistent ownership information");

            String kind;
            long expectPersisted;
            if (isPaxos)
            {
                long committedBefore = stream(before).mapToLong(Ballots.LatestBallots::permanent).max().orElse(0L);
                // anything accepted by a quorum should be persisted
                long acceptedBefore = stream(before).mapToLong(n -> n.accept).max().orElse(0L);
                int countAccepted = (int) stream(before).filter(n -> n.accept == acceptedBefore).count();
                expectPersisted = countAccepted >= quorum ? acceptedBefore : committedBefore;
                kind = countAccepted >= quorum ? "agreed" : "committed";
            }
            else
            {
                expectPersisted = stream(before).mapToLong(n -> n.persisted).max().orElse(0L);
                kind = "persisted";
            }

            int countAfter = (int) stream(after).filter(n -> n.persisted >= expectPersisted).count();
            if (countAfter < quorumRf)
                throw new AssertionError(String.format("%d: %d %s before %s but only persisted on %d after (out of %d)",
                                                       primaryKeys[pki], expectPersisted, kind, id, countAfter, quorumRf));
        }

    }
}
