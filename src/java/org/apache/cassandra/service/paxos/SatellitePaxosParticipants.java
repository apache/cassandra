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
package org.apache.cassandra.service.paxos;

import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaLayout;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.reads.tracked.TrackedRead;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;

/**
 * Paxos participants for SatelliteReplicationStrategy.
 *
 * Paxos consensus operates entirely within the primary DC. Satellites and secondary DCs do not participate in propose
 * / accept. However paxos reads and writes do need QoQ consistency to support quick failover. This class adds the
 * additional summary nodes to the prepare/read stage and instructs them to send summaries to the read coordinator.
 * Additional commit mutations are handled by the replication strategy itself.
 */
public class SatellitePaxosParticipants extends Paxos.Participants
{
    private static final Logger logger = LoggerFactory.getLogger(SatellitePaxosParticipants.class);

    /** Endpoints in satellite/secondary DCs that receive reads during prepare and writes during commit */
    private final EndpointsForToken additionalSummaryEndpoints;

    public SatellitePaxosParticipants(Epoch epoch,
                                      Keyspace keyspace,
                                      ConsistencyLevel consistencyForConsensus,
                                      ReplicaLayout.ForTokenWrite all,
                                      ReplicaLayout.ForTokenWrite electorate,
                                      EndpointsForToken live,
                                      Function<ClusterMetadata, Paxos.Participants> recompute,
                                      EndpointsForToken additionalSummaryEndpoints)
    {
        super(epoch, keyspace, consistencyForConsensus, all, electorate, live, recompute);
        this.additionalSummaryEndpoints = additionalSummaryEndpoints;
    }

    public EndpointsForToken getAdditionalSummaryEndpoints()
    {
        return additionalSummaryEndpoints;
    }

    @Override
    public int[] additionalSummaryHostIds(ClusterMetadata metadata)
    {
        if (additionalSummaryEndpoints.isEmpty())
            return super.additionalSummaryHostIds(metadata);

        int[] ids = new int[additionalSummaryEndpoints.size()];
        for (int i = 0; i < additionalSummaryEndpoints.size(); i++)
            ids[i] = metadata.directory.peerId(additionalSummaryEndpoints.endpoint(i)).id();
        return ids;
    }

    @Override
    public void onPrepareStarted(TrackedRead.Id readId, int dataNodeId, int[] summaryHostIds, ReadCommand readCommand)
    {
        if (additionalSummaryEndpoints.isEmpty() || readCommand == null)
            return;

        // Send standalone TRACKED_SUMMARY_REQ to each additional satellite endpoint.
        TrackedRead.SummaryRequest summaryRequest = new TrackedRead.SummaryRequest(readId, readCommand, dataNodeId, summaryHostIds);
        Message<TrackedRead.SummaryRequest> summaryMessage = Message.out(Verb.TRACKED_SUMMARY_REQ, summaryRequest);
        for (Replica replica : additionalSummaryEndpoints)
        {
            logger.trace("Sending satellite summary request for {} to {}", readId, replica.endpoint());
            MessagingService.instance().send(summaryMessage, replica.endpoint());
        }
    }
}
