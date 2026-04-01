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
package org.apache.cassandra.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.SatelliteFailoverState;
import org.apache.cassandra.locator.SatelliteReplicationStrategy;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tcm.ClusterMetadata;

/**
 * Verb handler for PAXOS2_COMMIT_REMOTE_REQ that wraps {@link MutationVerbHandler} with a failover state check
 * for SatelliteReplicationStrategy keyspaces.
 *
 * PAXOS2_COMMIT_REMOTE_REQ sends a normal {@link Mutation} that doesn't indicate it came from a paxos commit
 * so the standard MutationVerbHandler has no awareness that it originated from a paxos commit. This wrapper
 * rejects mutations during {@link SatelliteFailoverState.State#TRANSITION_ACK} to prevent stale paxos commits
 * from being applied to satellite/secondary DCs during failover.
 */
public class PaxosCommitRemoteMutationVerbHandler implements IVerbHandler<Mutation>
{
    public static final PaxosCommitRemoteMutationVerbHandler instance = new PaxosCommitRemoteMutationVerbHandler();
    private static final Logger logger = LoggerFactory.getLogger(PaxosCommitRemoteMutationVerbHandler.class);

    @Override
    public void doVerb(Message<Mutation> message)
    {
        Mutation mutation = message.payload;
        Keyspace keyspace = Keyspace.open(mutation.getKeyspaceName());
        AbstractReplicationStrategy strategy = keyspace.getReplicationStrategy();

        if (strategy instanceof SatelliteReplicationStrategy)
        {
            SatelliteReplicationStrategy srs = (SatelliteReplicationStrategy) strategy;
            ClusterMetadata metadata = ClusterMetadata.current();
            SatelliteFailoverState.FailoverInfo failoverInfo = srs.getFailoverInfo(mutation.key().getToken(), metadata);
            if (failoverInfo.getState() == SatelliteFailoverState.State.TRANSITION_ACK)
            {
                logger.debug("Rejecting PAXOS2_COMMIT_REMOTE_REQ for {} during TRANSITION_ACK", mutation.getKeyspaceName());
                MessagingService.instance().respondWithFailure(RequestFailureReason.UNKNOWN, message);
                return;
            }
        }

        MutationVerbHandler.instance.doVerb(message);
    }
}
