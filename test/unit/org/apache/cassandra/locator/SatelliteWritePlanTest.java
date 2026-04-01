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
package org.apache.cassandra.locator;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class SatelliteWritePlanTest extends SatelliteReplicationStrategyTestBase
{
    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> params()
    {
        return Arrays.asList(new Object[][] {
            { SatelliteFailoverState.State.NORMAL },
            { SatelliteFailoverState.State.TRANSITION_ACK },
            { SatelliteFailoverState.State.TRANSITION },
        });
    }

    private final SatelliteFailoverState.State failoverState;

    public SatelliteWritePlanTest(SatelliteFailoverState.State failoverState)
    {
        this.failoverState = failoverState;
    }

    private boolean isTransition()
    {
        return failoverState != SatelliteFailoverState.State.NORMAL;
    }

    private void applyFailoverState(SatelliteReplicationStrategy strategy)
    {
        if (!isTransition())
            return;

        SatelliteFailoverState.FailoverInfo info;
        switch (failoverState)
        {
            case TRANSITION_ACK: info = SatelliteFailoverState.FailoverInfo.transitionAck("dc1"); break;
            case TRANSITION: info = SatelliteFailoverState.FailoverInfo.transition("dc1"); break;
            default: throw new IllegalStateException();
        }
        strategy.setFailoverState(SatelliteFailoverState.FailoverStateMap.allRanges(info));
    }

    private CoordinationPlan.ForWrite callPlanForWrite(SatelliteReplicationStrategy strategy,
                                                       String keyspaceName, Token token)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        KeyspaceMetadata ksm = metadata.schema.getKeyspaces().getNullable(keyspaceName);
        Keyspace keyspace = Keyspace.mockKS(ksm);
        return strategy.planForWriteInternal(metadata, keyspace, ConsistencyLevel.QUORUM,
                                             (cm) -> ReplicaLayout.forTokenWriteLiveAndDown(cm, keyspace, token),
                                             ReplicaPlans.writeAll);
    }

    @Test
    public void testWriteContactsExcludeOtherSatellite() throws Exception
    {
        createDualDCKeyspace("dc1");
        SatelliteReplicationStrategy strategy = getSRS(DUAL_DC_KEYSPACE);
        applyFailoverState(strategy);
        ClusterMetadata metadata = ClusterMetadata.current();

        CoordinationPlan.ForWrite plan = callPlanForWrite(strategy, DUAL_DC_KEYSPACE, new LongToken(150));

        Set<String> dcs = replicaDCs(plan.replicas().contacts(), metadata);
        assertTrue("Should include dc1", dcs.contains("dc1"));
        assertTrue("Should include dc2", dcs.contains("dc2"));
        assertTrue("Should include sat1 (dc1's satellite)", dcs.contains("sat1"));
        assertFalse("Should NOT include sat2 (dc2's satellite)", dcs.contains("sat2"));
    }

    @Test
    public void testWriteContactsExcludeDisabledDC() throws Exception
    {
        createDisabledDCKeyspace();
        SatelliteReplicationStrategy strategy = getSRS(DISABLED_DC_KEYSPACE);
        applyFailoverState(strategy);
        ClusterMetadata metadata = ClusterMetadata.current();

        CoordinationPlan.ForWrite plan = callPlanForWrite(strategy, DISABLED_DC_KEYSPACE, new LongToken(150));

        Set<String> dcs = replicaDCs(plan.replicas().contacts(), metadata);
        assertTrue("Should include dc1", dcs.contains("dc1"));
        assertTrue("Should include sat1 (dc1's satellite)", dcs.contains("sat1"));
        assertFalse("Should NOT include dc2 (disabled)", dcs.contains("dc2"));
        assertFalse("Should NOT include sat2 (disabled dc2's satellite)", dcs.contains("sat2"));
    }

    @Test
    public void testWriteTrackerComposition() throws Exception
    {
        createDualDCKeyspace("dc1");
        SatelliteReplicationStrategy strategy = getSRS(DUAL_DC_KEYSPACE);
        applyFailoverState(strategy);
        ClusterMetadata metadata = ClusterMetadata.current();

        CoordinationPlan.ForWrite plan = callPlanForWrite(strategy, DUAL_DC_KEYSPACE, new LongToken(150));
        ResponseTracker tracker = plan.responses();

        assertTrue("Write should use CompositeTracker",
                   tracker instanceof CompositeTracker);

        Set<InetAddressAndPort> dc1Contacts = replicasInDC(plan.replicas().contacts(), "dc1", metadata);
        Set<InetAddressAndPort> sat1Contacts = replicasInDC(plan.replicas().contacts(), "sat1", metadata);

        int count = 0;
        for (InetAddressAndPort ep : dc1Contacts)
        {
            tracker.onResponse(ep);
            if (++count >= 2) break;
        }
        assertFalse("dc1 quorum alone should not suffice (1 of 3 groups)", tracker.isSuccessful());

        for (InetAddressAndPort ep : sat1Contacts)
            tracker.onResponse(ep);

        assertTrue("Should succeed with primary + satellite quorums (2 of 3 groups)", tracker.isSuccessful());
    }
}
