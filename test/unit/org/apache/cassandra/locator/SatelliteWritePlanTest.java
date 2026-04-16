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
import java.util.Collections;
import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.schema.AlterSchemaStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.dht.NormalizedRanges;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.locator.satellites.SatelliteFailover;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.transformations.AdvanceSatelliteFailoverState;
import org.apache.cassandra.tcm.transformations.AlterSchema;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class SatelliteWritePlanTest extends SatelliteReplicationStrategyTestBase
{
    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> params()
    {
        return Arrays.asList(new Object[][] {
            { SatelliteFailover.State.NORMAL },
            { SatelliteFailover.State.TRANSITION_ACK },
            { SatelliteFailover.State.TRANSITION },
        });
    }

    private final SatelliteFailover.State failoverState;

    public SatelliteWritePlanTest(SatelliteFailover.State failoverState)
    {
        this.failoverState = failoverState;
    }

    private boolean isTransition()
    {
        return failoverState != SatelliteFailover.State.NORMAL;
    }

    private void applyFailoverState(String keyspace) throws Exception
    {
        if (!isTransition())
            return;

        // ALTER to dc2 triggers failover from dc1 (all ranges in TRANSITION_ACK)
        alterKeyspacePrimary(keyspace, "dc2");

        if (failoverState == SatelliteFailover.State.TRANSITION)
        {
            Token min = DatabaseDescriptor.getPartitioner().getMinimumToken();
            NormalizedRanges<Token> fullRange = NormalizedRanges.normalizedRanges(
                Collections.singleton(new Range<>(min, min)));
            ClusterMetadataTestHelper.commit(new AdvanceSatelliteFailoverState(
            keyspace, fullRange, AdvanceSatelliteFailoverState.TargetState.TRANSITION));
        }
    }

    private void alterKeyspacePrimary(String keyspace, String newPrimary) throws Exception
    {
        String cql = "ALTER KEYSPACE " + keyspace + " WITH replication = {" +
                     "'class': 'SatelliteReplicationStrategy', " +
                     "'dc1': '3', " +
                     "'dc1.satellite.sat1': '3/3', " +
                     "'dc2': '3', " +
                     "'dc2.satellite.sat2': '3/3', " +
                     "'primary': '" + newPrimary + "'" +
                     "} AND replication_type = 'tracked'";
        AlterSchemaStatement stmt = (AlterSchemaStatement) QueryProcessor.parseStatement(cql)
            .prepare(ClientState.forInternalCalls());
        ClusterMetadataTestHelper.commit(new AlterSchema(stmt));
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
        applyFailoverState(DUAL_DC_KEYSPACE);
        SatelliteReplicationStrategy strategy = getSRS(DUAL_DC_KEYSPACE);
        ClusterMetadata metadata = ClusterMetadata.current();

        CoordinationPlan.ForWrite plan = callPlanForWrite(strategy, DUAL_DC_KEYSPACE, new LongToken(150));

        // After ALTER to dc2, primary is dc2, satellite is sat2. dc1 is secondary (included during failover).
        // In NORMAL state (dc1 primary), satellite is sat1. dc2 is secondary.
        String primarySat = isTransition() ? "sat2" : "sat1";
        String otherSat = isTransition() ? "sat1" : "sat2";

        Set<String> dcs = replicaDCs(plan.replicas().contacts(), metadata);
        assertTrue("Should include dc1", dcs.contains("dc1"));
        assertTrue("Should include dc2", dcs.contains("dc2"));
        assertTrue("Should include primary's satellite (" + primarySat + ")", dcs.contains(primarySat));
        assertFalse("Should NOT include other satellite (" + otherSat + ")", dcs.contains(otherSat));
    }

    @Test
    public void testWriteContactsExcludeDisabledDC() throws Exception
    {
        createDisabledDCKeyspace();
        SatelliteReplicationStrategy strategy = getSRS(DISABLED_DC_KEYSPACE);
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
        applyFailoverState(DUAL_DC_KEYSPACE);
        SatelliteReplicationStrategy strategy = getSRS(DUAL_DC_KEYSPACE);
        ClusterMetadata metadata = ClusterMetadata.current();

        CoordinationPlan.ForWrite plan = callPlanForWrite(strategy, DUAL_DC_KEYSPACE, new LongToken(150));
        ResponseTracker tracker = plan.responses();

        assertTrue("Write should use CompositeTracker",
                   tracker instanceof CompositeTracker);

        // After ALTER to dc2, primary is dc2 with satellite sat2.
        // In NORMAL state (dc1 primary), primary is dc1 with satellite sat1.
        String primaryDC = isTransition() ? "dc2" : "dc1";
        String primarySat = isTransition() ? "sat2" : "sat1";

        Set<InetAddressAndPort> primaryContacts = replicasInDC(plan.replicas().contacts(), primaryDC, metadata);
        Set<InetAddressAndPort> satContacts = replicasInDC(plan.replicas().contacts(), primarySat, metadata);

        int count = 0;
        for (InetAddressAndPort ep : primaryContacts)
        {
            tracker.onResponse(ep);
            if (++count >= 2) break;
        }
        assertFalse("Primary DC quorum alone should not suffice (1 of 3 groups)", tracker.isSuccessful());

        for (InetAddressAndPort ep : satContacts)
            tracker.onResponse(ep);

        assertTrue("Should succeed with primary + satellite quorums (2 of 3 groups)", tracker.isSuccessful());
    }
}
