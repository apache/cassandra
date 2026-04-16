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
import java.util.HashSet;
import java.util.Set;

import org.junit.Assume;
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
import org.apache.cassandra.service.reads.AlwaysSpeculativeRetryPolicy;
import org.apache.cassandra.service.reads.ReadCoordinator;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.transformations.AdvanceSatelliteFailoverState;
import org.apache.cassandra.tcm.transformations.AlterSchema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class SatelliteReadPlanTest extends SatelliteReplicationStrategyTestBase
{
    private static class MockFailoverInfo implements SatelliteFailover.Info
    {
        final String fromDc;
        final SatelliteFailover.State state;

        public MockFailoverInfo(String fromDc, SatelliteFailover.State state)
        {
            this.fromDc = fromDc;
            this.state = state;
        }

        @Override
        public SatelliteFailover.State stateForToken(Token token)
        {
            return state;
        }

        @Override
        public SatelliteFailover.State leastAdvancedState(Range<Token> range)
        {
            return state;
        }

        @Override
        public String getFromDC()
        {
            return fromDc;
        }
    }
    enum ReadType
    {
        TOKEN, RANGE
    }

    @Parameterized.Parameters(name = "{0}/{1}")
    public static Collection<Object[]> params()
    {
        return Arrays.asList(new Object[][] {
            { ReadType.TOKEN, SatelliteFailover.State.NORMAL },
            { ReadType.TOKEN, SatelliteFailover.State.TRANSITION_ACK },
            { ReadType.TOKEN, SatelliteFailover.State.TRANSITION },
            { ReadType.RANGE, SatelliteFailover.State.NORMAL },
            { ReadType.RANGE, SatelliteFailover.State.TRANSITION_ACK },
            { ReadType.RANGE, SatelliteFailover.State.TRANSITION },
        });
    }

    private final ReadType readType;
    private final SatelliteFailover.State failoverState;

    public SatelliteReadPlanTest(ReadType readType, SatelliteFailover.State failoverState)
    {
        this.readType = readType;
        this.failoverState = failoverState;
    }

    private boolean isTransition()
    {
        return failoverState != SatelliteFailover.State.NORMAL;
    }

    private SatelliteFailover.Info failoverInfo()
    {
        switch (failoverState)
        {
            case TRANSITION_ACK:
            case TRANSITION:
                return new MockFailoverInfo("dc1", failoverState);

            default: throw new IllegalStateException("No failover info for NORMAL");
        }
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

    private CoordinationPlan.ForRead<?, ?> createPlan(SatelliteReplicationStrategy strategy, String keyspaceName) throws Exception
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        KeyspaceMetadata ksm = metadata.schema.getKeyspaces().getNullable(keyspaceName);
        Keyspace keyspace = Keyspace.mockKS(ksm);

        switch (readType)
        {
            case TOKEN:
                return strategy.planForTokenRead(metadata, keyspace, TABLE_ID,
                                                 new LongToken(150), null,
                                                 ConsistencyLevel.QUORUM,
                                                 AlwaysSpeculativeRetryPolicy.INSTANCE,
                                                 ReadCoordinator.DEFAULT);
            case RANGE:
                return strategy.planForRangeRead(metadata, keyspace, TABLE_ID, null,
                                                 ConsistencyLevel.QUORUM,
                                                 Range.makeRowRange(new LongToken(100),
                                                                    new LongToken(200)),
                                                 1);
            default:
                throw new IllegalStateException();
        }
    }

    private ReplicaPlan.ForRead<?, ?> replicas(CoordinationPlan.ForRead<?, ?> plan)
    {
        return (ReplicaPlan.ForRead<?, ?>) plan.replicas();
    }

    private void assertNoDuplicateEndpoints(String label, Iterable<Replica> replicas)
    {
        Set<InetAddressAndPort> seen = new HashSet<>();
        for (Replica r : replicas)
            assertTrue(label + " contains duplicate endpoint: " + r.endpoint(),
                       seen.add(r.endpoint()));
    }

    @Test
    public void testReadPlanDualDC() throws Exception
    {
        createDualDCKeyspace("dc1");
        applyFailoverState(DUAL_DC_KEYSPACE);
        SatelliteReplicationStrategy strategy = getSRS(DUAL_DC_KEYSPACE);
        ClusterMetadata metadata = ClusterMetadata.current();

        CoordinationPlan.ForRead<?, ?> plan = createPlan(strategy, DUAL_DC_KEYSPACE);

        Set<String> contactDCs = replicaDCs(plan.replicas().contacts(), metadata);
        if (isTransition())
        {
            assertTrue("Should include dc2 (new primary)", contactDCs.contains("dc2"));
            assertTrue("Should include dc1 (old primary)", contactDCs.contains("dc1"));
        }
        else
        {
            assertTrue("Should include dc1", contactDCs.contains("dc1"));
            assertTrue("Should include sat1 (dc1's satellite)", contactDCs.contains("sat1"));
            assertFalse("Should NOT include sat2 (dc2's satellite)", contactDCs.contains("sat2"));
        }
    }

    @Test
    public void testReadPlanSingleDC() throws Exception
    {
        createSingleDCKeyspace();
        SatelliteReplicationStrategy strategy = getSRS(SINGLE_DC_KEYSPACE);
        ClusterMetadata metadata = ClusterMetadata.current();

        CoordinationPlan.ForRead<?, ?> plan = createPlan(strategy, SINGLE_DC_KEYSPACE);

        Set<String> contactDCs = replicaDCs(plan.replicas().contacts(), metadata);
        assertTrue("Should include dc1", contactDCs.contains("dc1"));
        assertTrue("Should include sat1", contactDCs.contains("sat1"));
        assertFalse("Should NOT include dc2", contactDCs.contains("dc2"));
        assertFalse("Should NOT include sat2", contactDCs.contains("sat2"));
    }

    @Test
    public void testReadPlanExcludesDisabledDC() throws Exception
    {
        createDisabledDCKeyspace();
        SatelliteReplicationStrategy strategy = getSRS(DISABLED_DC_KEYSPACE);
        ClusterMetadata metadata = ClusterMetadata.current();

        CoordinationPlan.ForRead<?, ?> plan = createPlan(strategy, DISABLED_DC_KEYSPACE);

        Set<String> contactDCs = replicaDCs(plan.replicas().contacts(), metadata);
        assertTrue("Should include dc1", contactDCs.contains("dc1"));
        assertTrue("Should include sat1 (dc1's satellite)", contactDCs.contains("sat1"));
        assertFalse("Should NOT include dc2 (disabled)", contactDCs.contains("dc2"));
        assertFalse("Should NOT include sat2 (disabled dc2's satellite)", contactDCs.contains("sat2"));
    }

    @Test
    public void testReadPlanPrimaryDCFirst() throws Exception
    {
        createDualDCKeyspace("dc1");
        applyFailoverState(DUAL_DC_KEYSPACE);
        SatelliteReplicationStrategy strategy = getSRS(DUAL_DC_KEYSPACE);
        ClusterMetadata metadata = ClusterMetadata.current();

        CoordinationPlan.ForRead<?, ?> plan = createPlan(strategy, DUAL_DC_KEYSPACE);

        String expectedPrimary = isTransition() ? "dc2" : "dc1";
        Replica first = plan.replicas().contacts().iterator().next();
        assertEquals("First contact should be in primary DC",
                     expectedPrimary, metadata.locator.location(first.endpoint()).datacenter);

        if (isTransition())
        {
            // dc2 contacts should appear before dc1 contacts
            boolean seenDc1 = false;
            for (Replica r : plan.replicas().contacts())
            {
                String dc = metadata.locator.location(r.endpoint()).datacenter;
                if (dc.equals("dc1"))
                    seenDc1 = true;
                if (dc.equals("dc2") && seenDc1)
                    fail("dc2 contact appeared after dc1 contact — primary should come first");
            }
        }
    }

    @Test
    public void testReadPlanNoMerge() throws Exception
    {
        createDualDCKeyspace("dc1");
        SatelliteReplicationStrategy strategy = getSRS(DUAL_DC_KEYSPACE);
        ClusterMetadata metadata = ClusterMetadata.current();

        CoordinationPlan.ForRead<?, ?> plan = createPlan(strategy, DUAL_DC_KEYSPACE);

        Set<String> candidateDCs = replicaDCs(replicas(plan).readCandidates(), metadata);
        assertTrue("Should have dc1 candidates", candidateDCs.contains("dc1"));
    }

    @Test
    public void testTransitionReadPlanMergesDCs() throws Exception
    {
        Assume.assumeTrue(isTransition());

        createDualDCKeyspace("dc1");
        applyFailoverState(DUAL_DC_KEYSPACE);
        SatelliteReplicationStrategy strategy = getSRS(DUAL_DC_KEYSPACE);
        ClusterMetadata metadata = ClusterMetadata.current();

        CoordinationPlan.ForRead<?, ?> plan = createPlan(strategy, DUAL_DC_KEYSPACE);

        Set<String> candidateDCs = replicaDCs(replicas(plan).readCandidates(), metadata);
        assertTrue("Merged candidates should include dc2", candidateDCs.contains("dc2"));
        assertTrue("Merged candidates should include dc1", candidateDCs.contains("dc1"));

        Set<String> liveAndDownDCs = replicaDCs(plan.replicas().liveAndDown(), metadata);
        assertTrue("Merged liveAndDown should include dc2", liveAndDownDCs.contains("dc2"));
        assertTrue("Merged liveAndDown should include dc1", liveAndDownDCs.contains("dc1"));
    }

    @Test
    public void testTransitionReadPlanNoDuplicates() throws Exception
    {
        Assume.assumeTrue(isTransition());

        createDualDCKeyspace("dc1");
        applyFailoverState(DUAL_DC_KEYSPACE);
        SatelliteReplicationStrategy strategy = getSRS(DUAL_DC_KEYSPACE);

        CoordinationPlan.ForRead<?, ?> plan = createPlan(strategy, DUAL_DC_KEYSPACE);
        ReplicaPlan.ForRead<?, ?> replicas = replicas(plan);

        assertNoDuplicateEndpoints("contacts", replicas.contacts());
        assertNoDuplicateEndpoints("candidates", replicas.readCandidates());
        assertNoDuplicateEndpoints("liveAndDown", replicas.liveAndDown());
    }

    @Test
    public void testTransitionReadPlanQuorum() throws Exception
    {
        Assume.assumeTrue(isTransition());

        createDualDCKeyspace("dc1");
        SatelliteReplicationStrategy strategy = getSRS(DUAL_DC_KEYSPACE);

        // Get the individual plan quorum before failover
        CoordinationPlan.ForRead<?, ?> primaryOnly = createPlan(strategy, DUAL_DC_KEYSPACE);
        int primaryQuorum = replicas(primaryOnly).readQuorum();

        // Apply failover state and get merged plan
        applyFailoverState(DUAL_DC_KEYSPACE);
        strategy = getSRS(DUAL_DC_KEYSPACE);

        CoordinationPlan.ForRead<?, ?> merged = createPlan(strategy, DUAL_DC_KEYSPACE);
        int mergedQuorum = replicas(merged).readQuorum();

        assertTrue("Merged quorum (" + mergedQuorum + ") should be >= primary quorum (" + primaryQuorum + ")",
                   mergedQuorum >= primaryQuorum);
    }
}
