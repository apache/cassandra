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

import java.util.Collections;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.schema.AlterSchemaStatement;
import org.apache.cassandra.dht.NormalizedRanges;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.locator.satellites.KeyspaceFailoverState;
import org.apache.cassandra.locator.satellites.SatelliteFailover;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.transformations.AdvanceSatelliteFailoverState;
import org.apache.cassandra.tcm.transformations.AlterSchema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for the satellite failover process state lifecycle.
 * Uses the SatelliteReplicationStrategyTestBase infrastructure for cluster setup.
 */
public class SatelliteFailoverIntegrationTest extends SatelliteReplicationStrategyTestBase
{
    @Test
    public void testAlterKeyspaceTriggerFailoverState() throws Exception
    {
        createDualDCKeyspace("dc1");

        // Verify no failover state initially
        ClusterMetadata metadata = ClusterMetadata.current();
        assertFalse(metadata.satelliteFailoverState.hasActiveTransfer(DUAL_DC_KEYSPACE));

        // ALTER KEYSPACE to change primary DC
        alterKeyspacePrimary(DUAL_DC_KEYSPACE, "dc2");

        // Verify failover state was created
        metadata = ClusterMetadata.current();
        assertTrue(metadata.satelliteFailoverState.hasActiveTransfer(DUAL_DC_KEYSPACE));

        KeyspaceFailoverState ksState = metadata.satelliteFailoverState.getKeyspaceState(DUAL_DC_KEYSPACE);
        assertNotNull(ksState);
        assertEquals("dc1", ksState.fromDC);
        assertFalse(ksState.hasRangesInState(SatelliteFailover.State.TRANSITION));
        assertTrue(ksState.hasRangesInState(SatelliteFailover.State.TRANSITION_ACK));
    }

    @Test
    public void testAdvanceFailoverStateTransformation() throws Exception
    {
        createDualDCKeyspace("dc1");
        alterKeyspacePrimary(DUAL_DC_KEYSPACE, "dc2");

        ClusterMetadata metadata = ClusterMetadata.current();
        assertTrue(metadata.satelliteFailoverState.hasActiveTransfer(DUAL_DC_KEYSPACE));

        // Submit AdvanceFailoverState to move ranges to TRANSITION
        NormalizedRanges<Token> fullRange = fullTokenRange();
        AdvanceSatelliteFailoverState advance = new AdvanceSatelliteFailoverState(
        DUAL_DC_KEYSPACE, fullRange, AdvanceSatelliteFailoverState.TargetState.TRANSITION);
        ClusterMetadataTestHelper.commit(advance);

        // Verify ranges moved
        metadata = ClusterMetadata.current();
        KeyspaceFailoverState ksState = metadata.satelliteFailoverState.getKeyspaceState(DUAL_DC_KEYSPACE);
        assertNotNull(ksState);
        assertFalse(ksState.hasRangesInState(SatelliteFailover.State.TRANSITION_ACK));
        assertTrue(ksState.hasRangesInState(SatelliteFailover.State.TRANSITION));
    }

    @Test
    public void testFullLifecycle() throws Exception
    {
        createDualDCKeyspace("dc1");
        alterKeyspacePrimary(DUAL_DC_KEYSPACE, "dc2");

        NormalizedRanges<Token> fullRange = fullTokenRange();

        // Phase 1: Advance all ranges TRANSITION_ACK → TRANSITION
        ClusterMetadataTestHelper.commit(new AdvanceSatelliteFailoverState(
        DUAL_DC_KEYSPACE, fullRange, AdvanceSatelliteFailoverState.TargetState.TRANSITION));

        ClusterMetadata metadata = ClusterMetadata.current();
        assertTrue(metadata.satelliteFailoverState.hasActiveTransfer(DUAL_DC_KEYSPACE));
        KeyspaceFailoverState ksState = metadata.satelliteFailoverState.getKeyspaceState(DUAL_DC_KEYSPACE);
        assertFalse(ksState.hasRangesInState(SatelliteFailover.State.TRANSITION_ACK));
        assertTrue(ksState.hasRangesInState(SatelliteFailover.State.TRANSITION));

        // Phase 2: Advance all ranges TRANSITION → NORMAL
        ClusterMetadataTestHelper.commit(new AdvanceSatelliteFailoverState(
        DUAL_DC_KEYSPACE, fullRange, AdvanceSatelliteFailoverState.TargetState.NORMAL));

        // Verify transfer complete -- keyspace entry removed
        metadata = ClusterMetadata.current();
        assertFalse(metadata.satelliteFailoverState.hasActiveTransfer(DUAL_DC_KEYSPACE));
        assertNull(metadata.satelliteFailoverState.getKeyspaceState(DUAL_DC_KEYSPACE));
    }

    @Test
    public void testIncrementalAdvancement() throws Exception
    {
        createDualDCKeyspace("dc1");
        alterKeyspacePrimary(DUAL_DC_KEYSPACE, "dc2");

        // Advance only a sub-range
        NormalizedRanges<Token> subRange = rangesOf(token(100), token(300));
        ClusterMetadataTestHelper.commit(new AdvanceSatelliteFailoverState(
        DUAL_DC_KEYSPACE, subRange, AdvanceSatelliteFailoverState.TargetState.TRANSITION));

        ClusterMetadata metadata = ClusterMetadata.current();
        KeyspaceFailoverState ksState = metadata.satelliteFailoverState.getKeyspaceState(DUAL_DC_KEYSPACE);
        assertNotNull(ksState);
        // Both states should be present (partial advancement)
        assertTrue(ksState.hasRangesInState(SatelliteFailover.State.TRANSITION_ACK));
        assertTrue(ksState.hasRangesInState(SatelliteFailover.State.TRANSITION));

        // Advance the sub-range to NORMAL
        ClusterMetadataTestHelper.commit(new AdvanceSatelliteFailoverState(
        DUAL_DC_KEYSPACE, subRange, AdvanceSatelliteFailoverState.TargetState.NORMAL));

        // Transfer still active -- remaining ranges still in TRANSITION_ACK
        metadata = ClusterMetadata.current();
        assertTrue(metadata.satelliteFailoverState.hasActiveTransfer(DUAL_DC_KEYSPACE));
        ksState = metadata.satelliteFailoverState.getKeyspaceState(DUAL_DC_KEYSPACE);
        assertTrue(ksState.hasRangesInState(SatelliteFailover.State.TRANSITION_ACK));
        assertFalse(ksState.hasRangesInState(SatelliteFailover.State.TRANSITION));
    }

    @Test
    public void testGetFailoverInfoNoActiveTransfer() throws Exception
    {
        createDualDCKeyspace("dc1");

        SatelliteReplicationStrategy srs = getSRS(DUAL_DC_KEYSPACE);
        ClusterMetadata metadata = ClusterMetadata.current();

        // No failover -- should return NORMAL for any token
        SatelliteFailover.Info info = srs.getFailoverInfo(metadata);
        SatelliteFailover.State state = info.stateForToken(token(150));
        assertEquals(SatelliteFailover.State.NORMAL, state);
        assertNull(info.getFromDC());
        assertFalse(state.isTransitioning());
    }

    @Test
    public void testGetFailoverInfoTransitionAck() throws Exception
    {
        createDualDCKeyspace("dc1");
        alterKeyspacePrimary(DUAL_DC_KEYSPACE, "dc2");

        SatelliteReplicationStrategy srs = getSRS(DUAL_DC_KEYSPACE);
        ClusterMetadata metadata = ClusterMetadata.current();

        // After ALTER, all ranges should be in TRANSITION_ACK
        SatelliteFailover.Info info = srs.getFailoverInfo(metadata);
        SatelliteFailover.State state = info.stateForToken(token(150));
        assertEquals(SatelliteFailover.State.TRANSITION_ACK, state);
        assertEquals("dc1", info.getFromDC());
        assertTrue(state.isTransitioning());
    }

    @Test
    public void testGetFailoverInfoTransition() throws Exception
    {
        createDualDCKeyspace("dc1");
        alterKeyspacePrimary(DUAL_DC_KEYSPACE, "dc2");

        // Advance all ranges to TRANSITION
        NormalizedRanges<Token> fullRange = fullTokenRange();
        ClusterMetadataTestHelper.commit(new AdvanceSatelliteFailoverState(
        DUAL_DC_KEYSPACE, fullRange, AdvanceSatelliteFailoverState.TargetState.TRANSITION));

        SatelliteReplicationStrategy srs = getSRS(DUAL_DC_KEYSPACE);
        ClusterMetadata metadata = ClusterMetadata.current();

        SatelliteFailover.Info info = srs.getFailoverInfo(metadata);
        SatelliteFailover.State state = info.stateForToken(token(150));
        assertEquals(SatelliteFailover.State.TRANSITION, state);
        assertEquals("dc1", info.getFromDC());
        assertTrue(state.isTransitioning());
    }

    @Test
    public void testGetFailoverInfoMixedStates() throws Exception
    {
        createDualDCKeyspace("dc1");
        alterKeyspacePrimary(DUAL_DC_KEYSPACE, "dc2");

        // Advance only a sub-range to TRANSITION
        NormalizedRanges<Token> subRange = rangesOf(token(100), token(300));
        ClusterMetadataTestHelper.commit(new AdvanceSatelliteFailoverState(
        DUAL_DC_KEYSPACE, subRange, AdvanceSatelliteFailoverState.TargetState.TRANSITION));

        SatelliteReplicationStrategy srs = getSRS(DUAL_DC_KEYSPACE);
        ClusterMetadata metadata = ClusterMetadata.current();

        // Token in advanced sub-range should be TRANSITION
        SatelliteFailover.Info info = srs.getFailoverInfo(metadata);
        assertEquals(SatelliteFailover.State.TRANSITION, info.stateForToken(token(150)));

        // Token outside advanced sub-range should still be TRANSITION_ACK
        assertEquals(SatelliteFailover.State.TRANSITION_ACK, info.stateForToken(token(500)));
    }

    @Test
    public void testGetFailoverInfoNormalAfterComplete() throws Exception
    {
        createDualDCKeyspace("dc1");
        alterKeyspacePrimary(DUAL_DC_KEYSPACE, "dc2");

        NormalizedRanges<Token> fullRange = fullTokenRange();
        ClusterMetadataTestHelper.commit(new AdvanceSatelliteFailoverState(
        DUAL_DC_KEYSPACE, fullRange, AdvanceSatelliteFailoverState.TargetState.TRANSITION));
        ClusterMetadataTestHelper.commit(new AdvanceSatelliteFailoverState(
        DUAL_DC_KEYSPACE, fullRange, AdvanceSatelliteFailoverState.TargetState.NORMAL));

        SatelliteReplicationStrategy srs = getSRS(DUAL_DC_KEYSPACE);
        ClusterMetadata metadata = ClusterMetadata.current();

        // After full lifecycle, should be back to NORMAL
        SatelliteFailover.Info info = srs.getFailoverInfo(metadata);
        SatelliteFailover.State state = info.stateForToken(token(150));
        assertEquals(SatelliteFailover.State.NORMAL, state);
        assertFalse(state.isTransitioning());
    }

    @Test
    public void testDropKeyspaceCleansUpFailoverState() throws Exception
    {
        createDualDCKeyspace("dc1");
        alterKeyspacePrimary(DUAL_DC_KEYSPACE, "dc2");

        // Verify failover state is active
        ClusterMetadata metadata = ClusterMetadata.current();
        assertTrue(metadata.satelliteFailoverState.hasActiveTransfer(DUAL_DC_KEYSPACE));

        // DROP the keyspace
        dropKeyspace(DUAL_DC_KEYSPACE);

        // Verify failover state was cleaned up
        metadata = ClusterMetadata.current();
        assertFalse(metadata.satelliteFailoverState.hasActiveTransfer(DUAL_DC_KEYSPACE));
        assertNull(metadata.satelliteFailoverState.getKeyspaceState(DUAL_DC_KEYSPACE));
    }

    @Test
    public void testStrategyChangeAwayFromSRSCleansUpFailoverState() throws Exception
    {
        createDualDCKeyspace("dc1");
        alterKeyspacePrimary(DUAL_DC_KEYSPACE, "dc2");

        // Verify failover state is active
        ClusterMetadata metadata = ClusterMetadata.current();
        assertTrue(metadata.satelliteFailoverState.hasActiveTransfer(DUAL_DC_KEYSPACE));

        // Change the keyspace to NetworkTopologyStrategy (away from SRS)
        alterKeyspaceToNTS(DUAL_DC_KEYSPACE);

        // Verify failover state was cleaned up
        metadata = ClusterMetadata.current();
        assertFalse(metadata.satelliteFailoverState.hasActiveTransfer(DUAL_DC_KEYSPACE));
        assertNull(metadata.satelliteFailoverState.getKeyspaceState(DUAL_DC_KEYSPACE));
    }

    // ========== Helpers ==========

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

    private static Token token(long value)
    {
        return DatabaseDescriptor.getPartitioner().getTokenFactory().fromString(Long.toString(value));
    }

    private static NormalizedRanges<Token> fullTokenRange()
    {
        Token min = DatabaseDescriptor.getPartitioner().getMinimumToken();
        return NormalizedRanges.normalizedRanges(Collections.singleton(new Range<>(min, min)));
    }

    private static NormalizedRanges<Token> rangesOf(Token left, Token right)
    {
        return NormalizedRanges.normalizedRanges(Collections.singleton(new Range<>(left, right)));
    }

    private void dropKeyspace(String keyspace) throws Exception
    {
        String cql = "DROP KEYSPACE " + keyspace;
        AlterSchemaStatement stmt = (AlterSchemaStatement) QueryProcessor.parseStatement(cql)
            .prepare(ClientState.forInternalCalls());
        ClusterMetadataTestHelper.commit(new AlterSchema(stmt));
    }

    private void alterKeyspaceToNTS(String keyspace) throws Exception
    {
        String cql = "ALTER KEYSPACE " + keyspace + " WITH replication = {" +
                     "'class': 'NetworkTopologyStrategy', " +
                     "'dc1': '3', " +
                     "'dc2': '3'" +
                     "}";
        AlterSchemaStatement stmt = (AlterSchemaStatement) QueryProcessor.parseStatement(cql)
            .prepare(ClientState.forInternalCalls());
        ClusterMetadataTestHelper.commit(new AlterSchema(stmt));
    }
}
