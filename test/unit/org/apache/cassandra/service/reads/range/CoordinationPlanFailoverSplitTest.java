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
package org.apache.cassandra.service.reads.range;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.schema.AlterSchemaStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.NormalizedRanges;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.locator.CoordinationPlan;
import org.apache.cassandra.locator.SatelliteReplicationStrategy;
import org.apache.cassandra.locator.SatelliteReplicationStrategyTestBase;
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
 * Tests for failover state boundary splitting in CoordinationPlanIterator.
 */
public class CoordinationPlanFailoverSplitTest extends SatelliteReplicationStrategyTestBase
{
    @Test
    public void testReSplitsBufferedRangesOnStateAdvancement() throws Exception
    {
        createDualDCKeyspace("dc1");
        alterKeyspacePrimary(DUAL_DC_KEYSPACE, "dc2");

        // Advance (100, 300] to TRANSITION -- creates a state boundary at token 300
        // State: (100, 300] = TRANSITION, everything else = TRANSITION_ACK
        NormalizedRanges<Token> subRange = rangesOf(token(100), token(300));
        ClusterMetadataTestHelper.commit(new AdvanceSatelliteFailoverState(
        DUAL_DC_KEYSPACE, subRange, AdvanceSatelliteFailoverState.TargetState.TRANSITION));

        Keyspace keyspace = Keyspace.open(DUAL_DC_KEYSPACE);

        // Query a range that spans multiple vnode ranges and failover state boundaries
        AbstractBounds<PartitionPosition> queryRange = new Range<>(token(0).minKeyBound(),
                                                                   token(600).maxKeyBound());

        // Collect plans from the iterator, advancing state between iterations
        List<AbstractBounds<PartitionPosition>> planRanges = new ArrayList<>();
        CoordinationPlanIterator iter = new CoordinationPlanIterator(
            queryRange, null, keyspace, null, ConsistencyLevel.LOCAL_QUORUM);

        // Consume first plan to populate the buffer
        assertTrue(iter.hasNext());
        CoordinationPlan.ForRangeRead firstPlan = iter.next();
        planRanges.add(firstPlan.replicas().range());

        // Advance (100, 200] to NORMAL -- this changes boundaries within what may be buffered
        NormalizedRanges<Token> advancedSubRange = rangesOf(token(100), token(200));
        ClusterMetadataTestHelper.commit(new AdvanceSatelliteFailoverState(
        DUAL_DC_KEYSPACE, advancedSubRange, AdvanceSatelliteFailoverState.TargetState.NORMAL));

        // Continue draining -- iterator should re-split buffered ranges against new state
        while (iter.hasNext())
            planRanges.add(iter.next().replicas().range());

        // Verify all plan ranges are contiguous (no gaps)
        for (int i = 0; i < planRanges.size() - 1; i++)
        {
            AbstractBounds<PartitionPosition> current = planRanges.get(i);
            AbstractBounds<PartitionPosition> next = planRanges.get(i + 1);
            assertEquals("Plans should be contiguous at index " + i,
                         current.right, next.left);
        }

        // Verify coverage
        assertEquals(queryRange.left, planRanges.get(0).left);
        assertEquals(queryRange.right, planRanges.get(planRanges.size() - 1).right);

        // Confirm state actually advanced -- token 150 should be NORMAL
        SatelliteReplicationStrategy srs = getSRS(DUAL_DC_KEYSPACE);
        ClusterMetadata metadata = ClusterMetadata.current();
        SatelliteFailover.Info info = srs.getFailoverInfo(metadata);
        assertEquals(SatelliteFailover.State.NORMAL, info.stateForToken(token(150)));
    }

    @Test
    public void testNoSplitWhenNoActiveTransfer() throws Exception
    {
        createDualDCKeyspace("dc1");

        Keyspace keyspace = Keyspace.open(DUAL_DC_KEYSPACE);
        AbstractBounds<PartitionPosition> queryRange = new Range<>(token(0).minKeyBound(),
                                                                   token(600).maxKeyBound());

        CoordinationPlanIterator iter = new CoordinationPlanIterator(
            queryRange, null, keyspace, null, ConsistencyLevel.LOCAL_QUORUM);

        // Should still produce plans (from vnode splitting) but no failover splitting
        List<AbstractBounds<PartitionPosition>> planRanges = new ArrayList<>();
        while (iter.hasNext())
            planRanges.add(iter.next().replicas().range());

        assertFalse(planRanges.isEmpty());

        // Verify contiguity
        for (int i = 0; i < planRanges.size() - 1; i++)
            assertEquals(planRanges.get(i).right, planRanges.get(i + 1).left);
    }

    @Test
    public void testSplitAtStateBoundary() throws Exception
    {
        createDualDCKeyspace("dc1");
        alterKeyspacePrimary(DUAL_DC_KEYSPACE, "dc2");

        // Advance full range to TRANSITION, then advance (100, 300] to NORMAL
        // This creates a NORMAL "hole" in the transitioning range
        NormalizedRanges<Token> fullRange = NormalizedRanges.normalizedRanges(
            Collections.singleton(new Range<>(DatabaseDescriptor.getPartitioner().getMinimumToken(),
                                             DatabaseDescriptor.getPartitioner().getMinimumToken())));
        ClusterMetadataTestHelper.commit(new AdvanceSatelliteFailoverState(
        DUAL_DC_KEYSPACE, fullRange, AdvanceSatelliteFailoverState.TargetState.TRANSITION));

        NormalizedRanges<Token> normalRange = rangesOf(token(100), token(300));
        ClusterMetadataTestHelper.commit(new AdvanceSatelliteFailoverState(
        DUAL_DC_KEYSPACE, normalRange, AdvanceSatelliteFailoverState.TargetState.NORMAL));

        // State: (100, 300] = NORMAL, everything else = TRANSITION
        Keyspace keyspace = Keyspace.open(DUAL_DC_KEYSPACE);
        ClusterMetadata metadata = ClusterMetadata.current();

        // Range (0, 400] spans: (0, 100] = TRANSITION, (100, 300] = NORMAL, (300, 400] = TRANSITION
        AbstractBounds<PartitionPosition> range = new Range<>(token(0).minKeyBound(),
                                                              token(400).maxKeyBound());

        List<AbstractBounds<PartitionPosition>> splits = CoordinationPlanIterator.splitAtFailoverBoundaries(
            range, keyspace, metadata);

        // Should have split into multiple sub-ranges (boundary at 100 and 300)
        assertNotNull("Should split at state boundaries", splits);
        assertTrue("Expected multiple splits, got " + splits.size(), splits.size() >= 2);

        // Verify contiguity
        for (int i = 0; i < splits.size() - 1; i++)
            assertEquals("Splits should be contiguous at index " + i,
                         splits.get(i).right, splits.get(i + 1).left);

        // Verify coverage
        assertEquals(range.left, splits.get(0).left);
        assertEquals(range.right, splits.get(splits.size() - 1).right);
    }

    @Test
    public void testNoSplitWhenRangeDoesNotCrossBoundary() throws Exception
    {
        createDualDCKeyspace("dc1");
        alterKeyspacePrimary(DUAL_DC_KEYSPACE, "dc2");

        // Advance full range to TRANSITION, then advance (100, 300] to NORMAL
        NormalizedRanges<Token> fullRange = NormalizedRanges.normalizedRanges(
            Collections.singleton(new Range<>(DatabaseDescriptor.getPartitioner().getMinimumToken(),
                                             DatabaseDescriptor.getPartitioner().getMinimumToken())));
        ClusterMetadataTestHelper.commit(new AdvanceSatelliteFailoverState(
        DUAL_DC_KEYSPACE, fullRange, AdvanceSatelliteFailoverState.TargetState.TRANSITION));

        NormalizedRanges<Token> normalRange = rangesOf(token(100), token(300));
        ClusterMetadataTestHelper.commit(new AdvanceSatelliteFailoverState(
        DUAL_DC_KEYSPACE, normalRange, AdvanceSatelliteFailoverState.TargetState.NORMAL));

        Keyspace keyspace = Keyspace.open(DUAL_DC_KEYSPACE);
        ClusterMetadata metadata = ClusterMetadata.current();

        // Range entirely within NORMAL region -- should not split
        AbstractBounds<PartitionPosition> range = new Range<>(token(150).minKeyBound(),
                                                              token(250).maxKeyBound());

        List<AbstractBounds<PartitionPosition>> splits = CoordinationPlanIterator.splitAtFailoverBoundaries(
            range, keyspace, metadata);

        assertNull("Should not split when range is entirely within one state", splits);
    }

    @Test
    public void testNoSplitForNonSRSKeyspace() throws Exception
    {
        // Create a simple NTS keyspace (non-SRS)
        String cql = "CREATE KEYSPACE non_srs_ks WITH replication = {" +
                     "'class': 'NetworkTopologyStrategy', 'dc1': '3'" +
                     "}";
        ClusterMetadataTestHelper.createKeyspace(cql);

        Keyspace keyspace = Keyspace.open("non_srs_ks");
        ClusterMetadata metadata = ClusterMetadata.current();

        AbstractBounds<PartitionPosition> range = new Range<>(token(0).minKeyBound(),
                                                              token(600).maxKeyBound());

        List<AbstractBounds<PartitionPosition>> splits = CoordinationPlanIterator.splitAtFailoverBoundaries(
            range, keyspace, metadata);

        assertNull("Non-SRS keyspace should never split at failover boundaries", splits);
    }

    @Test
    public void testNoExtraSplitWhenBoundaryAlignsWithVnodeToken() throws Exception
    {
        createDualDCKeyspace("dc1");
        alterKeyspacePrimary(DUAL_DC_KEYSPACE, "dc2");

        // Advance full range to TRANSITION, then advance a range that aligns with a vnode token to NORMAL
        // Vnode tokens in test setup: 100, 200, 300, 400, 500, 600, ...
        // Advance (MIN, 200] to NORMAL -- boundary at token 200 which is also a vnode boundary
        NormalizedRanges<Token> fullRange = NormalizedRanges.normalizedRanges(
            Collections.singleton(new Range<>(DatabaseDescriptor.getPartitioner().getMinimumToken(),
                                             DatabaseDescriptor.getPartitioner().getMinimumToken())));
        ClusterMetadataTestHelper.commit(new AdvanceSatelliteFailoverState(
        DUAL_DC_KEYSPACE, fullRange, AdvanceSatelliteFailoverState.TargetState.TRANSITION));

        NormalizedRanges<Token> normalRange = rangesOf(
            DatabaseDescriptor.getPartitioner().getMinimumToken(), token(200));
        ClusterMetadataTestHelper.commit(new AdvanceSatelliteFailoverState(
        DUAL_DC_KEYSPACE, normalRange, AdvanceSatelliteFailoverState.TargetState.NORMAL));

        Keyspace keyspace = Keyspace.open(DUAL_DC_KEYSPACE);

        // Query range spanning the aligned boundary
        AbstractBounds<PartitionPosition> queryRange = new Range<>(token(0).minKeyBound(),
                                                                   token(400).maxKeyBound());

        // The vnode split already cuts at token 200, so vnode sub-ranges are:
        // (0, 100], (100, 200], (200, 300], (300, 400]
        // The failover boundary is also at 200, so no EXTRA split should be needed within any vnode range
        // (0, 100] = NORMAL (no split), (100, 200] = NORMAL (no split),
        // (200, 300] = TRANSITION (no split), (300, 400] = TRANSITION (no split)
        CoordinationPlanIterator iter = new CoordinationPlanIterator(
            queryRange, null, keyspace, null, ConsistencyLevel.LOCAL_QUORUM);

        List<AbstractBounds<PartitionPosition>> planRanges = new ArrayList<>();
        while (iter.hasNext())
            planRanges.add(iter.next().replicas().range());

        // Should have exactly 4 plans (one per vnode range) -- no extra failover splits
        assertEquals("When failover boundary aligns with vnode boundary, no extra splits needed",
                     4, planRanges.size());

        // Verify contiguity
        for (int i = 0; i < planRanges.size() - 1; i++)
            assertEquals(planRanges.get(i).right, planRanges.get(i + 1).left);
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

    private static NormalizedRanges<Token> rangesOf(Token left, Token right)
    {
        return NormalizedRanges.normalizedRanges(Collections.singleton(new Range<>(left, right)));
    }
}
