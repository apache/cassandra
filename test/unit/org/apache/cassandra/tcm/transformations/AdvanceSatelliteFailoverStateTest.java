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
package org.apache.cassandra.tcm.transformations;

import java.io.IOException;
import java.util.Collections;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.NormalizedRanges;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.satellites.KeyspaceFailoverState;
import org.apache.cassandra.locator.satellites.SatelliteFailover;
import org.apache.cassandra.locator.satellites.SatelliteFailoverProcessState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeVersion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AdvanceSatelliteFailoverStateTest
{
    private static IPartitioner partitioner;

    @BeforeClass
    public static void setup() throws Exception
    {
        CassandraRelevantProperties.PARTITIONER.setString(Murmur3Partitioner.class.getName());
        ServerTestUtils.prepareServerNoRegister();
        partitioner = DatabaseDescriptor.getPartitioner();
    }

    private ClusterMetadata metadataWithFailover(String keyspace, String fromDC)
    {
        NormalizedRanges<Token> fullRange = fullTokenRange();
        SatelliteFailoverProcessState failoverState = SatelliteFailoverProcessState.EMPTY
            .withFailoverInitiated(keyspace, fromDC, Epoch.EMPTY, fullRange);

        return new ClusterMetadata(partitioner)
            .forceEpoch(Epoch.create(1))
            .transformer()
            .with(failoverState)
            .build().metadata;
    }

    @Test
    public void testAdvanceToTransition()
    {
        ClusterMetadata prev = metadataWithFailover("ks1", "DC1");

        NormalizedRanges<Token> advancing = rangesOf(token(100), token(200));
        AdvanceSatelliteFailoverState transformation = new AdvanceSatelliteFailoverState("ks1", advancing, AdvanceSatelliteFailoverState.TargetState.TRANSITION);

        Transformation.Result result = transformation.execute(prev);
        assertTrue(result.isSuccess());

        ClusterMetadata next = result.success().metadata;
        KeyspaceFailoverState ksState = next.satelliteFailoverState.getKeyspaceState("ks1");
        assertNotNull(ksState);
        assertTrue(ksState.hasRangesInState(SatelliteFailover.State.TRANSITION));
        assertEquals(SatelliteFailover.State.TRANSITION, ksState.stateForToken(token(150)));
    }

    @Test
    public void testAdvanceToNormal()
    {
        // First advance to TRANSITION
        ClusterMetadata prev = metadataWithFailover("ks1", "DC1");
        NormalizedRanges<Token> advancing = rangesOf(token(100), token(200));

        Transformation.Result result = new AdvanceSatelliteFailoverState("ks1", advancing, AdvanceSatelliteFailoverState.TargetState.TRANSITION)
            .execute(prev);
        prev = result.success().metadata;

        // Now advance to NORMAL
        result = new AdvanceSatelliteFailoverState("ks1", advancing, AdvanceSatelliteFailoverState.TargetState.NORMAL)
            .execute(prev);
        assertTrue(result.isSuccess());

        ClusterMetadata next = result.success().metadata;
        KeyspaceFailoverState ksState = next.satelliteFailoverState.getKeyspaceState("ks1");
        assertNotNull(ksState); // still has ack ranges
        assertFalse(ksState.hasRangesInState(SatelliteFailover.State.TRANSITION));
    }

    @Test
    public void testRejectNoActiveTransfer()
    {
        ClusterMetadata prev = new ClusterMetadata(partitioner).forceEpoch(Epoch.create(1));

        NormalizedRanges<Token> ranges = rangesOf(token(100), token(200));
        AdvanceSatelliteFailoverState transformation = new AdvanceSatelliteFailoverState("nonexistent", ranges, AdvanceSatelliteFailoverState.TargetState.TRANSITION);

        Transformation.Result result = transformation.execute(prev);
        assertTrue(result.isRejected());
    }

    @Test
    public void testTransferAutoCleanup()
    {
        ClusterMetadata prev = metadataWithFailover("ks1", "DC1");
        NormalizedRanges<Token> fullRange = fullTokenRange();

        // Advance all to TRANSITION
        Transformation.Result result = new AdvanceSatelliteFailoverState("ks1", fullRange, AdvanceSatelliteFailoverState.TargetState.TRANSITION)
            .execute(prev);
        prev = result.success().metadata;
        assertTrue(prev.satelliteFailoverState.hasActiveTransfer("ks1"));

        // Advance all to NORMAL -- should auto-remove keyspace
        result = new AdvanceSatelliteFailoverState("ks1", fullRange, AdvanceSatelliteFailoverState.TargetState.NORMAL)
            .execute(prev);
        ClusterMetadata next = result.success().metadata;
        assertFalse(next.satelliteFailoverState.hasActiveTransfer("ks1"));
    }

    @Test
    public void testConcurrentAdvancementOverlap()
    {
        ClusterMetadata prev = metadataWithFailover("ks1", "DC1");

        // Two overlapping ranges
        NormalizedRanges<Token> range1 = rangesOf(token(100), token(300));
        NormalizedRanges<Token> range2 = rangesOf(token(200), token(400));

        // First advancement succeeds
        Transformation.Result result1 = new AdvanceSatelliteFailoverState("ks1", range1, AdvanceSatelliteFailoverState.TargetState.TRANSITION)
            .execute(prev);
        assertTrue(result1.isSuccess());

        // Second advancement on the same base metadata also succeeds
        // (in practice TCM linearizability prevents this, but the transformation itself handles it gracefully)
        Transformation.Result result2 = new AdvanceSatelliteFailoverState("ks1", range2, AdvanceSatelliteFailoverState.TargetState.TRANSITION)
            .execute(prev);
        assertTrue(result2.isSuccess());
    }

    @Test
    public void testSerializationRoundtrip() throws IOException
    {
        NormalizedRanges<Token> ranges = rangesOf(token(100), token(200));
        AdvanceSatelliteFailoverState original = new AdvanceSatelliteFailoverState("ks1", ranges, AdvanceSatelliteFailoverState.TargetState.TRANSITION);

        DataOutputBuffer out = new DataOutputBuffer();
        AdvanceSatelliteFailoverState.serializer.serialize(original, out, NodeVersion.CURRENT_METADATA_VERSION);

        DataInputBuffer in = new DataInputBuffer(out.unsafeGetBufferAndFlip(), false);
        AdvanceSatelliteFailoverState deserialized = AdvanceSatelliteFailoverState.serializer.deserialize(in, NodeVersion.CURRENT_METADATA_VERSION);

        assertEquals(original.keyspace, deserialized.keyspace);
        assertEquals(original.targetState, deserialized.targetState);
        assertEquals(original.ranges, deserialized.ranges);
    }

    // ========== Helpers ==========

    private static Token token(long value)
    {
        return partitioner.getTokenFactory().fromString(Long.toString(value));
    }

    private static NormalizedRanges<Token> fullTokenRange()
    {
        Token min = partitioner.getMinimumToken();
        return NormalizedRanges.normalizedRanges(Collections.singleton(new Range<>(min, min)));
    }

    private static NormalizedRanges<Token> rangesOf(Token left, Token right)
    {
        return NormalizedRanges.normalizedRanges(Collections.singleton(new Range<>(left, right)));
    }
}
