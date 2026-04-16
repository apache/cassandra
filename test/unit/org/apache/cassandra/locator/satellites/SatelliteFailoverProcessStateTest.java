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
package org.apache.cassandra.locator.satellites;

import java.io.IOException;
import java.util.Collections;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.NormalizedRanges;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class SatelliteFailoverProcessStateTest
{
    private static Murmur3Partitioner partitioner;

    @BeforeClass
    public static void setup() throws Exception
    {
        CassandraRelevantProperties.PARTITIONER.setString(Murmur3Partitioner.class.getName());
        ServerTestUtils.prepareServerNoRegister();
        partitioner = Murmur3Partitioner.instance;
    }

    // ========== KeyspaceFailoverState tests ==========

    @Test
    public void testWithRangesTransitioning()
    {
        NormalizedRanges<Token> fullRange = fullRange();
        KeyspaceFailoverState state = KeyspaceFailoverState.create("DC1", Epoch.EMPTY, fullRange);

        // Advance a sub-range from TRANSITION_ACK to TRANSITION
        NormalizedRanges<Token> advancing = rangesOf(tk(100), tk(200));
        KeyspaceFailoverState updated = state.withRangesTransitioning(advancing);

        assertFalse(updated.isComplete());
        assertEquals("DC1", updated.fromDC);
        assertTrue(updated.hasRangesInState(SatelliteFailover.State.TRANSITION_ACK));
        assertTrue(updated.hasRangesInState(SatelliteFailover.State.TRANSITION));
        assertEquals(SatelliteFailover.State.TRANSITION, updated.stateForToken(tk(150)));
    }

    @Test
    public void testWithRangesNormal()
    {
        NormalizedRanges<Token> fullRange = fullRange();
        KeyspaceFailoverState state = KeyspaceFailoverState.create("DC1", Epoch.EMPTY, fullRange);

        // Advance all to TRANSITION first
        NormalizedRanges<Token> transitionRanges = rangesOf(tk(100), tk(200));
        state = state.withRangesTransitioning(transitionRanges);

        // Now advance transition ranges to NORMAL
        KeyspaceFailoverState updated = state.withRangesNormal(transitionRanges);

        assertFalse(updated.isComplete()); // still has ack ranges
        assertFalse(updated.hasRangesInState(SatelliteFailover.State.TRANSITION));
        assertTrue(updated.hasRangesInState(SatelliteFailover.State.TRANSITION_ACK));
    }

    @Test
    public void testTransferComplete()
    {
        NormalizedRanges<Token> fullRange = fullRange();
        KeyspaceFailoverState state = KeyspaceFailoverState.create("DC1", Epoch.EMPTY, fullRange);

        // Advance all ranges through TRANSITION_ACK → TRANSITION → NORMAL
        state = state.withRangesTransitioning(fullRange);
        assertFalse(state.isComplete());
        assertFalse(state.hasRangesInState(SatelliteFailover.State.TRANSITION_ACK));
        assertTrue(state.hasRangesInState(SatelliteFailover.State.TRANSITION));

        state = state.withRangesNormal(fullRange);
        assertTrue(state.isComplete());
    }

    @Test
    public void testKeyspaceFailoverStateSerialization() throws IOException
    {
        NormalizedRanges<Token> fullRange = fullRange();
        KeyspaceFailoverState state = KeyspaceFailoverState.create("DC1", Epoch.EMPTY, fullRange);
        state = state.withRangesTransitioning(rangesOf(tk(0), tk(100)));

        assertSerializationRoundTrip(state, KeyspaceFailoverState.serializer);
    }

    // ========== leastAdvancedState tests ==========

    @Test
    public void testLeastAdvancedStateUniform()
    {
        KeyspaceFailoverState state = KeyspaceFailoverState.create("DC1", Epoch.EMPTY, fullRange());

        assertEquals(SatelliteFailover.State.TRANSITION_ACK, state.leastAdvancedState(range(0, 100)));
        assertEquals(SatelliteFailover.State.TRANSITION_ACK, state.leastAdvancedState(fullRingRange()));
    }

    @Test
    public void testLeastAdvancedStatePartiallyAdvancedRange()
    {
        KeyspaceFailoverState state = KeyspaceFailoverState.create("DC1", Epoch.EMPTY, fullRange())
                                                           .withRangesTransitioning(rangesOf(tk(100), tk(200)));

        // entirely within the advanced sub-range
        assertEquals(SatelliteFailover.State.TRANSITION, state.leastAdvancedState(range(120, 180)));
        assertEquals(SatelliteFailover.State.TRANSITION, state.leastAdvancedState(range(100, 200)));

        // straddling the boundary in either direction: the un-advanced part must win
        assertEquals(SatelliteFailover.State.TRANSITION_ACK, state.leastAdvancedState(range(150, 250)));
        assertEquals(SatelliteFailover.State.TRANSITION_ACK, state.leastAdvancedState(range(50, 150)));
        assertEquals(SatelliteFailover.State.TRANSITION_ACK, state.leastAdvancedState(fullRingRange()));

        // abutting the advanced sub-range without overlapping it
        assertEquals(SatelliteFailover.State.TRANSITION_ACK, state.leastAdvancedState(range(0, 100)));
        assertEquals(SatelliteFailover.State.TRANSITION_ACK, state.leastAdvancedState(range(200, 300)));
    }

    @Test
    public void testLeastAdvancedStateAcrossAllStates()
    {
        KeyspaceFailoverState state = KeyspaceFailoverState.create("DC1", Epoch.EMPTY, fullRange())
                                                           .withRangesTransitioning(rangesOf(tk(100), tk(300)))
                                                           .withRangesNormal(rangesOf(tk(100), tk(200)));

        assertEquals(SatelliteFailover.State.NORMAL, state.leastAdvancedState(range(100, 200)));
        assertEquals(SatelliteFailover.State.TRANSITION, state.leastAdvancedState(range(150, 300)));
        assertEquals(SatelliteFailover.State.TRANSITION_ACK, state.leastAdvancedState(range(150, 400)));
    }

    @Test
    public void testLeastAdvancedStateWrapAround()
    {
        Token min = partitioner.getMinimumToken();
        KeyspaceFailoverState state = KeyspaceFailoverState.create("DC1", Epoch.EMPTY, fullRange())
                                                           .withRangesTransitioning(rangesOf(tk(200), min));

        // wrapping range fully inside the advanced sub-range
        assertEquals(SatelliteFailover.State.TRANSITION, state.leastAdvancedState(new Range<>(tk(300), min)));
        // wrapping range that also covers un-advanced tokens
        assertEquals(SatelliteFailover.State.TRANSITION_ACK, state.leastAdvancedState(new Range<>(tk(100), min)));
        // the wrap sentinel is the upper bound of the last interval
        assertEquals(SatelliteFailover.State.TRANSITION, state.leastAdvancedState(new Range<>(tk(200), min)));
    }

    @Test
    public void testLeastAdvancedStateNoActiveFailover()
    {
        assertEquals(SatelliteFailover.State.NORMAL, SatelliteFailover.Info.NORMAL.leastAdvancedState(range(0, 100)));
        assertEquals(SatelliteFailover.State.NORMAL, SatelliteFailover.Info.NORMAL.leastAdvancedState(fullRingRange()));
    }

    // ========== SatelliteFailoverProcessState tests ==========

    @Test
    public void testInitiateFailover()
    {
        NormalizedRanges<Token> fullRange = fullRange();
        SatelliteFailoverProcessState state = SatelliteFailoverProcessState.EMPTY
            .withFailoverInitiated("ks1", "DC1", Epoch.EMPTY, fullRange);

        assertTrue(state.hasActiveTransfer("ks1"));
        assertFalse(state.hasActiveTransfer("ks2"));

        KeyspaceFailoverState ksState = state.getKeyspaceState("ks1");
        assertNotNull(ksState);
        assertEquals("DC1", ksState.fromDC);
        assertTrue(ksState.hasRangesInState(SatelliteFailover.State.TRANSITION_ACK));
        assertFalse(ksState.hasRangesInState(SatelliteFailover.State.TRANSITION));
    }

    @Test
    public void testAdvanceRangesTransitioning()
    {
        NormalizedRanges<Token> fullRange = fullRange();
        SatelliteFailoverProcessState state = SatelliteFailoverProcessState.EMPTY
            .withFailoverInitiated("ks1", "DC1", Epoch.EMPTY, fullRange);

        NormalizedRanges<Token> advancing = rangesOf(tk(100), tk(200));
        state = state.withRangesTransitioning("ks1", advancing);

        assertTrue(state.hasActiveTransfer("ks1"));
        KeyspaceFailoverState ksState = state.getKeyspaceState("ks1");
        assertTrue(ksState.hasRangesInState(SatelliteFailover.State.TRANSITION));
    }

    @Test
    public void testCompletedTransferRemovesKeyspace()
    {
        NormalizedRanges<Token> fullRange = fullRange();
        SatelliteFailoverProcessState state = SatelliteFailoverProcessState.EMPTY
            .withFailoverInitiated("ks1", "DC1", Epoch.EMPTY, fullRange);

        // Advance all through TRANSITION_ACK → TRANSITION
        state = state.withRangesTransitioning("ks1", fullRange);
        assertTrue(state.hasActiveTransfer("ks1"));

        // Advance all through TRANSITION → NORMAL
        state = state.withRangesNormal("ks1", fullRange);
        assertFalse(state.hasActiveTransfer("ks1"));
        assertNull(state.getKeyspaceState("ks1"));
    }

    @Test
    public void testMultipleKeyspaces()
    {
        NormalizedRanges<Token> fullRange = fullRange();
        SatelliteFailoverProcessState state = SatelliteFailoverProcessState.EMPTY
            .withFailoverInitiated("ks1", "DC1", Epoch.EMPTY, fullRange)
            .withFailoverInitiated("ks2", "DC2", Epoch.EMPTY, fullRange);

        assertTrue(state.hasActiveTransfer("ks1"));
        assertTrue(state.hasActiveTransfer("ks2"));
        assertEquals("DC1", state.getKeyspaceState("ks1").fromDC);
        assertEquals("DC2", state.getKeyspaceState("ks2").fromDC);

        // Complete ks1, ks2 should remain
        state = state.withRangesTransitioning("ks1", fullRange);
        state = state.withRangesNormal("ks1", fullRange);
        assertFalse(state.hasActiveTransfer("ks1"));
        assertTrue(state.hasActiveTransfer("ks2"));
    }

    @Test
    public void testNoopOnMissingKeyspace()
    {
        SatelliteFailoverProcessState state = SatelliteFailoverProcessState.EMPTY;

        // These should be noops, not throw
        SatelliteFailoverProcessState same = state.withRangesTransitioning("nonexistent", fullRange());
        assertSame(state, same);

        same = state.withRangesNormal("nonexistent", fullRange());
        assertSame(state, same);
    }

    @Test
    public void testWithLastModified()
    {
        Epoch epoch1 = Epoch.create(1);
        Epoch epoch2 = Epoch.create(2);

        SatelliteFailoverProcessState state = new SatelliteFailoverProcessState(epoch1, Collections.emptyMap());
        SatelliteFailoverProcessState updated = state.withLastModified(epoch2);

        assertEquals(epoch1, state.lastModified);
        assertEquals(epoch2, updated.lastModified);
    }

    @Test
    public void testProcessStateSerialization() throws IOException
    {
        NormalizedRanges<Token> fullRange = fullRange();
        NormalizedRanges<Token> advancing = rangesOf(tk(100), tk(200));

        SatelliteFailoverProcessState original = SatelliteFailoverProcessState.EMPTY
            .withFailoverInitiated("ks1", "DC1", Epoch.EMPTY, fullRange)
            .withRangesTransitioning("ks1", advancing);

        assertSerializationRoundTrip(original, SatelliteFailoverProcessState.serializer);
    }

    @Test
    public void testEmptyStateSerialization() throws IOException
    {
        assertSerializationRoundTrip(SatelliteFailoverProcessState.EMPTY, SatelliteFailoverProcessState.serializer);
    }

    @Test
    public void testIncrementalAdvancement()
    {
        NormalizedRanges<Token> fullRange = fullRange();
        SatelliteFailoverProcessState state = SatelliteFailoverProcessState.EMPTY
            .withFailoverInitiated("ks1", "DC1", Epoch.EMPTY, fullRange);

        // Advance first batch
        NormalizedRanges<Token> batch1 = rangesOf(tk(0), tk(100));
        state = state.withRangesTransitioning("ks1", batch1);

        // Advance second batch
        NormalizedRanges<Token> batch2 = rangesOf(tk(100), tk(200));
        state = state.withRangesTransitioning("ks1", batch2);

        // First batch finishes barrier
        state = state.withRangesNormal("ks1", batch1);

        // Transfer still active (batch2 in TRANSITION, rest in TRANSITION_ACK)
        assertTrue(state.hasActiveTransfer("ks1"));
        KeyspaceFailoverState ks = state.getKeyspaceState("ks1");
        assertTrue(ks.hasRangesInState(SatelliteFailover.State.TRANSITION_ACK));
        assertTrue(ks.hasRangesInState(SatelliteFailover.State.TRANSITION));
    }

    // ========== Helpers ==========

    private static <T> void assertSerializationRoundTrip(T original, MetadataSerializer<T> serializer) throws IOException
    {
        DataOutputBuffer out = new DataOutputBuffer();
        serializer.serialize(original, out, NodeVersion.CURRENT_METADATA_VERSION);

        DataInputBuffer in = new DataInputBuffer(out.unsafeGetBufferAndFlip(), false);
        T deserialized = serializer.deserialize(in, NodeVersion.CURRENT_METADATA_VERSION);

        assertEquals(original, deserialized);
    }

    private static Token tk(long value)
    {
        return partitioner.getTokenFactory().fromString(Long.toString(value));
    }

    private static NormalizedRanges<Token> fullRange()
    {
        Token min = partitioner.getMinimumToken();
        return NormalizedRanges.normalizedRanges(Collections.singleton(new Range<>(min, min)));
    }

    private static NormalizedRanges<Token> rangesOf(Token left, Token right)
    {
        return NormalizedRanges.normalizedRanges(Collections.singleton(new Range<>(left, right)));
    }

    private static Range<Token> range(long left, long right)
    {
        return new Range<>(tk(left), tk(right));
    }

    private static Range<Token> fullRingRange()
    {
        Token min = partitioner.getMinimumToken();
        return new Range<>(min, min);
    }
}
