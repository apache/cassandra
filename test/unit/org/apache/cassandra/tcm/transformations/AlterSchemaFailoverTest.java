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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableList;

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
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.locator.SatelliteReplicationStrategy;
import org.apache.cassandra.locator.satellites.SatelliteFailover;
import org.apache.cassandra.locator.satellites.SatelliteFailoverProcessState;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata.KeyspaceDiff;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AlterSchemaFailoverTest
{
    private static IPartitioner partitioner;

    @BeforeClass
    public static void setup() throws Exception
    {
        CassandraRelevantProperties.PARTITIONER.setString(Murmur3Partitioner.class.getName());
        ServerTestUtils.prepareServerNoRegister();
        partitioner = DatabaseDescriptor.getPartitioner();
    }

    @Test
    public void testPrimaryDCChangeInitiatesFailover()
    {
        KeyspaceDiff diff = makeDiff("ks1", srsOptions("DC1", "DC2"), srsOptions("DC2", "DC2"));
        ImmutableList<KeyspaceDiff> altered = ImmutableList.of(diff);

        ClusterMetadata metadata = new ClusterMetadata(partitioner).forceEpoch(Epoch.create(1));
        ClusterMetadata.Transformer next = metadata.transformer();

        next = AlterSchema.maybeUpdateSatelliteFailoverState(SatelliteFailoverProcessState.EMPTY, next, altered, Keyspaces.none());
        ClusterMetadata result = next.build().metadata;

        assertTrue(result.satelliteFailoverState.hasActiveTransfer("ks1"));
        assertEquals("DC1", result.satelliteFailoverState.getKeyspaceState("ks1").fromDC);
        assertTrue(result.satelliteFailoverState.getKeyspaceState("ks1").hasRangesInState(SatelliteFailover.State.TRANSITION_ACK));
    }

    @Test
    public void testNoPrimaryChangeNoFailover()
    {
        // Same primary DC, but change something else (add a new DC) so a diff is produced
        Map<String, String> beforeOpts = srsOptions("DC1", "DC2");
        Map<String, String> afterOpts = new HashMap<>(srsOptions("DC1", "DC2"));
        afterOpts.put("DC3", "3");
        afterOpts.put("DC3.satellite.SA_DC3", "2/2");

        KeyspaceDiff diff = makeDiff("ks1", beforeOpts, afterOpts);
        ImmutableList<KeyspaceDiff> altered = ImmutableList.of(diff);

        ClusterMetadata metadata = new ClusterMetadata(partitioner).forceEpoch(Epoch.create(1));
        ClusterMetadata.Transformer next = metadata.transformer();

        next = AlterSchema.maybeUpdateSatelliteFailoverState(SatelliteFailoverProcessState.EMPTY, next, altered, Keyspaces.none());
        ClusterMetadata result = next.build().metadata;

        assertFalse(result.satelliteFailoverState.hasActiveTransfer("ks1"));
    }

    @Test
    public void testNonSatelliteKeyspaceIgnored()
    {
        // Use NTS-like options (no SatelliteReplicationStrategy class)
        Map<String, String> ntsOptions = new HashMap<>();
        ntsOptions.put("class", "NetworkTopologyStrategy");
        ntsOptions.put("DC1", "3");

        KeyspaceMetadata before = KeyspaceMetadata.create("ks1",
            KeyspaceParams.create(true, ntsOptions, ReplicationType.untracked));

        // NTS keyspaces don't produce a diff for params changes unless params differ
        // but we need to test that even if one is produced, non-SRS keyspaces are skipped.
        // Since params are the same here, diff would be empty. Let's force a params change:
        Map<String, String> ntsOptions2 = new HashMap<>(ntsOptions);
        ntsOptions2.put("DC2", "3");
        KeyspaceMetadata after = KeyspaceMetadata.create("ks1",
            KeyspaceParams.create(true, ntsOptions2, ReplicationType.untracked));

        Keyspaces ksBefore = Keyspaces.of(before);
        Keyspaces ksAfter = Keyspaces.of(after);
        KeyspacesDiff ksDiff = Keyspaces.diff(ksBefore, ksAfter);
        if (ksDiff.altered.isEmpty())
            return; // No diff produced, test is trivially correct

        ImmutableList<KeyspaceDiff> altered = ksDiff.altered;
        ClusterMetadata metadata = new ClusterMetadata(partitioner).forceEpoch(Epoch.create(1));
        ClusterMetadata.Transformer next = metadata.transformer();

        next = AlterSchema.maybeUpdateSatelliteFailoverState(SatelliteFailoverProcessState.EMPTY, next, altered, Keyspaces.none());
        ClusterMetadata result = next.build().metadata;

        assertFalse(result.satelliteFailoverState.hasActiveTransfer("ks1"));
    }

    @Test(expected = InvalidRequestException.class)
    public void testRejectConcurrentTransfer()
    {
        // Change the primary (DC1 -> DC2) while a transfer from DC1 is already in progress. DC1 is kept in the
        // after-topology so this exercises the primary-change rejection specifically (not the fromDC-removal guard).
        KeyspaceDiff diff = makeDiff("ks1", srsOptions("DC1", "DC2"), srsOptions("DC2", "DC1"));
        ImmutableList<KeyspaceDiff> altered = ImmutableList.of(diff);

        // Pre-existing active transfer
        NormalizedRanges<Token> fullRange = fullTokenRange();
        SatelliteFailoverProcessState existingState = SatelliteFailoverProcessState.EMPTY
            .withFailoverInitiated("ks1", "DC1", Epoch.EMPTY, fullRange);

        ClusterMetadata metadata = new ClusterMetadata(partitioner).forceEpoch(Epoch.create(1));
        ClusterMetadata.Transformer next = metadata.transformer();

        AlterSchema.maybeUpdateSatelliteFailoverState(existingState, next, altered, Keyspaces.none());
    }

    @Test(expected = InvalidRequestException.class)
    public void testRejectFailoverFromSatelliteLessSourceDC()
    {
        // DC1 is a full DC with NO satellite; DC2 has a satellite. Failing over away from DC1
        // (the source DC) must be rejected: the pipeline reconciles the source DC's tracked data
        // from its satellite, and there is none.
        KeyspaceDiff diff = makeDiff("ks1", satelliteLessDC1Opts("DC1"), satelliteLessDC1Opts("DC2"));
        ImmutableList<KeyspaceDiff> altered = ImmutableList.of(diff);

        ClusterMetadata metadata = new ClusterMetadata(partitioner).forceEpoch(Epoch.create(1));
        ClusterMetadata.Transformer next = metadata.transformer();

        AlterSchema.maybeUpdateSatelliteFailoverState(SatelliteFailoverProcessState.EMPTY, next, altered, Keyspaces.none());
    }

    @Test
    public void testPrimaryChangeWithOtherParamsStillTriggers()
    {
        // Change primary DC AND add a new DC simultaneously
        Map<String, String> beforeOpts = srsOptions("DC1", "DC2");
        Map<String, String> afterOpts = new HashMap<>(srsOptions("DC2", "DC2"));
        afterOpts.put("DC3", "3");

        KeyspaceDiff diff = makeDiff("ks1", beforeOpts, afterOpts);
        ImmutableList<KeyspaceDiff> altered = ImmutableList.of(diff);

        ClusterMetadata metadata = new ClusterMetadata(partitioner).forceEpoch(Epoch.create(1));
        ClusterMetadata.Transformer next = metadata.transformer();

        next = AlterSchema.maybeUpdateSatelliteFailoverState(SatelliteFailoverProcessState.EMPTY, next, altered, Keyspaces.none());
        ClusterMetadata result = next.build().metadata;

        assertTrue(result.satelliteFailoverState.hasActiveTransfer("ks1"));
    }

    @Test(expected = InvalidRequestException.class)
    public void testRejectRemovingFromDCWhileTransferActive()
    {
        // Active failover from DC1 (primary already moved to DC2). An alter that keeps the primary (DC2)
        // but drops DC1 -- the source DC the failover reconciles from -- must be rejected, or the failover
        // state would reference a datacenter no longer in the topology.
        KeyspaceDiff diff = makeDiff("ks1", srsOptions("DC2", "DC1"), srsOptions("DC2"));
        ImmutableList<KeyspaceDiff> altered = ImmutableList.of(diff);

        SatelliteFailoverProcessState existingState = SatelliteFailoverProcessState.EMPTY
            .withFailoverInitiated("ks1", "DC1", Epoch.EMPTY, fullTokenRange());

        ClusterMetadata metadata = new ClusterMetadata(partitioner).forceEpoch(Epoch.create(1));
        ClusterMetadata.Transformer next = metadata.transformer();

        AlterSchema.maybeUpdateSatelliteFailoverState(existingState, next, altered, Keyspaces.none());
    }

    @Test(expected = InvalidRequestException.class)
    public void testRejectRemovingFromDCSatelliteWhileTransferActive()
    {
        // Active failover from DC1. An alter that keeps DC1 as a full DC but removes its satellite (which the
        // TRANSITION barrier reconciles from) must be rejected, even though the primary (DC2) is unchanged.
        Map<String, String> beforeOpts = srsOptions("DC2", "DC1");
        Map<String, String> afterOpts = new HashMap<>(beforeOpts);
        afterOpts.remove("DC1.satellite.SA_DC1");

        KeyspaceDiff diff = makeDiff("ks1", beforeOpts, afterOpts);
        ImmutableList<KeyspaceDiff> altered = ImmutableList.of(diff);

        SatelliteFailoverProcessState existingState = SatelliteFailoverProcessState.EMPTY
            .withFailoverInitiated("ks1", "DC1", Epoch.EMPTY, fullTokenRange());

        ClusterMetadata metadata = new ClusterMetadata(partitioner).forceEpoch(Epoch.create(1));
        ClusterMetadata.Transformer next = metadata.transformer();

        AlterSchema.maybeUpdateSatelliteFailoverState(existingState, next, altered, Keyspaces.none());
    }

    @Test
    public void testUnrelatedAlterDuringActiveTransferAllowed()
    {
        // Active failover from DC1. An alter that keeps the primary, DC1, and DC1's satellite intact (here just
        // adding an unrelated DC3) must NOT be rejected.
        Map<String, String> afterOpts = new HashMap<>(srsOptions("DC2", "DC1"));
        afterOpts.put("DC3", "3");
        afterOpts.put("DC3.satellite.SA_DC3", "2/2");

        KeyspaceDiff diff = makeDiff("ks1", srsOptions("DC2", "DC1"), afterOpts);
        ImmutableList<KeyspaceDiff> altered = ImmutableList.of(diff);

        SatelliteFailoverProcessState existingState = SatelliteFailoverProcessState.EMPTY
            .withFailoverInitiated("ks1", "DC1", Epoch.EMPTY, fullTokenRange());

        // Seed the base metadata with the active transfer so an unchanged failover state is preserved through build().
        ClusterMetadata metadata = new ClusterMetadata(partitioner).forceEpoch(Epoch.create(1))
                                                                   .transformer().with(existingState).build().metadata;
        ClusterMetadata.Transformer next = metadata.transformer();

        next = AlterSchema.maybeUpdateSatelliteFailoverState(metadata.satelliteFailoverState, next, altered, Keyspaces.none());
        ClusterMetadata result = next.build().metadata;

        assertTrue(result.satelliteFailoverState.hasActiveTransfer("ks1"));
    }

    // ========== Helpers ==========

    /**
     * Create SRS replication options with primary=primaryDC and satellites for all full DCs.
     */
    private static Map<String, String> srsOptions(String primaryDC, String... fullDCs)
    {
        Map<String, String> opts = new HashMap<>();
        opts.put("class", SatelliteReplicationStrategy.class.getName());
        opts.put("primary", primaryDC);
        // Ensure primary DC is included
        opts.put(primaryDC, "3");
        opts.put(primaryDC + ".satellite.SA_" + primaryDC, "2/2");
        for (String dc : fullDCs)
        {
            opts.putIfAbsent(dc, "3");
            opts.putIfAbsent(dc + ".satellite.SA_" + dc, "2/2");
        }
        return opts;
    }

    /**
     * SRS options where DC1 is a full DC with NO satellite and DC2 is a full DC with a satellite.
     * Used to exercise failover from a satellite-less source DC.
     */
    private static Map<String, String> satelliteLessDC1Opts(String primaryDC)
    {
        Map<String, String> opts = new HashMap<>();
        opts.put("class", SatelliteReplicationStrategy.class.getName());
        opts.put("primary", primaryDC);
        opts.put("DC1", "3");                       // full DC, no satellite
        opts.put("DC2", "3");
        opts.put("DC2.satellite.SA_DC2", "2/2");    // only DC2 has a satellite
        return opts;
    }

    private static KeyspaceDiff makeDiff(String ksName, Map<String, String> beforeOpts, Map<String, String> afterOpts)
    {
        KeyspaceMetadata before = KeyspaceMetadata.create(ksName,
            KeyspaceParams.create(true, beforeOpts, ReplicationType.tracked));
        KeyspaceMetadata after = KeyspaceMetadata.create(ksName,
            KeyspaceParams.create(true, afterOpts, ReplicationType.tracked));

        Keyspaces ksBefore = Keyspaces.of(before);
        Keyspaces ksAfter = Keyspaces.of(after);
        KeyspacesDiff ksDiff = Keyspaces.diff(ksBefore, ksAfter);
        assertFalse("Expected a diff to be produced", ksDiff.altered.isEmpty());
        return ksDiff.altered.get(0);
    }

    private static NormalizedRanges<Token> fullTokenRange()
    {
        Token min = partitioner.getMinimumToken();
        return NormalizedRanges.normalizedRanges(Collections.singleton(new Range<>(min, min)));
    }
}
