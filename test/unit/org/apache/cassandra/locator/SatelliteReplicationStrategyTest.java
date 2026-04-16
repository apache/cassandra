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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ownership.ReplicaGroups;
import org.apache.cassandra.tcm.ownership.VersionedEndpoints;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SatelliteReplicationStrategyTest extends SatelliteReplicationStrategyTestBase
{
    @Test
    public void testValidSingleDCWithSatellite() throws Exception
    {
        String cql = "CREATE KEYSPACE " + KEYSPACE + " WITH replication = {" +
                     "'class': 'SatelliteReplicationStrategy', " +
                     "'dc1': '3', " +
                     "'dc1.satellite.sat1': '2/2', " +
                     "'primary': 'dc1'" +
                     "} AND replication_type = 'tracked'";

        ClusterMetadataTestHelper.createKeyspace(cql);

        SatelliteReplicationStrategy strategy = getSRS(KEYSPACE);

        assertEquals("dc1", strategy.getPrimaryDC());
        assertEquals(1, strategy.getDatacenters().size());
        assertTrue(strategy.getDatacenters().contains("dc1"));
        assertEquals(1, strategy.getSatellites().size());
        assertTrue(strategy.getSatellites().containsKey("sat1"));
    }

    @Test
    public void testValidMultipleDCsWithSatellites() throws Exception
    {
        String cql = "CREATE KEYSPACE " + KEYSPACE + " WITH replication = {" +
                     "'class': 'SatelliteReplicationStrategy', " +
                     "'dc1': '3', " +
                     "'dc1.satellite.sat1': '2/2', " +
                     "'dc2': '3', " +
                     "'dc2.satellite.sat2': '2/2', " +
                     "'primary': 'dc1'" +
                     "} AND replication_type = 'tracked'";

        ClusterMetadataTestHelper.createKeyspace(cql);

        SatelliteReplicationStrategy strategy = getSRS(KEYSPACE);

        assertEquals("dc1", strategy.getPrimaryDC());
        assertEquals(2, strategy.getDatacenters().size());
        assertEquals(2, strategy.getSatellites().size());
    }

    private void testConfigurationException(Map<String, String> options, String messageContains)
    {
        try
        {
            new SatelliteReplicationStrategy(KEYSPACE, options, ReplicationType.tracked);
            fail("ConfigurationException expected");
        }
        catch (ConfigurationException e)
        {
            assertTrue(e.getMessage().contains(messageContains));
        }
    }

    @Test
    public void testMissingPrimaryFails() throws Exception
    {
        Map<String, String> options = new HashMap<>();
        options.put("dc1", "3");

        testConfigurationException(options, "'primary' option is required");
    }

    @Test
    public void testPrimaryNotInFullDCsFails() throws Exception
    {
        Map<String, String> options = new HashMap<>();
        options.put("dc1", "3");
        options.put("primary", "dc2");

        testConfigurationException(options, "Primary datacenter 'dc2' must be defined");
    }

    @Test
    public void testUntrackedReplicationFails() throws Exception
    {
        Map<String, String> options = new HashMap<>();
        options.put("dc1", "3");
        options.put("primary", "dc1");

        SatelliteReplicationStrategy strategy = new SatelliteReplicationStrategy(
            KEYSPACE, options, ReplicationType.untracked);

        try
        {
            strategy.validateExpectedOptions(ClusterMetadata.current());
            fail("ConfigurationException expected");
        }
        catch (ConfigurationException e)
        {
            assertTrue(e.getMessage().contains("requires tracked replication"));
        }
    }

    @Test
    public void testPaxosV1Fails() throws Exception
    {
        Map<String, String> options = new HashMap<>();
        options.put("dc1", "3");
        options.put("primary", "dc1");

        SatelliteReplicationStrategy strategy = new SatelliteReplicationStrategy(
            KEYSPACE, options, ReplicationType.tracked);

        Config.PaxosVariant prev = DatabaseDescriptor.getPaxosVariant();
        try
        {
            DatabaseDescriptor.setPaxosVariant(Config.PaxosVariant.v1);
            strategy.validateExpectedOptions(ClusterMetadata.current());
            fail("ConfigurationException expected");
        }
        catch (ConfigurationException e)
        {
            assertTrue(e.getMessage().contains("requires paxos_variant=v2"));
        }
        finally
        {
            DatabaseDescriptor.setPaxosVariant(prev);
        }
    }

    @Test
    public void testDotsInDCNamesFails() throws Exception
    {
        Map<String, String> options = new HashMap<>();
        options.put("dc.1", "3");
        options.put("primary", "dc.1");

        testConfigurationException(options, "cannot contain dots");
    }

    @Test
    public void testOrphanedSatelliteFails() throws Exception
    {
        Map<String, String> options = new HashMap<>();
        options.put("dc1", "3");
        options.put("dc2.satellite.sat1", "2/2");
        options.put("primary", "dc1");

        testConfigurationException(options, "references non-existent full datacenter 'dc2'");
    }

    @Test
    public void testSatellitePartialWitnessFails() throws Exception
    {
        Map<String, String> options = new HashMap<>();
        options.put("dc1", "3");
        options.put("dc1.satellite.sat1", "3/1");
        options.put("primary", "dc1");

        testConfigurationException(options, "must all be witnesses");
    }

    @Test
    public void testSatelliteRequiresWitnessFormat() throws Exception
    {
        Map<String, String> options = new HashMap<>();
        options.put("dc1", "3");
        options.put("dc1.satellite.sat1", "3"); // Missing witness format
        options.put("primary", "dc1");

        testConfigurationException(options, "witness replicas using format");
    }

    @Test
    public void testReplicaCalculationWithSatellites() throws Exception
    {
        String cql = "CREATE KEYSPACE " + KEYSPACE + " WITH replication = {" +
                     "'class': 'SatelliteReplicationStrategy', " +
                     "'dc1': '3', " +
                     "'dc1.satellite.sat1': '3/3', " +
                     "'primary': 'dc1'" +
                     "} AND replication_type = 'tracked'";

        ClusterMetadataTestHelper.createKeyspace(cql);

        SatelliteReplicationStrategy strategy = getSRS(KEYSPACE);

        EndpointsForRange replicas = strategy.calculateNaturalReplicas(
            new LongToken(150), ClusterMetadata.current());

        // Should have 3 full replicas from dc1 + 3 satellite replicas from sat1
        assertEquals(6, replicas.size());

        int fullCount = 0;
        int witnessCount = 0;
        for (Replica replica : replicas)
        {
            if (replica.isFull())
                fullCount++;
            else
                witnessCount++;
        }

        assertEquals(3, fullCount);
        assertEquals(3, witnessCount);
    }

    private static ReplicaGroups readPlacement(String keyspace)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        KeyspaceMetadata ksm = metadata.schema.getKeyspaces().getNullable(keyspace);
        return metadata.placements.get(ksm.params.replication).reads;
    }

    @Test
    public void testDataPlacementCoversWholeRing() throws Exception
    {
        createDualDCKeyspace("dc1");

        ClusterMetadata metadata = ClusterMetadata.current();
        SatelliteReplicationStrategy strategy = getSRS(DUAL_DC_KEYSPACE);
        List<Range<Token>> ranges = metadata.tokenMap.toRanges();
        ReplicaGroups placement = readPlacement(DUAL_DC_KEYSPACE);

        assertEquals(ranges.size(), placement.size());
        for (Range<Token> range : ranges)
        {
            VersionedEndpoints.ForRange group = placement.forRange(range);
            assertNotNull("No replica group for range " + range + " in " + placement, group);
            assertEquals("Replica group for " + range + " is labelled with the wrong range", range, group.range());
            assertEquals("Mismatched replicas for " + range,
                         strategy.calculateNaturalReplicas(range.right, metadata).endpoints(),
                         group.get().endpoints());
        }
    }

    @Test
    public void testDataPlacementForTokenAtRingBoundaries() throws Exception
    {
        createDualDCKeyspace("dc1");

        ReplicaGroups placement = readPlacement(DUAL_DC_KEYSPACE);

        for (Token token : ClusterMetadata.current().tokenMap.tokens())
        {
            for (Token probe : List.of(token.decreaseSlightly(), token, token.increaseSlightly()))
                assertFalse("No replicas for token " + probe, placement.forToken(probe).get().isEmpty());
        }
    }

    @Test
    public void testDisableNonPrimaryDC() throws Exception
    {
        Map<String, String> options = new HashMap<>();
        options.put("dc1", "3");
        options.put("dc2", "3");
        options.put("dc2.disabled", "true");
        options.put("primary", "dc1");

        SatelliteReplicationStrategy strategy =
            new SatelliteReplicationStrategy(KEYSPACE, options, ReplicationType.tracked);

        assertTrue(strategy.isDisabled("dc2"));
        assertFalse(strategy.isDisabled("dc1"));
        assertEquals(1, strategy.getDisabledDatacenters().size());
        assertTrue(strategy.getDisabledDatacenters().contains("dc2"));

        // getDatacenters returns all DCs including disabled (config view)
        assertEquals(2, strategy.getDatacenters().size());
        assertTrue(strategy.getDatacenters().contains("dc2"));

        // getReplicationFactor(dc) returns configured RF even for disabled DCs
        assertEquals(3, strategy.getReplicationFactor("dc2").allReplicas);

        // aggregate RF includes all DCs (disabled does not affect placement)
        assertEquals(6, strategy.getReplicationFactor().allReplicas);
    }

    @Test
    public void testDisablePrimaryDCFails() throws Exception
    {
        Map<String, String> options = new HashMap<>();
        options.put("dc1", "3");
        options.put("dc2", "3");
        options.put("dc1.disabled", "true");
        options.put("primary", "dc1");

        testConfigurationException(options, "Primary datacenter 'dc1' cannot be disabled");
    }

    @Test
    public void testDisableNonExistentDCFails() throws Exception
    {
        Map<String, String> options = new HashMap<>();
        options.put("dc1", "3");
        options.put("dc99.disabled", "true");
        options.put("primary", "dc1");

        testConfigurationException(options, "not defined as a full datacenter");
    }

    @Test
    public void testDisabledDCSatelliteStillGetsReplicas() throws Exception
    {
        String cql = "CREATE KEYSPACE " + KEYSPACE + " WITH replication = {" +
                     "'class': 'SatelliteReplicationStrategy', " +
                     "'dc1': '3', " +
                     "'dc1.satellite.sat1': '3/3', " +
                     "'dc2': '3', " +
                     "'dc2.satellite.sat2': '3/3', " +
                     "'dc2.disabled': 'true', " +
                     "'primary': 'dc1'" +
                     "} AND replication_type = 'tracked'";

        ClusterMetadataTestHelper.createKeyspace(cql);

        SatelliteReplicationStrategy strategy = getSRS(KEYSPACE);

        EndpointsForRange replicas = strategy.calculateNaturalReplicas(
            new LongToken(150), ClusterMetadata.current());

        // Disabled does not affect placement — all DCs and satellites still get replicas
        // 3 full from dc1 + 3 full from dc2 + 3 witness from sat1 + 3 witness from sat2
        assertEquals(12, replicas.size());
    }

    @Test
    public void testDisabledInvalidValueFails() throws Exception
    {
        Map<String, String> options = new HashMap<>();
        options.put("dc1", "3");
        options.put("dc2", "3");
        options.put("dc2.disabled", "maybe");
        options.put("primary", "dc1");

        testConfigurationException(options, "expected 'true' or 'false'");
    }

    @Test
    public void testHasSameSettingsWithDisabled() throws Exception
    {
        Map<String, String> optionsA = new HashMap<>();
        optionsA.put("dc1", "3");
        optionsA.put("dc2", "3");
        optionsA.put("dc2.disabled", "true");
        optionsA.put("primary", "dc1");

        Map<String, String> optionsB = new HashMap<>();
        optionsB.put("dc1", "3");
        optionsB.put("dc2", "3");
        optionsB.put("dc2.disabled", "true");
        optionsB.put("primary", "dc1");

        Map<String, String> optionsC = new HashMap<>();
        optionsC.put("dc1", "3");
        optionsC.put("dc2", "3");
        optionsC.put("primary", "dc1");

        SatelliteReplicationStrategy strategyA =
            new SatelliteReplicationStrategy(KEYSPACE, optionsA, ReplicationType.tracked);
        SatelliteReplicationStrategy strategyB =
            new SatelliteReplicationStrategy(KEYSPACE, optionsB, ReplicationType.tracked);
        SatelliteReplicationStrategy strategyC =
            new SatelliteReplicationStrategy(KEYSPACE, optionsC, ReplicationType.tracked);

        assertTrue(strategyA.hasSameSettings(strategyB));
        assertFalse(strategyA.hasSameSettings(strategyC));
    }
}
