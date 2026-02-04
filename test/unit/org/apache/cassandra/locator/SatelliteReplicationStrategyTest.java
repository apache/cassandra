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

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.CassandraTestBase;
import org.apache.cassandra.CassandraTestBase.UseMurmur3Partitioner;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.Location;

import static org.apache.cassandra.CassandraTestBase.DisableMBeanRegistration;
import static org.apache.cassandra.CassandraTestBase.PrepareServerNoRegister;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@PrepareServerNoRegister
@DisableMBeanRegistration
@UseMurmur3Partitioner
public class SatelliteReplicationStrategyTest extends CassandraTestBase
{
    private static final String KEYSPACE = "test";

    @After
    public void teardown()
    {
        ServerTestUtils.resetCMS();
    }

    private void addToken(long token, String address, Location location) throws UnknownHostException
    {
        InetAddressAndPort addr = InetAddressAndPort.getByName(address);
        ClusterMetadataTestHelper.addEndpoint(addr, new LongToken(token), location);
    }

    private void setupDCs() throws UnknownHostException
    {
        Location dc1 = new Location("dc1", "rack1");
        Location dc2 = new Location("dc2", "rack1");
        Location sat1 = new Location("sat1", "rack1");
        Location sat2 = new Location("sat2", "rack1");

        // DC1
        addToken(100, "10.0.0.10", dc1);
        addToken(200, "10.0.0.11", dc1);
        addToken(300, "10.0.0.12", dc1);

        // DC2
        addToken(400, "10.1.0.10", dc2);
        addToken(500, "10.1.0.11", dc2);
        addToken(600, "10.1.0.12", dc2);

        // SAT1
        addToken(700, "10.2.0.10", sat1);
        addToken(800, "10.2.0.11", sat1);

        // SAT2
        addToken(900, "10.3.0.10", sat2);
        addToken(1000, "10.3.0.11", sat2);
    }

    private static SatelliteReplicationStrategy getSRS(String keyspace)
    {
        KeyspaceMetadata ksm = ClusterMetadata.current().schema.getKeyspaces().getNullable(keyspace);
        return (SatelliteReplicationStrategy) ksm.replicationStrategy;
    }

    @Test
    public void testValidSingleDCWithSatellite() throws Exception
    {
        setupDCs();

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
        setupDCs();

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

    private void testConfigurationException(Map<String, String> options, String messageContains) throws UnknownHostException
    {
        setupDCs();

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
        setupDCs();

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
        setupDCs();

        String cql = "CREATE KEYSPACE " + KEYSPACE + " WITH replication = {" +
                     "'class': 'SatelliteReplicationStrategy', " +
                     "'dc1': '3', " +
                     "'dc1.satellite.sat1': '2/2', " +
                     "'primary': 'dc1'" +
                     "} AND replication_type = 'tracked'";

        ClusterMetadataTestHelper.createKeyspace(cql);

        SatelliteReplicationStrategy strategy = getSRS(KEYSPACE);

        EndpointsForRange replicas = strategy.calculateNaturalReplicas(
            new LongToken(150), ClusterMetadata.current());

        // Should have 3 full replicas from dc1 + 2 satellite replicas from sat1
        assertEquals(5, replicas.size());

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
        assertEquals(2, witnessCount);
    }

    @Test
    public void testDisableNonPrimaryDC() throws Exception
    {
        setupDCs();

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
        setupDCs();

        String cql = "CREATE KEYSPACE " + KEYSPACE + " WITH replication = {" +
                     "'class': 'SatelliteReplicationStrategy', " +
                     "'dc1': '3', " +
                     "'dc1.satellite.sat1': '2/2', " +
                     "'dc2': '3', " +
                     "'dc2.satellite.sat2': '2/2', " +
                     "'dc2.disabled': 'true', " +
                     "'primary': 'dc1'" +
                     "} AND replication_type = 'tracked'";

        ClusterMetadataTestHelper.createKeyspace(cql);

        SatelliteReplicationStrategy strategy = getSRS(KEYSPACE);

        EndpointsForRange replicas = strategy.calculateNaturalReplicas(
            new LongToken(150), ClusterMetadata.current());

        // Disabled does not affect placement — all DCs and satellites still get replicas
        // 3 full from dc1 + 3 full from dc2 + 2 witness from sat1 + 2 witness from sat2
        assertEquals(10, replicas.size());
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
        setupDCs();

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
