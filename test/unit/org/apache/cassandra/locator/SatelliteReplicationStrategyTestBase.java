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
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.Location;

import static org.apache.cassandra.config.CassandraRelevantProperties.ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION;

public abstract class SatelliteReplicationStrategyTestBase
{
    protected static final String KEYSPACE = "test";
    protected static final TableId TABLE_ID = TableId.generate();
    protected static final String DUAL_DC_KEYSPACE = "dual_dc_test";
    protected static final String SINGLE_DC_KEYSPACE = "single_dc_test";
    protected static final String DISABLED_DC_KEYSPACE = "disabled_dc_test";

    @BeforeClass
    public static void setUpClass()
    {
        ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION.setBoolean(true);
        ServerTestUtils.daemonInitialization();
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
        DatabaseDescriptor.setPaxosVariant(Config.PaxosVariant.v2);
        ServerTestUtils.prepareServerNoRegister();
    }

    @Before
    public void setup() throws UnknownHostException
    {
        setupDCs();
    }

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
        addToken(900, "10.2.0.12", sat1);

        // SAT2
        addToken(1000, "10.3.0.10", sat2);
        addToken(1100, "10.3.0.11", sat2);
        addToken(1200, "10.3.0.12", sat2);
    }

    protected static SatelliteReplicationStrategy getSRS(String keyspace)
    {
        KeyspaceMetadata ksm = ClusterMetadata.current().schema.getKeyspaces().getNullable(keyspace);
        return (SatelliteReplicationStrategy) ksm.replicationStrategy;
    }

    protected void createDualDCKeyspace(String primary) throws Exception
    {
        String cql = "CREATE KEYSPACE " + DUAL_DC_KEYSPACE + " WITH replication = {" +
                     "'class': 'SatelliteReplicationStrategy', " +
                     "'dc1': '3', " +
                     "'dc1.satellite.sat1': '3/3', " +
                     "'dc2': '3', " +
                     "'dc2.satellite.sat2': '3/3', " +
                     "'primary': '" + primary + "'" +
                     "} AND replication_type = 'tracked'";
        ClusterMetadataTestHelper.createKeyspace(cql);
    }

    protected void createSingleDCKeyspace() throws Exception
    {
        String cql = "CREATE KEYSPACE " + SINGLE_DC_KEYSPACE + " WITH replication = {" +
                     "'class': 'SatelliteReplicationStrategy', " +
                     "'dc1': '3', " +
                     "'dc1.satellite.sat1': '3/3', " +
                     "'primary': 'dc1'" +
                     "} AND replication_type = 'tracked'";
        ClusterMetadataTestHelper.createKeyspace(cql);
    }

    protected void createDisabledDCKeyspace() throws Exception
    {
        String cql = "CREATE KEYSPACE " + DISABLED_DC_KEYSPACE + " WITH replication = {" +
                     "'class': 'SatelliteReplicationStrategy', " +
                     "'dc1': '3', " +
                     "'dc1.satellite.sat1': '3/3', " +
                     "'dc2': '3', " +
                     "'dc2.satellite.sat2': '3/3', " +
                     "'dc2.disabled': 'true', " +
                     "'primary': 'dc1'" +
                     "} AND replication_type = 'tracked'";
        ClusterMetadataTestHelper.createKeyspace(cql);
    }

    protected Set<String> replicaDCs(Iterable<Replica> replicas, ClusterMetadata metadata)
    {
        Set<String> dcs = new HashSet<>();
        for (Replica r : replicas)
            dcs.add(metadata.locator.location(r.endpoint()).datacenter);
        return dcs;
    }

    protected Set<InetAddressAndPort> replicasInDC(Iterable<Replica> replicas, String dc, ClusterMetadata metadata)
    {
        Set<InetAddressAndPort> eps = new HashSet<>();
        for (Replica r : replicas)
            if (metadata.locator.location(r.endpoint()).datacenter.equals(dc))
                eps.add(r.endpoint());
        return eps;
    }
}
