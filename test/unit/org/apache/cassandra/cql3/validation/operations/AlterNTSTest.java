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

package org.apache.cassandra.cql3.validation.operations;

import java.util.List;
import java.util.UUID;

import org.junit.Test;

import com.datastax.driver.core.PreparedStatement;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.StorageService;
import org.assertj.core.api.Assertions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class AlterNTSTest extends CQLTester
{
    @Test
    public void testDropColumnAsPreparedStatement() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (key int PRIMARY KEY, value int);");

        PreparedStatement prepared = sessionNet().prepare("ALTER TABLE " + KEYSPACE + "." + table + " DROP value;");

        executeNet("INSERT INTO %s (key, value) VALUES (1, 1)");
        assertRowsNet(executeNet("SELECT * FROM %s"), row(1, 1));

        sessionNet().execute(prepared.bind());

        executeNet("ALTER TABLE %s ADD value int");

        assertRows(execute("SELECT * FROM %s"), row(1, null));
    }

    @Test
    public void testCreateAlterKeyspacesRFWarnings() throws Throwable
    {
        requireNetwork();

        // NTS
        ClientWarn.instance.captureWarnings();
        String ks = createKeyspace("CREATE KEYSPACE %s WITH replication = {'class' : 'NetworkTopologyStrategy', '" + DATA_CENTER + "' : 3 }");
        List<String> warnings = ClientWarn.instance.getWarnings();
        assertEquals(1, warnings.size());
        Assertions.assertThat(warnings.get(0)).contains("Your replication factor 3 for keyspace " + ks + " is higher than the number of nodes 1 for datacenter " + DATA_CENTER);

        ClientWarn.instance.captureWarnings();
        execute("CREATE TABLE " + ks + ".t (k int PRIMARY KEY, v int)");
        warnings = ClientWarn.instance.getWarnings();
        assertNull(warnings);

        ClientWarn.instance.captureWarnings();
        execute("ALTER KEYSPACE " + ks + " WITH replication = {'class' : 'NetworkTopologyStrategy', '" + DATA_CENTER + "' : 2 }");
        warnings = ClientWarn.instance.getWarnings();
        assertEquals(1, warnings.size());
        Assertions.assertThat(warnings.get(0)).contains("Your replication factor 2 for keyspace " + ks + " is higher than the number of nodes 1 for datacenter " + DATA_CENTER);

        ClientWarn.instance.captureWarnings();
        execute("ALTER KEYSPACE " + ks + " WITH replication = {'class' : 'NetworkTopologyStrategy', '" + DATA_CENTER + "' : 1 }");
        warnings = ClientWarn.instance.getWarnings();
        assertNull(warnings);

        // SimpleStrategy
        ClientWarn.instance.captureWarnings();
        ks = createKeyspace("CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");
        warnings = ClientWarn.instance.getWarnings();
        assertEquals(1, warnings.size());
        Assertions.assertThat(warnings.get(0)).contains("Your replication factor 3 for keyspace " + ks + " is higher than the number of nodes 1");

        ClientWarn.instance.captureWarnings();
        execute("CREATE TABLE " + ks + ".t (k int PRIMARY KEY, v int)");
        warnings = ClientWarn.instance.getWarnings();
        assertNull(warnings);

        ClientWarn.instance.captureWarnings();
        execute("ALTER KEYSPACE " + ks + " WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 }");
        warnings = ClientWarn.instance.getWarnings();
        assertEquals(1, warnings.size());
        Assertions.assertThat(warnings.get(0)).contains("Your replication factor 2 for keyspace " + ks + " is higher than the number of nodes 1");

        ClientWarn.instance.captureWarnings();
        execute("ALTER KEYSPACE " + ks + " WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        warnings = ClientWarn.instance.getWarnings();
        assertNull(warnings);
    }

    @Test
    public void testAlterKeyspaceSystem_AuthWithNTSOnlyAcceptsConfiguredDataCenterNames() throws Throwable
    {
        requireAuthentication();

        // Add a peer
        StorageService.instance.getTokenMetadata().updateHostId(UUID.randomUUID(), InetAddressAndPort.getByName("127.0.0.2"));

        // Register an Endpoint snitch which returns fixed value for data center.
        DatabaseDescriptor.setEndpointSnitch(new IEndpointSnitch()
        {
            public String getRack(InetAddressAndPort endpoint) { return RACK1; }
            public String getDatacenter(InetAddressAndPort endpoint)
            {
                if(endpoint.getHostAddress(false).equalsIgnoreCase("127.0.0.2"))
                    return "datacenter2";
                return DATA_CENTER;
            }
            public <C extends ReplicaCollection<? extends C>> C sortedByProximity(InetAddressAndPort address, C addresses)
            {
                return null;
            }

            public int compareEndpoints(InetAddressAndPort target, Replica r1, Replica r2)
            {
                return 0;
            }

            // NOOP
            public void gossiperStarting() { }

            public boolean isWorthMergingForRangeQuery(ReplicaCollection<?> merged, ReplicaCollection<?> l1, ReplicaCollection<?> l2) { return false; }
        });

        // try modifying the system_auth keyspace without second DC which has active node.
        assertInvalidThrow(ConfigurationException.class, "ALTER KEYSPACE system_auth WITH replication = { 'class' : 'NetworkTopologyStrategy', '" + DATA_CENTER + "' : 2 }");
        execute("ALTER KEYSPACE " + SchemaConstants.AUTH_KEYSPACE_NAME + " WITH replication = {'class' : 'NetworkTopologyStrategy', '" + DATA_CENTER + "' : 1 , '" + DATA_CENTER_REMOTE + "' : 1 }");
    }
}
