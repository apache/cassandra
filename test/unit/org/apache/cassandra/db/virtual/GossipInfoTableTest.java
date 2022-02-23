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

package org.apache.cassandra.db.virtual;

import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;

import static org.assertj.core.api.Assertions.assertThat;

public class GossipInfoTableTest extends CQLTester
{
    private static final String KS_NAME = "vts";

    @SuppressWarnings("FieldCanBeLocal")
    private GossipInfoTable table;

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
    }

    @Before
    public void config()
    {
        table = new GossipInfoTable(KS_NAME);
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));
    }

    @Test
    public void testSelectAllWhenGossipInfoIsEmpty() throws Throwable
    {
        assertEmpty(execute("SELECT * FROM vts.gossip_info"));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testSelectAllWithStateTransitions() throws Throwable
    {
        try
        {
            requireNetwork(); // triggers gossiper

            UntypedResultSet resultSet = execute("SELECT * FROM vts.gossip_info");

            assertThat(resultSet.size()).isEqualTo(1);
            assertThat(Gossiper.instance.endpointStateMap.size()).isEqualTo(1);

            Optional<Map.Entry<InetAddressAndPort, EndpointState>> entry = Gossiper.instance.endpointStateMap.entrySet()
                                                                                                             .stream()
                                                                                                             .findFirst();
            assertThat(entry).isNotEmpty();

            UntypedResultSet.Row row = resultSet.one();
            assertThat(row.getColumns().size()).isEqualTo(40);

            InetAddressAndPort endpoint = entry.get().getKey();
            EndpointState localState = entry.get().getValue();

            assertThat(endpoint).isNotNull();
            assertThat(localState).isNotNull();
            assertThat(row.getInetAddress("address")).isEqualTo(endpoint.getAddress());
            assertThat(row.getInt("port")).isEqualTo(endpoint.getPort());
            assertThat(row.getString("hostname")).isEqualTo(endpoint.getHostName());
            assertThat(row.getInt("generation")).isEqualTo(localState.getHeartBeatState().getGeneration());

            assertValue(row, "status", localState, ApplicationState.STATUS);
            assertValue(row, "load", localState, ApplicationState.LOAD);
            assertValue(row, "schema", localState, ApplicationState.SCHEMA);
            assertValue(row, "dc", localState, ApplicationState.DC);
            assertValue(row, "rack", localState, ApplicationState.RACK);
            assertValue(row, "release_version", localState, ApplicationState.RELEASE_VERSION);
            assertValue(row, "removal_coordinator", localState, ApplicationState.REMOVAL_COORDINATOR);
            assertValue(row, "internal_ip", localState, ApplicationState.INTERNAL_IP);
            assertValue(row, "rpc_address", localState, ApplicationState.RPC_ADDRESS);
            assertValue(row, "severity", localState, ApplicationState.SEVERITY);
            assertValue(row, "net_version", localState, ApplicationState.NET_VERSION);
            assertValue(row, "host_id", localState, ApplicationState.HOST_ID);
            assertValue(row, "rpc_ready", localState, ApplicationState.RPC_READY);
            assertValue(row, "internal_address_and_port", localState, ApplicationState.INTERNAL_ADDRESS_AND_PORT);
            assertValue(row, "native_address_and_port", localState, ApplicationState.NATIVE_ADDRESS_AND_PORT);
            assertValue(row, "status_with_port", localState, ApplicationState.STATUS_WITH_PORT);
            assertValue(row, "sstable_versions", localState, ApplicationState.SSTABLE_VERSIONS);

            assertVersion(row, "status_version", localState, ApplicationState.STATUS);
            assertVersion(row, "load_version", localState, ApplicationState.LOAD);
            assertVersion(row, "schema_version", localState, ApplicationState.SCHEMA);
            assertVersion(row, "dc_version", localState, ApplicationState.DC);
            assertVersion(row, "rack_version", localState, ApplicationState.RACK);
            assertVersion(row, "release_version_version", localState, ApplicationState.RELEASE_VERSION);
            assertVersion(row, "removal_coordinator_version", localState, ApplicationState.REMOVAL_COORDINATOR);
            assertVersion(row, "internal_ip_version", localState, ApplicationState.INTERNAL_IP);
            assertVersion(row, "rpc_address_version", localState, ApplicationState.RPC_ADDRESS);
            assertVersion(row, "severity_version", localState, ApplicationState.SEVERITY);
            assertVersion(row, "net_version_version", localState, ApplicationState.NET_VERSION);
            assertVersion(row, "host_id_version", localState, ApplicationState.HOST_ID);
            assertVersion(row, "tokens_version", localState, ApplicationState.TOKENS);
            assertVersion(row, "rpc_ready_version", localState, ApplicationState.RPC_READY);
            assertVersion(row, "internal_address_and_port_version", localState, ApplicationState.INTERNAL_ADDRESS_AND_PORT);
            assertVersion(row, "native_address_and_port_version", localState, ApplicationState.NATIVE_ADDRESS_AND_PORT);
            assertVersion(row, "status_with_port_version", localState, ApplicationState.STATUS_WITH_PORT);
            assertVersion(row, "sstable_versions_version", localState, ApplicationState.SSTABLE_VERSIONS);
        }
        finally
        {
            // clean up the gossip states
            Gossiper.instance.clearUnsafe();
        }
    }

    private void assertValue(UntypedResultSet.Row row, String column, EndpointState localState, ApplicationState key)
    {
        if (row.has(column))
        {
            assertThat(localState.getApplicationState(key)).as("'%s' is expected to be not-null", key)
                                                           .isNotNull();
            assertThat(row.getString(column)).as("'%s' is expected to match column '%s'", key, column)
                                             .isEqualTo(localState.getApplicationState(key).value);
        }
        else
        {
            assertThat(localState.getApplicationState(key)).as("'%s' is expected to be null", key)
                                                           .isNull();
        }
    }

    private void assertVersion(UntypedResultSet.Row row, String column, EndpointState localState, ApplicationState key)
    {
        if (row.has(column))
        {
            assertThat(localState.getApplicationState(key)).as("'%s' is expected to be not-null", key)
                                                           .isNotNull();
            assertThat(row.getInt(column)).as("'%s' is expected to match column '%s'", key, column)
                                          .isEqualTo(localState.getApplicationState(key).version);
        }
        else
        {
            assertThat(localState.getApplicationState(key)).as("'%s' is expected to be null", key)
                                                           .isNull();
        }
    }
}
