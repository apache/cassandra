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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import org.awaitility.Awaitility;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;

import static com.google.common.collect.ImmutableList.of;
import static org.assertj.core.api.Assertions.assertThat;

public class GossipInfoTableTest extends CQLTester
{
    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
    }

    @Test
    public void testSelectAllWhenGossipInfoIsEmpty() throws Throwable
    {
        // we have not triggered gossiper yet
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace("vts_1",
                                                                      of(new GossipInfoTable("vts_1", HashMap::new))));
        assertEmpty(execute("SELECT * FROM vts_1.gossip_info"));
    }

    @Test
    public void testSelectAllWithStateTransitions() throws Throwable
    {
        try
        {
            requireNetwork(); // triggers gossiper

            ConcurrentMap<InetAddressAndPort, EndpointState> states = Gossiper.instance.endpointStateMap;
            Awaitility.await().until(() -> !states.isEmpty());
            Map.Entry<InetAddressAndPort, EndpointState> entry = states.entrySet().stream().findFirst()
                    .orElseThrow(AssertionError::new);
            InetAddressAndPort endpoint = entry.getKey();
            EndpointState localState = new EndpointState(entry.getValue());

            Supplier<Map<InetAddressAndPort, EndpointState>> endpointStateMapSupplier = () -> new HashMap<InetAddressAndPort, EndpointState>() {{put(endpoint, localState);}};

            VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace("vts_2",
                                                                          of(new GossipInfoTable("vts_2", endpointStateMapSupplier))));

            UntypedResultSet resultSet = execute("SELECT * FROM vts_2.gossip_info");

            assertThat(resultSet.size()).isEqualTo(1);
            UntypedResultSet.Row row = resultSet.one();
            assertThat(row.getColumns().size()).isEqualTo(66);

            assertThat(endpoint).isNotNull();
            assertThat(localState).isNotNull();
            assertThat(row.getInetAddress("address")).isEqualTo(endpoint.getAddress());
            assertThat(row.getInt("port")).isEqualTo(endpoint.getPort());
            assertThat(row.getString("hostname")).isEqualTo(endpoint.getHostName());
            assertThat(row.getInt("generation")).isEqualTo(localState.getHeartBeatState().getGeneration());
            assertThat(row.getInt("heartbeat")).isNotNull();

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
            assertValue(row, "disk_usage", localState, ApplicationState.DISK_USAGE);
            assertValue(row, "x_11_padding", localState, ApplicationState.X_11_PADDING);
            assertValue(row, "x1", localState, ApplicationState.X1);
            assertValue(row, "x2", localState, ApplicationState.X2);
            assertValue(row, "x3", localState, ApplicationState.X3);
            assertValue(row, "x4", localState, ApplicationState.X4);
            assertValue(row, "x5", localState, ApplicationState.X5);
            assertValue(row, "x6", localState, ApplicationState.X6);
            assertValue(row, "x7", localState, ApplicationState.X7);
            assertValue(row, "x8", localState, ApplicationState.X8);
            assertValue(row, "x9", localState, ApplicationState.X9);
            assertValue(row, "x10", localState, ApplicationState.X10);

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
            assertVersion(row, "disk_usage_version", localState, ApplicationState.DISK_USAGE);
            assertVersion(row, "x_11_padding", localState, ApplicationState.X_11_PADDING);
            assertVersion(row, "x1", localState, ApplicationState.X1);
            assertVersion(row, "x2", localState, ApplicationState.X2);
            assertVersion(row, "x3", localState, ApplicationState.X3);
            assertVersion(row, "x4", localState, ApplicationState.X4);
            assertVersion(row, "x5", localState, ApplicationState.X5);
            assertVersion(row, "x6", localState, ApplicationState.X6);
            assertVersion(row, "x7", localState, ApplicationState.X7);
            assertVersion(row, "x8", localState, ApplicationState.X8);
            assertVersion(row, "x9", localState, ApplicationState.X9);
            assertVersion(row, "x10", localState, ApplicationState.X10);
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
            String tableString = row.getString(column);
            String stateString = localState.getApplicationState(key).value;
            assertThat(tableString).as("'%s' is expected to match column '%s', table string: %s, state string: %s",
                                       key, column, tableString, stateString).isEqualTo(stateString);
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

            int tableVersion = row.getInt(column);
            int stateVersion = localState.getApplicationState(key).version;

            assertThat(tableVersion).as("'%s' is expected to match column '%s', table int: %s, state int: %s",
                                        key, column, tableVersion, stateVersion).isEqualTo(stateVersion);
        }
        else
        {
            assertThat(localState.getApplicationState(key)).as("'%s' is expected to be null", key)
                                                           .isNull();
        }
    }
}
