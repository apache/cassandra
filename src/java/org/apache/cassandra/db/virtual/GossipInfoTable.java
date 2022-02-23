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

import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.TableMetadata;

/**
 * A {@link VirtualTable} that return the Gossip information in tabular format.
 */
final class GossipInfoTable extends AbstractVirtualTable
{
    static final String TABLE_NAME = "gossip_info";
    static final String TABLE_COMMENT = "lists the gossip information for the cluster";

    static final String ADDRESS = "address";
    static final String PORT = "port";
    static final String HOSTNAME = "hostname";
    static final String GENERATION = "generation";
    static final String HEARTBEAT = "heartbeat";

    // "value" fields coming from @org.apache.cassandra.gms.ApplicationState
    static final String STATUS = "status";
    static final String LOAD = "load";
    static final String SCHEMA = "schema";
    static final String DC = "dc";
    static final String RACK = "rack";
    static final String RELEASE_VERSION = "release_version";
    static final String REMOVAL_COORDINATOR = "removal_coordinator";
    static final String INTERNAL_IP = "internal_ip";
    static final String RPC_ADDRESS = "rpc_address";
    static final String SEVERITY = "severity";
    static final String NET_VERSION = "net_version";
    static final String HOST_ID = "host_id";
    static final String RPC_READY = "rpc_ready";
    static final String INTERNAL_ADDRESS_AND_PORT = "internal_address_and_port";
    static final String NATIVE_ADDRESS_AND_PORT = "native_address_and_port";
    static final String STATUS_WITH_PORT = "status_with_port";
    static final String SSTABLE_VERSIONS = "sstable_versions";

    // "version" fields coming from @org.apache.cassandra.gms.ApplicationState
    static final String STATUS_VERSION = "status_version";
    static final String LOAD_VERSION = "load_version";
    static final String SCHEMA_VERSION = "schema_version";
    static final String DC_VERSION = "dc_version";
    static final String RACK_VERSION = "rack_version";
    static final String RELEASE_VERSION_VERSION = "release_version_version";
    static final String REMOVAL_COORDINATOR_VERSION = "removal_coordinator_version";
    static final String INTERNAL_IP_VERSION = "internal_ip_version";
    static final String RPC_ADDRESS_VERSION = "rpc_address_version";
    static final String SEVERITY_VERSION = "severity_version";
    static final String NET_VERSION_VERSION = "net_version_version";
    static final String HOST_ID_VERSION = "host_id_version";
    static final String TOKENS_VERSION = "tokens_version";
    static final String RPC_READY_VERSION = "rpc_ready_version";
    static final String INTERNAL_ADDRESS_AND_PORT_VERSION = "internal_address_and_port_version";
    static final String NATIVE_ADDRESS_AND_PORT_VERSION = "native_address_and_port_version";
    static final String STATUS_WITH_PORT_VERSION = "status_with_port_version";
    static final String SSTABLE_VERSIONS_VERSION = "sstable_versions_version";

    /**
     * Construct a new {@link GossipInfoTable} for the given {@code keyspace}.
     *
     * @param keyspace the name of the keyspace
     */
    GossipInfoTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, TABLE_NAME)
                           .comment(TABLE_COMMENT)
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(InetAddressType.instance))
                           .addPartitionKeyColumn(ADDRESS, InetAddressType.instance)
                           .addClusteringColumn(PORT, Int32Type.instance)
                           .addRegularColumn(HOSTNAME, UTF8Type.instance)
                           .addRegularColumn(GENERATION, Int32Type.instance)
                           .addRegularColumn(HEARTBEAT, Int32Type.instance)
                           .addRegularColumn(STATUS, UTF8Type.instance)
                           .addRegularColumn(LOAD, UTF8Type.instance)
                           .addRegularColumn(SCHEMA, UTF8Type.instance)
                           .addRegularColumn(DC, UTF8Type.instance)
                           .addRegularColumn(RACK, UTF8Type.instance)
                           .addRegularColumn(RELEASE_VERSION, UTF8Type.instance)
                           .addRegularColumn(REMOVAL_COORDINATOR, UTF8Type.instance)
                           .addRegularColumn(INTERNAL_IP, UTF8Type.instance)
                           .addRegularColumn(RPC_ADDRESS, UTF8Type.instance)
                           .addRegularColumn(SEVERITY, UTF8Type.instance)
                           .addRegularColumn(NET_VERSION, UTF8Type.instance)
                           .addRegularColumn(HOST_ID, UTF8Type.instance)
                           .addRegularColumn(RPC_READY, UTF8Type.instance)
                           .addRegularColumn(INTERNAL_ADDRESS_AND_PORT, UTF8Type.instance)
                           .addRegularColumn(NATIVE_ADDRESS_AND_PORT, UTF8Type.instance)
                           .addRegularColumn(STATUS_WITH_PORT, UTF8Type.instance)
                           .addRegularColumn(SSTABLE_VERSIONS, UTF8Type.instance)
                           .addRegularColumn(STATUS_VERSION, Int32Type.instance)
                           .addRegularColumn(LOAD_VERSION, Int32Type.instance)
                           .addRegularColumn(SCHEMA_VERSION, Int32Type.instance)
                           .addRegularColumn(DC_VERSION, Int32Type.instance)
                           .addRegularColumn(RACK_VERSION, Int32Type.instance)
                           .addRegularColumn(RELEASE_VERSION_VERSION, Int32Type.instance)
                           .addRegularColumn(REMOVAL_COORDINATOR_VERSION, Int32Type.instance)
                           .addRegularColumn(INTERNAL_IP_VERSION, Int32Type.instance)
                           .addRegularColumn(RPC_ADDRESS_VERSION, Int32Type.instance)
                           .addRegularColumn(SEVERITY_VERSION, Int32Type.instance)
                           .addRegularColumn(NET_VERSION_VERSION, Int32Type.instance)
                           .addRegularColumn(HOST_ID_VERSION, Int32Type.instance)
                           .addRegularColumn(TOKENS_VERSION, Int32Type.instance)
                           .addRegularColumn(RPC_READY_VERSION, Int32Type.instance)
                           .addRegularColumn(INTERNAL_ADDRESS_AND_PORT_VERSION, Int32Type.instance)
                           .addRegularColumn(NATIVE_ADDRESS_AND_PORT_VERSION, Int32Type.instance)
                           .addRegularColumn(STATUS_WITH_PORT_VERSION, Int32Type.instance)
                           .addRegularColumn(SSTABLE_VERSIONS_VERSION, Int32Type.instance)
                           .build());
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("deprecation")
    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        for (Map.Entry<InetAddressAndPort, EndpointState> entry : Gossiper.instance.endpointStateMap.entrySet())
        {
            InetAddressAndPort endpoint = entry.getKey();
            EndpointState localState = entry.getValue();

            result.row(endpoint.getAddress(), endpoint.getPort())
                  .column(HOSTNAME, endpoint.getHostName())
                  .column(GENERATION, getGeneration(localState))
                  .column(HEARTBEAT, getHeartBeat(localState))
                  .column(STATUS, getValue(localState, ApplicationState.STATUS))
                  .column(LOAD, getValue(localState, ApplicationState.LOAD))
                  .column(SCHEMA, getValue(localState, ApplicationState.SCHEMA))
                  .column(DC, getValue(localState, ApplicationState.DC))
                  .column(RACK, getValue(localState, ApplicationState.RACK))
                  .column(RELEASE_VERSION, getValue(localState, ApplicationState.RELEASE_VERSION))
                  .column(REMOVAL_COORDINATOR, getValue(localState, ApplicationState.REMOVAL_COORDINATOR))
                  .column(INTERNAL_IP, getValue(localState, ApplicationState.INTERNAL_IP))
                  .column(RPC_ADDRESS, getValue(localState, ApplicationState.RPC_ADDRESS))
                  .column(SEVERITY, getValue(localState, ApplicationState.SEVERITY))
                  .column(NET_VERSION, getValue(localState, ApplicationState.NET_VERSION))
                  .column(HOST_ID, getValue(localState, ApplicationState.HOST_ID))
                  .column(RPC_READY, getValue(localState, ApplicationState.RPC_READY))
                  .column(INTERNAL_ADDRESS_AND_PORT, getValue(localState, ApplicationState.INTERNAL_ADDRESS_AND_PORT))
                  .column(NATIVE_ADDRESS_AND_PORT, getValue(localState, ApplicationState.NATIVE_ADDRESS_AND_PORT))
                  .column(STATUS_WITH_PORT, getValue(localState, ApplicationState.STATUS_WITH_PORT))
                  .column(SSTABLE_VERSIONS, getValue(localState, ApplicationState.SSTABLE_VERSIONS))
                  .column(STATUS_VERSION, getVersion(localState, ApplicationState.STATUS))
                  .column(LOAD_VERSION, getVersion(localState, ApplicationState.LOAD))
                  .column(SCHEMA_VERSION, getVersion(localState, ApplicationState.SCHEMA))
                  .column(DC_VERSION, getVersion(localState, ApplicationState.DC))
                  .column(RACK_VERSION, getVersion(localState, ApplicationState.RACK))
                  .column(RELEASE_VERSION_VERSION, getVersion(localState, ApplicationState.RELEASE_VERSION))
                  .column(REMOVAL_COORDINATOR_VERSION, getVersion(localState, ApplicationState.REMOVAL_COORDINATOR))
                  .column(INTERNAL_IP_VERSION, getVersion(localState, ApplicationState.INTERNAL_IP))
                  .column(RPC_ADDRESS_VERSION, getVersion(localState, ApplicationState.RPC_ADDRESS))
                  .column(SEVERITY_VERSION, getVersion(localState, ApplicationState.SEVERITY))
                  .column(NET_VERSION_VERSION, getVersion(localState, ApplicationState.NET_VERSION))
                  .column(HOST_ID_VERSION, getVersion(localState, ApplicationState.HOST_ID))
                  .column(TOKENS_VERSION, getVersion(localState, ApplicationState.TOKENS))
                  .column(RPC_READY_VERSION, getVersion(localState, ApplicationState.RPC_READY))
                  .column(INTERNAL_ADDRESS_AND_PORT_VERSION, getVersion(localState, ApplicationState.INTERNAL_ADDRESS_AND_PORT))
                  .column(NATIVE_ADDRESS_AND_PORT_VERSION, getVersion(localState, ApplicationState.NATIVE_ADDRESS_AND_PORT))
                  .column(STATUS_WITH_PORT_VERSION, getVersion(localState, ApplicationState.STATUS_WITH_PORT))
                  .column(SSTABLE_VERSIONS_VERSION, getVersion(localState, ApplicationState.SSTABLE_VERSIONS));
        }
        return result;
    }

    /**
     * Return the heartbeat generation of a given {@link EndpointState} or null if {@code localState} is null.
     *
     * @param localState a nullable endpoint state
     * @return the heartbeat generation if available, null otherwise
     */
    private Integer getGeneration(EndpointState localState)
    {
        return localState == null ? null : localState.getHeartBeatState().getGeneration();
    }

    /**
     * Return the heartbeat version of a given {@link EndpointState} or null if {@code localState} is null.
     *
     * @param localState a nullable endpoint state
     * @return the heartbeat version if available, null otherwise
     */
    private Integer getHeartBeat(EndpointState localState)
    {
        return localState == null ? null : localState.getHeartBeatState().getHeartBeatVersion();
    }

    /**
     * Returns the value from the {@link VersionedValue} of a given {@link ApplicationState key}, or null
     * if {@code localState} is null or the {@link VersionedValue} does not exist in the {@link ApplicationState}.
     *
     * @param localState a nullable endpoint state
     * @param key        the key to the application state
     * @return the value, or null if not available
     */
    private String getValue(EndpointState localState, ApplicationState key)
    {
        VersionedValue value;
        return localState == null || (value = localState.getApplicationState(key)) == null ? null : value.value;
    }

    /**
     * Returns the version from the {@link VersionedValue} of a given {@link ApplicationState key}, or null
     * if {@code localState} is null or the {@link VersionedValue} does not exist in the {@link ApplicationState}.
     *
     * @param localState a nullable endpoint state
     * @param key        the key to the application state
     * @return the version, or null if not available
     */
    private Integer getVersion(EndpointState localState, ApplicationState key)
    {
        VersionedValue value;
        return localState == null || (value = localState.getApplicationState(key)) == null ? null : value.version;
    }
}
