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

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

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

    @SuppressWarnings("deprecation")
    static final Set<ApplicationState> APPLICATION_STATE_SET =
    EnumSet.of(ApplicationState.STATUS, ApplicationState.LOAD, ApplicationState.SCHEMA, ApplicationState.DC,
               ApplicationState.RACK, ApplicationState.RELEASE_VERSION, ApplicationState.REMOVAL_COORDINATOR,
               ApplicationState.INTERNAL_IP, ApplicationState.RPC_ADDRESS, ApplicationState.SEVERITY,
               ApplicationState.NET_VERSION, ApplicationState.HOST_ID, ApplicationState.TOKENS,
               ApplicationState.RPC_READY, ApplicationState.INTERNAL_ADDRESS_AND_PORT,
               ApplicationState.NATIVE_ADDRESS_AND_PORT, ApplicationState.STATUS_WITH_PORT,
               ApplicationState.SSTABLE_VERSIONS);

    /**
     * Construct a new {@link GossipInfoTable} for the given {@code keyspace}.
     *
     * @param keyspace the name of the keyspace
     */
    GossipInfoTable(String keyspace)
    {
        super(buildTableMetadata(keyspace));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        for (Map.Entry<InetAddressAndPort, EndpointState> entry : Gossiper.instance.endpointStateMap.entrySet())
        {
            InetAddressAndPort endpoint = entry.getKey();
            EndpointState localState = entry.getValue();

            SimpleDataSet dataSet = result.row(endpoint.getAddress(), endpoint.getPort())
                                          .column(HOSTNAME, endpoint.getHostName())
                                          .column(GENERATION, getGeneration(localState))
                                          .column(HEARTBEAT, getHeartBeat(localState));

            APPLICATION_STATE_SET.forEach(applicationState -> {
                String state = applicationState.name().toLowerCase();
                if (!"tokens".equals(state))
                {
                    // do not a column for the ApplicationState.TOKENS value
                    dataSet.column(state, getValue(localState, applicationState));
                }
                dataSet.column(state + "_version", getVersion(localState, applicationState));
            });
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

    /**
     * Builds the {@link TableMetadata} to be provided to the superclass
     *
     * @param keyspace the name of the keyspace
     * @return the TableMetadata class
     */
    private static TableMetadata buildTableMetadata(String keyspace)
    {
        TableMetadata.Builder builder = TableMetadata.builder(keyspace, TABLE_NAME)
                                                     .comment(TABLE_COMMENT)
                                                     .kind(TableMetadata.Kind.VIRTUAL)
                                                     .partitioner(new LocalPartitioner(InetAddressType.instance))
                                                     .addPartitionKeyColumn(ADDRESS, InetAddressType.instance)
                                                     .addClusteringColumn(PORT, Int32Type.instance)
                                                     .addRegularColumn(HOSTNAME, UTF8Type.instance)
                                                     .addRegularColumn(GENERATION, Int32Type.instance)
                                                     .addRegularColumn(HEARTBEAT, Int32Type.instance);

        APPLICATION_STATE_SET.stream()
                             .map(Enum::name)
                             .map(String::toLowerCase)
                             .forEach(state -> {
                                 if (!"tokens".equals(state))
                                 {
                                     // do not a column for the ApplicationState.TOKENS value
                                     builder.addRegularColumn(state, UTF8Type.instance);
                                 }
                                 builder.addRegularColumn(state + "_version", Int32Type.instance);
                             });

        return builder.build();
    }
}
