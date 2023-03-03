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

import java.util.HashSet;
import java.util.stream.Collectors;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.FBUtilities.getBroadcastAddressAndPort;

public class LocalTable extends AbstractVirtualTable
{
    public static final String BOOTSTRAPPED = "bootstrapped";
    public static final String BROADCAST_ADDRESS = "broadcast_address";
    public static final String BROADCAST_PORT = "broadcast_port";
    public static final String CLUSTER_NAME = "cluster_name";
    public static final String LISTEN_ADDRESS = "listen_address";
    public static final String LISTEN_PORT = "listen_port";
    public static final String CQL_VERSION = "cql_version";
    public static final String DATACENTER = "datacenter";
    public static final String RACK = "rack";
    public static final String HOST_ID = "host_id";
    public static final String GOSSIP_GENERATION = "gossip_generation";
    public static final String RELEASE_VERSION = "release_version";
    public static final String NATIVE_ADDRESS = "native_address";
    public static final String NATIVE_PORT = "native_port";
    public static final String NATIVE_PROTOCOL_VERSION = "native_protocol_version";
    public static final String PARTITIONER = "partitioner";
    public static final String SCHEMA_VERSION = "schema_version";
    public static final String TOKENS = "tokens";
    public static final String STATE = "state";
    public static final String STATUS = "status";
    public static final String KEY = "local";
    public static final String TRUNCATED_AT = "truncated_at";

    public LocalTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "local")
                           .comment("Information about local node")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(InetAddressType.instance))
                           .addPartitionKeyColumn(KEY, UTF8Type.instance)
                           .addRegularColumn(BOOTSTRAPPED, UTF8Type.instance)
                           .addRegularColumn(BROADCAST_ADDRESS, InetAddressType.instance)
                           .addRegularColumn(BROADCAST_PORT, Int32Type.instance)
                           .addRegularColumn(CLUSTER_NAME, UTF8Type.instance)
                           .addRegularColumn(CQL_VERSION, UTF8Type.instance)
                           .addRegularColumn(DATACENTER, UTF8Type.instance)
                           .addRegularColumn(GOSSIP_GENERATION, Int32Type.instance)
                           .addRegularColumn(HOST_ID, UUIDType.instance)
                           .addRegularColumn(LISTEN_ADDRESS, InetAddressType.instance)
                           .addRegularColumn(LISTEN_PORT, Int32Type.instance)
                           .addRegularColumn(NATIVE_ADDRESS, InetAddressType.instance)
                           .addRegularColumn(NATIVE_PORT, Int32Type.instance)
                           .addRegularColumn(NATIVE_PROTOCOL_VERSION, UTF8Type.instance)
                           .addRegularColumn(PARTITIONER, UTF8Type.instance)
                           .addRegularColumn(RACK, UTF8Type.instance)
                           .addRegularColumn(RELEASE_VERSION, UTF8Type.instance)
                           .addRegularColumn(SCHEMA_VERSION, UUIDType.instance)
                           .addRegularColumn(STATE, UTF8Type.instance)
                           .addRegularColumn(STATUS, UTF8Type.instance)
                           .addRegularColumn(TOKENS, SetType.getInstance(UTF8Type.instance, false))
//                           .addRegularColumn(TRUNCATED_AT, MapType.getInstance(UUIDType.instance, UTF8Type.instance, false)) todo?
                           .build());
    }

    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        ClusterMetadata cm = ClusterMetadata.current();
        NodeId peer = cm.myNodeId();
        NodeState nodeState = cm.directory.peerState(peer);
        NodeAddresses addresses = cm.directory.getNodeAddresses(peer);
        Location location = cm.directory.location(peer);
        result.row(KEY)
              .column(BOOTSTRAPPED, SystemKeyspace.BootstrapState.fromNodeState(nodeState).toString())
              .column(BROADCAST_ADDRESS, addresses.broadcastAddress.getAddress())
              .column(BROADCAST_PORT, addresses.broadcastAddress.getPort())
              .column(CLUSTER_NAME, DatabaseDescriptor.getClusterName())
              .column(CQL_VERSION, QueryProcessor.CQL_VERSION.toString())
              .column(DATACENTER, location.datacenter)
              .column(GOSSIP_GENERATION, Gossiper.instance.getCurrentGenerationNumber(FBUtilities.getBroadcastAddressAndPort()))
              .column(HOST_ID, peer.uuid)
              .column(LISTEN_ADDRESS, addresses.localAddress.getAddress())
              .column(LISTEN_PORT, addresses.localAddress.getPort())
              .column(NATIVE_ADDRESS, addresses.nativeAddress.getAddress())
              .column(NATIVE_PORT, addresses.nativeAddress.getPort())
              .column(NATIVE_PROTOCOL_VERSION, String.valueOf(ProtocolVersion.CURRENT.asInt()))
              .column(PARTITIONER, cm.partitioner.getClass().getName())
              .column(RACK, location.rack)
              .column(RELEASE_VERSION, cm.directory.version(peer).cassandraVersion.toString())
              .column(SCHEMA_VERSION, cm.schema.getVersion())
              .column(STATE, cm.directory.peerState(peer).toString())
              .column(STATUS, status(cm))
              .column(TOKENS, new HashSet<>(cm.tokenMap.tokens(peer).stream().map((token) -> token.getToken().getTokenValue().toString()).collect(Collectors.toList())));
              //.column(TRUNCATED_AT, status(cm)); // todo?

        return result;
    }

    private static String status(ClusterMetadata cm)
    {
        if (StorageService.instance.isDraining())
            return StorageService.Mode.DRAINING.toString();
        if (StorageService.instance.isDrained())
            return StorageService.Mode.DRAINED.toString();
        return cm.directory.peerState(getBroadcastAddressAndPort()).toString();
    }
}