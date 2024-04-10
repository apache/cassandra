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
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.membership.NodeVersion;


final class ClusterMetadataDirectoryTable extends AbstractVirtualTable
{
    private static final String NODE_ID = "node_id";
    private static final String HOST_ID = "host_id";
    private static final String STATE = "state";
    private static final String CASSANDRA_VERSION = "cassandra_version";
    private static final String SERIALIZATION_VERSION = "serialization_version";
    private static final String RACK = "rack";
    private static final String DC = "dc";
    private static final String BROADCAST_ADDRESS = "broadcast_address";
    private static final String BROADCAST_PORT = "broadcast_port";
    private static final String LOCAL_ADDRESS = "local_address";
    private static final String LOCAL_PORT = "local_port";
    private static final String NATIVE_ADDRESS = "native_address";
    private static final String NATIVE_PORT = "native_port";


    ClusterMetadataDirectoryTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "cluster_metadata_directory")
                           .comment("cluster metadata directory")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(LongType.instance))
                           .addPartitionKeyColumn(NODE_ID, Int32Type.instance)
                           .addRegularColumn(HOST_ID, UUIDType.instance)
                           .addRegularColumn(STATE, UTF8Type.instance)
                           .addRegularColumn(CASSANDRA_VERSION, UTF8Type.instance)
                           .addRegularColumn(SERIALIZATION_VERSION, Int32Type.instance)
                           .addRegularColumn(RACK, UTF8Type.instance)
                           .addRegularColumn(DC, UTF8Type.instance)
                           .addRegularColumn(BROADCAST_ADDRESS, InetAddressType.instance)
                           .addRegularColumn(BROADCAST_PORT, Int32Type.instance)
                           .addRegularColumn(LOCAL_ADDRESS, InetAddressType.instance)
                           .addRegularColumn(LOCAL_PORT, Int32Type.instance)
                           .addRegularColumn(NATIVE_ADDRESS, InetAddressType.instance)
                           .addRegularColumn(NATIVE_PORT, Int32Type.instance)
                           .build());
    }

    @Override
    public DataSet data()
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        Directory directory = metadata.directory;
        SimpleDataSet result = new SimpleDataSet(metadata());
        for (Map.Entry<NodeId, NodeState> entry : directory.states.entrySet())
        {
            NodeId nodeId = entry.getKey();
            NodeState nodeState = entry.getValue();
            NodeAddresses address = directory.getNodeAddresses(nodeId);
            Location location = directory.location(nodeId);
            NodeVersion version = directory.version(nodeId);
            result.row(nodeId.id())
                  .column(HOST_ID, nodeId.toUUID())
                  .column(STATE, nodeState.toString())
                  .column(CASSANDRA_VERSION, version != null ? version.cassandraVersion.toString() : null)
                  .column(SERIALIZATION_VERSION, version != null ? version.serializationVersion : null)
                  .column(RACK, location != null ? location.rack : null)
                  .column(DC, location != null ? location.datacenter : null)
                  .column(BROADCAST_ADDRESS, address != null ? address.broadcastAddress.getAddress() : null)
                  .column(BROADCAST_PORT, address != null ? address.broadcastAddress.getPort() : null)
                  .column(LOCAL_ADDRESS, address != null ? address.localAddress.getAddress() : null)
                  .column(LOCAL_PORT, address != null ? address.localAddress.getPort() : null)
                  .column(NATIVE_ADDRESS, address != null ? address.nativeAddress.getAddress() : null)
                  .column(NATIVE_PORT, address != null ? address.nativeAddress.getPort() : null);
        }
        return result;
    }
}
