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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.MultiStepOperation;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.membership.NodeVersion;


public final class ClusterMetadataDirectoryTable extends AbstractVirtualTable
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
    private static final String TOKENS = "tokens";
    private static final String MULTI_STEP_OPERATION = "multi_step_operation";


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
                           .addRegularColumn(TOKENS, ListType.getInstance(UTF8Type.instance, false))
                           .addRegularColumn(MULTI_STEP_OPERATION, MapType.getInstance(UTF8Type.instance, UTF8Type.instance, false))
                           .build());
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        for (Map.Entry<Long, Map<String, Object>> entry : directory(true).entrySet())
        {
            result = result.row(entry.getKey().intValue());
            for (Map.Entry<String, Object> row : entry.getValue().entrySet())
                result = result.column(row.getKey(), row.getValue());
        }
        return result;
    }

    public static Map<Long, Map<String, Object>> directory(boolean tokens)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        Directory directory = metadata.directory;
        Map<Long, Map<String, Object>> result = new LinkedHashMap<>();

        for (Map.Entry<NodeId, NodeState> entry : directory.states.entrySet())
        {
            NodeId nodeId = entry.getKey();
            NodeState nodeState = entry.getValue();
            NodeAddresses address = directory.getNodeAddresses(nodeId);
            Location location = directory.location(nodeId);
            NodeVersion version = directory.version(nodeId);
            Map<String, Object> row = new HashMap<>();
            row.put(HOST_ID, nodeId.toUUID());
            row.put(STATE, nodeState.toString());
            row.put(CASSANDRA_VERSION, version != null ? version.cassandraVersion.toString() : null);
            row.put(SERIALIZATION_VERSION, version != null ? version.serializationVersion : null);
            row.put(RACK, location != null ? location.rack : null);
            row.put(DC, location != null ? location.datacenter : null);
            row.put(BROADCAST_ADDRESS, address != null ? address.broadcastAddress.getAddress() : null);
            row.put(BROADCAST_PORT, address != null ? address.broadcastAddress.getPort() : null);
            row.put(LOCAL_ADDRESS, address != null ? address.localAddress.getAddress() : null);
            row.put(LOCAL_PORT, address != null ? address.localAddress.getPort() : null);
            row.put(NATIVE_ADDRESS, address != null ? address.nativeAddress.getAddress() : null);
            row.put(NATIVE_PORT, address != null ? address.nativeAddress.getPort() : null);
            if (tokens)
                row.put(TOKENS, tokensToString(metadata.tokenMap.tokens(nodeId)));
            MultiStepOperation<?> mso = metadata.inProgressSequences.get(nodeId);
            if (mso != null)
                row.put(MULTI_STEP_OPERATION, ImmutableMap.of("kind", mso.kind().name(),
                                                              "status", mso.status(),
                                                              "nextStep", mso.nextStep().name()));
            result.put((long)nodeId.id(), row);
        }
        return result;
    }

    private static List<String> tokensToString(List<Token> tokens)
    {
        return tokens.stream().map(Object::toString).collect(Collectors.toList());
    }
}
