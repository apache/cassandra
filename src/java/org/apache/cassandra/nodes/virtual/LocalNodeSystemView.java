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
package org.apache.cassandra.nodes.virtual;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.nodes.LocalInfo;
import org.apache.cassandra.nodes.Nodes;

public final class LocalNodeSystemView extends NodeSystemView
{
    public static final String KEY = "local";

    public LocalNodeSystemView()
    {
        super(NodesSystemViews.virtualFromLegacy(NodesSystemViews.LocalMetadata, NodeConstants.LOCAL_VIEW_NAME));
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet dataset = new SimpleDataSet(metadata());

        // Have to copy the current PeerInfo object as it may change while we're constructing the row,
        // so null-values could sneak in and cause NPEs during serialization.
        LocalInfo l = Nodes.local().get().copy();

        dataset = dataset.row(KEY)
                         //+ "bootstrapped text,"
                         .column("bootstrapped", safeToString(l.getBootstrapState()))
                         //+ "broadcast_address inet,"
                         .column("broadcast_address",
                                 l.getBroadcastAddressAndPort() == null ? null
                                                                        : l.getBroadcastAddressAndPort().address)
                         //+ "broadcast_port int,"
                         .column("broadcast_port",
                                 l.getBroadcastAddressAndPort() == null ? null
                                                                        : l.getBroadcastAddressAndPort().port)
                         //+ "cluster_name text,"
                         .column("cluster_name", l.getClusterName())
                         //+ "cql_version text,"
                         .column("cql_version", safeToString(l.getCqlVersion()))
                         //+ "data_center text,"
                         .column("data_center", safeToString(l.getDataCenter()))
                         //+ "gossip_generation int,"
                         .column("gossip_generation", l.getGossipGeneration())
                         //+ "listen_address inet,"
                         .column("listen_address",
                                 l.getListenAddressAndPort() == null ? null
                                                                     : l.getListenAddressAndPort().address)
                         //+ "listen_address int,"
                         .column("listen_port",
                                 l.getListenAddressAndPort() == null ? null
                                                                     : l.getListenAddressAndPort().port)
                         //+ "rpc_address inet,"
                         .column("rpc_address",
                                 l.getNativeTransportAddressAndPort() == null ? null
                                                                              : l.getNativeTransportAddressAndPort().address)
                         //+ "rpc_port int,"
                         .column("rpc_port",
                                 l.getNativeTransportAddressAndPort() == null ? null
                                                                              : l.getNativeTransportAddressAndPort().port)
                         //+ "native_protocol_version text,"
                         .column("native_protocol_version", l.getNativeProtocolVersion())
                         //+ "partitioner text,"
                         .column("partitioner", l.getPartitioner())
                         //+ "truncated_at map<uuid, blob>,"
                         .column("truncated_at", truncationRecords(l));

        return completeRow(dataset, l);
    }

    public Map<UUID, ByteBuffer> truncationRecords(LocalInfo l)
    {
        Map<UUID, LocalInfo.TruncationRecord> truncationRecords = l.getTruncationRecords();
        if (truncationRecords.isEmpty())
            return null;
        return truncationRecords.entrySet()
                                .stream()
                                .collect(Collectors.toMap(e -> e.getKey(),
                                                          e -> truncationAsMapEntry(e.getValue()),
                                                          (a, b) -> a,
                                                          this::newMap));
    }

    public <K, V> Map<K, V> newMap()
    {
        return new HashMap<>(16);
    }

    private static ByteBuffer truncationAsMapEntry(LocalInfo.TruncationRecord truncationRecord)
    {
        try (DataOutputBuffer out = DataOutputBuffer.scratchBuffer.get())
        {
            CommitLogPosition.serializer.serialize(truncationRecord.position, out);
            out.writeLong(truncationRecord.truncatedAt);
            return out.asNewBuffer();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
