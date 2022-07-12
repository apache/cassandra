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

import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.nodes.Nodes;

/**
 * A system view of the system.peers_v2 table
 */
public final class PeersSystemView extends NodeSystemView
{
    public PeersSystemView()
    {
        super(NodesSystemViews.virtualFromLegacy(NodesSystemViews.PeersV2Metadata, NodeConstants.PEERS_V2_VIEW_NAME));
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet dataset = new SimpleDataSet(metadata());

        Nodes.peers()
             .stream()
             .forEach(p -> {
                 // Have to copy the current PeerInfo object as it may change while we're constructing the row,
                 // so null-values could sneak in and cause NPEs during serialization.
                 p = p.copy();

                 dataset.row(p.getPeer().address, p.getPeer().port)
                        //+ "preferred_ip inet,"
                        .column("preferred_ip",
                                p.getPreferred() == null ? null
                                                         : p.getPreferred().address)
                        //+ "preferred_port int,"
                        .column("preferred_port",
                                p.getPreferred() == null ? null : p.getPreferred().port)
                        //+ "native_address inet,"
                        .column("native_address",
                                p.getNativeTransportAddressAndPort() == null ? null
                                                                             : p.getNativeTransportAddressAndPort().address)
                       //+ "native_port int,"
                       .column("native_port",
                               p.getNativeTransportAddressAndPort() == null ? null
                                                                            : p.getNativeTransportAddressAndPort().port);

                 completeRow(dataset, p);
             });

        return dataset;
    }
}
