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

package org.apache.cassandra.distributed.test.log;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.tcm.ClusterMetadata;

import static org.apache.cassandra.net.Verb.TCM_FETCH_PEER_LOG_REQ;
import static org.apache.cassandra.net.Verb.TCM_REPLICATION;
import static org.junit.Assert.assertEquals;

public class FetchLogFromPeersDCTest extends TestBaseImpl
{

    @Test
    public void catchupCoordinatorBehindTestPlacements() throws Exception
    {
        try (Cluster cluster = init(builder().withNodes(4).withConfig(c -> c.with(Feature.NETWORK, Feature.GOSSIP))
                                             .withoutVNodes()
                                             .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(4))
                                             .withNodeIdTopology(NetworkTopology.networkTopology(4, (i) -> NetworkTopology.dcAndRack("dc" + (i <= 2 ? 0 : 1), "rack" + i)))
                                             .start()))
        {
            cluster.schemaChange(withKeyspace("alter keyspace %s with replication = {'class':'NetworkTopologyStrategy', 'dc0':2, 'dc1':2}"));
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key)"));
            cluster.filters().inbound().verbs(TCM_REPLICATION.id).from(1).to(3, 4).drop();
            // don't allow the dc1 nodes to catch up from eachother - we should catch up from the actual originator of the message:
            cluster.filters().inbound().verbs(TCM_FETCH_PEER_LOG_REQ.id).from(3, 4).to(3,4).drop();
            cluster.get(1).schemaChangeInternal(withKeyspace("alter table %s.tbl with comment='abc'"));
            cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (id) values (1)"), ConsistencyLevel.ALL);
            long epoch = cluster.get(1).callOnInstance(() -> ClusterMetadata.current().epoch.getEpoch());
            cluster.forEach(i -> i.runOnInstance(() -> {
                assertEquals(epoch, ClusterMetadata.current().epoch.getEpoch());
            }));
        }
    }
}
