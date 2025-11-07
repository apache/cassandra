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

package org.apache.cassandra.distributed.test;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;

public class HibernatingGossipOnlyMembersTest extends TestBaseImpl
{
    @Test
    public void testTruncationWithHibernatingGossipOnlyNodes() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .start()))
        {
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int primary key, t int)");
            cluster.get(1).runOnInstance(injectPeerIntoGossip("127.0.0.99"));
            // Even though the hibernating peer is considered unreachable, it won't prevent the truncation
            cluster.coordinator(1).execute("TRUNCATE " + KEYSPACE + ".tbl", ConsistencyLevel.ALL);
        }
    }

    /**
     * Add a hibernating peer to gossip which is not a member of the cluster. This can be a legacy
     * of upgrades, where previously this kind of non-member were never purged from gossip.
     */
    private IIsolatedExecutor.SerializableRunnable injectPeerIntoGossip(String address)
    {
        return () -> {
            Map<ApplicationState, VersionedValue> appState = new EnumMap<>(ApplicationState.class);
            appState.put(ApplicationState.STATUS_WITH_PORT, VersionedValue.unsafeMakeVersionedValue("hibernate,true", 1));
            EndpointState epState = new EndpointState(HeartBeatState.empty(), appState);
            Map<InetAddressAndPort, EndpointState> hibernating = new HashMap<>();
            hibernating.put(InetAddressAndPort.getByNameUnchecked(address), epState);
            Gossiper.runInGossipStageBlocking(() -> Gossiper.instance.applyStateLocally(hibernating));
        };
    }
}
