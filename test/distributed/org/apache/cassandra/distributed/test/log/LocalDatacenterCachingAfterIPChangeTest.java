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

import java.io.IOException;

import org.junit.Test;

import org.apache.cassandra.dht.Datacenters;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.tcm.ClusterMetadata;

import static org.apache.cassandra.distributed.shared.ClusterUtils.stopUnchecked;
import static org.apache.cassandra.distributed.shared.ClusterUtils.updateAddress;
import static org.junit.Assert.assertEquals;

public class LocalDatacenterCachingAfterIPChangeTest
{
    @Test
    public void localDeterministicTest() throws IOException
    {
        // When a node changes broadcast address, it submits a Startup transformation to update ClusterMetadata.
        // local_deterministic and local_random CMSCommitMemberPreferencePolicy variants use Locator.local() to
        // identify the local DC when choosing the CMS member to submit. In this scenario, the newly restarted
        // node's changed address means it can't yet retrieve its correct DC/rack from the replayed ClusterMetadata.
        // The locality of the CMS member is ultimately just an optimisation, so using an incorrect DC (the default is
        // UNKNOWN_DC) doesn't matter, but we need to ensure that this value isn't cached by the Locator.

        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NATIVE_PROTOCOL)
                                                        .set("cms_commit_member_preference_policy","local_deterministic"))
                                      .start())
        {
            stopUnchecked(cluster.get(3));

            // change the IP of the node
            updateAddress(cluster.get(3), "127.0.0.4");

            cluster.get(3).startup();

            cluster.get(3).runOnInstance(() -> {
                assertEquals(ClusterMetadata.current().locator.local().datacenter, Datacenters.thisDatacenter());
            });
        }
    }

    @Test
    public void withCreateKeyspaceReplayedTest() throws IOException
    {
        // Simulates the InitialLocationProvider on the modified node missing a location associated with
        // the new address. This is relevant as replaying the CREATE KEYSPACE statement from the log during
        // startup will validate the replication params using Datacenters.getValidDatacenters which has the
        // potential to initialize Datacenters.DCHandle and this in turn fetches the local DC via Locator.local().
        // With the newly restarted node's RegistrationStatus still being UNREGISTERED due to the address change,
        // Locator.local() will defer to the InitialLocationProvider. This location is then cached by DCHandle for
        // the lifetime of the jvm, which may not be appropriate as the value supplied by the InitialLocationProvider
        // is only intended for use when registering with the CMS on a node's very first boot and it may conflict with
        // the actual value in ClusterMetadata (i.e. the provider may have missing, incorrect or outdated config).

        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NATIVE_PROTOCOL))
                                      .start())
        {
            cluster.schemaChange("CREATE KEYSPACE ks1 WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter0':2}");
            stopUnchecked(cluster.get(3));

            // change the IP of the node
            updateAddress(cluster.get(3), "127.0.0.4");

            cluster.get(3).startup();

            cluster.get(3).runOnInstance(() -> {
                assertEquals(ClusterMetadata.current().locator.local().datacenter, Datacenters.thisDatacenter());
            });
        }
    }
}