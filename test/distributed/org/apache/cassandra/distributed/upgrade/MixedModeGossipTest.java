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

package org.apache.cassandra.distributed.upgrade;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.net.Verb;
import org.assertj.core.api.Assertions;

public class MixedModeGossipTest extends UpgradeTestBase
{
    Pattern expectedNormalStatus = Pattern.compile("STATUS:\\d+:NORMAL,-?\\d+");
    Pattern expectedNormalStatusWithPort = Pattern.compile("STATUS_WITH_PORT:\\d+:NORMAL,-?\\d+");
    Pattern expectedNormalX3 = Pattern.compile("X3:\\d+:NORMAL,-?\\d+");

    @Test
    public void testStatusFieldShouldExistInOldVersionNodes() throws Throwable
    {
        new TestCase()
        .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
        .nodes(3)
        .nodesToUpgradeOrdered(1, 2, 3)
        // all upgrades from v30 up, excluding v30->v3X and from v40
        .singleUpgrade(v30)
        .singleUpgrade(v3X)
        .setup(c -> {})
        .runAfterNodeUpgrade((cluster, node) -> {
            if (node == 1) {
                checkPeerGossipInfoShouldContainNormalStatus(cluster, 2);
                checkPeerGossipInfoShouldContainNormalStatus(cluster, 3);
            }
            if (node == 2) {
                checkPeerGossipInfoShouldContainNormalStatus(cluster, 3);
            }
        })
        .runAfterClusterUpgrade(cluster -> {
            // wait 1 minute for `org.apache.cassandra.gms.Gossiper.upgradeFromVersionSupplier` to update
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MINUTES);
            checkPeerGossipInfoOfAllNodesShouldContainNewStatusAfterUpgrade(cluster);
        })
        .run();
    }

    /**
     * Similar to {@link #testStatusFieldShouldExistInOldVersionNodes}, but in an edge case that
     * 1) node2 and node3 cannot gossip with each other.
     * 2) node2 sends SYN to node1 first when upgrading.
     * 3) node3 is at the lower version during the cluster upgrade
     * In this case, node3 gossip info does not contain STATUS field for node2
     */
    @Test
    public void testStatusFieldShouldExistInOldVersionNodesEdgeCase() throws Throwable
    {
        AtomicReference<IMessageFilters.Filter> n1GossipSynBlocker = new AtomicReference<>();
        new TestCase()
        .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
        .nodes(3)
        .nodesToUpgradeOrdered(1, 2, 3)
        // all upgrades from v30 up, excluding v30->v3X and from v40
        .singleUpgrade(v30)
        .singleUpgrade(v3X)
        .setup(cluster -> {
            // node2 and node3 gossiper cannot talk with each other
            cluster.filters().verbs(Verb.GOSSIP_DIGEST_SYN.id).from(2).to(3).drop();
            cluster.filters().verbs(Verb.GOSSIP_DIGEST_SYN.id).from(3).to(2).drop();
        })
        .runAfterNodeUpgrade((cluster, node) -> {
            // let node2 sends the SYN to node1 first
            if (node == 1)
            {
                IMessageFilters.Filter filter = cluster.filters().verbs(Verb.GOSSIP_DIGEST_SYN.id).from(1).to(2).drop();
                n1GossipSynBlocker.set(filter);
            }
            else if (node == 2)
            {
                n1GossipSynBlocker.get().off();
                String node3GossipView = cluster.get(3).nodetoolResult("gossipinfo").getStdout();
                String node2GossipState = getGossipStateOfNode(node3GossipView, "/127.0.0.2");
                Assertions.assertThat(node2GossipState)
                          .as("The node2's gossip state from node3's perspective should contain status. " +
                              "And it should carry an unrecognized field X3 with NORMAL.")
                          .containsPattern(expectedNormalStatus)
                          .containsPattern(expectedNormalX3);
            }
        })
        .runAfterClusterUpgrade(cluster -> {
            // wait 1 minute for `org.apache.cassandra.gms.Gossiper.upgradeFromVersionSupplier` to update
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MINUTES);
            checkPeerGossipInfoOfAllNodesShouldContainNewStatusAfterUpgrade(cluster);
        })
        .run();
    }

    private void checkPeerGossipInfoOfAllNodesShouldContainNewStatusAfterUpgrade(UpgradeableCluster cluster)
    {
        for (int i = 1; i <= 3; i++)
        {
            int n = i;
            checkPeerGossipInfo(cluster, i, (gossipInfo, peers) -> {
                for (String p : peers)
                {
                    Assertions.assertThat(getGossipStateOfNode(gossipInfo, p))
                              .as(String.format("%s gossip state in node%s should not contain STATUS " +
                                                "and should contain STATUS_WITH_PORT.", p, n))
                              .doesNotContain("STATUS:")
                              .containsPattern(expectedNormalStatusWithPort);
                }
            });
        }
    }

    private void checkPeerGossipInfoShouldContainNormalStatus(UpgradeableCluster cluster, int node)
    {
        checkPeerGossipInfo(cluster, node, (gossipInfo, peers) -> {
            for (String n : peers)
            {
                Assertions.assertThat(getGossipStateOfNode(gossipInfo, n))
                          .containsPattern(expectedNormalStatus);
            }
        });
    }

    private void checkPeerGossipInfo(UpgradeableCluster cluster, int node, BiConsumer<String, Set<String>> verifier)
    {
        Set<Integer> peers = new HashSet<>(Arrays.asList(1, 2, 3));
        peers.remove(node);
        String gossipInfo = cluster.get(node).nodetoolResult("gossipinfo").getStdout();
        verifier.accept(gossipInfo, peers.stream().map(i -> "127.0.0." + i).collect(Collectors.toSet()));
    }

    private String getGossipStateOfNode(String rawOutput, String nodeInterested)
    {
        String temp = rawOutput.substring(rawOutput.indexOf(nodeInterested));
        int nextSlashIndex = temp.indexOf('/', 1);
        if (nextSlashIndex != -1)
            return temp.substring(0, nextSlashIndex);
        else
            return temp;
    }
}
