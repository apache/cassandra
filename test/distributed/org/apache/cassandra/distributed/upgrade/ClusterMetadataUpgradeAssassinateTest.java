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

import com.google.common.collect.Streams;
import org.junit.Test;

import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.ClusterMetadata;

public class ClusterMetadataUpgradeAssassinateTest extends UpgradeTestBase
{
    @Test
    public void simpleUpgradeTest() throws Throwable
    {
        new TestCase()
        .nodes(3)
        .nodesToUpgrade(1, 2)
        .withConfig((cfg) -> cfg.with(Feature.NETWORK, Feature.GOSSIP))
        .upgradesToCurrentFrom(v50)
        .setup((cluster) -> {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
            cluster.get(3).shutdown().get();
            cluster.get(1).nodetoolResult("assassinate", "127.0.0.3").asserts().success();
        })
        .runAfterClusterUpgrade((cluster) -> {
            checkPlacements(cluster.get(1), false);
            checkPlacements(cluster.get(2), false);
            cluster.get(1).nodetoolResult("cms", "initialize").asserts().success();
            checkPlacements(cluster.get(1), false);
            checkPlacements(cluster.get(2), false);
        }).run();
    }
    static void checkPlacements(IUpgradeableInstance i, boolean shouldExist)
    {
        checkPlacements(i, "127.0.0.3", shouldExist);
    }
    static void checkPlacements(IUpgradeableInstance i, String host, boolean shouldExist)
    {
        ((IInvokableInstance) i).runOnInstance(() -> {
            ClusterMetadata metadata = ClusterMetadata.current();
            InetAddressAndPort ep = InetAddressAndPort.getByNameUnchecked(host);
            metadata.placements.asMap().forEach((key, value) -> {
                if (key.isMeta())
                    return;
                boolean existsInPlacements = Streams.concat(value.reads.endpoints.stream(),
                                                            value.writes.endpoints.stream())
                                                    .anyMatch(fr -> fr.endpoints().contains(ep));
                if (shouldExist != existsInPlacements)
                    throw new IllegalStateException(ep + " should" + (shouldExist ? "" : " not")+ " be in placements " + key + " : " + value);

            });
        });
    }
}
