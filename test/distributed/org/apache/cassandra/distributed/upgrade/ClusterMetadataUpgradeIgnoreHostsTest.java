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

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class ClusterMetadataUpgradeIgnoreHostsTest extends UpgradeTestBase
{
    @Test
    public void upgradeIgnoreHostsTest() throws Throwable
    {
        new TestCase()
        .nodes(3)
        .nodesToUpgrade(1, 2, 3)
        .withConfig((cfg) -> cfg.with(Feature.NETWORK, Feature.GOSSIP)
                                .set(Constants.KEY_DTEST_FULL_STARTUP, true))
        .upgradesToCurrentFrom(v41)
        .setup((cluster) -> {
            cluster.schemaChange(withKeyspace("ALTER KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor':2}"));
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        })
        .runAfterClusterUpgrade((cluster) -> {
            // todo; isolate node 3 - actually shutting it down makes us throw exceptions when test finishes
            cluster.filters().allVerbs().to(3).drop();
            cluster.filters().allVerbs().from(3).drop();
            cluster.get(1).nodetoolResult("cms", "initialize").asserts().failure(); // node3 unreachable
            cluster.get(1).nodetoolResult("cms", "initialize", "--ignore", "127.0.0.1").asserts().failure(); // can't ignore localhost
            cluster.get(1).nodetoolResult("cms", "initialize", "--ignore", "127.0.0.3").asserts().success();
            cluster.get(1).nodetoolResult("cms", "reconfigure", "2").asserts().success();
        }).run();
    }

    @Test
    public void upgradeIgnoreHostsNonUpgradedTest() throws Throwable
    {
        new TestCase()
                .nodes(3)
                .nodesToUpgrade(1, 2)
                .withConfig((cfg) -> cfg.with(Feature.NETWORK, Feature.GOSSIP)
                        .set(Constants.KEY_DTEST_FULL_STARTUP, true))
                .upgradesToCurrentFrom(v41)
                .setup((cluster) -> {
                    cluster.schemaChange(withKeyspace("ALTER KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor':2}"));
                    cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
                })
                .runAfterClusterUpgrade((cluster) -> {
                    cluster.filters().allVerbs().to(3).drop();
                    cluster.filters().allVerbs().from(3).drop();
                    cluster.get(1).nodetoolResult("cms", "initialize", "--ignore", "127.0.0.3").asserts().success();
                    cluster.get(3).shutdown().get();
                    cluster.filters().reset();
                    cluster.get(3).setVersion(Versions.find().getLatest(CURRENT));
                    cluster.get(3).startup();
                    cluster.schemaChange(withKeyspace("ALTER TABLE " + KEYSPACE + ".tbl with comment = 'test'"));
                    ((IInvokableInstance)cluster.get(3)).runOnInstance(() -> {
                        Epoch current = ClusterMetadata.current().epoch;
                        if (current.isBefore(Epoch.FIRST))
                            throw new AssertionError("Epoch was not incremented as expected, still at " + current);
                    });

                    // Verify that CMS identifier has propagated across the nodes as this asserts that the DOWN node
                    // did not affect the common serialization version for metadata.
                    Set<Long> identifiers = new HashSet<>();
                    Pattern p = Pattern.compile(".*CMS Identifier\\: ([\\d]*).*", Pattern.DOTALL);
                    cluster.forEach(i -> {
                        Matcher m = p.matcher(i.nodetoolResult("cms").getStdout());
                        assertTrue(m.matches());
                        identifiers.add(Long.parseLong(m.group(1)));
                    });
                    assertEquals(1, identifiers.size());
                    assertNotEquals(ClusterMetadata.EMPTY_METADATA_IDENTIFIER, (long) identifiers.iterator().next());
                    cluster.get(1).nodetoolResult("cms", "reconfigure", "2").asserts().success();
                }).run();
    }
}
