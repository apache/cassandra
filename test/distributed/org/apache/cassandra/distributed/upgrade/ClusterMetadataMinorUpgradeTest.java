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

import java.util.function.Consumer;

import com.vdurmont.semver4j.Semver;

import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.distributed.test.log.mso.IPChangeWithMSOBase;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.sequences.Move;
import org.apache.cassandra.tcm.serialization.Version;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ClusterMetadataMinorUpgradeTest extends UpgradeTestBase
{
    @Test
    public void disallowUpgradingMetadataVersionTest() throws Throwable
    {
        Consumer<UpgradeableCluster.Builder> builderUpdater = builder -> builder.withInstanceInitializer((cl, i) -> IPChangeWithMSOBase.BBHelper.install(2, Move.class, cl, i));
        new TestCase()
        .nodes(3)
        .nodesToUpgrade(1, 2)
        .withBuilder(builderUpdater)
        .withConfig((cfg) -> cfg.with(Feature.NETWORK, Feature.GOSSIP)
                                .set(Constants.KEY_DTEST_FULL_STARTUP, true)
                                .set(Constants.KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN, false))
        .strictSingleUpgradeToCurrent(new Semver("6.0-alpha2", Semver.SemverType.LOOSE))
        .setup((cluster) -> {
            cluster.get(2).nodetoolResult("move", "12345").asserts().failure();
        })
        .runAfterClusterUpgrade((cluster) -> {
            cluster.get(3).shutdown().get();
            cluster.get(3).setVersion(Versions.find().getLatest(v60));
            // we have an ongoing (failed) Move MSO, we are not allowed to startup
            try (WithProperties prop = new WithProperties().set(CassandraRelevantProperties.DTEST_AVOID_SYSTEM_EXIT, true))
            {
                cluster.get(3).startup();
                fail();
            }
            catch (Exception e)
            {
                assertTrue(e.getCause().getMessage().contains("would bump the serialization version from"));
            }
            cluster.get(3).shutdown().get();
            cluster.get(2).nodetoolResult("abortmove").asserts().success();
            // no ongoing MSO, startup is fine
            cluster.get(3).startup();
            assertTrue(((IInvokableInstance)cluster.get(1)).callOnInstance(() -> ClusterMetadata.current().directory.versions.values().stream().allMatch((v) -> v.serializationVersion >= Version.V10.asInt())));
        }).run();
    }
}
