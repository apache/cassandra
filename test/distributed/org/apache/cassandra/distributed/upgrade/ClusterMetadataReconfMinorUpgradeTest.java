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

import com.vdurmont.semver4j.Semver;

import org.junit.Test;

import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.tcm.ClusterMetadata;

public class ClusterMetadataReconfMinorUpgradeTest extends UpgradeTestBase
{
    @Test
    public void testCatchupResolution() throws Throwable
    {
        new TestCase()
        .nodes(4)
        .withConfig((cfg) -> cfg.with(Feature.NETWORK, Feature.GOSSIP)
                                .set(Constants.KEY_DTEST_FULL_STARTUP, true))
        .strictSingleUpgradeToCurrent(new Semver("6.0-alpha2", Semver.SemverType.LOOSE))
        .setup((cluster) -> {
            cluster.get(1).nodetoolResult("cms", "reconfigure", "3").asserts().success();
        })
        .runAfterClusterUpgrade((cluster) -> {
            long currentEpoch = ((IInvokableInstance)cluster.get(1)).callOnInstance(() -> ClusterMetadata.current().epoch.getEpoch());
            // make sure we need to catch up after bounce
            for (int epoch = 11; epoch <= currentEpoch; epoch++)
                cluster.get(4).executeInternal("delete from system.local_metadata_log where epoch=" + epoch);
            cluster.get(4).executeInternal("truncate system.metadata_snapshots");
            cluster.get(4).flush("system");
            cluster.get(4).shutdown().get();
            cluster.get(4).startup();
            cluster.get(4).logs().watchFor("Deserialized V8-serialized streamCandidates in V9");
        }).run();
    }
}
