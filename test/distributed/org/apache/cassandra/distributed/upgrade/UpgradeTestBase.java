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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.impl.Instance;
import org.apache.cassandra.distributed.impl.Versions;
import org.apache.cassandra.distributed.impl.Versions.Version;
import org.apache.cassandra.distributed.test.DistributedTestBase;

import static org.apache.cassandra.distributed.impl.Versions.Major;
import static org.apache.cassandra.distributed.impl.Versions.find;

public class UpgradeTestBase extends DistributedTestBase
{
    public static interface RunOnCluster
    {
        public void run(UpgradeableCluster cluster) throws Throwable;
    }

    public static interface RunOnClusterAndNode
    {
        public void run(UpgradeableCluster cluster, int node) throws Throwable;
    }

    public static class TestVersions
    {
        final Version initial;
        final Version[] upgrade;

        public TestVersions(Version initial, Version ... upgrade)
        {
            this.initial = initial;
            this.upgrade = upgrade;
        }
    }

    public static class TestCase implements Instance.ThrowingRunnable
    {
        private final Versions versions;
        private final List<TestVersions> upgrade = new ArrayList<>();
        private int nodeCount = 3;
        private RunOnCluster setup;
        private RunOnClusterAndNode runAfterNodeUpgrade;
        private RunOnCluster runAfterClusterUpgrade;
        private final Set<Integer> nodesToUpgrade = new HashSet<>();

        public TestCase()
        {
            this(find());
        }

        public TestCase(Versions versions)
        {
            this.versions = versions;
        }

        public TestCase nodes(int nodeCount)
        {
            this.nodeCount = nodeCount;
            return this;
        }

        public TestCase upgrade(Major initial, Major ... upgrade)
        {
            this.upgrade.add(new TestVersions(versions.getLatest(initial),
                                              Arrays.stream(upgrade)
                                                    .map(versions::getLatest)
                                                    .toArray(Version[]::new)));
            return this;
        }

        public TestCase upgrade(Version initial, Version ... upgrade)
        {
            this.upgrade.add(new TestVersions(initial, upgrade));
            return this;
        }

        public TestCase setup(RunOnCluster setup)
        {
            this.setup = setup;
            return this;
        }

        public TestCase runAfterNodeUpgrade(RunOnClusterAndNode runAfterNodeUpgrade)
        {
            this.runAfterNodeUpgrade = runAfterNodeUpgrade;
            return this;
        }

        public TestCase runAfterClusterUpgrade(RunOnCluster runAfterClusterUpgrade)
        {
            this.runAfterClusterUpgrade = runAfterClusterUpgrade;
            return this;
        }

        public void run() throws Throwable
        {
            if (setup == null)
                throw new AssertionError();
            if (upgrade.isEmpty())
                throw new AssertionError();
            if (runAfterClusterUpgrade == null && runAfterNodeUpgrade == null)
                throw new AssertionError();
            if (runAfterClusterUpgrade == null)
                runAfterClusterUpgrade = (c) -> {};
            if (runAfterNodeUpgrade == null)
                runAfterNodeUpgrade = (c, n) -> {};
            if (nodesToUpgrade.isEmpty())
                for (int n = 1; n <= nodeCount; n++)
                    nodesToUpgrade.add(n);

            for (TestVersions upgrade : this.upgrade)
            {
                try (UpgradeableCluster cluster = init(UpgradeableCluster.create(nodeCount, upgrade.initial)))
                {
                    setup.run(cluster);

                    for (Version version : upgrade.upgrade)
                    {
                        for (int n : nodesToUpgrade)
                        {
                            cluster.get(n).shutdown().get();
                            cluster.get(n).setVersion(version);
                            cluster.get(n).startup();
                            runAfterNodeUpgrade.run(cluster, n);
                        }

                        runAfterClusterUpgrade.run(cluster);
                    }
                }

            }
        }
        public TestCase nodesToUpgrade(int ... nodes)
        {
            for (int n : nodes)
            {
                nodesToUpgrade.add(n);
            }
            return this;
        }
    }

}
