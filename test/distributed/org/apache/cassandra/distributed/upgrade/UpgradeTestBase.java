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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableList;
import com.vdurmont.semver4j.Semver;
import com.vdurmont.semver4j.Semver.SemverType;

import org.junit.After;
import org.junit.BeforeClass;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.shared.DistributedTestBase;
import org.apache.cassandra.distributed.shared.ThrowingRunnable;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.distributed.shared.Versions.Version;
import static org.apache.cassandra.distributed.shared.Versions.find;

public class UpgradeTestBase extends DistributedTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(UpgradeTestBase.class);

    @After
    public void afterEach()
    {
        System.runFinalization();
        System.gc();
    }

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        ICluster.setup();
    }


    public UpgradeableCluster.Builder builder()
    {
        return UpgradeableCluster.build();
    }

    public static interface RunOnCluster
    {
        public void run(UpgradeableCluster cluster) throws Throwable;
    }

    public static interface RunOnClusterAndNode
    {
        public void run(UpgradeableCluster cluster, int node) throws Throwable;
    }

    public static final Semver v30 = new Semver("3.0.0-alpha1", SemverType.LOOSE);
    public static final Semver v3X = new Semver("3.11.0", SemverType.LOOSE);
    public static final Semver v40 = new Semver("4.0-alpha1", SemverType.LOOSE);
    public static final Semver v41 = new Semver("4.1-alpha1", SemverType.LOOSE);

    protected static final List<Pair<Semver,Semver>> SUPPORTED_UPGRADE_PATHS = ImmutableList.of(
        Pair.create(v30, v3X),
        Pair.create(v30, v40),
        Pair.create(v30, v41),
        Pair.create(v3X, v40),
        Pair.create(v3X, v41),
        Pair.create(v40, v41));

    // the last is always the current
    public static final Semver CURRENT = SUPPORTED_UPGRADE_PATHS.get(SUPPORTED_UPGRADE_PATHS.size() - 1).right;

    public static class TestVersions
    {
        final Version initial;
        final Version upgrade;

        public TestVersions(Version initial, Version upgrade)
        {
            this.initial = initial;
            this.upgrade = upgrade;
        }
    }

    public static class TestCase implements ThrowingRunnable
    {
        private final Versions versions;
        private final List<TestVersions> upgrade = new ArrayList<>();
        private int nodeCount = 3;
        private RunOnCluster setup;
        private RunOnClusterAndNode runBeforeNodeRestart;
        private RunOnClusterAndNode runAfterNodeUpgrade;
        private RunOnCluster runAfterClusterUpgrade;
        private final Set<Integer> nodesToUpgrade = new LinkedHashSet<>();
        private Consumer<IInstanceConfig> configConsumer;
        private Consumer<UpgradeableCluster.Builder> builderConsumer;

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

        /** performs all supported upgrade paths that exist in between from and CURRENT (inclusive) **/
        public TestCase upgradesFrom(Semver from)
        {
            return upgrades(from, CURRENT);
        }

        /** performs all supported upgrade paths that exist in between from and to (inclusive) **/
        public TestCase upgrades(Semver from, Semver to)
        {
            SUPPORTED_UPGRADE_PATHS.stream()
                .filter(upgradePath -> (upgradePath.left.compareTo(from) >= 0 && upgradePath.right.compareTo(to) <= 0))
                .forEachOrdered(upgradePath ->
                {
                    this.upgrade.add(
                            new TestVersions(versions.getLatest(upgradePath.left), versions.getLatest(upgradePath.right)));
                });
            return this;
        }

        /** Will test this specific upgrade path **/
        public TestCase singleUpgrade(Semver from)
        {
            this.upgrade.add(new TestVersions(versions.getLatest(from), versions.getLatest(CURRENT)));
            return this;
        }

        public TestCase setup(RunOnCluster setup)
        {
            this.setup = setup;
            return this;
        }

        public TestCase runBeforeNodeRestart(RunOnClusterAndNode runBeforeNodeRestart)
        {
            this.runBeforeNodeRestart = runBeforeNodeRestart;
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

        public TestCase withConfig(Consumer<IInstanceConfig> config)
        {
            this.configConsumer = config;
            return this;
        }

        public TestCase withBuilder(Consumer<UpgradeableCluster.Builder> builder)
        {
            this.builderConsumer = builder;
            return this;
        }

        public void run() throws Throwable
        {
            if (setup == null)
                throw new AssertionError();
            if (upgrade.isEmpty())
                throw new AssertionError("no upgrade paths have been specified (or exist)");
            if (runAfterClusterUpgrade == null && runAfterNodeUpgrade == null)
                throw new AssertionError();
            if (runBeforeNodeRestart == null)
                runBeforeNodeRestart = (c, n) -> {};
            if (runAfterClusterUpgrade == null)
                runAfterClusterUpgrade = (c) -> {};
            if (runAfterNodeUpgrade == null)
                runAfterNodeUpgrade = (c, n) -> {};
            if (nodesToUpgrade.isEmpty())
                for (int n = 1; n <= nodeCount; n++)
                    nodesToUpgrade.add(n);

            for (TestVersions upgrade : this.upgrade)
            {
                logger.info("testing upgrade from {} to {}", upgrade.initial.version, upgrade.upgrade.version);
                try (UpgradeableCluster cluster = init(UpgradeableCluster.create(nodeCount, upgrade.initial, configConsumer, builderConsumer)))
                {
                    setup.run(cluster);

                    for (int n : nodesToUpgrade)
                    {
                        cluster.get(n).shutdown().get();
                        cluster.get(n).setVersion(upgrade.upgrade);
                        runBeforeNodeRestart.run(cluster, n);
                        cluster.get(n).startup();
                        runAfterNodeUpgrade.run(cluster, n);
                    }

                    runAfterClusterUpgrade.run(cluster);
                }
            }
        }
        public TestCase nodesToUpgrade(int ... nodes)
        {
            Set<Integer> set = new HashSet<>(nodes.length);
            for (int n : nodes)
            {
                set.add(n);
            }
            nodesToUpgrade.addAll(set);
            return this;
        }

        public TestCase nodesToUpgradeOrdered(int ... nodes)
        {
            for (int n : nodes)
            {
                nodesToUpgrade.add(n);
            }
            return this;
        }
     }

    protected TestCase allUpgrades(int nodes, int... toUpgrade)
    {
        return new TestCase().nodes(nodes)
                             .upgradesFrom(v30)
                             .nodesToUpgrade(toUpgrade);
    }

    protected static int primaryReplica(List<Long> initialTokens, Long token)
    {
        int primary = 1;

        for (Long initialToken : initialTokens)
        {
            if (token <= initialToken)
            {
                break;
            }

            primary++;
        }

        return primary;
    }

    protected static Long tokenFrom(int key)
    {
        DecoratedKey dk = Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(key));
        return (Long) dk.getToken().getTokenValue();
    }

    protected static int nextNode(int current, int numNodes)
    {
        return current == numNodes ? 1 : current + 1;
    }
}
