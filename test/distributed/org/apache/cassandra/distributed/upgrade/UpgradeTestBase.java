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
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.vdurmont.semver4j.Semver;
import com.vdurmont.semver4j.Semver.SemverType;

import org.junit.After;
import org.junit.Assume;
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
import org.apache.cassandra.utils.SimpleGraph;

import static org.apache.cassandra.distributed.shared.Versions.Version;
import static org.apache.cassandra.distributed.shared.Versions.find;
import static org.apache.cassandra.utils.SimpleGraph.sortedVertices;

public class UpgradeTestBase extends DistributedTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(UpgradeTestBase.class);

    @After
    public void afterEach()
    {
        triggerGC();
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
    public static final Semver v50 = new Semver("5.0-alpha1", SemverType.LOOSE);

    protected static final SimpleGraph<Semver> SUPPORTED_UPGRADE_PATHS = new SimpleGraph.Builder<Semver>()
                                                                         .addEdge(v30, v3X)
                                                                         .addEdge(v30, v40)
                                                                         .addEdge(v30, v41)
                                                                         .addEdge(v3X, v40)
                                                                         .addEdge(v3X, v41)
                                                                         .addEdge(v40, v41)
                                                                         .addEdge(v40, v50)
                                                                         .addEdge(v41, v50)
                                                                         .build();

    // the last is always the current
    public static final Semver CURRENT = SimpleGraph.max(SUPPORTED_UPGRADE_PATHS);
    public static final Semver OLDEST = SimpleGraph.min(SUPPORTED_UPGRADE_PATHS);

    public static class TestVersions
    {
        final Version initial;
        final List<Version> upgrade;
        final List<Semver> upgradeVersions;

        public TestVersions(Version initial, List<Version> upgrade)
        {
            this.initial = initial;
            this.upgrade = upgrade;
            this.upgradeVersions = upgrade.stream().map(v -> v.version).collect(Collectors.toList());
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestVersions that = (TestVersions) o;
            return Objects.equals(initial.version, that.initial.version) && Objects.equals(upgradeVersions, that.upgradeVersions);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(initial.version, upgradeVersions);
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append(initial.version).append(" -> ");
            sb.append(upgradeVersions);
            return sb.toString();
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
        private RunOnCluster runBeforeClusterUpgrade;
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

        /** performs all supported upgrade paths that exist in between lowerBound and end on CURRENT (inclusive)
         * {@code upgradesToCurrentFrom(3.0); // produces: 3.0 -> CURRENT, 3.11 -> CURRENT, …}
         **/
        public TestCase upgradesToCurrentFrom(Semver lowerBound)
        {
            return upgradesTo(lowerBound, CURRENT);
        }

        /**
         * performs all supported upgrade paths to the "to" target; example
         * {@code upgradesTo(3.0, 4.0); // produces: 3.0 -> 4.0, 3.11 -> 4.0}
         */
        public TestCase upgradesTo(Semver lowerBound, Semver to)
        {
            List<TestVersions> upgrade = new ArrayList<>();
            NavigableSet<Semver> vertices = sortedVertices(SUPPORTED_UPGRADE_PATHS);
            for (Semver start : vertices.subSet(lowerBound, true, to, false))
            {
                // only include pairs that are allowed, and start or end on CURRENT
                if (SUPPORTED_UPGRADE_PATHS.hasEdge(start, to) && edgeTouchesTarget(start, to, CURRENT))
                    upgrade.add(new TestVersions(versions.getLatest(start), Collections.singletonList(versions.getLatest(to))));
            }
            logger.info("Adding upgrades of\n{}", upgrade.stream().map(TestVersions::toString).collect(Collectors.joining("\n")));
            this.upgrade.addAll(upgrade);
            return this;
        }

        /**
         * performs all supported upgrade paths from the "from" target; example
         * {@code upgradesFrom(4.0, 4.2); // produces: 4.0 -> 4.1, 4.0 -> 4.2}
         */
        public TestCase upgradesFrom(Semver from, Semver upperBound)
        {
            List<TestVersions> upgrade = new ArrayList<>();
            NavigableSet<Semver> vertices = sortedVertices(SUPPORTED_UPGRADE_PATHS);
            for (Semver end : vertices.subSet(from, false, upperBound, true))
            {
                // only include pairs that are allowed, and start or end on CURRENT
                if (SUPPORTED_UPGRADE_PATHS.hasEdge(from, end) && edgeTouchesTarget(from, end, CURRENT))
                    upgrade.add(new TestVersions(versions.getLatest(from), Collections.singletonList(versions.getLatest(end))));
            }
            logger.info("Adding upgrades of\n{}", upgrade.stream().map(TestVersions::toString).collect(Collectors.joining("\n")));
            this.upgrade.addAll(upgrade);
            return this;
        }

        /**
         * performs all supported upgrade paths that exist in between from and to that include the current version.
         * This call is equivalent to calling {@code upgradesTo(from, CURRENT).upgradesFrom(CURRENT, to)}.
         **/
        public TestCase upgrades(Semver lowerBound, Semver upperBound)
        {
            Assume.assumeTrue("Unable to do any upgrades in bounds (" + lowerBound + ", " + upperBound + "); does not cover CURRENT=" + CURRENT, rangeCoversTarget(lowerBound, upperBound, CURRENT));
            if (lowerBound.compareTo(CURRENT) < 0)
                upgradesTo(lowerBound, CURRENT);
            if (CURRENT.compareTo(upperBound) < 0)
                upgradesFrom(CURRENT, upperBound);
            return this;
        }

        private static boolean rangeCoversTarget(Semver lowerBound, Semver upperBound, Semver target)
        {
            // target >= from && target <= to
            return target.isGreaterThanOrEqualTo(lowerBound) && target.isLowerThanOrEqualTo(upperBound);
        }

        /** returns true if the target version has the same major and minor as either the from or the to version **/
        private static boolean edgeTouchesTarget(Semver from, Semver to, Semver target)
        {
            switch (from.diff(target))
            {
                default:
                    return true;
                case MAJOR:
                case MINOR:
                    // fall through
            }
            switch (to.diff(target))
            {
                default:
                    return true;
                case MAJOR:
                case MINOR:
                    return false;
            }
        }

        /** Will test this specific upgrade path **/
        public TestCase singleUpgradeToCurrentFrom(Semver from)
        {
            if (!SUPPORTED_UPGRADE_PATHS.hasEdge(from, CURRENT))
                throw new AssertionError("Upgrading from " + from + " to " + CURRENT + " isn't directly supported and must go through other versions first; supported paths: " + SUPPORTED_UPGRADE_PATHS.findPaths(from, CURRENT));
            TestVersions tests = new TestVersions(this.versions.getLatest(from), Arrays.asList(this.versions.getLatest(CURRENT)));
            logger.info("Adding upgrade of {}", tests);
            this.upgrade.add(tests);
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

        public TestCase runBeforeClusterUpgrade(RunOnCluster runBeforeClusterUpgrade)
        {
            this.runBeforeClusterUpgrade = runBeforeClusterUpgrade;
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
            if (runBeforeClusterUpgrade == null)
                runBeforeClusterUpgrade = (c) -> {};
            if (runAfterClusterUpgrade == null)
                runAfterClusterUpgrade = (c) -> {};
            if (runAfterNodeUpgrade == null)
                runAfterNodeUpgrade = (c, n) -> {};
            if (nodesToUpgrade.isEmpty())
                for (int n = 1; n <= nodeCount; n++)
                    nodesToUpgrade.add(n);

            int offset = 0;
            for (TestVersions upgrade : this.upgrade)
            {
                logger.info("testing upgrade from {} to {}", upgrade.initial.version, upgrade.upgradeVersions);
                try (UpgradeableCluster cluster = init(UpgradeableCluster.create(nodeCount, upgrade.initial, configConsumer, builderConsumer)))
                {
                    setup.run(cluster);

                    for (Version nextVersion : upgrade.upgrade)
                    {
                        try
                        {
                            runBeforeClusterUpgrade.run(cluster);

                            for (int n : nodesToUpgrade)
                            {
                                cluster.get(n).shutdown().get();
                                triggerGC();
                                cluster.get(n).setVersion(nextVersion);
                                runBeforeNodeRestart.run(cluster, n);
                                cluster.get(n).startup();
                                runAfterNodeUpgrade.run(cluster, n);
                            }

                            runAfterClusterUpgrade.run(cluster);

                            cluster.checkAndResetUncaughtExceptions();
                        }
                        catch (Throwable t)
                        {
                            throw new AssertionError(String.format("Error in test '%s' while upgrading to '%s'; successful upgrades %s", upgrade, nextVersion.version, this.upgrade.stream().limit(offset).collect(Collectors.toList())), t);
                        }
                    }
                }
                offset++;
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

    private static void triggerGC()
    {
        System.runFinalization();
        System.gc();
    }

    protected TestCase allUpgrades(int nodes, int... toUpgrade)
    {
        return new TestCase().nodes(nodes)
                             .upgradesToCurrentFrom(v30)
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
