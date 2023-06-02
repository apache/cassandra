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
import org.junit.Assert;
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
        final Version targetVersion;
        final Semver targetSemver;

        public TestVersions(Version initial, Version target)
        {
            this.initial = initial;
            this.targetVersion = target;
            this.targetSemver = target.version;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestVersions that = (TestVersions) o;
            return Objects.equals(initial.version, that.initial.version) && Objects.equals(targetSemver, that.targetSemver);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(initial.version, targetSemver);
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append(initial.version).append(" -> ");
            sb.append(targetSemver);
            return sb.toString();
        }
    }

    public static class TestCase implements ThrowingRunnable
    {
        private final Versions versions;
        private final List<TestVersions> upgrade = new ArrayList<>();
        // the minimal and maximal C* version that this test case is applicable for
        // by default the test case is applicable for all versions
        private Semver minimalApplicableVersion = OLDEST;
        private Semver maximalApplicableVersion = CURRENT;
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

        private TestCase(Versions versions)
        {
            this.versions = versions;
        }

        public TestCase nodes(int nodeCount)
        {
            this.nodeCount = nodeCount;
            return this;
        }

        /**
         * @param minimalSupportedVersion the minimal version that this test case is applicable for
         *
         * This function sets the lower bound of test case's applicability
         * the test case will run (see {@link #run}) all supported direct upgrades source -> target
         * such that:
         * - source >= minimalSupportedVersion
         * - source == CURRENT or target == CURRENT
         *
         * example:
         * with CURRENT = v50 and minimalSupportedVersion = v30 the test case will run:
         * - v40 -> v50
         * - v41 -> v50
         * with CURRENT = v41 and minimalSupportedVersion = v30 the test case will run:
         * - v30 -> v41
         * - v3x -> v41
         * - v40 -> v41
         * with CURRENT = v3x and minimalSupportedVersion = v30 the test case will run:
         * - v30 -> v3x
         */
        public TestCase minimalApplicableVersion(Semver minimalSupportedVersion)
        {
            this.minimalApplicableVersion = minimalSupportedVersion;
            addUpgrades();
            return this;
        }

        /**
         * @param minimalSupportedVersion the minimal version that this test case is applicable for
         * @param maximalSupportedVersion the maximal version that this test case is applicable for
         *
         * This function sets both the lower and the upper bounds of test case's applicability
         * the test case will run (see {@link #run}) all supported direct upgrades source -> target
         * such that:
         * - source >= minimalSupportedVersion
         * - target <= maximalSupportedVersion
         * - source == CURRENT or target == CURRENT
         *
         * example:
         * with CURRENT = v50, minimalSupportedVersion = v30, and maximalSupportedVersion = v41
         * the test case will not run any upgrades.
         * with CURRENT = v41, minimalSupportedVersion = v30, and maximalSupportedVersion = v41
         * the test case will run:
         * - v30 -> v41
         * - v3x -> v41
         * - v40 -> v41
         */
        public TestCase applicableVersionsRange(Semver minimalSupportedVersion, Semver maximalSupportedVersion)
        {
            this.minimalApplicableVersion = minimalSupportedVersion;
            this.maximalApplicableVersion = maximalSupportedVersion;
            addUpgrades();
            return this;
        }

        /**
         * this is a deprecated, backwards-compatible API for upgrade tests, that allows to specify which upgrades to perform
         *
         * upgradesToCurrentFrom(Semver from) sets the upgrades to perform to be all supported direct upgrades (upgrade
         * paths with length 1) to the CURRENT version, but omitting versions older than the given "from" version
         *
         * This intent is best expressed by the newer alternatives:
         * - appplicableVersionsRange(from, CURRENT)
         * or, perhaps, depending on the actual case:
         * - minimalApplicableVersion(from)
         *
         * please note, that upgrades that do not involve the CURRENT version are not meant to be run in upgrade tests
         *
         * Beware, there may be no supported direct upgrade path from -> CURRENT, and this is OK.
         * e.g. when CURRENT == v4
         * {@code upgradesToCurrentStartingNoEarlierThan(3.0); // produces: 3.0 -> CURRENT, 3.11 -> CURRENT, …}
         * and when CURRENT == v5
         * {@code upgradesToCurrentStartingNoEarlierThan(3.0); // produces: 4.0 -> CURRENT, 4.1 -> CURRENT, …}
         * and when CURRENT == v3.11
         * {@code upgradesToCurrentStartingNoEarlierThan(3.0); // produces: 3.0 -> CURRENT, …}
         **/
        @Deprecated
        public TestCase upgradesToCurrentFrom(Semver from)
        {
            return applicableVersionsRange(from, CURRENT);
        }

        /**
         * On pre-5.0 versions there used to be a public upgradesTo(Semver from, Semver to) method here.
         * It's semantics was:
         * upgradesTo(Semver from, Semver to) sets the upgrades to perform to be all direct upgrades (upgrade paths with length 1)
         * with source not older than the given "from" version to the fixed "to" version. However, since the CURRENT
         * version must be either source or the target version, this meant either:
         * a single CURRENT -> "to" upgrade, or
         * all direct upgrades "source" -> CURRENT, where "source" >= "from"
         *
         * The method was not used in tests, and it is unlikely that you need such semantics in your test.
         * Thus, it was removed.
         *
         * Should you need it, please reconsider if the above semantics is what you really want.
         * Please check:
         * - minimalApplicableVersion(from)
         * - maximalApplicableVersion(to)
         * - applicableVersionsRange(from, to)
         * for alternatives
         */

        /**
         * On pre-5.0 versions there used to be a public upgradesFrom(Semver from, Semver to) method here.
         * It's semantics was:
         * upgradesFrom(Semver from, Semver to) sets the upgrades to perform to be all direct upgrades (upgrade paths with length 1)
         * with source fixed to "from" and target belonging to (from, to] range. However, since the CURRENT
         * version must be either source or the target version, this meant either:
         * a single "from" -> CURRENT upgrade, or
         * all direct upgrades CURRENT -> "target", where "target" <= "to"
         *
         * The method was not used in tests, and it is unlikely that you need such semantics in your test.
         * Thus, it was removed.
         *
         * Should you need it, please reconsider if the above semantics is what you really want.
         * Please check:
         * - minimalApplicableVersion(from)
         * - maximalApplicableVersion(to)
         * - applicableVersionsRange(from, to)
         * for alternatives
         */

        /**
         * this is a deprecated, backwards-compatible API for upgrade tests, that allows to specify which upgrades to perform
         *
         * upgrades(Semver from, Semver to) sets the upgrades to perform to be all supported direct upgrades (upgrade
         * paths with length 1) with either source or target version being the CURRENT version, and the other
         * end belonging to the [from, to] range.
         *
         * This intent is best expressed by the newer alternatives:
         * - appplicableVersionsRange(from, to)
         * or, perhaps, depending on the actual case:
         * - minimalApplicableVersion(from)
         **/
        @Deprecated
        public TestCase upgrades(Semver from, Semver to)
        {
            applicableVersionsRange(from, to);
            return this;
        }

        private void addUpgrades()
        {
            Assert.assertEquals("upgrade paths already defined?", this.upgrade, Collections.emptyList());
            NavigableSet<Semver> vertices = sortedVertices(SUPPORTED_UPGRADE_PATHS);
            for (Semver end : vertices.subSet(this.minimalApplicableVersion, true, this.maximalApplicableVersion, true))
            {
                addSingleUpgrade(end, CURRENT);
                addSingleUpgrade(CURRENT, end);
            }
        }

        /** Will test this specific upgrade path **/
        public TestCase singleUpgradeToCurrentFrom(Semver from)
        {
            if (!SUPPORTED_UPGRADE_PATHS.hasEdge(from, CURRENT))
                throw new AssertionError("Upgrading from " + from + " to " + CURRENT + " isn't directly supported and must go through other versions first; supported paths: " + SUPPORTED_UPGRADE_PATHS.findPaths(from, CURRENT));
            addSingleUpgrade(from, CURRENT);
            return this;
        }

        private void addSingleUpgrade(Semver from, Semver to)
        {
            if (SUPPORTED_UPGRADE_PATHS.hasEdge(from, to))
            {
                TestVersions singleUpgrade = new TestVersions(this.versions.getLatest(from), this.versions.getLatest(to));
                logger.info("Adding upgrade of {}", singleUpgrade);
                this.upgrade.add(singleUpgrade);
            }
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
            {
                throw new AssertionError(String.format("no upgrade paths have been specified (or exist) for the given applicability range: %s -> %s; CURRENT=%s",
                                                       minimalApplicableVersion, maximalApplicableVersion, CURRENT));
            }
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
                logger.info("testing upgrade from {} to {}", upgrade.initial.version, upgrade.targetSemver);
                try (UpgradeableCluster cluster = init(UpgradeableCluster.create(nodeCount, upgrade.initial, configConsumer, builderConsumer)))
                {
                    setup.run(cluster);

                    Version nextVersion = upgrade.targetVersion;
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
                             .applicableVersionsRange(OLDEST, CURRENT)
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
