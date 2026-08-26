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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableMap;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.locator.EndpointsByReplica;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ownership.MovementMap;
import org.apache.cassandra.tcm.serialization.Version;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ClusterMetadataMinorUpgradeTest extends UpgradeTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(ClusterMetadataMinorUpgradeTest.class);

    /**
     * This can currently only be run manually by building the 6.0-alpha2 dtest jar
     */
    @Ignore
    @Test
    public void disallowUpgradingMetadataVersionTest() throws Throwable
    {
        final int nodeCount = 3;
        Versions dtestVersions = Versions.find();
        Versions.Version earliestVersion = dtestVersions.get("6.0-alpha2");
        Consumer<IInstanceConfig> configUpdater = config -> config.with(Feature.NETWORK, Feature.GOSSIP)
                                                                  .set(Constants.KEY_DTEST_FULL_STARTUP, true)
                                                                  .set(Constants.KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN, false);
        Consumer<UpgradeableCluster.Builder> builderUpdater = builder -> builder.withInstanceInitializer(ClusterMetadataMinorUpgradeTest.BBInstaller::installUpgradeVersionBB);
        try (UpgradeableCluster cluster = UpgradeableCluster.create(nodeCount, earliestVersion, configUpdater, builderUpdater))
        {
            Versions.Version upgradeVersion = dtestVersions.getLatest(CURRENT);
            upgradeInstance(cluster.get(1), upgradeVersion);
            upgradeInstance(cluster.get(2), upgradeVersion);

            NodeToolResult res = cluster.get(2).nodetoolResult("move", "12345");
            res.asserts().failure();
            cluster.get(3).shutdown().get();
            cluster.get(3).setVersion(upgradeVersion);

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
            assertTrue(((IInvokableInstance) cluster.get(1)).callOnInstance(() -> ClusterMetadata.current().directory.versions.values().stream().allMatch((v) -> v.serializationVersion >= Version.V10.asInt())));
        }
    }

    private void upgradeInstance(IUpgradeableInstance instance, Versions.Version upgradeTo) throws ExecutionException, InterruptedException, TimeoutException
    {
        int instanceId = instance.config().num();
        logger.info("Shutting down instance {} to upgrade to {}", instanceId, upgradeTo.version);
        instance.shutdown(true).get(60, TimeUnit.SECONDS);
        logger.info("Starting instanceId {} on version {}", instanceId, upgradeTo.version);
        instance.setVersion(upgradeTo);
        instance.startup();
        logger.info("Started instanceId {}", instanceId);
    }

    public static class BBInstaller
    {
        public static void installUpgradeVersionBB(ClassLoader classLoader, Integer num)
        {
            if (num != 2)
                return;
            try
            {
                new ByteBuddy().rebase(MovementMap.class)
                               .method(named("asMap"))
                               .intercept(MethodDelegation.to(ClusterMetadataMinorUpgradeTest.BBInterceptor.class))
                               .make()
                               .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            }
            catch (NoClassDefFoundError noClassDefFoundError)
            {
                logger.info("... but no class def", noClassDefFoundError);
            }
            catch (Throwable tr)
            {
                logger.info("Unable to intercept upgradeFromVersion method", tr);
                throw tr;
            }
        }
    }

    public static class BBInterceptor
    {
        public static ImmutableMap<ReplicationParams, EndpointsByReplica> movementMap(@SuperCall Callable<ImmutableMap<ReplicationParams, EndpointsByReplica>> zuper) throws Exception
        {
            throw new RuntimeException("EXPECTED");
        }
    }
}
