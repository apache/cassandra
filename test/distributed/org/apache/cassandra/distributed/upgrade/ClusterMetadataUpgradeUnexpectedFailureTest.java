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

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;

import org.junit.Test;

import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.migration.Election;

import static net.bytebuddy.matcher.ElementMatchers.named;

public class ClusterMetadataUpgradeUnexpectedFailureTest extends UpgradeTestBase
{
    @Test
    public void upgradeFailsUnexpectedlyAfterInitiation() throws Throwable
    {
        Consumer<UpgradeableCluster.Builder > builderUpdater = builder -> builder.withInstanceInitializer(BBInstaller::installUpgradeVersionBB);
        new TestCase()
                .nodes(3)
                .nodesToUpgrade(1, 2, 3)
                .withConfig((cfg) -> cfg.with(Feature.NETWORK, Feature.GOSSIP)
                        .set(Constants.KEY_DTEST_FULL_STARTUP, true))
                .upgradesToCurrentFrom(v41)
                .withBuilder(builderUpdater)
                .setup((cluster) -> {
                    cluster.schemaChange(withKeyspace("ALTER KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor':2}"));
                    cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
                })
                .runAfterClusterUpgrade((cluster) -> {
                    // we injected a BB helper to trigger an unexpected failure on the first attempt at initialization.
                    // i.e. an exception that must be caught as opposed to a mismatch in metadata or down peer.
                    cluster.get(1).nodetoolResult("cms", "initialize").asserts().failure().errorContains("Something unexpected went wrong");
                    // handling the failure should have included cleaning up any state so that another attempt can be
                    // made, which this time should succeed.
                    cluster.get(1).nodetoolResult("cms", "initialize").asserts().success();
                }).run();
    }

    public static class BBInstaller
    {
        public static void installUpgradeVersionBB(ClassLoader classLoader, Integer num)
        {
            try
            {
                new ByteBuddy().rebase(Election.class)
                               .method(named("finish"))
                               .intercept(MethodDelegation.to(BBInterceptor.class))
                               .make()
                               .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            }
            catch (NoClassDefFoundError noClassDefFoundError)
            {
                throw noClassDefFoundError;
            }
            catch (Throwable tr)
            {
                throw tr;
            }
        }
    }

    public static class BBInterceptor
    {
        private static final AtomicBoolean firstAttempt = new AtomicBoolean(true);

        @SuppressWarnings("unused")
        public static void finish(Set<InetAddressAndPort> sendTo, @SuperCall Callable<Void> zuper)
        {
            if (firstAttempt.getAndSet(false))
                throw new IllegalStateException("Something unexpected went wrong");

            try
            {
                zuper.call();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}
