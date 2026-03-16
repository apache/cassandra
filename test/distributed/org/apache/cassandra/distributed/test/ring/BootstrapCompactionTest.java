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

package org.apache.cassandra.distributed.test.ring;

import java.util.concurrent.Callable;

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.tcm.sequences.BootstrapAndJoin;
import org.apache.cassandra.tcm.sequences.SequenceState;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertTrue;

public class BootstrapCompactionTest extends TestBaseImpl
{
    @Test
    public void testCompactionEnabledDuringBootstrap() throws Exception
    {
        int originalNodeCount = 2;
        int expandedNodeCount = originalNodeCount + 1;

        try (Cluster cluster = init(builder().withNodes(originalNodeCount)
                                             .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(expandedNodeCount))
                                             .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                             .withInstanceInitializer(BB::install)
                                             .withConfig(config -> config.with(NETWORK, GOSSIP))
                                             .start()))
        {
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key)"));
            IInstanceConfig config = cluster.newInstanceConfig()
                                            .set(Constants.KEY_DTEST_FULL_STARTUP, true)
                                            .set("auto_bootstrap", true);

            IInvokableInstance newInstance = cluster.bootstrap(config);
            // BB below asserts that autocompaction is enabled at each step in the join sequence
            newInstance.startup(cluster);
        }
    }

    public static class BB
    {
        public static void install(ClassLoader cl, int i)
        {
            if (i == 3)
            {
                new ByteBuddy().rebase(BootstrapAndJoin.class)
                               .method(named("executeNext"))
                               .intercept(MethodDelegation.to(BB.class))
                               .make()
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        public static SequenceState executeNext(@SuperCall Callable<SequenceState> zuper) throws Exception
        {
            boolean isEnabled = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").getCompactionStrategyManager().isEnabled();
            assertTrue("Autocompaction should be enabled during the bootstrap", isEnabled);
            return zuper.call();
        }
    }
}
