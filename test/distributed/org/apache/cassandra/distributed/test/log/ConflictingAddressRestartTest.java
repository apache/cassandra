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

package org.apache.cassandra.distributed.test.log;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.junit.Assert.assertTrue;

public class ConflictingAddressRestartTest extends TestBaseImpl
{
    @Test
    public void testRegisterWithConflictingAddress() throws IOException, ExecutionException, InterruptedException
    {
        try (Cluster cluster = init(builder()
                                    .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(3))
                                    .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(3, "dc0", "rack0"))
                                    .withConfig(config -> config.with(GOSSIP))
                                    .withNodes(2).start()))
        {
            cluster.get(2).shutdown().get();

            IInstanceConfig config = cluster.newInstanceConfig();
            config.set("listen_address", cluster.get(2).config().get("listen_address"));
            IInvokableInstance newInstance = cluster.bootstrap(config);
            try
            {
                newInstance.startup();
            }
            catch (Exception e)
            {
                assertTrue(e.getMessage().contains("conflicts with existing node"));
            }
        }
    }

    @Test
    @Ignore //todo; fails due to not being able to restart nodes, figure that out
    public void testStartupWithConflictingAddress() throws IOException, ExecutionException, InterruptedException
    {
        try (Cluster cluster = init(builder()
                                    .withConfig(config -> config.with(GOSSIP))
                                    .withNodes(2).start()))
        {
            cluster.get(2).shutdown().get();
            cluster.get(2).config().set("listen_address", cluster.get(1).config().get("listen_address"));
            try
            {
                cluster.get(2).startup();
            }
            catch (Exception e)
            {
                assertTrue(e.getMessage().contains("conflicts with existing node"));
            }
        }
    }
}
