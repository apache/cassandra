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

package org.apache.cassandra.distributed.test.hostreplacement;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertTrue;

public class AssassinateCMSNodeTest extends TestBaseImpl
{
    @Test
    public void assassinateCMSNodeTest() throws IOException, ExecutionException, InterruptedException
    {
        try (Cluster cluster = init(builder().withNodes(3)
                                        .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                        .start()))
        {
            cluster.get(1).nodetoolResult("cms", "reconfigure", "3").asserts().success();
            InetSocketAddress toAssassinate = cluster.get(2).broadcastAddress();
            cluster.get(2).shutdown().get();
            cluster.get(1).nodetoolResult("assassinate", toAssassinate.getHostString()).asserts().success();
            cluster.get(1).runOnInstance(() -> assertTrue(ClusterMetadata.current().isCMSMember(FBUtilities.getBroadcastAddressAndPort())));
            cluster.get(3).runOnInstance(() -> assertTrue(ClusterMetadata.current().isCMSMember(FBUtilities.getBroadcastAddressAndPort())));
            cluster.get(1).nodetoolResult("cms").asserts().success();
        }
    }
}
