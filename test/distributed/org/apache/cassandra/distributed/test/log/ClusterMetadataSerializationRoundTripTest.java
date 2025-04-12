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

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.NodeVersion;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

public class ClusterMetadataSerializationRoundTripTest extends TestBaseImpl
{
    @Test
    public void testEmptyPlacements() throws IOException
    {
        try (Cluster cluster = init(builder().withNodes(1)
                                             .withConfig(c -> c.with(Feature.NETWORK, Feature.GOSSIP))
                                             .withInstanceInitializer(BBHelper::install)
                                             .start()))
        {
            cluster.schemaChange("create keyspace x with replication = { 'class': 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'dc1': '3'}");
            cluster.get(1).runOnInstance(() -> {
                Epoch epoch = ClusterMetadata.current().epoch;
                try
                {
                    String dump = ClusterMetadataService.instance().dumpClusterMetadata(epoch, epoch, NodeVersion.CURRENT_METADATA_VERSION);
                    ClusterMetadataService.instance().loadClusterMetadata(dump);
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    // shame, allow us to create a keyspace with a bad replication strategy
    public static class BBHelper
    {
        public static void install(ClassLoader cl, int i)
        {
            new ByteBuddy().rebase(ReplicationParams.class)
                           .method(named("validate").and(takesArguments(3)))
                           .intercept(MethodDelegation.to(BBHelper.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
            new ByteBuddy().rebase(NetworkTopologyStrategy.class)
                           .method(named("validateExpectedOptions").and(takesArguments(1)))
                           .intercept(MethodDelegation.to(BBHelper.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static void validate(String name, ClientState state, ClusterMetadata metadata) {}

        public static void validateExpectedOptions(ClusterMetadata metadata) throws ConfigurationException {}
    }
}
