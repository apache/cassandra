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
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.sequences.ReconfigureCMS;
import org.apache.cassandra.tcm.transformations.cms.PrepareCMSReconfiguration;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class ReconfigureCMSStreamingFailureTest extends TestBaseImpl
{
    @Test
    public void testNodetoolFailureWhenStreamingErrorOccurs() throws IOException, ExecutionException, InterruptedException
    {
        try (Cluster cluster = init(builder().withNodes(3)
                                             .withConfig(c -> c.with(Feature.NETWORK))
                                             .withInstanceInitializer(BB::install)
                                             .start()))
        {
            cluster.forEach(i -> i.runOnInstance(() -> BB.failStreaming.set(true)));
            cluster.get(1)
                   .nodetoolResult("cms", "reconfigure", "3")
                   .asserts()
                   .failure()
                   .stderrContains("Some data streaming failed. Use nodetool to check CMS reconfiguration state and resume.");
            String status = cluster.get(1).nodetoolResult("cms", "reconfigure", "--status").getStdout();
            assertTrue(status.contains("ACTIVE: [/127.0.0"));
            assertTrue(status.contains("ADDITIONS: [/127.0.0"));
            cluster.forEach(i -> assertTrue(i.callOnInstance(() -> PrepareCMSReconfiguration.needsReconfiguration(ClusterMetadata.current()))));

            cluster.forEach(i -> i.runOnInstance(() -> BB.failStreaming.set(false)));
            cluster.get(1).nodetoolResult("cms", "reconfigure", "--resume").asserts().success();
            cluster.forEach(i -> assertFalse(i.callOnInstance(() -> PrepareCMSReconfiguration.needsReconfiguration(ClusterMetadata.current()))));
            assertTrue(cluster.get(1).callOnInstance(() -> {
                ClusterMetadata metadata = ClusterMetadata.current();
                return metadata.fullCMSMemberIds().containsAll(metadata.directory.peerIds());
            }));
        }
    }

    public static class BB
    {
        public static AtomicBoolean failStreaming = new AtomicBoolean(false);
        public static void install(ClassLoader cl, int i)
        {
            new ByteBuddy().rebase(ReconfigureCMS.class)
                           .method(named("streamRanges"))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static void streamRanges(Replica replicaForStreaming,
                                        Set<InetAddressAndPort> streamCandidates,
                                        @SuperCall Callable<Void> zuper) throws Exception
        {
            if (failStreaming.get())
                throw new IOException("failed to connect to " + replicaForStreaming.endpoint() + " for streaming data");

            zuper.call();
        }
    }
}
