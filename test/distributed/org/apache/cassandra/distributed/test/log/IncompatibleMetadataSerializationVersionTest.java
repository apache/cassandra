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

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.serialization.Version;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.tcm.membership.NodeVersion.CURRENT_METADATA_VERSION;

public class IncompatibleMetadataSerializationVersionTest extends TestBaseImpl
{
    @Test
    public void incompatibleVersionsCauseStartupFailureTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(2)
                                        .withInstanceInitializer(BB::install)
                                        .createWithoutStarting())
        {
            cluster.get(1).startup();
            // node1 has joined as normal so any entries committed to the metadata log will be serialized with
            // NodeVersion.CURRENT_METADATA_VERSION. We will join node2, but the BB class used as an instanceInitializer
            // will force it not to recognise this version. This simulates a node running an older, incompatible version
            // attempting to join the cluster and should fail as the metadata log and snapshots it receives at startup
            // are unreadable to it.
            // We'll also set up the uncaught exceptions filter so that errors reported by node2 do not automatically
            // trigger a failure, so that we can assert that the specific error we're expecting is thrown and logged.
            cluster.setUncaughtExceptionsFilter((i, t) -> i != 2);
            try
            {
                cluster.get(2).startup();
                Assert.fail("Node2 startup should fail due to unsupported metadata versions");
            }
            catch (Exception e)
            {
                String expectedError = String.format("Unsupported metadata version \\(%s\\)", CURRENT_METADATA_VERSION.asInt());
                Assert.assertFalse(cluster.get(2)
                                          .logs()
                                          .grep(expectedError)
                                          .getResult()
                                          .isEmpty());
            }
        }
    }

    public static class BB
    {
        static void install(ClassLoader cl, int node)
        {
            // only change behaviour of node2
            if (node == 2)
            {
                new ByteBuddy().rebase(Version.class)
                               .method(named("fromInt"))
                               .intercept(MethodDelegation.to(BB.class))
                               .make()
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);

                new ByteBuddy().rebase(NodeVersion.class)
                               .method(named("serializationVersion"))
                               .intercept(MethodDelegation.to(BB.class))
                               .make()
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        public static Version serializationVersion()
        {
            // This is called during node startup when initializing the LogState class and in particular its static
            // defaultMessageSerializer field. We will emulate the behaviour of a node running an old version.
            return Version.V0;
        }

        public static Version fromInt(int i)
        {
            // Behave as if the supplied version is invalid, unless it is the V0 value we are returning from the other
            // intercepted method. This will cause any other version encountered, such as when receiving versioned log
            // entries from another node, to appear unreadable.
            if (i == Version.V0.asInt())
                return Version.V0;

            throw new IllegalArgumentException("Unsupported metadata version (" + i + ")");
        }
    }

}
