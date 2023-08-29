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
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.cql3.statements.schema.CreateKeyspaceStatement;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.log.FuzzTestBase;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.tcm.ClusterMetadata;

import static net.bytebuddy.matcher.ElementMatchers.named;

public class StopProcessingExceptionTest extends FuzzTestBase
{
    @Test
    public void stopProcessingExceptionTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(2)
                                        .withInstanceInitializer(BBFailHelper::install)
                                        .start())
        {
            long mark = cluster.get(2).logs().mark();
            cluster.get(2).runOnInstance(() -> BBFailHelper.enabled.set(true));
            cluster.coordinator(1).execute("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 2};",
                                           ConsistencyLevel.ALL);
            cluster.get(2).logs().watchFor(mark, "All subsequent epochs will be ignored");
        }
    }

    public static class BBFailHelper
    {
        static void install(ClassLoader cl, int nodeNumber)
        {
            if (nodeNumber == 2)
                new ByteBuddy().rebase(CreateKeyspaceStatement.class)
                               .method(named("apply"))
                               .intercept(MethodDelegation.to(BBFailHelper.class))
                               .make()
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static AtomicBoolean enabled = new AtomicBoolean(false);

        public static Keyspaces apply(ClusterMetadata metadata, @SuperCall Callable<Keyspaces> zuper) throws Exception
        {
            if (enabled.get())
                throw new RuntimeException();

            return zuper.call();
        }
    }
}
