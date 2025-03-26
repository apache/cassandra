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

package org.apache.cassandra.distributed.test.tcm;

import java.io.IOException;

import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.transformations.CustomTransformation;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

public class FailureDetectorRecomputeTest extends TestBaseImpl
{
    @Test
    public void readTest() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withInstanceInitializer(BB::install)
                                           .start()))
        {
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key)"));
            cluster.get(1).runOnInstance(() -> BB.enabled.set(true));
            for (int i = 0; i < 10; i++)
                cluster.coordinator(1).execute(withKeyspace("select * from %s.tbl where id=?"), ConsistencyLevel.QUORUM, i);
        }
    }

    @Test
    public void writeTest() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withInstanceInitializer(BB::install)
                                           .start()))
        {
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key)"));
            cluster.get(1).runOnInstance(() -> BB.enabled.set(true));
            for (int i = 0; i < 10; i++)
                cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (id) values (?)"), ConsistencyLevel.QUORUM, i);
        }
    }

    public static class BB
    {
        public static AtomicBoolean enabled = new AtomicBoolean();

        public static void install(ClassLoader cl, int i)
        {
            new ByteBuddy().rebase(FailureDetector.class)
                           .method(named("isAlive").and(takesArguments(1)))
                           .intercept(MethodDelegation.to(FailureDetectorRecomputeTest.BB.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);

            new ByteBuddy().rebase(ReplicaPlan.AbstractForRead.class)
                           .method(named("stillAppliesTo").and(takesArguments(1)))
                           .intercept(MethodDelegation.to(FailureDetectorRecomputeTest.BB.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        static int downNode = 1;
        public static boolean isAlive(InetAddressAndPort ep)
        {
            if (!enabled.get())
                return true;
            enabled.set(false);
            ClusterMetadataService.instance().commit(CustomTransformation.make("hello"));
            enabled.set(true);
            return !ep.equals(InetAddressAndPort.getByNameUnchecked("127.0.0." + ((downNode % 3) + 1)));
        }

        public static boolean stillAppliesTo(ClusterMetadata metadata, @SuperCall Callable<Boolean> zuper) throws Exception
        {
            if (!enabled.get())
                return true;
            downNode++;
            return zuper.call();
        }
    }
}
