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

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.PaxosBackedProcessor;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.transformations.TriggerSnapshot;
import org.apache.cassandra.utils.Shared;

import static net.bytebuddy.matcher.ElementMatchers.named;

public class CMSShutdownTest extends TestBaseImpl
{
    @Test
    public void shutdownCMSCoincidingWithUnsuccessfulCommit() throws Exception
    {
        // This test simulates a CMS node attempting to commit an entry to the log but being unable
        // to obtain consensus from other CMS members while it is also shutting down itself.
        try (Cluster cluster = Cluster.build(2)
                                      .withConfig(c -> c.with(Feature.values()))
                                      .withInstanceInitializer(BBHelper::install)
                                      .start())
        {
            cluster.get(1).runOnInstance(CommitHelper::scheduleCommits);
            State.latch.await();
        }
    }

    private static class CommitHelper
    {
        public static void commitTransformations()
        {
            // Continuously attempt to commit a log entry, meanwhile counting down the
            // latch ensures that every commit will fail as if unable to obtain consensus
            // from other CMS members
            State.latch.countDown();
            for (; ;)
                ClusterMetadataService.instance().commit(TriggerSnapshot.instance);
        }

        public static void scheduleCommits()
        {
            Stage.MISC.execute(CommitHelper::commitTransformations);
        }
    }

    @Shared
    public static class State
    {
        public static final CountDownLatch latch = new CountDownLatch(1);
    }

    public static class BBHelper
    {
        static void install(ClassLoader cl, int node)
        {
            if (node != 1) return;
            new ByteBuddy().rebase(PaxosBackedProcessor.class)
                           .method(named("tryCommitOne"))
                           .intercept(MethodDelegation.to(BBHelper.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static boolean tryCommitOne(Entry.Id entryId, Transformation transform, Epoch previousEpoch, Epoch nextEpoch, @SuperCall Callable<Boolean> call) throws Exception
        {
            if (State.latch.getCount() == 1)
                return call.call();
            return false;
        }
    }
}
