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

package org.apache.cassandra.distributed.test.log.mso;

import java.util.Collection;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;

import org.junit.Test;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.sequences.Move;
import org.apache.cassandra.tcm.sequences.SequenceState;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.junit.Assert.assertEquals;

public class AllowIPChangeWithMoveTest extends IPChangeWithMSOBase
{
    static int TO_MOVE = 5;
    @Test
    public void testMove() throws Exception
    {
        runTest(BBHelper::install,
                (cluster) -> {
                    long token = cluster.get(TO_MOVE).callOnInstance(() -> {
                        ClusterMetadata metadata = ClusterMetadata.current();
                        Collection<Token> tokens = metadata.tokenMap.tokens(metadata.myNodeId());
                        return tokens.iterator().next().getLongValue();
                    });
                    cluster.get(TO_MOVE).nodetoolResult("move", String.valueOf(token - 1000)).asserts().failure();
                },
                (cluster) -> {
                    cluster.get(TO_MOVE + 6).runOnInstance(() -> {
                        ClusterMetadata metadata = ClusterMetadata.current();
                        assertEquals(NodeState.MOVING, metadata.directory.peerState(metadata.myNodeId()));
                    });
                    // resume decommission
                    cluster.get(TO_MOVE + 6).nodetoolResult("move", "--resume").asserts().success();
                });
    }

    public static class BBHelper
    {
        public static AtomicBoolean enabled = new AtomicBoolean(true);
        public static AtomicInteger cnt = new AtomicInteger();
        // step 1 = after StartLeave, step 2 = after MidLeave
        private static final int failStep = new Random().nextInt(2) + 1;
        public static void install(ClassLoader cl, int i)
        {
            if (i != TO_MOVE)
               return;

            new ByteBuddy().rebase(Move.class)
                           .method(named("executeNext"))
                           .intercept(MethodDelegation.to(BBHelper.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static SequenceState executeNext(@SuperCall Callable<SequenceState> zuper) throws Exception
        {
            if (enabled.get())
            {
                if (cnt.incrementAndGet() > failStep)
                    throw new RuntimeException("EXPECTED");
            }
            return zuper.call();
        }
    }
}
