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

package org.apache.cassandra.distributed.test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.metrics.SlotGroupingMetrics;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.StorageService;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.junit.Assert.fail;

/**
 * Base class for replica slot grouping tests.
 *
 * Provides shared constants, ByteBuddy helpers, and utility methods used by both
 * {@link ReplicaSlotGroupingTest} (topology scenarios) and
 * {@link ReplicaSlotGroupingAdvancedTest} (ack permutations, failure handling, Paxos).
 */
public abstract class ReplicaSlotGroupingTestBase extends TestBaseImpl
{
    protected static final Logger logger = LoggerFactory.getLogger(ReplicaSlotGroupingTestBase.class);
    protected static final String KEYSPACE = "replica_slot_grouping_test";

    /** Verbs to block when simulating a node being unreachable (mutations + Paxos V2 phases). */
    protected static final int[] BLOCK_NODE_VERBS = {
        Verb.MUTATION_REQ.id,
        Verb.PAXOS2_PREPARE_REQ.id, Verb.PAXOS2_PROPOSE_REQ.id,
        Verb.PAXOS2_COMMIT_AND_PREPARE_REQ.id, Verb.PAXOS_COMMIT_REQ.id
    };

    /**
     * ByteBuddy helper to keep bootstrapping nodes in PENDING/JOINING state.
     * Configure which nodes to intercept via {@link #targetNodes} before building the cluster.
     */
    public static class BootstrapBB
    {
        public static final AtomicBoolean keepNodeInPendingState = new AtomicBoolean(true);
        public static final Set<Integer> targetNodes = new HashSet<>(Arrays.asList(4));

        public static void resetForNodes(Integer... nodes)
        {
            targetNodes.clear();
            targetNodes.addAll(Arrays.asList(nodes));
            keepNodeInPendingState.set(true);
        }

        public static void install(ClassLoader cl, Integer nodeNum)
        {
            if (!targetNodes.contains(nodeNum))
                return;
            new ByteBuddy().
                rebase(StorageService.class).
                method(named("bootstrapFinished")).
                intercept(MethodDelegation.to(BootstrapBB.class)).
                make().
                load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static void bootstrapFinished(@SuperCall Callable<Boolean> zuper) throws Exception
        {
            while (keepNodeInPendingState.get())
            {
                logger.info("Keeping node in pending state by throwing error");
                throw new RuntimeException("Keep node in joining state");
            }
            logger.info("Done keeping node in pending state. Node will finish join soon.");
            zuper.call();
        }
    }

    protected void setSlotGroupingEnabled(Cluster cluster, boolean enabled)
    {
        cluster.forEach(instance ->
            instance.runOnInstance(() ->
                DatabaseDescriptor.setReplicaSlotGroupingEnabled(enabled)));
    }

    protected void setSlotGroupingEnabled(IInvokableInstance node, boolean enabled)
    {
        node.runOnInstance(() -> DatabaseDescriptor.setReplicaSlotGroupingEnabled(enabled));
    }

    protected static long getStaleFallbacks(IInvokableInstance node)
    {
        return node.callOnInstance(() -> SlotGroupingMetrics.staleFallbacks.getCount());
    }

    protected static long getConstraintViolations(IInvokableInstance node)
    {
        return node.callOnInstance(() -> SlotGroupingMetrics.constraintViolations.getCount());
    }

    protected void assertMetricsUnchanged(IInvokableInstance node, long staleBefore, long cvBefore, String scenario)
    {
        long staleAfter = getStaleFallbacks(node);
        long cvAfter = getConstraintViolations(node);
        Assert.assertEquals("StaleFallbacks should not change during " + scenario, staleBefore, staleAfter);
        Assert.assertEquals("ConstraintViolations should not change during " + scenario, cvBefore, cvAfter);
        logger.info("Metrics unchanged during {}: stale={}, cv={}", scenario, staleAfter, cvAfter);
    }

    protected static int pkInTransitioningRange(Cluster cluster)
    {
        return pkInRange(cluster.get(3), cluster.get(4));
    }

    protected static int pkInRange(IInstance lb, IInstance ub)
    {
        return nextPkInRange(lb, ub, 0);
    }

    protected static int nextPkInRange(IInstance lb, IInstance ub, int startPk)
    {
        Token lbToken = Murmur3Partitioner.instance.getTokenFactory().
            fromString(lb.config().getString("initial_token"));
        Token ubToken = Murmur3Partitioner.instance.getTokenFactory().
            fromString(ub.config().getString("initial_token"));
        boolean wrapping = lbToken.compareTo(ubToken) > 0;
        int pk = startPk;
        int maxIterations = 2_000_000;
        for (int i = 0; i < maxIterations; i++, pk++)
        {
            Token pkt = Murmur3Partitioner.instance.getToken(Int32Type.instance.decompose(pk));
            boolean inRange;
            if (wrapping)
                inRange = lbToken.compareTo(pkt) < 0 || ubToken.compareTo(pkt) >= 0;
            else
                inRange = lbToken.compareTo(pkt) < 0 && ubToken.compareTo(pkt) >= 0;
            if (inRange)
                return pk;
        }
        throw new AssertionError("Could not find pk in range after " + maxIterations + " iterations");
    }

    protected void executeSuccessfulWrite(IInvokableInstance node, int pk, ConsistencyLevel cl) throws Exception
    {
        node.coordinator().execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, ?, ?)",
                                   cl, pk, 1, 1);
    }

    protected void executeWriteExpectingTimeout(IInvokableInstance node, int pk, ConsistencyLevel cl) throws Exception
    {
        try
        {
            node.coordinator().execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, ?, ?)",
                                       cl, pk, 1, 1);
            fail("Expected write to timeout but it succeeded");
        }
        catch (Exception e)
        {
            String name = e.getClass().getName();
            if (!name.contains("WriteTimeout") && !name.contains("WriteFailure"))
            {
                throw new AssertionError("Expected WriteTimeout or WriteFailure but got " + name, e);
            }
        }
    }

    protected void executeSuccessfulLwt(IInvokableInstance node, int pk) throws Exception
    {
        node.coordinator().execute(
            "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, ?, ?) IF NOT EXISTS",
            ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM, pk, 1, 1);
        node.coordinator().execute(
            "DELETE FROM " + KEYSPACE + ".tbl WHERE pk = ? AND ck = ? IF EXISTS",
            ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM, pk, 1);
    }

    protected void executeLwtExpectingTimeout(IInvokableInstance node, int pk) throws Exception
    {
        try
        {
            node.coordinator().execute(
                "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, ?, ?) IF NOT EXISTS",
                ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM, pk, 1, 1);
            fail("Expected LWT to timeout but it succeeded");
        }
        catch (Exception e)
        {
            String name = e.getClass().getName();
            if (!name.contains("CasWriteTimeout") &&
                !name.contains("WriteTimeout") &&
                !name.contains("WriteFailure"))
            {
                throw new AssertionError("Expected CAS/WriteTimeout or WriteFailure but got " + name, e);
            }
        }
    }

    protected void executeWriteExpectingFailureOrTimeout(IInvokableInstance node, int pk,
                                                         ConsistencyLevel cl) throws Exception
    {
        try
        {
            node.coordinator().execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, ?, ?)",
                                       cl, pk, 1, 1);
            fail("Expected write to fail but it succeeded");
        }
        catch (Exception e)
        {
            String name = e.getClass().getName();
            if (!name.contains("WriteFailure") && !name.contains("WriteTimeout"))
            {
                throw new AssertionError("Expected WriteFailure or WriteTimeout but got " + name, e);
            }
        }
    }

    protected void executeLwtExpectingFailureOrTimeout(IInvokableInstance node, int pk) throws Exception
    {
        try
        {
            node.coordinator().execute(
                "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, ?, ?) IF NOT EXISTS",
                ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM, pk, 1, 1);
            fail("Expected LWT to fail but it succeeded");
        }
        catch (Exception e)
        {
            String name = e.getClass().getName();
            if (!name.contains("WriteFailure") && !name.contains("WriteTimeout") &&
                !name.contains("CasWriteTimeout"))
            {
                throw new AssertionError("Expected WriteFailure or WriteTimeout but got " + name, e);
            }
        }
    }
}
