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

package org.apache.cassandra.distributed.test.accord.journal;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.durability.DurabilityService;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.utils.async.AsyncChains;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.index.accord.RouteJournalIndex;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.RetryStrategy;
import org.apache.cassandra.service.accord.AccordJournalTable;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.utils.Shared;
import org.apache.cassandra.utils.concurrent.CountDownLatch;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.schema.SchemaConstants.ACCORD_KEYSPACE_NAME;
import static org.apache.cassandra.service.accord.AccordKeyspace.JOURNAL;

/**
 * This is a specific history of {@link StatefulJournalRestartTest}, which offers a more general way of testing this
 */
public class JournalAccessRouteIndexOnStartupRaceTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(JournalAccessRouteIndexOnStartupRaceTest.class);

    @Test
    public void test() throws IOException
    {
        try (Cluster cluster = Cluster.build(1).withInstanceInitializer(BBHelper::install).start())
        {
            IInvokableInstance node = cluster.get(1);
            node.nodetoolResult("disableautocompaction", ACCORD_KEYSPACE_NAME, JOURNAL).asserts().success();

            init(cluster);
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl(pk int primary key) WITH " + TransactionalMode.full.asCqlParam()));
            ClusterUtils.awaitAccordEpochReady(cluster, ClusterUtils.getCurrentEpoch(node).getEpoch());

            insert(node, KEYSPACE, "tbl");

            logger.info("Restarting instance with blocked 2i, triggering race condition");
            ClusterUtils.stopUnchecked(node);
            State.block();
            node.startup();
        }
    }

    private static void insert(IInvokableInstance node, String ks, String table)
    {
        node.runOnInstance(() -> {
            AccordService accord = (AccordService) AccordService.instance();
            TableMetadata metadata = Keyspace.open(ks).getColumnFamilyStore(table).metadata();
            Ranges ranges = Ranges.single(TokenRange.fullRange(metadata.id, metadata.partitioner));
            for (int i = 0; i < 10; i++)
            {
                AsyncChains.getBlockingAndRethrow(accord.sync(null, Timestamp.NONE, ranges, null, DurabilityService.SyncLocal.Self, DurabilityService.SyncRemote.Quorum, 10L, TimeUnit.MINUTES));

                accord.journal().closeCurrentSegmentForTestingIfNonEmpty();
                accord.journal().runCompactorForTesting();
            }
        });
    }

    @Shared
    public static class State
    {
        private static volatile CountDownLatch LATCH = null;

        public static void block()
        {
            LATCH = CountDownLatch.newCountDownLatch(1);
        }

        public static void unblock()
        {
            if (LATCH == null) return;
            LATCH.decrement();
            LATCH = null;
        }

        public static void fromServer()
        {
            CountDownLatch latch = LATCH;
            if (latch == null) return;
            latch.awaitThrowUncheckedOnInterrupt();
        }
    }

    public static class BBHelper
    {
        public static void install(ClassLoader cl, int id)
        {
            new ByteBuddy().rebase(RouteJournalIndex.class)
                           .method(named("getInitializationTask"))
                           .intercept(MethodDelegation.to(BBHelper.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);

            new ByteBuddy().rebase(AccordJournalTable.class)
                           .method(named("maybeWait"))
                           .intercept(MethodDelegation.to(BBHelper.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static Callable<?> getInitializationTask(@SuperCall Callable<Callable<?>> zuper) throws Exception
        {
            Callable<?> delegate = zuper.call();
            return () -> {
                State.fromServer();
                return delegate.call();
            };
        }

        public static void maybeWait(RetryStrategy retry, int i, @SuperCall Runnable zuper)
        {
            if (i > 10)
                State.unblock();
            zuper.run();
        }
    }
}
