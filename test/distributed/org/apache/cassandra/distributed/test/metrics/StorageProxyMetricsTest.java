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

package org.apache.cassandra.distributed.test.metrics;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import org.junit.Assert;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.StorageProxyMetricsManager;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.service.paxos.Paxos;
import org.apache.cassandra.service.paxos.PaxosCommit;
import org.apache.cassandra.service.paxos.PaxosPropose;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.net.Verb.PAXOS2_PREPARE_REQ;
import static org.apache.cassandra.net.Verb.PAXOS_PREPARE_REQ;

public class StorageProxyMetricsTest extends TestBaseImpl
{
    @Test
    public void paxosReadTimeoutTest() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(3).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))"));
            for (int i = 0; i < 10; i++)
                cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (pk, ck ,v) values (0, ?, 1)"), ConsistencyLevel.ALL, i);

            IMessageFilters.Filter filterPrepareV1 = cluster.filters().inbound().verbs(PAXOS_PREPARE_REQ.id).to(2, 3).drop();
            // Serial and non-serial reads have separates code paths, so exercise them both
            cluster.get(1).runOnInstance(() -> Paxos.setPaxosVariant(Config.PaxosVariant.v1));
            testPaxosReadTimeout(cluster);
            filterPrepareV1.off();

            IMessageFilters.Filter filterPrepareV2 = cluster.filters().inbound().verbs(PAXOS2_PREPARE_REQ.id).to(2, 3).drop();
            cluster.get(1).runOnInstance(() -> Paxos.setPaxosVariant(Config.PaxosVariant.v2));
            testPaxosReadTimeout(cluster);
            filterPrepareV2.off();
        }
    }

    @Test
    public void paxosLWTTimeoutTest() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(3).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))"));
            for (int i = 0; i < 10; i++)
                cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (pk, ck ,v) values (0, ?, 1)"), ConsistencyLevel.ALL, i);

            IMessageFilters.Filter filterPrepareV1 = cluster.filters().inbound().verbs(PAXOS_PREPARE_REQ.id).to(2, 3).drop();
            // Serial and non-serial reads have separates code paths, so exercise them both
            cluster.get(1).runOnInstance(() -> Paxos.setPaxosVariant(Config.PaxosVariant.v1));
            testLWTTimeout(cluster);
            filterPrepareV1.off();

            IMessageFilters.Filter filterPrepareV2 = cluster.filters().inbound().verbs(PAXOS2_PREPARE_REQ.id).to(2, 3).drop();
            cluster.get(1).runOnInstance(() -> Paxos.setPaxosVariant(Config.PaxosVariant.v2));
            testLWTTimeout(cluster);
            filterPrepareV2.off();
        }
    }

    @Test
    public void paxosLWTTimeoutOnMaybeSupercededTest() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(3).withInstanceInitializer(BBProposeMaybeSideEffectsSupersededStatus::install).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))"));
            for (int i = 0; i < 10; i++)
                cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (pk, ck ,v) values (0, ?, 1)"), ConsistencyLevel.ALL, i);

            cluster.get(1).runOnInstance(() -> Paxos.setPaxosVariant(Config.PaxosVariant.v2));
            testLWTTimeout(cluster);
        }
    }

    @Test
    public void paxosLWTProposeFailuresTest() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(3).withInstanceInitializer(BBProposeFailureStatus::install).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))"));
            for (int i = 0; i < 10; i++)
                cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (pk, ck ,v) values (0, ?, 1)"), ConsistencyLevel.ALL, i);

            cluster.get(1).runOnInstance(() -> Paxos.setPaxosVariant(Config.PaxosVariant.v2));
            testLWTFailure(cluster);
        }
    }

    @Test
    public void paxosLWTCommitFailuresTest() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(3).withInstanceInitializer(BBCommitFailureStatus::install).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))"));
            for (int i = 0; i < 10; i++)
                cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (pk, ck ,v) values (0, ?, 1)"), ConsistencyLevel.ALL, i);

            cluster.get(1).runOnInstance(() -> Paxos.setPaxosVariant(Config.PaxosVariant.v2));
            testLWTFailure(cluster);
        }
    }

    public void testPaxosReadTimeout(Cluster cluster)
    {
        String readQuery = withKeyspace("SELECT pk,ck,v from %s.tbl WHERE pk=0");
        long countBefore = cluster.get(1).callOnInstance(() ->
                                                         StorageProxyMetricsManager.getMetrics(KEYSPACE, org.apache.cassandra.db.ConsistencyLevel.SERIAL).casReadMetrics.timeouts.getCount());
        try
        {
            cluster.coordinator(1).execute(readQuery, ConsistencyLevel.SERIAL);
        }
        catch (Exception e)
        {
            // expected
        }
        long countAfter = cluster.get(1).callOnInstance(() ->
                                                        StorageProxyMetricsManager.getMetrics(KEYSPACE, org.apache.cassandra.db.ConsistencyLevel.SERIAL).casReadMetrics.timeouts.getCount());
        Assert.assertEquals(1, countAfter - countBefore);
    }

    public void testLWTTimeout(Cluster cluster)
    {
        String lwtQuery = withKeyspace("UPDATE %s.tbl SET v=10 WHERE pk=0 AND ck=1 IF EXISTS");
        long countBefore = cluster.get(1).callOnInstance(() ->
                                                         StorageProxyMetricsManager.getMetrics(KEYSPACE, org.apache.cassandra.db.ConsistencyLevel.SERIAL).casWriteMetrics.timeouts.getCount());
        try
        {
            cluster.coordinator(1).execute(lwtQuery, ConsistencyLevel.LOCAL_QUORUM);
        }
        catch (Exception e)
        {
            // expected
        }
        long countAfter = cluster.get(1).callOnInstance(() ->
                                                        StorageProxyMetricsManager.getMetrics(KEYSPACE, org.apache.cassandra.db.ConsistencyLevel.SERIAL).casWriteMetrics.timeouts.getCount());
        Assert.assertEquals(1, countAfter - countBefore);
    }

    public void testLWTFailure(Cluster cluster)
    {
        String lwtQuery = withKeyspace("UPDATE %s.tbl SET v=10 WHERE pk=0 AND ck=1 IF EXISTS");
        long countBefore = cluster.get(1).callOnInstance(() ->
                                                         StorageProxyMetricsManager.getMetrics(KEYSPACE, org.apache.cassandra.db.ConsistencyLevel.SERIAL).casWriteMetrics.failures.getCount());
        try
        {
            cluster.coordinator(1).execute(lwtQuery, ConsistencyLevel.LOCAL_QUORUM);
        }
        catch (Exception e)
        {
            // expected
        }
        long countAfter = cluster.get(1).callOnInstance(() ->
                                                        StorageProxyMetricsManager.getMetrics(KEYSPACE, org.apache.cassandra.db.ConsistencyLevel.SERIAL).casWriteMetrics.failures.getCount());
        Assert.assertEquals(1, countAfter - countBefore);
    }

    public static class BBProposeMaybeSideEffectsSupersededStatus
    {
        private static void install(ClassLoader cl, Integer instanceId)
        {
            new ByteBuddy().rebase(PaxosPropose.class)
                           .method(named("status"))
                           .intercept(MethodDelegation.to(StorageProxyMetricsTest.BBProposeMaybeSideEffectsSupersededStatus.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static PaxosPropose.Status status(@SuperCall Callable<PaxosPropose.Status> zuper) throws Exception
        {
            return new PaxosPropose.Superseded(Ballot.none(), PaxosPropose.Superseded.SideEffects.MAYBE);
        }
    }

    public static class BBProposeFailureStatus
    {
        private static final Map<InetAddressAndPort, RequestFailureReason> failures = new HashMap<>();

        private static void install(ClassLoader cl, Integer instanceId)
        {
            new ByteBuddy().rebase(PaxosPropose.class)
                           .method(named("status"))
                           .intercept(MethodDelegation.to(StorageProxyMetricsTest.BBProposeFailureStatus.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static PaxosPropose.Status status(@SuperCall Callable<PaxosPropose.Status> zuper) throws Exception
        {
            failures.put(InetAddressAndPort.getByName("127.0.0.1"), RequestFailureReason.UNKNOWN);
            failures.put(InetAddressAndPort.getByName("127.0.0.2"), RequestFailureReason.UNKNOWN);
            failures.put(InetAddressAndPort.getByName("127.0.0.3"), RequestFailureReason.UNKNOWN);
            return new PaxosPropose.MaybeFailure(new Paxos.MaybeFailure(true, 3, 2, 0, failures));
        }
    }

    public static class BBCommitFailureStatus
    {
        private static final Map<InetAddressAndPort, RequestFailureReason> failures = new HashMap<>();

        private static void install(ClassLoader cl, Integer instanceId)
        {
            new ByteBuddy().rebase(PaxosCommit.class)
                           .method(named("status"))
                           .intercept(MethodDelegation.to(StorageProxyMetricsTest.BBCommitFailureStatus.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static PaxosCommit.Status status(@SuperCall Callable<PaxosCommit.Status> zuper) throws Exception
        {
            failures.put(InetAddressAndPort.getByName("127.0.0.1"), RequestFailureReason.UNKNOWN);
            failures.put(InetAddressAndPort.getByName("127.0.0.2"), RequestFailureReason.UNKNOWN);
            failures.put(InetAddressAndPort.getByName("127.0.0.3"), RequestFailureReason.UNKNOWN);
            return new PaxosCommit.Status(new Paxos.MaybeFailure(true, 3, 2, 0, failures));
        }
    }
}
