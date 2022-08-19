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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.implementation.bind.annotation.SuperMethod;
import net.bytebuddy.implementation.bind.annotation.This;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.utils.AssertionUtils;
import org.apache.cassandra.exceptions.CasWriteTimeoutException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.paxos.Paxos;
import org.apache.cassandra.utils.concurrent.Awaitable;
import org.apache.cassandra.utils.concurrent.Condition;
import org.assertj.core.api.Assertions;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.utils.AssertionUtils.isThrowable;

public class RequestTimeoutTest extends TestBaseImpl
{
    private static final AtomicInteger NEXT = new AtomicInteger(0);
    public static final int COORDINATOR = 1;
    private static Cluster CLUSTER;

    @BeforeClass
    public static void init() throws IOException
    {
        CLUSTER = Cluster.build(3)
                         .withConfig(c -> c.set("truncate_request_timeout", "10s"))
                         .withInstanceInitializer(BB::install)
                         .start();
        init(CLUSTER);
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, v int)"));
    }

    @AfterClass
    public static void cleanup()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Before
    public void before()
    {
        CLUSTER.get(COORDINATOR).runOnInstance(() -> MessagingService.instance().callbacks.unsafeClear());
        CLUSTER.filters().reset();
        BB.reset();
    }

    @Test
    public void insert()
    {
        CLUSTER.filters().verbs(Verb.MUTATION_REQ.id).to(2).drop();
        Assertions.assertThatThrownBy(() -> CLUSTER.coordinator(COORDINATOR).execute(withKeyspace("INSERT INTO %s.tbl (pk, v) VALUES (?, ?)"), ConsistencyLevel.ALL, NEXT.getAndIncrement(), NEXT.getAndIncrement()))
                  .is(isThrowable(WriteTimeoutException.class));
        BB.assertIsTimeoutTrue();
    }

    @Test
    public void update()
    {
        CLUSTER.filters().verbs(Verb.MUTATION_REQ.id).to(2).drop();
        Assertions.assertThatThrownBy(() -> CLUSTER.coordinator(COORDINATOR).execute(withKeyspace("UPDATE %s.tbl SET v=? WHERE pk=?"), ConsistencyLevel.ALL, NEXT.getAndIncrement(), NEXT.getAndIncrement()))
                  .is(isThrowable(WriteTimeoutException.class));
        BB.assertIsTimeoutTrue();
    }

    @Test
    public void batchInsert()
    {
        CLUSTER.filters().verbs(Verb.MUTATION_REQ.id).to(2).drop();
        Assertions.assertThatThrownBy(() -> CLUSTER.coordinator(COORDINATOR).execute(batch(withKeyspace("INSERT INTO %s.tbl (pk, v) VALUES (?, ?)")), ConsistencyLevel.ALL, NEXT.getAndIncrement(), NEXT.getAndIncrement()))
                  .is(isThrowable(WriteTimeoutException.class));
        BB.assertIsTimeoutTrue();
    }

    @Test
    public void rangeSelect()
    {
        CLUSTER.filters().verbs(Verb.RANGE_REQ.id).to(2).drop();
        Assertions.assertThatThrownBy(() -> CLUSTER.coordinator(COORDINATOR).execute(withKeyspace("SELECT * FROM %s.tbl"), ConsistencyLevel.ALL))
                  .is(isThrowable(ReadTimeoutException.class));
        BB.assertIsTimeoutTrue();
    }

    @Test
    public void select()
    {
        CLUSTER.filters().verbs(Verb.READ_REQ.id).to(2).drop();
        Assertions.assertThatThrownBy(() -> CLUSTER.coordinator(COORDINATOR).execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk=?"), ConsistencyLevel.ALL, NEXT.getAndIncrement()))
                  .is(isThrowable(ReadTimeoutException.class));
        BB.assertIsTimeoutTrue();
    }

    @Test
    public void truncate()
    {
        CLUSTER.filters().verbs(Verb.TRUNCATE_REQ.id).to(2).drop();
        Assertions.assertThatThrownBy(() -> CLUSTER.coordinator(COORDINATOR).execute(withKeyspace("TRUNCATE %s.tbl"), ConsistencyLevel.ALL))
                  .is(AssertionUtils.rootCauseIs(TimeoutException.class));
        BB.assertIsTimeoutTrue();
    }

    // don't call BB.assertIsTimeoutTrue(); for CAS, as it has its own logic

    @Test
    public void casV2PrepareInsert()
    {
        withPaxos(Config.PaxosVariant.v2);

        CLUSTER.filters().verbs(Verb.PAXOS2_PREPARE_REQ.id).to(2, 3).drop();
        Assertions.assertThatThrownBy(() -> CLUSTER.coordinator(COORDINATOR).execute(withKeyspace("INSERT INTO %s.tbl (pk, v) VALUES (?, ?) IF NOT EXISTS"), ConsistencyLevel.ALL, NEXT.getAndIncrement(), NEXT.getAndIncrement()))
                  .is(isThrowable(CasWriteTimeoutException.class));
    }

    @Test
    public void casV2PrepareSelect()
    {
        withPaxos(Config.PaxosVariant.v2);

        CLUSTER.filters().verbs(Verb.PAXOS2_PREPARE_REQ.id).to(2, 3).drop();
        Assertions.assertThatThrownBy(() -> CLUSTER.coordinator(COORDINATOR).execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk=?"), ConsistencyLevel.SERIAL, NEXT.getAndIncrement()))
                  .is(isThrowable(ReadTimeoutException.class)); // why does write have its own type but not read?
    }

    @Test
    public void casV2CommitInsert()
    {
        withPaxos(Config.PaxosVariant.v2);

        CLUSTER.filters().verbs(Verb.PAXOS_COMMIT_REQ.id).to(2, 3).drop();
        Assertions.assertThatThrownBy(() -> CLUSTER.coordinator(COORDINATOR).execute(withKeyspace("INSERT INTO %s.tbl (pk, v) VALUES (?, ?) IF NOT EXISTS"), ConsistencyLevel.ALL, NEXT.getAndIncrement(), NEXT.getAndIncrement()))
                  .is(isThrowable(CasWriteTimeoutException.class));
    }

    private static void withPaxos(Config.PaxosVariant variant)
    {
        CLUSTER.forEach(i -> i.runOnInstance(() -> Paxos.setPaxosVariant(variant)));
    }

    private static String batch(String cql)
    {
        return "BEGIN " + BatchStatement.Type.UNLOGGED.name() + " BATCH\n" + cql + "\nAPPLY BATCH";
    }

    public static class BB
    {
        public static void install(ClassLoader cl, int num)
        {
            if (num != COORDINATOR)
                return;
            new ByteBuddy().rebase(Condition.Async.class)
                           .method(named("await").and(takesArguments(2)))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);

            new ByteBuddy().rebase(RequestCallback.class)
                           .method(named("isTimeout"))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static boolean await(long time, TimeUnit units, @This Awaitable self, @SuperMethod Method method) throws InterruptedException, InvocationTargetException, IllegalAccessException
        {
            // make sure that the underline condition is met before returnning true
            // this way its know that the timeouts triggered!
            while (!((boolean) method.invoke(self, time, units)))
            {
            }
            return true;
        }

        private static final AtomicInteger TIMEOUTS = new AtomicInteger(0);
        public static boolean isTimeout(Map<InetAddressAndPort, RequestFailureReason> failureReasonByEndpoint, @SuperCall Callable<Boolean> fn) throws Exception
        {
            boolean timeout = fn.call();
            if (timeout)
                TIMEOUTS.incrementAndGet();
            return timeout;
        }

        public static void assertIsTimeoutTrue()
        {
            int timeouts = CLUSTER.get(COORDINATOR).callOnInstance(() -> TIMEOUTS.getAndSet(0));
            Assertions.assertThat(timeouts).isGreaterThan(0);
        }

        public static void reset()
        {
            CLUSTER.get(COORDINATOR).runOnInstance(() -> TIMEOUTS.set(0));
        }
    }
}
