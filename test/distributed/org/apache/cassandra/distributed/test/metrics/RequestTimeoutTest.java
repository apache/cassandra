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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperMethod;
import net.bytebuddy.implementation.bind.annotation.This;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.distributed.util.AssertionUtils;
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
import static org.apache.cassandra.distributed.util.AssertionUtils.is;

public class RequestTimeoutTest extends TestBaseImpl
{
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
        CLUSTER.filters().reset();
    }

    @Test
    public void insert()
    {
        CLUSTER.filters().verbs(Verb.MUTATION_REQ.id).to(2).drop();
        Assertions.assertThatThrownBy(() -> CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, v) VALUES (?, ?)"), ConsistencyLevel.ALL, 0, 0))
                  .is(is(WriteTimeoutException.class));
    }

    @Test
    public void update()
    {
        CLUSTER.filters().verbs(Verb.MUTATION_REQ.id).to(2).drop();
        Assertions.assertThatThrownBy(() -> CLUSTER.coordinator(1).execute(withKeyspace("UPDATE %s.tbl SET v=? WHERE pk=?"), ConsistencyLevel.ALL, 0, 0))
                  .is(is(WriteTimeoutException.class));
    }

    @Test
    public void batchInsert()
    {
        CLUSTER.filters().verbs(Verb.MUTATION_REQ.id).to(2).drop();
        Assertions.assertThatThrownBy(() -> CLUSTER.coordinator(1).execute(batch(withKeyspace("INSERT INTO %s.tbl (pk, v) VALUES (?, ?)")), ConsistencyLevel.ALL, 0, 0))
                  .is(is(WriteTimeoutException.class));
    }

    @Test
    public void rangeSelect()
    {
        CLUSTER.filters().verbs(Verb.RANGE_REQ.id).to(2).drop();
        Assertions.assertThatThrownBy(() -> CLUSTER.coordinator(1).execute(withKeyspace("SELECT * FROM %s.tbl"), ConsistencyLevel.ALL))
                  .is(is(ReadTimeoutException.class));
    }

    @Test
    public void select()
    {
        CLUSTER.filters().verbs(Verb.READ_REQ.id).to(2).drop();
        Assertions.assertThatThrownBy(() -> CLUSTER.coordinator(1).execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk=0"), ConsistencyLevel.ALL))
                  .is(is(ReadTimeoutException.class));
    }

    @Test
    public void truncate()
    {
        CLUSTER.filters().verbs(Verb.TRUNCATE_REQ.id).to(2).drop();
        Assertions.assertThatThrownBy(() -> CLUSTER.coordinator(1).execute(withKeyspace("TRUNCATE %s.tbl"), ConsistencyLevel.ALL))
                  .is(AssertionUtils.rootCauseIs(TimeoutException.class));
    }

    @Test
    public void casV2PrepareInsert()
    {
        withPaxos(Config.PaxosVariant.v2);

        CLUSTER.filters().verbs(Verb.PAXOS2_PREPARE_REQ.id).to(2, 3).drop();
        Assertions.assertThatThrownBy(() -> CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, v) VALUES (?, ?) IF NOT EXISTS"), ConsistencyLevel.ALL, 0, 0))
                  .is(is(CasWriteTimeoutException.class));
    }

    @Test
    public void casV2PrepareSelect()
    {
        withPaxos(Config.PaxosVariant.v2);

        CLUSTER.filters().verbs(Verb.PAXOS2_PREPARE_REQ.id).to(2, 3).drop();
        Assertions.assertThatThrownBy(() -> CLUSTER.coordinator(1).execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk=0"), ConsistencyLevel.SERIAL, 0, 0))
                  .is(is(ReadTimeoutException.class)); // why does write have its own type but not read?
    }

    @Test
    public void casV2CommitInsert()
    {
        withPaxos(Config.PaxosVariant.v2);

        CLUSTER.filters().verbs(Verb.PAXOS_COMMIT_REQ.id).to(2, 3).drop();
        Assertions.assertThatThrownBy(() -> CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, v) VALUES (?, ?) IF NOT EXISTS"), ConsistencyLevel.ALL, 0, 0))
                  .is(is(CasWriteTimeoutException.class));
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
        public static void install(ClassLoader cl, Integer num)
        {
            new ByteBuddy().rebase(Condition.Async.class)
                           .method(named("await").and(takesArguments(2)))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static boolean await(long time, TimeUnit units, @This Awaitable self, @SuperMethod Method method) throws InterruptedException, InvocationTargetException, IllegalAccessException
        {
            // attempt to decouple the two usages of write_request_timeout: await(write_request_timeout) and message expire
            return (boolean) method.invoke(self, time * 10, units);
        }
    }
}
