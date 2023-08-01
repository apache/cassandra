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

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.transport.messages.ResultMessage.Rows;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ByteBuddyExamplesTest extends TestBaseImpl
{
    @Test
    public void writeFailureTest() throws Throwable
    {
        try(Cluster cluster = init(Cluster.build(1)
                                          .withInstanceInitializer(BBFailHelper::install)
                                          .start()))
        {
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int primary key, t int)");
            try
            {
                cluster.coordinator(1).execute("insert into " + KEYSPACE + ".tbl (id, t) values (1, 1)", ConsistencyLevel.ALL);
                fail("Should fail");
            }
            catch (RuntimeException e)
            {
                // expected
            }
        }
    }

    public static class BBFailHelper
    {
        static void install(ClassLoader cl, int nodeNumber)
        {
            new ByteBuddy().redefine(ModificationStatement.class)
                           .method(named("execute"))
                           .intercept(MethodDelegation.to(BBFailHelper.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }
        public static ResultMessage execute()
        {
            throw new RuntimeException();
        }
    }

    @Test
    public void countTest() throws IOException
    {
        try(Cluster cluster = init(Cluster.build(2)
                                          .withInstanceInitializer(BBCountHelper::install)
                                          .start()))
        {
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int primary key, bytebuddy_test_column int)");
            cluster.coordinator(1).execute("select * from " + KEYSPACE + ".tbl;", ConsistencyLevel.ALL);
            cluster.coordinator(2).execute("select * from " + KEYSPACE + ".tbl;", ConsistencyLevel.ALL);
            cluster.get(1).runOnInstance(() -> {
                assertEquals(1, BBCountHelper.count.get());
            });
            cluster.get(2).runOnInstance(() -> {
                assertEquals(0, BBCountHelper.count.get());
            });

        }
    }

    public static class BBCountHelper
    {
        static AtomicInteger count = new AtomicInteger();
        static void install(ClassLoader cl, int nodeNumber)
        {
            if (nodeNumber != 1)
                return;
            new ByteBuddy().rebase(SelectStatement.class)
                           .method(named("execute").and(takesArguments(3)))
                           .intercept(MethodDelegation.to(BBCountHelper.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static ResultMessage.Rows execute(QueryState state, QueryOptions options, long queryStartNanoTime, @SuperCall Callable<ResultMessage.Rows> r) throws Exception
        {
            Rows res = r.call();

            if (res.result.metadata.names.stream().map(ColumnSpecification::toString).collect(Collectors.toList()).contains("bytebuddy_test_column"))
                count.incrementAndGet();

            return res;
        }
    }

}
