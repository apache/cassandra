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

package org.apache.cassandra.distributed.test.accord;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.Shared;
import org.apache.cassandra.utils.TimeUUID;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

public class AccordSingleTokenBatchTest extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordSingleTokenBatchTest.class);

    @Override
    protected Logger logger()
    {
        return logger;
    }

    @BeforeClass
    public static void setupClass() throws IOException
    {
        AccordTestBase.setupCluster(builder -> builder
                                               .withoutVNodes()
                                               .withInstanceInitializer(BBHelper::install)
                                               .withConfig(config ->
                                                           config
                                                           .with(Feature.NETWORK, Feature.GOSSIP)), 6);
    }

    @Test
    public void accordSinglePartitionKeyBatchTest() throws Throwable
    {
        List<String> ddls = Arrays.asList("DROP KEYSPACE IF EXISTS " + KEYSPACE + ';',
                                          "CREATE KEYSPACE " + KEYSPACE + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}",
                                          "CREATE TABLE " + qualifiedAccordTableName + " (k int PRIMARY KEY, v int) WITH transactional_mode='full'",
                                          "CREATE TABLE " + qualifiedRegularTableName + " (k int PRIMARY KEY, v int)");
        test(ddls, cluster -> {
            cluster.coordinator(1).execute("BEGIN BATCH\n" +
                                           "INSERT INTO " + qualifiedAccordTableName + " (k, v) VALUES (1, 2);\n" +
                                           "INSERT INTO " + qualifiedRegularTableName + " (k, v) VALUES (1, 3);\n" +
                                           "APPLY BATCH;", ConsistencyLevel.ONE); // Chore: Double check consistency level semantics

            SimpleQueryResult r1 = cluster.coordinator(1).executeWithResult("SELECT * FROM " + qualifiedAccordTableName + " WHERE k = 1", ConsistencyLevel.ONE);
            SimpleQueryResult r2 = cluster.coordinator(1).executeWithResult("SELECT * FROM " + qualifiedRegularTableName + " WHERE k = 1", ConsistencyLevel.ONE);

            assert(r1.toObjectArrays().length == 1);
            assert(r2.toObjectArrays().length == 1);

            assert(State.batchLogPath.get());
        });
    }

    @Shared
    public static class State
    {
        public static AtomicBoolean batchLogPath = new AtomicBoolean(false);
    }

    public static class BBHelper
    {
        static void install(ClassLoader cl, int nodeNumber)
        {
            new ByteBuddy().rebase(StorageProxy.class)
                           .method(named("syncWriteToBatchlog").and(takesArguments(4)))
                           .intercept(MethodDelegation.to(BBHelper.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static void syncWriteToBatchlog(Collection<Mutation> mutations, ReplicaPlan.ForWrite replicaPlan, TimeUUID uuid, Dispatcher.RequestTime requestTime, @SuperCall Callable<Void> r) throws Exception
        {
            State.batchLogPath.set(true);
            r.call();
        }
    }
}
