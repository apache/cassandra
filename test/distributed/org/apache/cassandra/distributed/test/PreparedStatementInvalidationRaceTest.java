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

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.TableMetadata;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;

public class PreparedStatementInvalidationRaceTest extends TestBaseImpl
{
    @Test
    public void testInvalidationRace() throws Exception
    {
        try (ICluster<IInvokableInstance> c = init(builder().withNodes(1)
                                                            .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL))
                                                            .withInstanceInitializer(BBHelper::install)
                                                            .start()))
        {
            try (com.datastax.driver.core.Cluster cluster = com.datastax.driver.core.Cluster.builder()
                                                                                            .addContactPoint("127.0.0.1")
                                                                                            .build();
                 Session s = cluster.connect())
            {
                s.execute(withKeyspace("CREATE TABLE %s.tbl (pk int primary key)"));
                PreparedStatement prepared = s.prepare(withKeyspace("select pk from %s.tbl where pk = ?"));
                s.execute(prepared.bind(1));
                c.get(1).runOnInstance(() -> BBHelper.enabled.set(true));
                Thread t = new Thread(() -> s.execute(withKeyspace("alter table %s.tbl add x int")));
                t.start();
                c.get(1).runOnInstance(() -> await(BBHelper.initializeKeyspaceInstancesDone));
                // This is where the race existed before - we used to invalidate the statement in
                // initializeKeyspaceInstances, but the schema change is not yet committed so the
                // next execute would reprepare with the wrong tablemetadata (and keep it forever).
                // Now we invalidate after committing - so the last execute below will reprepare the
                // query, with the correct tablemetadata
                s.execute(prepared.bind(1));
                c.get(1).runOnInstance(() -> {
                    BBHelper.enabled.set(false);
                    BBHelper.delayCommittingSchemaChange.countDown();
                });
                t.join();
                s.execute(prepared.bind(1));
                c.get(1).runOnInstance(() -> {
                    TableMetadata tableMetadata = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").metadata();
                    Collection<QueryHandler.Prepared> serverSidePrepared = QueryProcessor.instance.getPreparedStatements().values();
                    assertEquals(1, serverSidePrepared.size());
                    for (QueryHandler.Prepared ssp : QueryProcessor.instance.getPreparedStatements().values())
                        assertEquals(tableMetadata.epoch, ((SelectStatement)ssp.statement).table.epoch);
                });
            }
        }
    }

    public static class BBHelper
    {
        static AtomicBoolean enabled = new AtomicBoolean();
        static CountDownLatch delayCommittingSchemaChange = new CountDownLatch(1);
        static CountDownLatch initializeKeyspaceInstancesDone = new CountDownLatch(1);

        public static void install(ClassLoader cl, int i)
        {
            new ByteBuddy().rebase(DistributedSchema.class)
                           .method(named("initializeKeyspaceInstances").and(takesArguments(2)))
                           .intercept(MethodDelegation.to(BBHelper.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static void initializeKeyspaceInstances(DistributedSchema prev, boolean loadSSTables, @SuperCall Callable<Void> zuper)
        {
            try
            {
                zuper.call();
                if (enabled.get())
                {
                    initializeKeyspaceInstancesDone.countDown();
                    delayCommittingSchemaChange.await();
                }
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    private static void await(CountDownLatch cdl)
    {
        try
        {
            cdl.await();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }
}