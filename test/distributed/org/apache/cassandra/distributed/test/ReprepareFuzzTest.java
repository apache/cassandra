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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.PreparedStatementHelper;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.impl.RowUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throwables;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class ReprepareFuzzTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(ReprepareFuzzTest.class);

    @Test
    public void fuzzTest() throws Throwable
    {
        try (ICluster<IInvokableInstance> c = builder().withNodes(1)
                                                       .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL))
                                                       .withInstanceInitializer(PrepareBehaviour::alwaysNewBehaviour)
                                                       .start())
        {
            // Long string to make us invalidate caches occasionally
            String veryLongString = "very";
            for (int i = 0; i < 2; i++)
                veryLongString += veryLongString;
            final String qualified = "SELECT pk as " + veryLongString + "%d, ck as " + veryLongString + "%d FROM ks%d.tbl";
            final String unqualified = "SELECT pk as " + veryLongString + "%d, ck as " + veryLongString + "%d FROM tbl";

            int KEYSPACES = 3;
            final int STATEMENTS_PER_KS = 3;

            for (int i = 0; i < KEYSPACES; i++)
            {
                c.schemaChange(withKeyspace("CREATE KEYSPACE ks" + i + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"));
                c.schemaChange(withKeyspace("CREATE TABLE ks" + i + ".tbl (pk int, ck int, PRIMARY KEY (pk, ck));"));
                for (int j = 0; j < i; j++)
                    c.coordinator(1).execute("INSERT INTO ks" + i + ".tbl (pk, ck) VALUES (?, ?)", ConsistencyLevel.QUORUM, 1, j);
            }

            List<Thread> threads = new ArrayList<>();
            AtomicBoolean interrupt = new AtomicBoolean(false);
            AtomicReference<Throwable> thrown = new AtomicReference<>();

            int INFREQUENT_ACTION_COEF = 10;

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            for (int i = 0; i < FBUtilities.getAvailableProcessors() * 2; i++)
            {
                int seed = i;
                threads.add(new Thread(() -> {
                    com.datastax.driver.core.Cluster cluster = null;
                    Session session = null;

                    try
                    {
                        Random rng = new Random(seed);
                        int usedKsIdx = -1;
                        String usedKs = null;
                        Map<Pair<Integer, Integer>, PreparedStatement> qualifiedStatements = new HashMap<>();
                        Map<Pair<Integer, Integer>, PreparedStatement> unqualifiedStatements = new HashMap<>();

                        cluster = com.datastax.driver.core.Cluster.builder()
                                                                  .addContactPoint("127.0.0.1")
                                                                  .build();
                        session = cluster.connect();
                        while (!interrupt.get() && (System.nanoTime() < deadline))
                        {
                            final int ks = rng.nextInt(KEYSPACES);
                            final int statementIdx = rng.nextInt(STATEMENTS_PER_KS);
                            final Pair<Integer, Integer> statementId = Pair.create(ks, statementIdx);

                            int v = rng.nextInt(INFREQUENT_ACTION_COEF + 1);
                            Action[] pool;
                            if (v == INFREQUENT_ACTION_COEF)
                                pool = infrequent;
                            else
                                pool = frequent;

                            Action action = pool[rng.nextInt(pool.length)];
                            switch (action)
                            {
                                case EXECUTE_QUALIFIED:
                                    if (!qualifiedStatements.containsKey(statementId))
                                        continue;

                                    try
                                    {
                                        int counter = 0;
                                        for (Iterator<Object[]> iter = RowUtil.toObjects(session.execute(qualifiedStatements.get(statementId).bind())); iter.hasNext(); )
                                        {
                                            Object[] current = iter.next();
                                            int v0 = (int) current[0];
                                            int v1 = (int) current[1];
                                            Assert.assertEquals(v0, 1);
                                            Assert.assertEquals(v1, counter++);
                                        }
                                        Assert.assertEquals(ks, counter);
                                    }
                                    catch (Throwable t)
                                    {
                                        if (t.getCause() != null &&
                                            t.getCause().getMessage().contains("Statement was prepared on keyspace"))
                                            continue;

                                        throw t;
                                    }

                                    break;
                                case EXECUTE_UNQUALIFIED:
                                    if (!unqualifiedStatements.containsKey(statementId))
                                        continue;

                                    try
                                    {
                                        int counter = 0;
                                        for (Iterator<Object[]> iter = RowUtil.toObjects(session.execute(unqualifiedStatements.get(statementId).bind())); iter.hasNext(); )
                                        {
                                            Object[] current = iter.next();
                                            int v0 = (int) current[0];
                                            int v1 = (int) current[1];
                                            Assert.assertEquals(v0, 1);
                                            Assert.assertEquals(v1, counter++);
                                        }
                                        Assert.assertEquals(unqualifiedStatements.get(statementId).getQueryKeyspace() + " " + usedKs + " " + statementId,
                                                            Integer.parseInt(unqualifiedStatements.get(statementId).getQueryKeyspace().replace("ks", "")),
                                                            counter);

                                    }
                                    catch (Throwable t)
                                    {
                                        if (t.getCause() != null &&
                                            t.getCause().getMessage().contains("Statement was prepared on keyspace"))
                                            continue;

                                        throw t;
                                    }

                                    break;
                                case PREPARE_QUALIFIED:
                                {
                                    String qs = String.format(qualified, statementIdx, statementIdx, ks);
                                    String keyspace = "ks" + ks;
                                    PreparedStatement preparedQualified = session.prepare(qs);

                                    // With prepared qualified, keyspace will be set to the keyspace of the statement when it was first executed
                                    PreparedStatementHelper.assertHashWithoutKeyspace(preparedQualified, qs, keyspace);
                                    qualifiedStatements.put(statementId, preparedQualified);
                                }
                                break;
                                case PREPARE_UNQUALIFIED:
                                    try
                                    {
                                        String qs = String.format(unqualified, statementIdx, statementIdx, ks);
                                        PreparedStatement preparedUnqalified = session.prepare(qs);
                                        Assert.assertEquals(preparedUnqalified.getQueryKeyspace(), usedKs);
                                        PreparedStatementHelper.assertHashWithKeyspace(preparedUnqalified, qs, usedKs);
                                        unqualifiedStatements.put(Pair.create(usedKsIdx, statementIdx), preparedUnqalified);
                                    }
                                    catch (InvalidQueryException iqe)
                                    {
                                        if (!iqe.getMessage().contains("No keyspace has been"))
                                            throw iqe;
                                    }
                                    catch (Throwable t)
                                    {
                                        if (usedKs == null)
                                        {
                                            // ignored
                                            continue;
                                        }

                                        throw t;
                                    }
                                    break;
                                case CLEAR_CACHES:
                                    c.get(1).runOnInstance(() -> {
                                        SystemKeyspace.loadPreparedStatements((id, query, keyspace) -> {
                                            if (rng.nextBoolean())
                                                QueryProcessor.instance.evictPrepared(id);
                                            return true;
                                        });
                                    });
                                    break;
                                case RELOAD_FROM_TABLES:
                                    c.get(1).runOnInstance(QueryProcessor::clearPreparedStatementsCache);
                                    c.get(1).runOnInstance(() -> QueryProcessor.instance.preloadPreparedStatements());
                                    break;
                                case SWITCH_KEYSPACE:
                                    usedKsIdx = ks;
                                    usedKs = "ks" + ks;
                                    session.execute("USE " + usedKs);
                                    break;
                                case FORGET_PREPARED:
                                    Map<Pair<Integer, Integer>, PreparedStatement> toCleanup = rng.nextBoolean() ? qualifiedStatements : unqualifiedStatements;
                                    Set<Pair<Integer, Integer>> toDrop = new HashSet<>();
                                    for (Pair<Integer, Integer> e : toCleanup.keySet())
                                    {
                                        if (rng.nextBoolean())
                                            toDrop.add(e);
                                    }

                                    for (Pair<Integer, Integer> e : toDrop)
                                        toCleanup.remove(e);
                                    toDrop.clear();
                                    break;
                                case RECONNECT:
                                    session.close();
                                    cluster.close();
                                    cluster = com.datastax.driver.core.Cluster.builder()
                                                                              .addContactPoint("127.0.0.1")
                                                                              .build();
                                    session = cluster.connect();
                                    qualifiedStatements.clear();
                                    unqualifiedStatements.clear();
                                    usedKs = null;
                                    usedKsIdx = -1;
                                    break;
                            }
                        }
                    }
                    catch (Throwable t)
                    {
                        interrupt.set(true);
                        t.printStackTrace();
                        while (true)
                        {
                            Throwable seen = thrown.get();
                            Throwable merged = Throwables.merge(seen, t);
                            if (thrown.compareAndSet(seen, merged))
                                break;
                        }
                        throw t;
                    }
                    finally
                    {
                        if (session != null)
                            session.close();
                        if (cluster != null)
                            cluster.close();
                    }
                }));
            }

            for (Thread thread : threads)
                thread.start();

            for (Thread thread : threads)
                thread.join();

            if (thrown.get() != null)
                throw thrown.get();
        }
    }

    private enum Action
    {
        EXECUTE_QUALIFIED,
        EXECUTE_UNQUALIFIED,
        PREPARE_QUALIFIED,
        PREPARE_UNQUALIFIED,
        CLEAR_CACHES,
        FORGET_PREPARED,
        RELOAD_FROM_TABLES,
        SWITCH_KEYSPACE,
        RECONNECT
    }

    private static Action[] frequent = new Action[]{ Action.EXECUTE_QUALIFIED,
                                                     Action.EXECUTE_UNQUALIFIED,
                                                     Action.PREPARE_QUALIFIED,
                                                     Action.PREPARE_UNQUALIFIED,
                                                     Action.SWITCH_KEYSPACE};

    private static Action[] infrequent = new Action[]{ Action.CLEAR_CACHES,
                                                       Action.FORGET_PREPARED,
                                                       Action.RELOAD_FROM_TABLES,
                                                       Action.RECONNECT
    };

    public static class PrepareBehaviour
    {
        static void alwaysNewBehaviour(ClassLoader cl, int nodeNumber)
        {
            DynamicType.Builder.MethodDefinition.ReceiverTypeDefinition<QueryProcessor> klass =
            new ByteBuddy().rebase(QueryProcessor.class)
                           .method(named("useNewPreparedStatementBehaviour"))
                           .intercept(MethodDelegation.to(AlwaysNewBehaviour.class));
            klass.make()
                 .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }
    }

    public static class AlwaysNewBehaviour
    {
        public static boolean useNewPreparedStatementBehaviour()
        {
            return true;
        }
    }
}