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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import accord.primitives.Txn;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.implementation.bind.annotation.This;
import org.apache.cassandra.cql3.transactions.ReferenceValue;
import org.apache.cassandra.distributed.api.QueryResults;
import org.apache.cassandra.distributed.util.QueryResultUtil;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.coordinate.Preempted;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.TransactionStatement;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.AccordTestUtils;
import org.apache.cassandra.utils.AssertionUtils;
import org.apache.cassandra.utils.FailingConsumer;
import org.assertj.core.util.Arrays;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.junit.Assert.assertArrayEquals;

public abstract class AccordTestBase extends TestBaseImpl
{
    protected static final AtomicInteger COUNTER = new AtomicInteger(0);

    protected static Cluster SHARED_CLUSTER;
    
    protected String currentTable;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @BeforeClass
    public static void setupClass() throws IOException
    {
        SHARED_CLUSTER = createCluster();
    }

    @AfterClass
    public static void teardown()
    {
        if (SHARED_CLUSTER != null)
            SHARED_CLUSTER.close();
    }
    
    @Before
    public void setup()
    {
        currentTable = KEYSPACE + ".tbl" + COUNTER.getAndIncrement();
    }

    protected static void assertRow(Cluster cluster, String query, int k, int c, int v)
    {
        Object[][] result = cluster.coordinator(1).execute(query, ConsistencyLevel.QUORUM);
        assertArrayEquals(new Object[]{new Object[] {k, c, v}}, result);
    }

    protected void test(String tableDDL, FailingConsumer<Cluster> fn) throws Exception
    {
        test(Collections.singletonList(tableDDL), fn);
    }

    protected void test(List<String> ddls, FailingConsumer<Cluster> fn) throws Exception
    {
        for (String ddl : ddls)
            SHARED_CLUSTER.schemaChange(ddl);
        SHARED_CLUSTER.forEach(node -> node.runOnInstance(() -> AccordService.instance().createEpochFromConfigUnsafe()));

        // Evict commands from the cache immediately to expose problems loading from disk.
        SHARED_CLUSTER.forEach(node -> node.runOnInstance(() -> AccordService.instance().setCacheSize(0)));

        try
        {
            fn.accept(SHARED_CLUSTER);
        }
        finally
        {
            SHARED_CLUSTER.filters().reset();
        }
    }

    protected void test(FailingConsumer<Cluster> fn) throws Exception
    {
        test("CREATE TABLE " + currentTable + " (k int, c int, v int, primary key (k, c))", fn);
    }

    private static Cluster createCluster() throws IOException
    {
        // need to up the timeout else tests get flaky
        // disable vnode for now, but should enable before trunk
        return init(Cluster.build(2)
                           .withoutVNodes()
                           .withConfig(c -> c.with(Feature.NETWORK).set("write_request_timeout", "10s").set("transaction_timeout", "15s"))
                           .withInstanceInitializer(EnforceUpdateDoesNotPerformRead::install)
                           .start());
    }

    private static SimpleQueryResult execute(Cluster cluster, String check, Object... boundValues)
    {
        return cluster.coordinator(1).executeWithResult(check, ConsistencyLevel.ANY, boundValues);
    }

    protected static SimpleQueryResult assertRowEquals(Cluster cluster, SimpleQueryResult expected, String check, Object... boundValues)
    {
        SimpleQueryResult result = execute(cluster, check, boundValues);
        QueryResultUtil.assertThat(result).isEqualTo(expected);
        return result;
    }

    protected static SimpleQueryResult assertRowEquals(Cluster cluster, Object[] row, String check, Object... boundValues)
    {
        return assertRowEquals(cluster, QueryResults.builder().row(row).build(), check, boundValues);
    }

    // TODO: Retry on preemption may become unnecessary after the Unified Log is integrated.
    protected SimpleQueryResult assertRowEqualsWithPreemptedRetry(Cluster cluster, Object[] row, String check, Object... boundValues)
    {
        return assertRowWithPreemptedRetry(cluster, QueryResults.builder().row(row).build(), check, boundValues);
    }

    protected SimpleQueryResult assertEmptyWithPreemptedRetry(Cluster cluster, String check, Object... boundValues)
    {
        return assertRowWithPreemptedRetry(cluster, QueryResults.builder().build(), check, boundValues);
    }

    private SimpleQueryResult assertRowWithPreemptedRetry(Cluster cluster, SimpleQueryResult expected, String check, Object... boundValues)
    {
        SimpleQueryResult result = executeWithRetry(cluster, check, boundValues);
        QueryResultUtil.assertThat(result).isEqualTo(expected);
        return result;
    }

    private SimpleQueryResult executeWithRetry0(int count, Cluster cluster, String check, Object... boundValues)
    {
        try
        {
            return execute(cluster, check, boundValues);
        }
        catch (Throwable t)
        {
            if (AssertionUtils.rootCauseIs(Preempted.class).matches(t))
            {
                logger.warn("[Retry attempt={}] Preempted failure for {}", count, check);
                return executeWithRetry0(count + 1, cluster, check, boundValues);
            }

            throw t;
        }
    }

    protected SimpleQueryResult executeWithRetry(Cluster cluster, String check, Object... boundValues)
    {
        // is this method safe?
        cluster.get(1).runOnInstance(() -> {
            TransactionStatement stmt = AccordTestUtils.parse(check);
            if (!isIdempotent(stmt))
                throw new AssertionError("Unable to retry txn that is not idempotent: cql=" + check);
        });
        return executeWithRetry0(0, cluster, check, boundValues);
    }

    public static boolean isIdempotent(TransactionStatement statement)
    {
        for (ModificationStatement update : statement.getUpdates())
        {
            if (!isIdempotent(update))
                return false;
        }
        return true;
    }

    private static boolean isIdempotent(ModificationStatement update)
    {
        update.migrateReadRequiredOperations();
        // ReferenceValue.Constant is used during migration, which means a case like "a += 1"
        // ReferenceValue.Substitution uses a LET reference, so rerunning would always just see the new state
        long numConstants = update.getSubstitutions().stream()
                                  .filter(f -> f.getValue() instanceof ReferenceValue.Constant)
                                  .filter(f -> !f.getKind().name().contains("Setter"))
                                  .count();
        return numConstants == 0;
    }

    public static class EnforceUpdateDoesNotPerformRead
    {
        public static void install(ClassLoader classLoader, Integer num)
        {
            new ByteBuddy().rebase(ModificationStatement.class)
                           .method(named("readRequiredLists"))
                           .intercept(MethodDelegation.to(EnforceUpdateDoesNotPerformRead.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
        }

        @SuppressWarnings("unused")
        public static Map<?, ?> readRequiredLists(@This ModificationStatement stmt, @SuperCall Callable<Map<?, ?>> fn) throws Exception
        {
            Map<?, ?> map = fn.call();
            if (map != null)
            {
                // if the call tree has a TransactionStatement, then fail as this violates the query
                for (StackTraceElement e : Thread.currentThread().getStackTrace())
                    if (TransactionStatement.class.getCanonicalName().equals(e.getClassName()))
                        throw new IllegalStateException("Attempted to load required partition!");
            }
            return map;
        }
    }
    
    protected abstract Logger logger();
}
