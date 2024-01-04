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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.common.base.Splitter;
import com.google.common.primitives.Ints;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.impl.SimpleProgressLog;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.implementation.bind.annotation.This;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.TransactionStatement;
import org.apache.cassandra.cql3.transactions.ReferenceValue;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.Cluster.Builder;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableRunnable;
import org.apache.cassandra.distributed.api.QueryResults;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.shared.AssertUtils;
import org.apache.cassandra.distributed.shared.Metrics;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.distributed.util.QueryResultUtil;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.AccordTestUtils;
import org.apache.cassandra.service.accord.exceptions.ReadPreemptedException;
import org.apache.cassandra.service.accord.exceptions.WritePreemptedException;
import org.apache.cassandra.service.consensus.migration.ConsensusTableMigrationState.ConsensusMigrationState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.AssertionUtils;
import org.apache.cassandra.utils.FailingConsumer;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.junit.Assert.assertArrayEquals;

public abstract class AccordTestBase extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(AccordTestBase.class);
    private static final int MAX_RETRIES = 10;

    protected static final AtomicInteger COUNTER = new AtomicInteger(0);

    protected static Cluster SHARED_CLUSTER;

    protected String tableName;
    protected String qualifiedTableName;

    public static void setupCluster(Function<Builder, Builder> options, int nodes) throws IOException
    {
        SHARED_CLUSTER = createCluster(nodes, options);
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
        tableName = "tbl" + COUNTER.getAndIncrement();
        qualifiedTableName = KEYSPACE + '.' + tableName;
    }

    @After
    public void tearDown() throws Exception
    {
        for (IInvokableInstance instance : SHARED_CLUSTER)
            instance.runOnInstance(() -> SimpleProgressLog.PAUSE_FOR_TEST = false);
    }

    protected static void assertRowSerial(Cluster cluster, String query, int k, int c, int v, int s)
    {
        Object[][] result = cluster.coordinator(1).execute(query, ConsistencyLevel.SERIAL);
        assertArrayEquals(new Object[]{new Object[] {k, c, v, s}}, result);
    }

    protected static void assertRowSerial(Cluster cluster, String query, Object[]... expected)
    {
        Object[][] result = cluster.coordinator(1).execute(query, ConsistencyLevel.SERIAL);
        AssertUtils.assertRows(result, expected);
    }

    protected void test(String tableDDL, FailingConsumer<Cluster> fn) throws Exception
    {
        test(Collections.singletonList(tableDDL), fn);
    }

    public static void ensureTableIsAccordManaged(Cluster cluster, String ksname, String tableName)
    {
        cluster.get(1).runOnInstance(() -> {
            // TODO: remove when accord enabled is handled via schema
            TableMetadata metadata = Schema.instance.getTableMetadata(ksname, tableName);
            if (metadata == null)
                return; // bad plumbing from shared utils....
            AccordService.instance().ensureTableIsAccordManaged(metadata.id);
        });
    }

    protected void test(List<String> ddls, FailingConsumer<Cluster> fn) throws Exception
    {
        for (String ddl : ddls)
            SHARED_CLUSTER.schemaChange(ddl);

        ensureTableIsAccordManaged(SHARED_CLUSTER, KEYSPACE, tableName);
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
        test("CREATE TABLE " + qualifiedTableName + " (k int, c int, v int, primary key (k, c))", fn);
    }

    protected static ConsensusMigrationState getMigrationStateSnapshot(IInvokableInstance instance) throws IOException
    {
        byte[] serializedBytes = instance.callOnInstance(() -> {
            DataOutputBuffer output = new DataOutputBuffer();
            try
            {
                ConsensusMigrationState.serializer.serialize(
                ClusterMetadata.current().consensusMigrationState,
                output, Version.V0);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            return output.toByteArray();
        });
        DataInputPlus input = new DataInputBuffer(serializedBytes);
        return ConsensusMigrationState.serializer.deserialize(input, Version.V0);
    }

    protected static int getAccordCoordinateCount()
    {
        return getAccordWriteCount() + getAccordReadCount();
    }

    protected static int getCasWriteCount(int coordinatorIndex)
    {
        return Ints.checkedCast(getMetrics(coordinatorIndex).getCounter("org.apache.cassandra.metrics.ClientRequest.Latency.CASWrite"));
    }

    protected static int getCasPrepareCount(int coordinatorIndex)
    {
        return Ints.checkedCast(getMetrics(coordinatorIndex).getCounter("org.apache.cassandra.metrics.keyspace.CasPrepareLatency.distributed_test_keyspace"));
    }

    protected static int getAccordWriteCount()
    {
        return getAccordWriteCount(1);
    }

    protected static int getAccordWriteCount(int coordinatorIndex)
    {
        return Ints.checkedCast(getMetrics(coordinatorIndex).getCounter("org.apache.cassandra.metrics.ClientRequest.Latency.AccordWrite"));
    }

    protected static int getAccordReadCount()
    {
        return getAccordReadCount(1);
    }

    protected static int getAccordReadCount(int coordinatorIndex)
    {
        return Ints.checkedCast(getMetrics(coordinatorIndex).getCounter("org.apache.cassandra.metrics.ClientRequest.Latency.AccordRead"));
    }

    protected static int getAccordMigrationRejects(int coordinatorIndex)
    {
        return Ints.checkedCast(getMetrics(coordinatorIndex).getCounter("org.apache.cassandra.metrics.ClientRequest.AccordMigrationRejects.AccordWrite"));
    }

    protected static int getAccordMigrationSkippedReads()
    {
        // Skipped reads can occur at any node so sum them
        long sum = 0;
        for (IInvokableInstance instance : SHARED_CLUSTER)
            sum += instance.metrics().getCounter("org.apache.cassandra.metrics.ClientRequest.MigrationSkippedReads.AccordWrite");
        return Ints.checkedCast(sum);
    }

    protected static int getKeyMigrationCount(int coordinatorIndex)
    {
        return Ints.checkedCast(getMetrics(coordinatorIndex).getCounter("org.apache.cassandra.metrics.Table.KeyMigrationLatency.all"));
    }

    protected static int getCasWriteBeginRejects(int coordinatorIndex)
    {
        return Ints.checkedCast(getMetrics(coordinatorIndex).getCounter("org.apache.cassandra.metrics.ClientRequest.PaxosBeginMigrationRejects.CASWrite"));
    }

    protected static int getCasReadBeginRejects(int coordinatorIndex)
    {
        return Ints.checkedCast(getMetrics(coordinatorIndex).getCounter("org.apache.cassandra.metrics.ClientRequest.PaxosBeginMigrationRejects.CASRead"));
    }

    protected static int getCasWriteAcceptRejects(int coordinatorIndex)
    {
        return Ints.checkedCast(getMetrics(coordinatorIndex).getCounter("org.apache.cassandra.metrics.ClientRequest.PaxosAcceptMigrationRejects.CASWrite"));
    }

    protected static int getCasReadAcceptRejects(int coordinatorIndex)
    {
        return Ints.checkedCast(getMetrics(coordinatorIndex).getCounter("org.apache.cassandra.metrics.ClientRequest.PaxosAcceptMigrationRejects.CASRead"));
    }

    protected static Metrics getMetrics(int coordinatorIndex)
    {
        return SHARED_CLUSTER.get(coordinatorIndex).metrics();
    }

    protected static void forEach(SerializableRunnable runnable)
    {
        for (IInvokableInstance instance : SHARED_CLUSTER)
            instance.runOnInstance(runnable);
    }

    private static Cluster createCluster(int nodes, Function<Builder, Builder> options) throws IOException
    {
        // need to up the timeout else tests get flaky
        // disable vnode for now, but should enable before trunk
        Cluster.Builder builder = Cluster.build(nodes)
                           .withoutVNodes()
                           .withConfig(c -> c.with(Feature.NETWORK, Feature.GOSSIP).set("write_request_timeout", "10s")
                                                                   .set("transaction_timeout", "15s")
                                             .set("transaction_timeout", "15s"))
                           .withInstanceInitializer(EnforceUpdateDoesNotPerformRead::install);
        builder = options.apply(builder);
        return init(builder.start());
    }

    protected static SimpleQueryResult executeAsTxn(Cluster cluster, String check, Object... boundValues)
    {
        String normalized = wrapInTxn(check);
        logger.info("Executing transaction statement:\n{}", normalized);
        return cluster.coordinator(1).executeWithResult(normalized, ConsistencyLevel.ANY, boundValues);
    }

    protected static SimpleQueryResult execute(Cluster cluster, String check, Object... boundValues)
    {
        logger.info("Executing statement:\n{}", check);
        return cluster.coordinator(1).executeWithResult(check, ConsistencyLevel.ANY, boundValues);
    }

    private static SimpleQueryResult execute(Cluster cluster, String check, ConsistencyLevel cl, Object... boundValues)
    {
        return cluster.coordinator(1).executeWithResult(check, cl, boundValues);
    }

    protected static SimpleQueryResult assertRowEquals(Cluster cluster, SimpleQueryResult expected, String check, ConsistencyLevel cl, Object... boundValues)
    {
        SimpleQueryResult result = execute(cluster, check, cl, boundValues);
        QueryResultUtil.assertThat(result).isEqualTo(expected);
        return result;
    }

    protected static SimpleQueryResult assertRowEquals(Cluster cluster, SimpleQueryResult expected, String check, Object... boundValues)
    {
        SimpleQueryResult result = execute(cluster, check, boundValues);
        QueryResultUtil.assertThat(result).isEqualTo(expected);
        return result;
    }

    protected static SimpleQueryResult assertRowEquals(Cluster cluster, Object[] row, String check, ConsistencyLevel cl, Object... boundValues)
    {
        return assertRowEquals(cluster, QueryResults.builder().row(row).build(), check, cl, boundValues);
    }

    protected static SimpleQueryResult assertRowEquals(Cluster cluster, Object[] row, String check, Object... boundValues)
    {
        return assertRowEquals(cluster, QueryResults.builder().row(row).build(), check, boundValues);
    }

    // TODO: Retry on preemption may become unnecessary after the Unified Log is integrated.
    protected static SimpleQueryResult assertRowEqualsWithPreemptedRetry(Cluster cluster, Object[] row, String check, Object... boundValues)
    {
        return assertRowWithPreemptedRetry(cluster, QueryResults.builder().row(row).build(), check, boundValues);
    }

    protected SimpleQueryResult assertEmptyWithPreemptedRetry(Cluster cluster, String check, Object... boundValues)
    {
        return assertRowWithPreemptedRetry(cluster, QueryResults.builder().build(), check, boundValues);
    }

    private static SimpleQueryResult assertRowWithPreemptedRetry(Cluster cluster, SimpleQueryResult expected, String check, Object... boundValues)
    {
        SimpleQueryResult result = executeWithRetry(cluster, check, boundValues);
        QueryResultUtil.assertThat(result).isEqualTo(expected);
        return result;
    }

    private static SimpleQueryResult executeWithRetry0(int count, Cluster cluster, String check, Object... boundValues)
    {
        try
        {
            return execute(cluster, check, boundValues);
        }
        catch (RuntimeException ex)
        {
            if (count <= MAX_RETRIES && (AssertionUtils.rootCauseIs(ReadPreemptedException.class).matches(ex) || AssertionUtils.rootCauseIs(WritePreemptedException.class).matches(ex)))
            {
                logger.warn("[Retry attempt={}] Preempted failure for\n{}", count, check);
                return executeWithRetry0(count + 1, cluster, check, boundValues);
            }

            throw ex;
        }
    }

    public static SimpleQueryResult executeWithRetry(Cluster cluster, String check, Object... boundValues)
    {
        check = wrapInTxn(check);

        // is this method safe?

        if (!isIdempotent(cluster, check))
            throw new AssertionError("Unable to retry txn that is not idempotent: cql=\n" + check);

        return executeWithRetry0(0, cluster, check, boundValues);
    }

    private static boolean isIdempotent(Cluster cluster, String cql)
    {
        return cluster.get(1).callOnInstance(() -> {
            TransactionStatement stmt = AccordTestUtils.parse(cql);
            return isIdempotent(stmt);
        });
    }

    private static String wrapInTxn(String statement)
    {
        if (!statement.trim().toUpperCase().startsWith("BEGIN TRANSACTION"))
        {
            statement = statement.trim();
            statement = Arrays.stream(statement.split("\\n")).collect(Collectors.joining("\n  ", "BEGIN TRANSACTION\n  ", "\nCOMMIT TRANSACTION"));
        }
        return statement;
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
        // ReferenceValue.Constant is used during migration, which means a case like "a += 1"
        // ReferenceValue.Substitution uses a LET reference, so rerunning would always just see the new state
        long numConstants = update.getSubstitutions().stream()
                                  .filter(f -> f.getValue() instanceof ReferenceValue.Constant)
                                  .filter(f -> !f.getKind().name().contains("Setter"))
                                  .count();
        return numConstants == 0;
    }

    static List<String> tokens()
    {
        return SHARED_CLUSTER.stream()
                             .flatMap(i -> StreamSupport.stream(Splitter.on(",").split(i.config().getString("initial_token")).spliterator(), false))
                             .collect(Collectors.toList());
    }

    static List<ByteBuffer> tokensToKeys(List<String> tokens)
    {
        return tokens.stream()
                     .map(t -> (Murmur3Partitioner.LongToken) Murmur3Partitioner.instance.getTokenFactory().fromString(t))
                     .map(Murmur3Partitioner.LongToken::keyForToken)
                     .collect(Collectors.toList());
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
