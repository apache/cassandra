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
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.RoutingKey;
import accord.coordinate.Invalidated;
import accord.impl.progresslog.DefaultProgressLogs;
import accord.messages.PreAccept;
import accord.primitives.PartialKeyRoute;
import accord.primitives.Routable.Domain;
import accord.primitives.Route;
import accord.primitives.TxnId;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.implementation.bind.annotation.This;
import org.apache.cassandra.batchlog.BatchlogManager;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.TransactionStatement;
import org.apache.cassandra.cql3.transactions.ReferenceValue;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.Cluster.Builder;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableRunnable;
import org.apache.cassandra.distributed.api.QueryResults;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.impl.Instance;
import org.apache.cassandra.distributed.shared.AssertUtils;
import org.apache.cassandra.distributed.shared.Metrics;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.distributed.util.QueryResultUtil;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.accord.exceptions.ReadPreemptedException;
import org.apache.cassandra.service.accord.exceptions.WritePreemptedException;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.service.consensus.migration.ConsensusMigrationState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.AssertionUtils;
import org.apache.cassandra.utils.FailingConsumer;

import static java.lang.String.format;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.db.SystemKeyspace.CONSENSUS_MIGRATION_STATE;
import static org.apache.cassandra.db.SystemKeyspace.PAXOS;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.schema.SchemaConstants.SYSTEM_KEYSPACE_NAME;
import static org.junit.Assert.assertArrayEquals;

public abstract class AccordTestBase extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(AccordTestBase.class);
    private static final int MAX_RETRIES = 10;

    protected static final AtomicInteger COUNTER = new AtomicInteger(0);

    protected static Cluster SHARED_CLUSTER;

    protected String accordTableName;
    protected String qualifiedAccordTableName;
    protected String regularTableName;
    protected String qualifiedRegularTableName;

    protected final TransactionalMode transactionalMode;

    protected AccordTestBase()
    {
        this.transactionalMode = TransactionalMode.full;
    }

    protected AccordTestBase(TransactionalMode transactionalMode) {
        this.transactionalMode = transactionalMode;
    }

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
        accordTableName = "accordtbl" + COUNTER.getAndIncrement();
        qualifiedAccordTableName = KEYSPACE + '.' + accordTableName;
        regularTableName = "regulartbl" + COUNTER.getAndIncrement();
        qualifiedRegularTableName = KEYSPACE + '.' + regularTableName;
    }

    @After
    public void tearDown() throws Exception
    {
        for (IInvokableInstance instance : SHARED_CLUSTER)
            instance.runOnInstance(() -> DefaultProgressLogs.unsafePauseForTesting(false));
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

    protected List<String> createTables(String tableFormat, String... qualifiedTables)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (String qualifiedTable : qualifiedTables)
            builder.add(format(tableFormat, qualifiedTable));
        return builder.build();
    }

    public static void ensureTableIsAccordManaged(Cluster cluster, String ksname, String tableName)
    {
        cluster.get(1).runOnInstance(() -> {
            TableMetadata metadata = Schema.instance.getTableMetadata(ksname, tableName);
            if (metadata == null)
                return; // bad plumbing from shared utils....
            Assert.assertTrue(metadata.params.transactionalMode.accordIsEnabled);
        });
    }

    protected void test(List<String> ddls, FailingConsumer<Cluster> fn) throws Exception
    {
        for (String ddl : ddls)
            SHARED_CLUSTER.schemaChange(ddl);

        // Evict commands from the cache immediately to expose problems loading from disk.
//        SHARED_CLUSTER.forEach(node -> node.runOnInstance(() -> AccordService.instance().setCacheSize(0)));

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
        test("CREATE TABLE " + qualifiedAccordTableName + " (k int, c int, v int, primary key (k, c)) WITH transactional_mode='" + transactionalMode + "'", fn);
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

    protected static int getRetryOnDifferentSystemCount(int coordinatorIndex)
    {
        return Ints.checkedCast(getMetrics(coordinatorIndex).getCounter("org.apache.cassandra.metrics.ClientRequest.RetryDifferentSystem.Write"));
    }

    protected int getMutationsRejectedOnWrongSystemCount()
    {
        long sum = 0;
        for (IInvokableInstance instance : SHARED_CLUSTER)
            sum += instance.metrics().getCounter("org.apache.cassandra.metrics.Table.MutationsRejectedOnWrongSystem." + qualifiedAccordTableName);
        return Ints.checkedCast(sum);
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
                                         .withConfig(c -> c.with(Feature.GOSSIP)
                                                           .set("sasi_indexes_enabled", "true")
                                                           .set("write_request_timeout", "10s")
                                                           .set("transaction_timeout", "15s")
                                                           .set("native_transport_timeout", "30s")
                                                           .set("accord.ephemeral_read_enabled", "false")
                                                           .set("accord.shard_durability_target_splits", "1")
                                                           .set("accord.shard_durability_cycle", "60s")
                                                           .set("accord.command_store_shard_count", "2")
                                                           .set("accord.queue_shard_count", "2"))
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

    private static boolean hasRootCause(Throwable ex, Class<? extends RuntimeException> klass)
    {
        return AssertionUtils.rootCauseIs(klass).matches(ex);

    }

    private static SimpleQueryResult executeWithRetry0(int count, Cluster cluster, IInvokableInstance inst, String check, Object... boundValues)
    {
        try
        {
            logger.info("Executing statement:\n{}", check);
            return inst.coordinator().executeWithResult(check, ConsistencyLevel.ANY, boundValues);
        }
        catch (RuntimeException ex)
        {
            if (count <= MAX_RETRIES && (hasRootCause(ex, ReadPreemptedException.class) || hasRootCause(ex, WritePreemptedException.class) || hasRootCause(ex, Invalidated.class)))
            {
                logger.warn("[Retry attempt={}] Preempted failure for\n{}", count, check);
                return executeWithRetry0(count + 1, cluster, inst, check, boundValues);
            }
            TxnId txnId = maybeExtractId(ex);
            if (txnId != null)
            {
                // query the cluster to find its status...
                StringBuilder sb = new StringBuilder();
                sb.append("Txn ").append(txnId).append(" timed out...\n");
                ClusterUtils.queryTxnStateAsString(sb, cluster, txnId);
                throw new AssertionError(sb.toString(), ex.getCause());
            }
            throw ex;
        }
    }

    private static TxnId maybeExtractId(Throwable ex)
    {
        if (hasRootCause(ex, ReadPreemptedException.class)
            || hasRootCause(ex, WritePreemptedException.class)
            || hasRootCause(ex, ReadTimeoutException.class)
            || hasRootCause(ex, WriteTimeoutException.class))
        {
            try
            {
                return TxnId.parse(ex.getMessage());
            }
            catch (Throwable t)
            {
                // ignore
            }
        }
        return null;
    }

    public static SimpleQueryResult executeWithRetry(Cluster cluster, String check, Object... boundValues)
    {
        return executeWithRetry(cluster, cluster.get(1), check, boundValues);
    }

    public static SimpleQueryResult executeWithRetry(Cluster cluster, IInvokableInstance inst, String check, Object... boundValues)
    {
        // is this method safe?

        if (!isIdempotent(inst, check))
            throw new AssertionError("Unable to retry txn that is not idempotent: cql=\n" + check);

        return executeWithRetry0(0, cluster, inst, check, boundValues);
    }

    public static Boolean isIdempotent(IInvokableInstance inst, String cql)
    {
        return inst.callOnInstance(() -> {
            CQLStatement.Raw parsed = QueryProcessor.parseStatement(cql);
            if (parsed instanceof TransactionStatement.Parsed)
            {
                TransactionStatement stmt = (TransactionStatement) parsed.prepare(ClientState.forInternalCalls());
                return isIdempotent(stmt);
            }
            else if (parsed instanceof ModificationStatement.Parsed)
            {
                ModificationStatement stmt = (ModificationStatement) parsed.prepare(ClientState.forInternalCalls());
                return isIdempotent(stmt);
            }
            else
            {
                throw new IllegalArgumentException("Unexpected type: " + parsed.getClass());
            }
        });
    }

    protected static String wrapInTxn(String statement)
    {
        if (!statement.trim().toUpperCase().startsWith("BEGIN TRANSACTION"))
        {
            statement = statement.trim();
            statement = Arrays.stream(statement.split("\\n"))
                              .map(line -> line.trim().endsWith(";") ? line : line + ';')
                              .collect(Collectors.joining("\n  ", "BEGIN TRANSACTION\n  ", "\nCOMMIT TRANSACTION"));
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

    protected void alterTableTransactionalMode(TransactionalMode mode)
    {
        SHARED_CLUSTER.schemaChange(format("ALTER TABLE %s WITH %s", qualifiedAccordTableName, mode.asCqlParam()));
    }

    protected static void pauseHints()
    {
        forEach(() -> HintsService.instance.pauseDispatch());
    }

    protected static void deleteAllHints()
    {
        forEach(() -> HintsService.instance.deleteAllHintsUnsafe());
    }

    protected static void pauseBatchlog()
    {
        forEach(() -> BatchlogManager.instance.pauseReplay());
    }

    protected static void unpauseHints()
    {
        forEach(() -> HintsService.instance.resumeDispatch());
    }

    protected static void unpauseBatchlog()
    {
        forEach(() -> BatchlogManager.instance.resumeReplay());
    }

    protected static void blockMutationAndPreAccept(Cluster cluster)
    {
        cluster.filters().outbound().messagesMatching((from, to, message) -> {
            if (message.verb() == Verb.MUTATION_REQ.id)
            {
                String keyspace = cluster.get(to).callsOnInstance(() -> ((Message<Mutation>) Instance.deserializeMessage(message)).payload.getKeyspaceName()).call();
                if (keyspace.equals(KEYSPACE))
                    return true;
            }
            if (message.verb() == Verb.ACCORD_PRE_ACCEPT_REQ.id)
            {
                boolean drop = cluster.get(to).callsOnInstance(() -> {
                    PreAccept preAccept = (PreAccept)Instance.deserializeMessage(message).payload;
                    Route<?> route = preAccept.scope;
                    if (route.domain() == Domain.Key)
                        for (RoutingKey key : (PartialKeyRoute)route)
                        {
                            AccordRoutingKey routingKey = (AccordRoutingKey)key;
                            ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(routingKey.table());
                            if (cfs.getKeyspaceName().equals(KEYSPACE))
                                return true;
                        }
                    return false;
                }).call();
                if (drop)
                    return true;
            }
            return false;
        }).drop();
    }

    protected static void truncateSystemTables()
    {
        SHARED_CLUSTER.coordinator(1).execute("TRUNCATE " + SYSTEM_KEYSPACE_NAME + "." + SystemKeyspace.BATCHES, ALL);
        SHARED_CLUSTER.coordinator(1).execute(format("TRUNCATE TABLE %s.%s", SYSTEM_KEYSPACE_NAME, CONSENSUS_MIGRATION_STATE), ALL);
        SHARED_CLUSTER.coordinator(1).execute(format("TRUNCATE TABLE %s.%s", SYSTEM_KEYSPACE_NAME, PAXOS), ALL);
    }

    protected static Stream<UUID> hostIds()
    {
        return Stream.concat(ClusterMetadata.current().directory.peerIds()
                                                                .stream()
                                                                .map(ClusterMetadata.current().directory::hostId),
                             Stream.of(HintsService.RETRY_ON_DIFFERENT_SYSTEM_UUID));
    }

    protected static boolean hasPendingHints()
    {
        return hostIds().map(HintsService.instance::getTotalHintsSize)
                        .anyMatch(size -> size > 0);
    }
}
