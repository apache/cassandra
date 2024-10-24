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
import java.net.InetSocketAddress;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.RoutingKey;
import accord.messages.PreAccept;
import accord.primitives.PartialKeyRoute;
import accord.primitives.Routable.Domain;
import accord.primitives.Route;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.Util;
import org.apache.cassandra.batchlog.BatchlogManager;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.Config.PaxosVariant;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.api.IMessageSink;
import org.apache.cassandra.distributed.api.QueryResults;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.impl.Instance;
import org.apache.cassandra.distributed.impl.TestChangeListener;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.exceptions.CoordinatorBehindException;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.metrics.AccordClientRequestMetrics;
import org.apache.cassandra.metrics.ClientRequestsMetricsHolder;
import org.apache.cassandra.metrics.HintsServiceMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.service.consensus.migration.ConsensusKeyMigrationState;
import org.apache.cassandra.service.consensus.migration.ConsensusRequestRouter;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Promise;
import org.eclipse.jetty.util.ConcurrentHashSet;

import static java.lang.String.format;
import static org.apache.cassandra.Util.expectException;
import static org.apache.cassandra.Util.spinAssertEquals;
import static org.apache.cassandra.config.CassandraRelevantProperties.HINT_DISPATCH_INTERVAL_MS;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.shared.ClusterUtils.getNextEpoch;
import static org.apache.cassandra.distributed.shared.ClusterUtils.pauseAfterEnacting;
import static org.apache.cassandra.distributed.shared.ClusterUtils.pauseBeforeEnacting;
import static org.apache.cassandra.distributed.shared.ClusterUtils.unpauseEnactment;
import static org.apache.cassandra.distributed.test.accord.AccordMigrationRaceTestBase.Scenario.BATCHLOG_FAILED_ROUTING_THEN_HINT;
import static org.apache.cassandra.distributed.test.accord.AccordMigrationRaceTestBase.Scenario.BATCHLOG_FAILED_TIMEOUT_THEN_HINT;
import static org.apache.cassandra.distributed.test.accord.AccordMigrationRaceTestBase.Scenario.BATCHLOG_SUCCESSFUL_ROUTING;
import static org.apache.cassandra.distributed.test.accord.AccordMigrationRaceTestBase.Scenario.HINT;
import static org.apache.cassandra.distributed.test.accord.AccordMigrationRaceTestBase.Scenario.MUTATION;
import static org.apache.cassandra.distributed.util.QueryResultUtil.assertThat;
import static org.apache.cassandra.exceptions.RequestFailureReason.RETRY_ON_DIFFERENT_TRANSACTION_SYSTEM;
import static org.apache.cassandra.utils.Throwables.runUnchecked;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/*
 * Test that non-transactional write operations such as regular mutations, batch log, and hints
 * all detect when a migration is in progress, and then retry on the correct system.
 */
public abstract class AccordMigrationRaceTestBase extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordMigrationRaceTestBase.class);

    private static final int CLUSTERING_VALUE = 1;

    private static final String TABLE_FMT = "CREATE TABLE %s (id int, c int, v int, PRIMARY KEY ((id), c));";

    public static final int PKEY_ACCORD = 3;
    public static final int PKEY_NORMAL = 0;

    private static IPartitioner partitioner;

    private static Token minToken;

    private static Token maxToken;

    private static Token midToken;

    private static Token upperMidToken;

    private static Token lowerMidToken;

    private static ICoordinator coordinator;

    private final static TestMessageSink messageSink = new TestMessageSink();
    private static class TestMessageSink implements IMessageSink
    {
        private final Queue<Pair<InetSocketAddress,IMessage>> messages = new ConcurrentLinkedQueue<>();
        private final Set<InetSocketAddress> blackholed = new ConcurrentHashSet<>();

        public void reset()
        {
            messages.clear();
            blackholed.clear();
        }

        @Override
        public void accept(InetSocketAddress to, IMessage message) {
            messages.offer(Pair.create(to,message));
            IInstance i = SHARED_CLUSTER.get(to);
            if (blackholed.contains(to) || blackholed.contains(message.from()))
                return;
            if (i != null)
                i.receiveMessage(message);
        }
    }

    enum Scenario
    {
        // Apply the mutation from the coordinator directly without going through hinting
        MUTATION(false, false, false, false, false),
        // Hint from the initial mutation coordination
        HINT(true, false, true, false, true),
        // Apply the mutation from the batchlog directly
        BATCHLOG_SUCCESSFUL_ROUTING(false, true, true, true, false),
        // Have the batchlog use hints to apply the mutation after failing to route, migrating back from Accord this is a timeout because you can't get Accord to fail at routing
        // it either executes correctly in the old epoch or times out waiting for the new one to arrive
        BATCHLOG_FAILED_ROUTING_THEN_HINT(false, true, true, true, true),
        // Have the batchlog use hints to apply the mutation after a timeout
        BATCHLOG_FAILED_TIMEOUT_THEN_HINT(false, true, true, true, true),
        ;

        final boolean initiallyEnableHints;
        final boolean initiallyEnableBatchlogReplay;
        final boolean initiallyBlockTestKeyspaceMutations;
        final boolean passesThroughBatchlog;
        final boolean deliversViaHint;

        Scenario(boolean initiallyEnableHints, boolean initiallyEnableBatchlogReplay, boolean initiallyBlockTestKeyspaceMutations, boolean passesThroughBatchlog, boolean deliversViaHint)
        {
            this.initiallyEnableHints = initiallyEnableHints;
            this.initiallyEnableBatchlogReplay = initiallyEnableBatchlogReplay;
            this.initiallyBlockTestKeyspaceMutations = initiallyBlockTestKeyspaceMutations;
            this.passesThroughBatchlog = passesThroughBatchlog;
            this.deliversViaHint = deliversViaHint;
        }
    }

    private final boolean migrateAwayFromAccord;

    protected AccordMigrationRaceTestBase()
    {
        this.migrateAwayFromAccord = migratingAwayFromAccord();
    }

    protected abstract boolean migratingAwayFromAccord();

    @Override
    protected Logger logger()
    {
        return logger;
    }

    @BeforeClass
    public static void setupClass() throws IOException
    {
        HINT_DISPATCH_INTERVAL_MS.setLong(100);
        ServerTestUtils.daemonInitialization();
        // Otherwise repair complains if you don't specify a keyspace
        CassandraRelevantProperties.SYSTEM_TRACES_DEFAULT_RF.setInt(3);
        AccordTestBase.setupCluster(builder -> builder.appendConfig(config -> config.set("paxos_variant", PaxosVariant.v2.name())
                                                                                    .set("accord.shard_durability_cycle", "1m")
                                                                                    .set("accord.shard_durability_target_splits", "1")
                                                                                    .set("accord.shard_durability_cycle", "60s")
                                                                                    .set("write_request_timeout", "2s")
                                                                                    .set("accord.range_migration", "explicit")), 3);
        partitioner = FBUtilities.newPartitioner(SHARED_CLUSTER.get(1).callsOnInstance(() -> DatabaseDescriptor.getPartitioner().getClass().getSimpleName()).call());
        StorageService.instance.setPartitionerUnsafe(partitioner);
        ServerTestUtils.prepareServerNoRegister();
        minToken = partitioner.getMinimumToken();
        maxToken = partitioner.getMaximumTokenForSplitting();
        midToken = partitioner.midpoint(minToken, maxToken);
        upperMidToken = partitioner.midpoint(midToken, maxToken);
        lowerMidToken = partitioner.midpoint(minToken, midToken);
        coordinator = SHARED_CLUSTER.coordinator(1);
        SHARED_CLUSTER.setMessageSink(messageSink);
    }

    @AfterClass
    public static void tearDownClass()
    {
        StorageService.instance.resetPartitionerUnsafe();
    }

    @After
    public void tearDown() throws Exception
    {
        messageSink.reset();
        forEach(() -> {
            BatchlogManager.instance.resumeReplay();
            HintsService.instance.deleteAllHintsUnsafe();
            HintsService.instance.resumeDispatch();
        });
        SHARED_CLUSTER.forEach(ClusterUtils::clearAndUnpause);
        super.tearDown();
        // Reset migration state
        forEach(() -> {
            ConsensusRequestRouter.resetInstance();
            ConsensusKeyMigrationState.reset();
        });
        truncateSystemTables();
    }

    private ListenableFuture<Void> alterTableTransactionalModeAsync(TransactionalMode mode)
    {
        ListenableFutureTask<Void> task = ListenableFutureTask.create(() -> {
            coordinator.execute(format("ALTER TABLE %s WITH %s", qualifiedAccordTableName, mode.asCqlParam()), ALL);
        }, null);
        Thread asyncThread = new Thread(task, "Alter table transaction mode " + mode);
        asyncThread.setDaemon(true);
        asyncThread.start();
        return task;
    }

    @Test
    public void testSplitAndRetryNonSerialUnloggedBatchTwoTablesOnePkey() throws Throwable
    {
        testSplitAndRetryMutationCoordination(twoTableBatchInsert(false, PKEY_ACCORD, PKEY_ACCORD, 1), validateTwoTable(PKEY_ACCORD));
    }

    @Test
    public void testSplitAndRetryNonSerialUnloggedBatchTwoTablesOnePkeyHinting() throws Throwable
    {
        // Accord doesn't hint if a write times out
        if (!migrateAwayFromAccord)
            testSplitAndRetryHintDelivery(twoTableBatchInsert(false, PKEY_ACCORD, PKEY_ACCORD, 1), validateTwoTable(PKEY_ACCORD));
    }

    @Test
    public void testSplitAndRetryNonSerialUnloggedBatchTwoTablesTwoPkey() throws Throwable
    {
        testSplitAndRetryMutationCoordination(twoTableBatchInsert(false, PKEY_ACCORD, PKEY_NORMAL, 1), validateTwoTable(PKEY_NORMAL));
    }

    @Test
    public void testSplitAndRetryNonSerialUnloggedBatchTwoTablesHinting() throws Throwable
    {
        // Accord doesn't hint if a write times out
        if (!migrateAwayFromAccord)
            testSplitAndRetryHintDelivery(twoTableBatchInsert(false, PKEY_ACCORD, PKEY_NORMAL, 1), validateTwoTable(PKEY_NORMAL));
    }

    @Test
    public void testSplitAndRetryNonSerialUnloggedBatchSingleTable() throws Throwable
    {
        testSplitAndRetryMutationCoordination(singleTableBatchInsert(false, PKEY_ACCORD, PKEY_NORMAL, 1), this::validateSingleTable);
    }

    @Test
    public void testSplitAndRetryNonSerialUnloggedBatchSingleTableHinting() throws Throwable
    {
        // Accord doesn't hint if a write times out
        if (!migrateAwayFromAccord)
            testSplitAndRetryHintDelivery(singleTableBatchInsert(false, PKEY_ACCORD, PKEY_NORMAL, 1), this::validateSingleTable);
    }

    /*
     * This doesn't really test much since on top of testSplitAndRetryNonSerialUnloggedBatchTwoTablesOnePkey since it is
     * a single table & key and will be converted to an unlogged batch
     */
    @Test
    public void testSplitAndRetryNonSerialLoggedBatchTwoTablesOnePkey() throws Throwable
    {
        testSplitAndRetryMutationCoordination(twoTableBatchInsert(true, PKEY_ACCORD, PKEY_ACCORD, 1), validateTwoTable(PKEY_ACCORD));
    }

    @Test
    public void testSplitAndRetryNonSerialLoggedBatchTwoTablesTwoPkey() throws Throwable
    {
        testSplitAndRetryMutationCoordination(twoTableBatchInsert(true, PKEY_ACCORD, PKEY_NORMAL, 1), validateTwoTable(PKEY_NORMAL));
    }

    @Test
    public void testSplitAndRetryNonSerialLoggedBatchTwoTablesTwoPkeyDeliverViaBatchLog() throws Throwable
    {
        testSplitAndRetryBatchlogDelivery(twoTableBatchInsert(true, PKEY_ACCORD, PKEY_NORMAL, 1), validateTwoTable(PKEY_NORMAL));
    }

    @Test
    public void testSplitAndRetryNonSerialLoggedBatchTwoTablesTwoPkeyHintedViaBatchLogTimeout() throws Throwable
    {
        testSplitAndRetryHintDeliveryAfterBatchlogTimeout(twoTableBatchInsert(true, PKEY_ACCORD, PKEY_NORMAL, 1), validateTwoTable(PKEY_NORMAL));
    }

    @Test
    public void testSplitAndRetryNonSerialLoggedBatchTwoTablesTwoPkeyHintedViaBatchLogRoutingFailure() throws Throwable
    {
        testSplitAndRetryHintDeliveryAfterBatchlogRoutingFailure(twoTableBatchInsert(true, PKEY_ACCORD, PKEY_NORMAL, 1), validateTwoTable(PKEY_NORMAL));
    }

    /*
     * Test that a logged batch writing to a migrating table and a non-migrating table can
     */
    @Test
    public void testSplitAndRetryNonSerialLoggedBatchSingleTable() throws Throwable
    {
        testSplitAndRetryBatchlogDelivery(singleTableBatchInsert(true, PKEY_ACCORD, PKEY_NORMAL, 1), this::validateSingleTable);
    }

    @Test
    public void testSplitAndRetryNonSerialLoggedBatchSingleTableDeliverViaBatchLog() throws Throwable
    {
        testSplitAndRetryBatchlogDelivery(singleTableBatchInsert(true, PKEY_ACCORD, PKEY_NORMAL, 1), this::validateSingleTable);
    }

    @Test
    public void testSplitAndRetryNonSerialLoggedBatchSingleTableHintedViaBatchLogTimeout() throws Throwable
    {
        testSplitAndRetryHintDeliveryAfterBatchlogTimeout(singleTableBatchInsert(true, PKEY_ACCORD, PKEY_NORMAL, 1), this::validateSingleTable);
    }

    @Test
    public void testSplitAndRetryNonSerialLoggedBatchSingleTableHintedViaBatchLogRoutingFailure() throws Throwable
    {
        testSplitAndRetryHintDeliveryAfterBatchlogRoutingFailure(singleTableBatchInsert(true, PKEY_ACCORD, PKEY_NORMAL, 1), this::validateSingleTable);
    }

    private void testSplitAndRetryMutationCoordination(String batchCQL, Consumer<Cluster> validation) throws Throwable
    {
        testSplitAndRetry(batchCQL, validation, MUTATION);
    }

    private void testSplitAndRetryBatchlogDelivery(String batchCQL, Consumer<Cluster> validation) throws Throwable
    {
        testSplitAndRetry(batchCQL, validation, BATCHLOG_SUCCESSFUL_ROUTING);
    }

    private void testSplitAndRetryHintDeliveryAfterBatchlogTimeout(String batchCQL, Consumer<Cluster> validation) throws Throwable
    {
        testSplitAndRetry(batchCQL, validation, BATCHLOG_FAILED_TIMEOUT_THEN_HINT);
    }

    private void testSplitAndRetryHintDeliveryAfterBatchlogRoutingFailure(String batchCQL, Consumer<Cluster> validation) throws Throwable
    {
        testSplitAndRetry(batchCQL, validation, BATCHLOG_FAILED_ROUTING_THEN_HINT);
    }

    private void testSplitAndRetryHintDelivery(String batchCQL, Consumer<Cluster> validation) throws Throwable
    {
        testSplitAndRetry(batchCQL, validation, HINT);
    }

    private void validateSingleTable(Cluster cluster)
    {
        SimpleQueryResult expected = QueryResults.builder()
                                                 .columns("id", "c", "v")
                                                 .row(PKEY_NORMAL, 1, 1)
                                                 .row(PKEY_ACCORD, 1, 1)
                                                 .build();
        cluster.forEach(instance -> {
            assertThat(instance.executeInternalWithResult("SELECT * FROM " + qualifiedAccordTableName)).isEqualTo(expected);
        });
    }

    private Consumer<Cluster> validateTwoTable(int secondPkey)
    {
        return cluster -> {
            SimpleQueryResult expectedAccord = QueryResults.builder()
                                                           .columns("id", "c", "v")
                                                           .row(PKEY_ACCORD, 1, 1)
                                                           .build();
            cluster.forEach(instance -> assertThat(instance.executeInternalWithResult("SELECT * FROM " + qualifiedAccordTableName)).isEqualTo(expectedAccord));

            SimpleQueryResult expectedNormal = QueryResults.builder()
                                                           .columns("id", "c", "v")
                                                           .row(secondPkey, 1, 1)
                                                           .build();
            cluster.forEach(instance -> assertThat(instance.executeInternalWithResult("SELECT * FROM " + qualifiedRegularTableName)).isEqualTo(expectedNormal));
        };
    }

    /*
     * Test if the coordinator is behind that the request can be re-split and routed to the correct systems
     * without surfacing an error
     */
    private void testSplitAndRetry(String batchCQL, Consumer<Cluster> validation, Scenario scenario) throws Throwable
    {
        test(createTables(TABLE_FMT, qualifiedRegularTableName, qualifiedAccordTableName),
             cluster -> {
                 // Only enable these when testing it works from a specific instance
                 forEach(() -> BatchlogManager.instance.pauseReplay());
                 forEach(() -> HintsService.instance.pauseDispatch());

                 // Node 3 is always the out of sync node
                 IInvokableInstance outOfSyncInstance = setUpOutOfSyncNode(cluster);

                 // Force the batchlog Accord txn to run after this write txn in the new epoch where it
                 // will trigger RetryDifferentSystem
                 if (scenario == BATCHLOG_FAILED_ROUTING_THEN_HINT && migrateAwayFromAccord)
                     writeAccordRowViaAccord();

                 // Need to be able to block writing to the test keyspace forcing batchlog replay
                 // without also failing writes to the batch log
                 if (scenario.initiallyBlockTestKeyspaceMutations)
                     cluster.filters().outbound().messagesMatching((from, to, message) -> {
                         if (message.verb() == Verb.MUTATION_REQ.id)
                         {
                             String keyspace = cluster.get(to).callsOnInstance(() -> ((Message<Mutation>)Instance.deserializeMessage(message)).payload.getKeyspaceName()).call();
                             if (keyspace.equals(KEYSPACE))
                                 return true;
                         }
                         if (message.verb() == Verb.ACCORD_PRE_ACCEPT_REQ.id && !migrateAwayFromAccord)
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

                 forEach(() -> BatchlogManager.instance.pauseReplay());

                 // If testing batch log delivery the coordinator needs to be a node other than the node that is behind on
                 // topology updates so that the batch log writes (and thus replay) can be done on the node that is out of sync
                 int coordinatorIndex = scenario.initiallyEnableBatchlogReplay ? 2 : 3;
                 IInvokableInstance instance = cluster.get(coordinatorIndex);
                 ICoordinator coordinator = instance.coordinator();
                 int startRetryCount = getRetryOnDifferentSystemCount(coordinatorIndex);
                 // If testing routing at mutation coordination then Node 1 and 2 will both rejected the mutation because it is in a migrating range
                 int startRejectedCount = getMutationsRejectedOnWrongSystemCount();
                 logger.info("Executing batch insert");
                 Future<SimpleQueryResult> resultFuture = coordinator.asyncExecuteWithResult(batchCQL, ALL);

                 // Testing either batch log delivery or hint delivery via batchlog
                 if (scenario.initiallyBlockTestKeyspaceMutations)
                 {
                     // Expect initial write failure
                     expectException(() -> {
                         try
                         {
                             return resultFuture.get();
                         }
                         catch (ExecutionException e)
                         {
                             throw (Exception) e.getCause();
                         }
                     }, WriteTimeoutException.class);
                 }

                 if (scenario.passesThroughBatchlog)
                 {
                     // At this stage we want the batch log to fail because it misrouted the queries to the wrong system
                     // not because it timed out not getting a response. We only did that with mutations as a quick
                     // way to populate the batch log. Could almost as easily have constructed the mutation and put it
                     // in the batch log directly
                     if (scenario == Scenario.BATCHLOG_FAILED_ROUTING_THEN_HINT || scenario == BATCHLOG_SUCCESSFUL_ROUTING)
                        cluster.filters().reset();

                     // We only want the batch log to have access to the correct topology if we are testing its
                     // ability to handle misrouted things
                     if (scenario == Scenario.BATCHLOG_SUCCESSFUL_ROUTING)
                         unpauseEnactment(outOfSyncInstance);

                     // Unfortunately the batch won't be replayed until some time has passed because the starting time
                     // for replay is the current time - timeout
                     // Don't wait here for the batchlog if we need to spin on the creation of the Accord transaction
                     // and then unpause to test Accord routing failure
                     boolean unpauseAfterBatchLogCreatesTransaction = migrateAwayFromAccord && scenario == BATCHLOG_FAILED_ROUTING_THEN_HINT;
                     if (!unpauseAfterBatchLogCreatesTransaction)
                        Thread.sleep(BatchlogManager.BATCHLOG_REPLAY_TIMEOUT + DatabaseDescriptor.getWriteRpcTimeout(TimeUnit.MILLISECONDS));
                     messageSink.reset();

                     // Force batch log delivery (or hint delivery) on the node that was out of sync, but should be in sync once we unpause
                     // This demonstrates it can split the mutation correctly or forward it to hinting if it fails
                     outOfSyncInstance.runOnInstance(() -> runUnchecked(() -> {
                         // We don't want hints for any reason that might apply the mutation and make the test look like it succeeded
                         assertTrue(HintsService.instance.isDispatchPaused());
                         // The failed write will have written hints
                         HintsService.instance.deleteAllHintsUnsafe();
                         assertFalse(hasPendingHints());
                         BatchlogManager.instance.resumeReplay();

                         // Unpausing needs to be done async because it waits for the batch log replay
                         Promise<Void> unpaused = new AsyncPromise<>();
                         if (unpauseAfterBatchLogCreatesTransaction)
                         {
                             logger.info("Creating thread to unpause after batchlog creates Accord transaction");
                             new Thread(() -> {
                                     try
                                     {
                                         // Unpause so it can route incorrectly instead of timing out waiting to fetch the epoch, need the transaction to be created first
                                         // otherwise it will just be routed straight to non-Accord.
                                         logger.info("Spinning waiting on a transaction");
                                         Util.spinUntilTrue(() -> !((AccordService)AccordService.instance()).node().coordinating().isEmpty(), 20);
                                         logger.info("Found transaction, unpausing");
                                         TestChangeListener.instance.unpause();
                                         unpaused.trySuccess(null);
                                     }
                                     catch (Throwable t)
                                     {
                                         unpaused.tryFailure(t);
                                     }
                                 }).start();
                         }
                         else
                         {
                             // Force replay so mosts tests don't have to wait
                             BatchlogManager.instance.forceBatchlogReplay();
                             unpaused.trySuccess(null);
                         }
                         // Fetch errors
                         unpaused.get();
                         // Ensure the batch log did or didn't create pending hints depending on the test scenario
                         spinAssertEquals(scenario == BATCHLOG_FAILED_TIMEOUT_THEN_HINT || scenario == BATCHLOG_FAILED_ROUTING_THEN_HINT, () -> hasPendingHints(), 20);
                     }));
                 }

                 // Mutation successfully applied from the coordinator after retrying scenario
                 if (scenario == MUTATION)
                 {
                     // Don't want to mistakenly have hints applying the mutation
                     forEach(() -> assertTrue(HintsService.instance.isDispatchPaused()));
                     // Check for the error differently depending on what system should be seeing an error
                     if (migrateAwayFromAccord)
                     {
                         // Accord will block until we unpause enactment so to test the routing we wait until the transaction
                         // has started so the epoch it is created in is the old one
                         Util.spinUntilTrue(() -> outOfSyncInstance.callOnInstance(() -> !((AccordService)AccordService.instance()).node().coordinating().isEmpty()), 20);
                         logger.info("Accord node is now coordinating something");
                         try
                         {
                             validation.accept(cluster);
                             throw new AssertionError("Expected validation to fail");
                         }
                         catch (AssertionError e)
                         {
                             //ignored
                         }
                     }
                     else
                     {
                         Stopwatch sw = Stopwatch.createStarted();
                         spinAssertEquals(startRejectedCount + 2, 10, () -> getMutationsRejectedOnWrongSystemCount() - startRejectedCount);
                         logger.info("Took {}ms to get mutations rejected on wrong system", sw.elapsed(TimeUnit.MILLISECONDS));
                     }

                     logger.info("Unpausing out of sync instance");
                     // Testing regular mutation coordination retry loop let coordinator get up to date and retry
                     unpauseEnactment(outOfSyncInstance);

                     try
                     {
                         resultFuture.get();
                     }
                     catch (ExecutionException e)
                     {
                         // This is expected when inverting the migration
                         if (migrateAwayFromAccord && e.getCause() instanceof CoordinatorBehindException)
                             throw e;
                         throw e;
                     }

                     if (!migrateAwayFromAccord)
                     {
                         int endRetryCount = getRetryOnDifferentSystemCount(coordinatorIndex);
                         int endRejectedCount = getMutationsRejectedOnWrongSystemCount();
                         assertEquals(1, endRetryCount - startRetryCount);
                         // Expect only two nodes to reject since they enacted the new epoch
                         assertEquals(2, endRejectedCount - startRejectedCount);
                     }
                 }

                 // Anything related to making sure hints are delivered goes here
                 if (scenario.deliversViaHint)
                 {
                     // Don't want to mistakenly have hints applying the mutation before we enable it on just one instance
                     forEach(() -> assertTrue(HintsService.instance.isDispatchPaused()));
                     // The filters wouldn't have been reset yet if they were needed to make the batchlog or original mutation time out
                     // Need to reset so Hints can use Accord txns
                     cluster.filters().reset();
                     long startingAccordTimeouts = outOfSyncInstance.callOnInstance(() -> ClientRequestsMetricsHolder.accordWriteMetrics.timeouts.getCount());
                     long startingAccordPreempted = outOfSyncInstance.callOnInstance(() -> ClientRequestsMetricsHolder.accordWriteMetrics.preempted.getCount());
                     long startingAccordMigrationRejects = outOfSyncInstance.callOnInstance(() -> ClientRequestsMetricsHolder.accordWriteMetrics.accordMigrationRejects.getCount());
                     long startingHintTimeouts = outOfSyncInstance.callOnInstance(() -> HintsServiceMetrics.hintsTimedOut.getCount());
                     outOfSyncInstance.runOnInstance(() -> HintsService.instance.resumeDispatch());
                     // The initial hinting attempt should fail, unless it's a batchlog routing failure in which
                     // case the coordinator has already caught up so the hint will succeed on the first try
                     // Can only really have this case for BATCHLOG_FAILED_TIMEOUT_THEN_HINT because Accord timeouts don't
                     // write hints so there is nothing to test
                     if (migrateAwayFromAccord && scenario == BATCHLOG_FAILED_TIMEOUT_THEN_HINT)
                     {
                         Callable<Boolean> test = () -> outOfSyncInstance.callOnInstance(() -> {
                             HintsService.instance.flushAndFsyncBlockingly();
                             logger.info("startingAccordTimeouts {}, startingAccordPreempts {}, startingAccordMigrationRejects {}, startingHintTimeouts {}, accord timeouts {}, accordPreempts {}, accordMigrationRejects {}, hint timeouts {}", startingAccordTimeouts, startingAccordPreempted, startingAccordMigrationRejects, startingHintTimeouts, ClientRequestsMetricsHolder.accordWriteMetrics.timeouts.getCount(), ClientRequestsMetricsHolder.accordWriteMetrics.preempted.getCount(), ClientRequestsMetricsHolder.accordWriteMetrics.accordMigrationRejects.getCount(), HintsServiceMetrics.hintsTimedOut.getCount());
                             AccordClientRequestMetrics accordMetrics = ClientRequestsMetricsHolder.accordWriteMetrics;
                             return accordMetrics.timeouts.getCount() >= (startingAccordTimeouts + 1) && HintsServiceMetrics.hintsTimedOut.getCount() >= (startingHintTimeouts + 1);
                         });
                         Util.spinUntilTrue(test, 40);
                     }
                     else if (!migrateAwayFromAccord)
                     {
                         // Expect two retry on different system responses when migrating from Paxos to Accord, one from each
                         // node that knows it is on the wrong system
                         Util.spinUntilTrue(() -> messageSink.messages.stream().filter(p -> {
                             if (p.right.verb() != Verb.FAILURE_RSP.id)
                                 return false;
                             if (!p.left.equals(outOfSyncInstance.broadcastAddress()))
                                 return false;
                             RequestFailureReason reason = ((RequestFailure) Instance.deserializeMessage(p.right).payload).reason;
                             if (reason == RETRY_ON_DIFFERENT_TRANSACTION_SYSTEM)
                                 return true;
                             return false;
                         }).count() == 2, 20);
                     }
                     // After this hints should deliver and the final validation should succeed
                     // if we don't unpause enactment
                     unpauseEnactment(outOfSyncInstance);
                 }

                 // Accord commit is async and might take a while, but the data should end up as expected
                 Util.spinUntilSuccess(() -> validation.accept(cluster));
             });
    }

    /*
     * Set up 3 to be behind and unaware of the migration while 1 and 2 are aware
     */
    private IInvokableInstance setUpOutOfSyncNode(Cluster cluster) throws Throwable
    {
        IInvokableInstance i1 = cluster.get(1);
        IInvokableInstance i2 = cluster.get(2);
        IInvokableInstance i3 = cluster.get(3);
        alterTableTransactionalMode(TransactionalMode.full);
        Epoch nextEpoch = getNextEpoch(i1);
        // Node 3 will coordinate the query and not be aware that the migration has begun
        Callable<?> pausedBeforeEnacting = pauseBeforeEnacting(i3, nextEpoch);
        // In batch log delivery cases i2 will be the coordinator and we need to be sure that it has enacted the latest epoch
        Callable<?> i2PausedAfterEnacting = pauseAfterEnacting(i2, nextEpoch);

        ListenableFuture<?> result = nodetoolAsync(coordinator, "consensus_admin", "begin-migration", "-st", midToken.toString(), "-et", maxToken.toString(), "-tp", "accord", KEYSPACE, accordTableName);

        if (migrateAwayFromAccord)
        {
            pausedBeforeEnacting.call();
            i2PausedAfterEnacting.call();
            unpauseEnactment(i2);
            unpauseEnactment(i3);
            result.get();
            long migratingEpoch = nextEpoch.getEpoch();
            Util.spinUntilTrue(() -> cluster.stream().allMatch(instance -> instance.callOnInstance(() -> ClusterMetadata.current().epoch.equals(Epoch.create(migratingEpoch)))), 10);
            nextEpoch = getNextEpoch(i1);
            pausedBeforeEnacting = pauseBeforeEnacting(i3, nextEpoch);
            i2PausedAfterEnacting = pauseAfterEnacting(i2, nextEpoch);
            // In the reverse direction doing the alter automatically reverses the migratin without a need to call begin migration on any ranges
            result = alterTableTransactionalModeAsync(TransactionalMode.off);
        }

        // Wait for everyone to get to where they are supposed to be
        try
        {
            pausedBeforeEnacting.call();
        }
        catch (Throwable t)
        {
            if (result.isDone())
            {
                try
                {
                    result.get();
                }
                catch (ExecutionException e)
                {
                    t.addSuppressed(e);
                    throw t;
                }
            }
            throw t;
        }
        i2PausedAfterEnacting.call();
        // Unpause on 1 and 2 where we want them aware of the migration
        unpauseEnactment(i1);
        unpauseEnactment(i2);
        // nodetool should be able to complete now
        result.get();
        return i3;
    }

    private String twoTableBatchInsert(boolean logged, int pkey1, int pkey2, int value)
    {
        return batch(logged,
                     insertCQL(qualifiedAccordTableName, pkey1, value),
                     insertCQL(qualifiedRegularTableName, pkey2, value));
    }

    private String singleTableBatchInsert(boolean logged, int pkey1, int pkey2, int value)
    {
        return batch(logged,
                     insertCQL(qualifiedAccordTableName, pkey1, value),
                     insertCQL(qualifiedAccordTableName, pkey2, value));
    }

    private static String insertCQL(String qualifiedTableName, int pkey, int value)
    {
        return format("INSERT INTO %s ( id, c, v ) VALUES ( %d, %d, %d )", qualifiedTableName, pkey, CLUSTERING_VALUE, value);
    }

    // Prevents the creation of transactions in an older epoch because later writes need to order after earlier
    private void writeAccordRowViaAccord()
    {
        logger.info("Initiating Accord row write");
        SHARED_CLUSTER.coordinator(1).execute(insertCQL(qualifiedAccordTableName, PKEY_ACCORD, 99), ConsistencyLevel.QUORUM);
        logger.info("Finished Accord row write");
    }
}
