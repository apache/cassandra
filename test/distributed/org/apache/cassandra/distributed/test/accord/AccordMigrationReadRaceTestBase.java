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
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableSet;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.primitives.Ranges;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.Config.PaxosVariant;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.api.IMessageSink;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.accord.InteropTokenRangeTest.TokenOperator;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.RepairResult;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.service.accord.repair.AccordRepair.AccordRepairResult;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.service.consensus.migration.ConsensusMigrationRepairResult;
import org.apache.cassandra.service.consensus.migration.ConsensusTableMigration;
import org.apache.cassandra.service.consensus.migration.TransactionalMigrationFromMode;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throwables.ThrowingRunnable;
import org.assertj.core.api.Assertions;
import org.eclipse.jetty.util.ConcurrentHashSet;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static org.apache.cassandra.Util.spinAssertEquals;
import static org.apache.cassandra.dht.Murmur3Partitioner.LongToken.keyForToken;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.shared.ClusterUtils.getNextEpoch;
import static org.apache.cassandra.distributed.shared.ClusterUtils.pauseBeforeEnacting;
import static org.apache.cassandra.distributed.shared.ClusterUtils.unpauseEnactment;
import static org.apache.cassandra.distributed.test.accord.InteropTokenRangeTest.TokenOperator.gt;
import static org.apache.cassandra.distributed.test.accord.InteropTokenRangeTest.TokenOperator.gte;
import static org.apache.cassandra.distributed.test.accord.InteropTokenRangeTest.TokenOperator.lt;
import static org.apache.cassandra.distributed.test.accord.InteropTokenRangeTest.TokenOperator.lte;
import static org.apache.cassandra.distributed.util.QueryResultUtil.assertThat;
import static org.apache.cassandra.utils.ByteBufferUtil.bytesToHex;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.junit.Assert.assertEquals;

/*
 * Test that non-transactional read operations migrating to/from a mode where Accord ignores commit consistency levels
 * and does aysnc commit are routed correctly. Currently this is just TransactionalMode.full
 */
public abstract class AccordMigrationReadRaceTestBase extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordMigrationReadRaceTestBase.class);
    private static final int TEST_BOUNDS_CONCURRENCY = 32;
    // Set BATCH_INDEX to the failing batch and this to true to find out the query index, then set QUERY_INDEX
    private static final boolean EXECUTE_BATCH_QUERIES_SERIALLY = false;
    // Specify only a single batch or query to run
    private static final Integer BATCH_INDEX = null;
    private static final Integer QUERY_INDEX = null;
    private static final String TABLE_FMT = "CREATE TABLE %s (pk blob, c int, v int, PRIMARY KEY ((pk), c));";

    private static IPartitioner partitioner;

    private static Range<Token> migratingRange;

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

    private final boolean migrateAwayFromAccord;

    protected AccordMigrationReadRaceTestBase()
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
        ServerTestUtils.daemonInitialization();
        // Otherwise repair complains if you don't specify a keyspace
        CassandraRelevantProperties.SYSTEM_TRACES_DEFAULT_RF.setInt(3);
        AccordTestBase.setupCluster(builder -> builder.appendConfig(config -> config.set("paxos_variant", PaxosVariant.v2.name())
                                                                                    .set("read_request_timeout", "10s")
                                                                                    .set("range_request_timeout", "10s")
                                                                                    .set("write_request_timeout", "10s")
                                                                                    .set("accord.range_migration", "explicit")), 3);
        partitioner = FBUtilities.newPartitioner(SHARED_CLUSTER.get(1).callsOnInstance(() -> DatabaseDescriptor.getPartitioner().getClass().getSimpleName()).call());
        StorageService.instance.setPartitionerUnsafe(partitioner);
        ServerTestUtils.prepareServerNoRegister();
        LongToken migrationStart = new LongToken(Long.valueOf(SHARED_CLUSTER.get(2).callOnInstance(() -> DatabaseDescriptor.getInitialTokens().iterator().next())));
        LongToken migrationEnd = new LongToken(Long.valueOf(SHARED_CLUSTER.get(3).callOnInstance(() -> DatabaseDescriptor.getInitialTokens().iterator().next())));
        migratingRange = new Range<>(migrationStart, migrationEnd);
        coordinator = SHARED_CLUSTER.coordinator(1);
        SHARED_CLUSTER.setMessageSink(messageSink);
        buildData();
    }

    private static final int NUM_PARTITIONS = 1000;
    private static final int ROWS_PER_PARTITION = 10;
    private static final Object[][][] data = new Object[NUM_PARTITIONS][][];
    private static final Object[][] dataFlat = new Object[NUM_PARTITIONS * ROWS_PER_PARTITION][];
    private static ByteBuffer pkeyAccord;
    private static int pkeyAccordDataIndex;

    private static void buildData()
    {
        Random r = new Random(0);
        long[] tokens = new long[NUM_PARTITIONS];
        for (int i = 0; i < tokens.length; i++)
            tokens[i] = r.nextLong();
        Arrays.sort(tokens);

        for (int i = 0; i < NUM_PARTITIONS; i++)
        {
            data[i] = new Object[ROWS_PER_PARTITION][];
            ByteBuffer pk = keyForToken(tokens[i]);
            for (int j = 0; j < ROWS_PER_PARTITION; j++)
            {
                int clustering = r.nextInt();
                data[i][j] = new Object[] { pk, clustering, 42 };
            }
            Arrays.sort(data[i], Comparator.comparing(row -> (Integer)row[1]));
        }
        for (int i = 0; i < NUM_PARTITIONS; i++)
        {
            for (int j = 0; j < ROWS_PER_PARTITION; j++)
            {
                int idx = i * ROWS_PER_PARTITION + j;
                dataFlat[idx] = new Object[] { data[i][j][0], data[i][j][1], data[i][j][2] };
                if (migratingRange.contains(Murmur3Partitioner.instance.getToken((ByteBuffer)data[i][j][0])))
                {
                    pkeyAccord = (ByteBuffer)data[i][j][0];
                    pkeyAccordDataIndex = i;
                }
            }
        }
    }

    @AfterClass
    public static void tearDownClass()
    {
        StorageService.instance.resetPartitionerUnsafe();
    }

    @After
    public void tearDown() throws Exception
    {
        super.tearDown();
        messageSink.reset();
        SHARED_CLUSTER.forEach(ClusterUtils::clearAndUnpause);
        super.tearDown();
    }

    private void loadData() throws Exception
    {
        logger.info("Starting data load");
        Stopwatch sw = Stopwatch.createStarted();
        List<java.util.concurrent.Future<SimpleQueryResult>> inserts = new ArrayList<>();
        for (int i = 0; i < NUM_PARTITIONS; i++)
        {
            for (int j = 0; j < ROWS_PER_PARTITION; j++)
                inserts.add(coordinator.asyncExecuteWithResult(insertCQL(qualifiedAccordTableName, (ByteBuffer)data[i][j][0], (int)data[i][j][1], (int)data[i][j][2]), ALL));

            if (i % 100 == 0)
            {
                for (java.util.concurrent.Future<SimpleQueryResult> insert : inserts)
                    insert.get();
                inserts.clear();
            }
        }
        logger.info("Data load took %dms", sw.elapsed(TimeUnit.MILLISECONDS));
    }

    private NavigableSet<Long> boundsTokens()
    {
        long migratingRangeStart = migratingRange.left.getLongValue();
        long migratingRangeEnd = migratingRange.right.getLongValue();
        NavigableSet<Long> set = new TreeSet<>();
        set.add(migratingRangeStart - 1);
        set.add(migratingRangeStart);
        set.add(migratingRangeStart + 1);
        set.add(migratingRangeEnd - 1);
        set.add(migratingRangeEnd);
        set.add(migratingRangeEnd + 1);
        set.add(Long.MAX_VALUE);
        set.add(Long.MIN_VALUE + 1);
        set.add(0L);
        return set;
    }

    private void loadOverlapData()
    {
        for (long token : boundsTokens())
            coordinator.executeWithResult(insertCQL(qualifiedAccordTableName, keyForToken(token), 42, 43), ALL);
    }

    @Test
    public void testKeyRouting() throws Throwable
    {
       String readCQL = "SELECT * FROM " + qualifiedAccordTableName + " WHERE pk = 0x" + bytesToHex(pkeyAccord);
       testSplitAndRetry(readCQL, this::loadData, result -> assertThat(result).isDeepEqualTo(data[pkeyAccordDataIndex]));
    }

    @Test
    public void testRangeRouting() throws Throwable
    {
        String cql = "SELECT * FROM " + qualifiedAccordTableName + " WHERE token(pk) > " + Murmur3Partitioner.MINIMUM.token;
        testSplitAndRetry(cql, this::loadData, result -> {
            assertThat(result).isDeepEqualTo(dataFlat);
        });
    }

    @Test
    public void testBounds() throws Throwable
    {
        NavigableSet<Long> tokens = boundsTokens();
        Queue<String> queries = new ArrayDeque<>();
        Queue<Consumer<SimpleQueryResult>> validations = new ArrayDeque<>();
        Queue<String> retryExpectedQueries = new ArrayDeque<>();
        Queue<Consumer<SimpleQueryResult>> retryExpectedValidations = new ArrayDeque<>();
        for (long firstToken : tokens)
        {
            ByteBuffer pk = keyForToken(firstToken);
            for (TokenOperator op : TokenOperator.values())
            {
                String cql = "SELECT * FROM %s WHERE " + op.condition;
                cql = cql.replace("?", "0x" + bytesToHex(pk));
                NavigableSet<Long> expectedTokens = op.expected(firstToken, tokens);
                boolean expectRetry = op.intersects(firstToken, migratingRange);
                Consumer<SimpleQueryResult> validation = result -> {
                    Assertions.assertThat(InteropTokenRangeTest.tokens(result))
                              .describedAs("Token %d with operator %s", firstToken, op.condition)
                              .isEqualTo(expectedTokens);
                };
                if (expectRetry)
                {
                    retryExpectedQueries.add(cql);
                    retryExpectedValidations.add(validation);
                }
                else
                {
                    queries.add(cql);
                    validations.add(validation);
                }
            }

            for (long secondToken : tokens)
            {
                for (TokenOperator lt : Arrays.asList(lt, lte))
                {
                    for (TokenOperator gt : Arrays.asList(gt, gte))
                    {
                        ByteBuffer gtPk = keyForToken(secondToken);
                        String cql = "SELECT * FROM %s WHERE " + lt.condition + " AND " + gt.condition;
                        cql = cql.replaceFirst("\\?", "0x" + bytesToHex(pk));
                        cql = cql.replaceFirst("\\?", "0x" + bytesToHex(gtPk));
                        NavigableSet<Long> expectedTokens = new TreeSet<>(Sets.intersection(lt.expected(firstToken, tokens), gt.expected(secondToken, tokens)));
                        Consumer<SimpleQueryResult> validation = result -> {
                            Assertions.assertThat(InteropTokenRangeTest.tokens(result))
                                      .describedAs("LT Token %d GT Token %d with operators %s / %s", firstToken, secondToken, lt.condition, gt.condition)
                                      .isEqualTo(expectedTokens);
                        };
                        boolean expectRetry = lt.intersects(firstToken, migratingRange) && gt.intersects(secondToken, migratingRange);
                        // This evaluates to no rows without actually executing
                        if (firstToken == secondToken && (lt == TokenOperator.lt || gt == TokenOperator.gt))
                            expectRetry = false;
                        if (firstToken < secondToken)
                            expectRetry = false;
                        if (expectRetry)
                        {
                            retryExpectedQueries.add(cql);
                            retryExpectedValidations.add(validation);
                        }
                        else
                        {
                            queries.add(cql);
                            validations.add(validation);
                        }
                    }
                }

                ByteBuffer rhsPK = keyForToken(secondToken);
                String cql = "SELECT * FROM %s WHERE token(pk) BETWEEN token(?) AND token(?)";
                cql = cql.replaceFirst("\\?", "0x" + bytesToHex(pk));
                cql = cql.replaceFirst("\\?", "0x" + bytesToHex(rhsPK));
                NavigableSet<Long> expectedTokens = new TreeSet<>(Sets.intersection(gte.expected(firstToken, tokens), lte.expected(secondToken, tokens)));
                Consumer<SimpleQueryResult> validation = result -> {
                    Assertions.assertThat(InteropTokenRangeTest.tokens(result))
                              .describedAs("Between token %d and %d with operator token(pk) BETWEEN token(?) AND token(?)", firstToken, secondToken)
                              .isEqualTo(expectedTokens);
                };
                // Cassandra straight up returns the wrong answer here so until it is fixed skip it
                // https://issues.apache.org/jira/browse/CASSANDRA-20154
                if (firstToken > secondToken)
                    continue;
                boolean expectRetry = gte.intersects(firstToken, migratingRange) && lte.intersects(secondToken, migratingRange);
                if (expectRetry)
                {
                    retryExpectedQueries.add(cql);
                    retryExpectedValidations.add(validation);
                }
                else
                {
                    queries.add(cql);
                    validations.add(validation);
                }
            }
        }

        testBoundsBatches(queries, validations, false);
        testBoundsBatches(retryExpectedQueries, retryExpectedValidations, true);
    }

    private void testBoundsBatches(Queue<String> queries, Queue<Consumer<SimpleQueryResult>> validations, boolean expectRetry) throws Throwable
    {
        List<String> queryBatch = new ArrayList<>();
        List<Consumer<SimpleQueryResult>> validationBatch = new ArrayList<>();
        int batchCount = 0;
        while (!queries.isEmpty())
        {
            queryBatch.add(queries.poll());
            validationBatch.add(validations.poll());
            if (queryBatch.size() == TEST_BOUNDS_CONCURRENCY)
            {
                if (BATCH_INDEX == null || BATCH_INDEX == batchCount)
                {
                    logger.info("Executing batch {} with query count {}", batchCount, queryBatch.size());
                    testBoundsBatch(queryBatch, validationBatch, expectRetry, batchCount);
                }
                else
                {
                    logger.info("Skipping batch {}", batchCount);
                }
                batchCount++;
                queryBatch.clear();
                validationBatch.clear();
            }
        }

        if (!queryBatch.isEmpty())
        {
            logger.info("Executing trailing batch " + batchCount + " with query count " + queryBatch.size());
            testBoundsBatch(queryBatch, validationBatch, expectRetry, batchCount);
        }
    }

    private void testBoundsBatch(List<String> readCQL, List<Consumer<SimpleQueryResult>> validation, boolean expectRetry, int batchCount) throws Throwable
    {
        if (EXECUTE_BATCH_QUERIES_SERIALLY)
        {
            for (int i = 0; i < readCQL.size(); i++)
            {
                if (QUERY_INDEX == null || QUERY_INDEX == i)
                {
                    logger.info("Executing query from batch {} query index {}", batchCount, i);
                    String cql = format(readCQL.get(i), qualifiedAccordTableName);
                    testSplitAndRetry(ImmutableList.of(cql), this::loadOverlapData, ImmutableList.of(validation.get(i)), expectRetry);
                    tearDown();
                    setup();
                    afterEach();
                }
                else
                {
                    logger.info("Skipping query from batch {} query index {}", batchCount, i);
                }
            }
        }
        else
        {
            readCQL = readCQL.stream().map(cql -> format(cql, qualifiedAccordTableName)).collect(toImmutableList());
            testSplitAndRetry(readCQL, this::loadOverlapData, validation, expectRetry);
            tearDown();
            setup();
            afterEach();
        }
    }

    private void testSplitAndRetry(String readCQL, ThrowingRunnable load, Consumer<SimpleQueryResult> validation) throws Throwable
    {
        testSplitAndRetry(ImmutableList.of(readCQL), load, ImmutableList.of(validation),true);
    }

    private void testSplitAndRetry(List<String> readCQL, ThrowingRunnable load, List<Consumer<SimpleQueryResult>> validation, boolean expectRetry) throws Throwable
    {
        test(createTables(TABLE_FMT, qualifiedAccordTableName),
             cluster -> {
                 load.run();
                 // Node 3 is always the out of sync node
                 IInvokableInstance outOfSyncInstance = setUpOutOfSyncNode(cluster);
                 ICoordinator coordinator = outOfSyncInstance.coordinator();
                 int startMigrationRejectCount = getAccordReadMigrationRejects(3);
                 int startRetryCount = getReadRetryOnDifferentSystemCount(outOfSyncInstance);
                 int startRejectedCount = getReadsRejectedOnWrongSystemCount();
                 logger.info("Executing reads " + readCQL + " expect retry " + expectRetry);
                 List<Future<SimpleQueryResult>> results = readCQL.stream()
                                                                  .map(read -> coordinator.asyncExecuteWithResult(read, ALL))
                                                                  .collect(toImmutableList());

                 if (migrateAwayFromAccord && expectRetry)
                 {
                     int expectedTransactions = readCQL.size();
                     // Accord will block until we unpause enactment so to test the routing we wait until the transaction
                     // has started so the epoch it is created in is the old one
                     Util.spinUntilTrue(() -> outOfSyncInstance.callOnInstance(() -> {
                         logger.info("Coordinating {}", AccordService.instance().node().coordinating());
                         return AccordService.instance().node().coordinating().size() == expectedTransactions;
                     }));

                     logger.info("Accord node is now coordinating something, unpausing so it can continue to execute");
                 }

                 if (!migrateAwayFromAccord && expectRetry)
                     spinAssertEquals(readCQL.size() * 2, 10, () -> getReadsRejectedOnWrongSystemCount() - startRejectedCount);

                 // Accord can't finish the transaction without unpausing
                 if (expectRetry || migrateAwayFromAccord)
                 {
                     logger.info("Unpausing out of sync instance before waiting on result");
                     // Testing read coordination retry loop let coordinator get up to date and retry
                     unpauseEnactment(outOfSyncInstance);
                 }

                 try
                 {
                     for (int i = 0; i < results.size(); i++)
                     {
                         SimpleQueryResult result = results.get(i).get();
                         logger.info("Result for: " + readCQL.get(i));
                         logger.info(result.toString());
                         try
                         {
                             validation.get(i).accept(result);
                         }
                         catch (Throwable t)
                         {
                             logger.info("Query index {} failed", i);
                             throw t;
                         }
                     }
                 }
                 catch (ExecutionException e)
                 {
                     throw e;
                 }

                 if (!expectRetry)
                 {
                     logger.info("Unpausing out of sync instance after waiting on result");
                     // Testing read coordination retry loop let coordinator get up to date and retry
                     unpauseEnactment(outOfSyncInstance);
                 }

                 int endRetryCount = getReadRetryOnDifferentSystemCount(outOfSyncInstance);
                 int endRejectedCount = getReadsRejectedOnWrongSystemCount();
                 int endMigrationRejects = getAccordReadMigrationRejects(3);
                 if (expectRetry)
                 {
                     if (migrateAwayFromAccord)
                     {
                         assertEquals(readCQL.size(), endRetryCount - startRetryCount);
                         assertEquals(readCQL.size(), endMigrationRejects - startMigrationRejectCount);
                     }
                     else
                     {
                         assertEquals(1 * readCQL.size(), endRetryCount - startRetryCount);
                         // Expect only two nodes to reject since they enacted the new epoch
                         assertEquals(2 * readCQL.size(), endRejectedCount - startRejectedCount);
                     }
                 }
                 else
                 {
                     assertEquals(0, endRetryCount - startRetryCount);
                     assertEquals(0, endRejectedCount - startRejectedCount);
                 }
             });
    }

    /*
     * Set up 3 to be behind and unaware of the migration having progressed to the point where reads need to
     * be on a different system while 1 and 2 are aware
     */
    private IInvokableInstance setUpOutOfSyncNode(Cluster cluster) throws Throwable
    {
        IInvokableInstance i1 = cluster.get(1);
        IInvokableInstance i2 = cluster.get(2);
        IInvokableInstance i3 = cluster.get(3);

        long afterAlter = getNextEpoch(i1).getEpoch();
        logger.info("Epoch after alter {}", afterAlter);
        if (migrateAwayFromAccord)
            alterTableTransactionalMode(TransactionalMode.off, TransactionalMigrationFromMode.full);
        else
            alterTableTransactionalMode(TransactionalMode.full);
        Util.spinUntilTrue(() -> cluster.stream().allMatch(instance -> instance.callOnInstance(() -> ClusterMetadata.current().epoch.equals(Epoch.create(afterAlter)))), 10);

        long afterMigrationStart = getNextEpoch(i1).getEpoch();
        logger.info("Epoch after migration start {}", afterMigrationStart);
        long waitFori1Andi2ToEnact = afterMigrationStart;
        // Migrating away from Accord need i3 to pause before enacting
        if (migrateAwayFromAccord)
            pauseBeforeEnacting(i3, Epoch.create(afterMigrationStart));
        // Reads are allowed until Accord thinks it owns the range and can start doing async commit and ignoring consistency levels
        nodetool(coordinator, "consensus_admin", "begin-migration", "-st", migratingRange.left.toString(), "-et", migratingRange.right.toString(), KEYSPACE, accordTableName);

        if (!migrateAwayFromAccord)
        {
            // Migration to Accord does not have Accord read until the migration has completed a data repair and then an Accord repair
            Util.spinUntilTrue(() -> cluster.stream().allMatch(instance -> instance.callOnInstance(() -> ClusterMetadata.current().epoch.equals(Epoch.create(afterMigrationStart)))), 10);

            long afterRepair = getNextEpoch(i1).getEpoch();
            logger.info("Epoch after repair {}", afterRepair);
            // First repair only does the data and allows Accord to read, but doesn't require reads to be done through Accord
            nodetool(i2, "repair", "-skip-paxos", "-skip-accord", "-st", migratingRange.left.toString(), "-et", migratingRange.right.toString(), KEYSPACE, accordTableName);
            Util.spinUntilTrue(() -> cluster.stream().allMatch(instance -> instance.callOnInstance(() -> ClusterMetadata.current().epoch.equals(Epoch.create(afterRepair)))), 10);

            long afterRepairCompletionHandler = getNextEpoch(i1).getEpoch();
            logger.info("Epoch after repair completion handler {}", afterRepairCompletionHandler);
            waitFori1Andi2ToEnact = afterRepairCompletionHandler;
            // Node 3 will coordinate the query and not be aware that the migration has begun
            pauseBeforeEnacting(i3, Epoch.create(afterRepairCompletionHandler));

            // Unfortunately can't run real repair because it can't complete with i3 not responding because it's stuck waiting
            // on TCM so fake the completion of the repair by invoking the completion handler directly
            String keyspace = KEYSPACE;
            String table = accordTableName;
            long migratingTokenStart = migratingRange.left.getLongValue();
            long migratingTokenEnd = migratingRange.right.getLongValue();
            Future<?> result = SHARED_CLUSTER.get(1).asyncRunsOnInstance(() ->
                                                                         {
                                                                             Epoch startEpoch = ClusterMetadata.current().epoch;
                                                                             TableId tableId = Schema.instance.getTableMetadata(keyspace, table).id;
                                                                             List<Range<Token>> ranges = ImmutableList.of(new Range<>(new LongToken(migratingTokenStart), new LongToken(migratingTokenEnd)));
                                                                             RepairJobDesc desc = new RepairJobDesc(null, null, keyspace, table, ranges);
                                                                             TokenRange range = TokenRange.create(new TokenKey(tableId, new LongToken(migratingTokenStart)), new TokenKey(tableId, new LongToken(migratingTokenEnd)));
                                                                             Ranges accordRanges = Ranges.of(range);
                                                                             AccordRepairResult accordRepairResult = new AccordRepairResult(accordRanges, TimeUnit.MILLISECONDS.toMicros(currentTimeMillis()));
                                                                             ConsensusMigrationRepairResult repairResult = ConsensusMigrationRepairResult.fromRepair(startEpoch, accordRepairResult, true, true, true, false, false);
                                                                             ConsensusTableMigration.completedRepairJobHandler.onSuccess(new RepairResult(desc, null, repairResult));
                                                                         }).call();
            result.get();
        }

        long waitFori1Andi2ToEnactFinal = waitFori1Andi2ToEnact;
        // Make sure 1 and 2 are up to date
        for (int i = 1; i < 3; i++)
        {
            int instanceIndex = i;
            Util.spinUntilTrue(() -> cluster.get(instanceIndex).callOnInstance(() -> ClusterMetadata.current().epoch.equals(Epoch.create(waitFori1Andi2ToEnactFinal))), 10);
        }

        return i3;
    }

    private static String insertCQL(String qualifiedTableName, ByteBuffer pkey, int clustering, int value)
    {
        return format("INSERT INTO %s ( pk, c, v ) VALUES ( 0x%s, %d, %d )", qualifiedTableName, bytesToHex(pkey), clustering, value);
    }
}
