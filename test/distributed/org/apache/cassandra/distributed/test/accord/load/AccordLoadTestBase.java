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

package org.apache.cassandra.distributed.test.accord.load;

import java.io.IOException;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.RateLimiter;

import org.agrona.collections.IntArrayList;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.Catchup;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.ExecutionContext;
import accord.local.SafeCommand;
import accord.primitives.PartialDeps;
import accord.primitives.TxnId;
import accord.utils.Functions;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.test.accord.AccordTestBase;
import org.apache.cassandra.distributed.test.accord.load.LoadSettings.ClusterChaos;
import org.apache.cassandra.metrics.AccordCoordinatorMetrics;
import org.apache.cassandra.metrics.AccordExecutorMetrics;
import org.apache.cassandra.metrics.ShardedDecayingHistograms.ShardedDecayingHistogram;
import org.apache.cassandra.metrics.ShardedHistogram;
import org.apache.cassandra.metrics.SnapshottingTimer;
import org.apache.cassandra.net.ArtificialLatency;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.api.AccordAgent;
import org.apache.cassandra.service.accord.debug.AccordTracing;
import org.apache.cassandra.service.accord.debug.AccordTracing.Message;
import org.apache.cassandra.service.accord.debug.CoordinationKinds;
import org.apache.cassandra.service.accord.debug.TxnKindsAndDomains;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import static accord.coordinate.Coordination.CoordinationKind.Client;
import static accord.coordinate.Coordination.CoordinationKind.Execute;
import static accord.local.BootstrapReason.LOG_CORRUPTED;
import static accord.local.BootstrapReason.LOG_INCOMPLETE;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.UNIT_TESTS;
import static org.apache.cassandra.distributed.test.accord.load.LoadSettings.ClusterChaos.REBOOTSTRAP_RESET;
import static org.apache.cassandra.service.accord.debug.AccordTracing.BucketMode.LEAKY;
import static org.apache.cassandra.service.accord.debug.AccordTracing.BucketMode.RING;
import static org.apache.cassandra.service.accord.debug.AccordTracing.BucketMode.SLOWEST;

public class AccordLoadTestBase extends AccordTestBase
{
    private static long CHAOS_TIMEOUT_NANOS = TimeUnit.MINUTES.toNanos(10L);
    private static final Logger logger = LoggerFactory.getLogger(AccordLoadTestBase.class);

    @Before
    public void setup()
    {
        setupCluster();
        super.setup();
    }

    public void setupCluster()
    {
        setupCluster(5);
    }

    public void setupCluster(int nodeCount)
    {
        setupCluster(nodeCount, config -> {
            config.with(Feature.NETWORK, Feature.GOSSIP)
                  .set("accord.shard_durability_target_splits", "8")
                  .set("accord.shard_durability_max_splits", "16")
                  .set("accord.shard_durability_cycle", "1m")
                  .set("accord.queue_submission_model", "SIGNAL")
                  .set("accord.command_store_shard_count", "8")
                  .set("accord.queue_thread_count", "4")
                  .set("accord.queue_shard_count", "1")
                  .set("accord.replica_execution", "ALL")
                  .set("accord.send_stable", "TO_ALL_REPLICA_EXECUTABLE_ELSE_FOR_READS")
                  .set("accord.send_minimal", "false")
                  .set("accord.catchup_on_start_fail_latency", "2m");
        });
    }

    public void setupCluster(int nodeCount, Consumer<IInstanceConfig> configure)
    {
        Invariants.require(SHARED_CLUSTER == null);
        try { SHARED_CLUSTER = createCluster(nodeCount, builder -> builder.withDCs(nodeCount).withConfig(configure)); }
        catch (IOException e) { throw new RuntimeException(e); }
        CassandraRelevantProperties.SIMULATOR_STARTED.setString(Long.toString(MILLISECONDS.toSeconds(currentTimeMillis())));
    }

    public void testLoad(final LoadSettings settings) throws Exception
    {
        Cluster cluster = SHARED_CLUSTER;
        cluster.schemaChange("CREATE TABLE " + qualifiedAccordTableName + " (k int, v int, PRIMARY KEY(k)) WITH transactional_mode = 'full'");
//        long seed = new SecureRandom().nextLong();
        long seed = 3705626102508196273L;
        try
        {
            final ConcurrentHashMap<Verb, AtomicInteger> verbs = new ConcurrentHashMap<>();
            cluster.filters().outbound().messagesMatching(new IMessageFilters.Matcher()
            {
                @Override
                public boolean matches(int i, int i1, IMessage iMessage)
                {
                    verbs.computeIfAbsent(Verb.fromId(iMessage.verb()), ignore -> new AtomicInteger()).incrementAndGet();
                    return false;
                }
            }).drop();

            boolean waitForTransactions = settings.totalTransactions < Long.MAX_VALUE;
            boolean waitForClusterChaos = settings.totalClusterChaos < Long.MAX_VALUE;

            int clientCount = settings.clients < 0 ? cluster.size() : settings.clients;
            long nextRepairAt = settings.repairInterval;
            long nextCompactionAt = settings.compactionInterval;
            long nextJournalFlushAt = settings.journalFlushInterval;
            long nextDataFlushAt = settings.dataFlushInterval;
            long nextCfkFlushAt = settings.cfkFlushInterval;
            long nextChaosAt = settings.clusterChaosInterval;
            final ExecutorService chaosExecutor = Executors.newFixedThreadPool(settings.clusterChaosConcurrency, new NamedThreadFactory("ClusterChaos"));
            final ExecutorService clientExecutor = Executors.newFixedThreadPool(clientCount, new NamedThreadFactory("Client"));
            final BitSet initialised = new BitSet();
            final Supplier<ClusterChaos> clusterChaos = settings.clusterChaos.apply(seed);

            List<ChaosActive> chaosActive = new ArrayList<>();
            cluster.get(1).nodetoolResult("cms", "reconfigure", "datacenter1:1", "datacenter2:1", "datacenter3:1").asserts().success();
            if (settings.cfkCompactionPeriodSeconds < Integer.MAX_VALUE && settings.cfkCompactionPeriodSeconds > 0)
            {
                cluster.forEach(i -> i.acceptOnInstance(period -> {
                    ((AccordService) AccordService.instance()).journal().compactor().updateCompactionPeriod(period, SECONDS);
                }, settings.cfkCompactionPeriodSeconds));
            }

            if (settings.artificialLatencies != null)
            {
                for (int i = 0 ; i < cluster.size() ; ++i)
                {
                    StringBuilder str = new StringBuilder();
                    for (int j = 0 ; j < settings.artificialLatencies[i].length ; ++j)
                    {
                        if (j > 0)
                            str.append(",");
                        str.append("datacenter")
                           .append(j + 1)
                           .append(':')
                           .append(settings.artificialLatencies[i][j])
                           .append("ms");
                    }
                    cluster.get(i + 1).acceptOnInstance(latencies -> {
                        ArtificialLatency.setArtificialLatencies(latencies);
                        ArtificialLatency.setArtificialLatencyOnlyPermittedConsistencyLevels(false);
                        ArtificialLatency.setArtificialLatencyVerbs(ArtificialLatency.recommendedVerbs());
                        ArtificialLatency.setEnabled(true);
                    }, str.toString());
                }
            }

            if (settings.traceSlowest > 0f)
            {
                float traceSlowest = settings.traceSlowest;
                for (int i = 0 ; i < cluster.size() ; ++i)
                {
                    cluster.get(i + 1).runOnInstance(() -> {
                        AccordTracing tracing = ((AccordAgent) AccordService.unsafeInstance().agent()).tracing();
                        tracing.setPattern(1, pattern -> pattern.withChance(traceSlowest)
                                                                .withKinds(TxnKindsAndDomains.parse("{K*}"))
                                                                .withTraceNew(CoordinationKinds.ALL),
                                           SLOWEST, -1, 2, LEAKY, 10, 1, CoordinationKinds.ALL);
                    });
                }
            }

            if (settings.traceLast > 0)
            {
                int traceLast = settings.traceLast;
                for (int i = 0 ; i < cluster.size() ; ++i)
                {
                    cluster.get(i + 1).runOnInstance(() -> {
                        AccordTracing tracing = ((AccordAgent) AccordService.unsafeInstance().agent()).tracing();
                        tracing.setPattern(2, pattern -> pattern.withKinds(TxnKindsAndDomains.parse("{KW}"))
                                                                    .withTraceNew(CoordinationKinds.ALL),
                                           RING, -1, traceLast, LEAKY, 10, 1, CoordinationKinds.ALL);
                    });
                }
            }

            final AtomicBoolean stop = new AtomicBoolean();
            final AtomicBoolean pauseOrStop = new AtomicBoolean();
            final WaitQueue waitQueue = WaitQueue.newWaitQueue();
            final Random random = new Random();
            final Semaphore completed = new Semaphore(0);
            final AtomicIntegerArray coordinatorIndexes = new AtomicIntegerArray(clientCount);
            final Set<Integer> chaosCandidates = new ConcurrentSkipListSet<>();
            for (int i = 1; i <= cluster.size() ; ++i)
                chaosCandidates.add(i);
            final List<java.util.concurrent.Future<?>> clients = new ArrayList<>();
            final AtomicReferenceArray<RateLimiter> rateLimiters = new AtomicReferenceArray<>(clientCount);
            final AtomicReference<EstimatedHistogram> readHistogram = new AtomicReference<>(new EstimatedHistogram(200));
            final AtomicReference<EstimatedHistogram> writeHistogram = new AtomicReference<>(new EstimatedHistogram(200));
            final List<String> chaosHistory = new ArrayList<>();
            if (settings.clients >= cluster.size())
                throw new IllegalArgumentException("Cannot have more clients than nodes");
            if (settings.clusterChaosInterval < Integer.MAX_VALUE && settings.clients + 1 >= cluster.size())
                throw new IllegalArgumentException("If restarting, cannot have as many clients as nodes, as must reroute client requests during restart");

            int clientRatePerSecond = Math.min(settings.ratePerSecond, settings.minRatePerSecond) / clientCount;
            for (int client = 0 ; client < clientCount ; ++client)
            {
                rateLimiters.set(client, RateLimiter.create(clientRatePerSecond));
                final int clientIndex = client;
                coordinatorIndexes.set(client, client + 1);
                clients.add(clientExecutor.submit(() -> {
                    final Semaphore inFlight = new Semaphore(settings.clientConcurrency);
                    while (true)
                    {
                        while (pauseOrStop.get())
                        {
                            if (stop.get())
                                break;

                            WaitQueue.Signal signal = waitQueue.register();
                            if (pauseOrStop.get()) signal.awaitThrowUncheckedOnInterrupt();
                            else signal.cancel();
                        }

                        int coordinatorIdx = coordinatorIndexes.get(clientIndex);
                        ICoordinator coordinator = cluster.coordinator(coordinatorIdx);
                        try
                        {
                            rateLimiters.get(clientIndex).acquire();
                            inFlight.acquire();
                            long commandStart = System.nanoTime();
                            IntArrayList keys = new IntArrayList(settings.keysPerOperation, -1);
                            for (int i = 0 ; i < settings.keysPerOperation ; ++i)
                            {
                                int k = settings.keySelector.getAsInt();
                                if (!keys.containsInt(k))
                                    keys.add(k);
                            }
                            if (!keys.intStream().allMatch(initialised::get))
                            {
                                coordinator.executeWithResult((success, fail) -> {
                                    inFlight.release();
                                    completed.release();
                                    if (fail == null)
                                    {
                                        long elapsed = System.nanoTime() - commandStart;
                                        writeHistogram.get().add(NANOSECONDS.toMicros(elapsed));
                                        synchronized (initialised)
                                        {
                                            keys.forEachInt(initialised::set);
                                        }
                                    }
                                    else
                                    {
                                        logger.error("{}", fail.toString());
                                    }
                                }, "UPDATE " + qualifiedAccordTableName + " SET v = 0 WHERE k IN ?", ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM, keys);
                            }
                            else if (random.nextFloat() < settings.readRatio)
                            {
                                coordinator.executeWithResult((success, fail) -> {
                                    inFlight.release();
                                    completed.release();
                                    if (fail == null)
                                        readHistogram.get().add(NANOSECONDS.toMicros(System.nanoTime() - commandStart));
                                }, "BEGIN TRANSACTION\n" +
                                   "SELECT * FROM " + qualifiedAccordTableName + " WHERE k IN ?;\n" +
                                   "COMMIT TRANSACTION;", ConsistencyLevel.SERIAL, keys
                                );
                            }
                            else
                            {
                                coordinator.executeWithResult((success, fail) -> {
                                    inFlight.release();
                                    completed.release();
                                    if (fail == null)
                                    {
                                        long elapsed = System.nanoTime() - commandStart;
                                        writeHistogram.get().add(NANOSECONDS.toMicros(elapsed));
                                    }
                                    else
                                        logger.error("{}", fail.toString());
                                }, "BEGIN TRANSACTION\n" +
                                   //                               "UPDATE " + qualifiedAccordTableName + " SET v = ? WHERE k = ?;\n" +
                                   "UPDATE " + qualifiedAccordTableName + " SET v += ? WHERE k IN ?;\n" +
                                   "COMMIT TRANSACTION;", ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM, random.nextInt(100), keys);
                            }
                        }
                        catch (RejectedExecutionException e)
                        {
                            inFlight.release();
                        }
                        catch (InterruptedException e)
                        {
                            throw new UncheckedInterruptedException(e);
                        }
                    }
                }));
            }

            int targetClientRatePerSecond = settings.ratePerSecond / clientCount;
            int nextRateLimitIncrease = settings.increaseRatePerSecondInterval;
            long remainingTransactions = settings.totalTransactions;
            int remainingClusterChaos = settings.totalClusterChaos;
            while (true)
            {
                long batchStart = System.nanoTime();
                int batchSize = 0;

                if (completed.tryAcquire(settings.batchSize, settings.batchPeriodNanos, NANOSECONDS))
                    batchSize = settings.batchSize;
                batchSize += completed.drainPermits();

                if (clientRatePerSecond < targetClientRatePerSecond)
                {
                    if ((nextRateLimitIncrease -= batchSize) <= 0)
                    {
                        clientRatePerSecond = Math.min(clientRatePerSecond * 2, targetClientRatePerSecond);
                        for (int i = 0 ; i < clientCount ; ++i)
                            rateLimiters.set(i, RateLimiter.create(clientRatePerSecond));
                        nextRateLimitIncrease = settings.increaseRatePerSecondInterval;
                    }
                }

                if ((nextRepairAt -= batchSize) <= 0)
                {
                    nextRepairAt += settings.repairInterval;
                    System.out.println("repairing...");
                    cluster.coordinator(1).instance().nodetool("repair", qualifiedAccordTableName);
                }

                if ((nextCompactionAt -= batchSize) <= 0)
                {
                    nextCompactionAt += settings.compactionInterval;
                    compactJournalCfs(cluster);
                }

                if ((nextJournalFlushAt -= batchSize) <= 0)
                {
                    nextJournalFlushAt += settings.journalFlushInterval;
                    flushJournal(cluster);
                }

                if ((nextDataFlushAt -= batchSize) <= 0)
                {
                    nextDataFlushAt += settings.dataFlushInterval;
                    flushData(cluster);
                }

                if ((nextCfkFlushAt -= batchSize) <= 0)
                {
                    nextCfkFlushAt += settings.cfkFlushInterval;
                    flushCfk(cluster);
                }

                if ((nextChaosAt -= batchSize) <= 0)
                {
                    Iterator<ChaosActive> iter = chaosActive.iterator();
                    while (iter.hasNext())
                    {
                        ChaosActive chaos = iter.next();
                        if (chaos.future.isDone())
                        {
                            chaos.future.get();
                            iter.remove();
                            Invariants.require(cluster.size() <= chaosActive.size() + chaosCandidates.size());
                        }
                        else
                        {
                            long elapsedNanos = Clock.Global.nanoTime() - chaos.startedAt;
                            if (elapsedNanos >= CHAOS_TIMEOUT_NANOS)
                                throw new AssertionError("Chaos " + chaos.kind + " has been running for " + NANOSECONDS.toSeconds(elapsedNanos) + "s with seed " + seed);
                        }
                    }

                    if (chaosActive.size() < settings.clusterChaosConcurrency)
                    {
                        if (remainingClusterChaos > 0)
                        {
                            --remainingClusterChaos;
                            nextChaosAt += settings.clusterChaosInterval;
                            ClusterChaos chaos = clusterChaos.get();
                            chaosActive.add(new ChaosActive(chaos, chaos(cluster, coordinatorIndexes, chaosCandidates, chaosExecutor, random, chaos, chaosHistory)));
                        }
                        else if (chaosActive.isEmpty() && (remainingTransactions <= 0 || !waitForTransactions))
                        {
                            break;
                        }
                    }
                }

                if ((remainingTransactions -= batchSize) <= 0 && (remainingClusterChaos <= 0 || !waitForClusterChaos))
                    break;

                long nowMillis = System.currentTimeMillis();
                EstimatedHistogram reads = readHistogram.getAndSet(new EstimatedHistogram(200));
                EstimatedHistogram writes = writeHistogram.getAndSet(new EstimatedHistogram(200));

                maybePrintSlowestTraces(cluster, settings);
                maybePrintLastTraces(cluster, pauseOrStop, settings, waitQueue);
                printInstanceMetrics(nowMillis, cluster);
                printRates(nowMillis, batchStart, batchSize, reads, writes);
                printVerbs(nowMillis, verbs);
            }
        }
        catch (Throwable t)
        {
            t.printStackTrace();
            System.exit(1);
        }

        logger.info("Workload completed successfully");
    }

    static void safeForEach(Cluster cluster, IIsolatedExecutor.SerializableRunnable run)
    {
        safeForEach(cluster, ignore -> run.run(), null);
    }

    static <P> void safeForEach(Cluster cluster, IIsolatedExecutor.SerializableConsumer<P> consumer, P param)
    {
        for (IInvokableInstance i : cluster)
        {
            try
            {
                if (!i.isShutdown())
                {
                    i.acceptOnInstance(consumer, param);
                }
            }
            catch (Throwable t)
            {
                logger.error("", t);
            }
        }
    }

    private void compactJournalCfs(Cluster cluster)
    {
        System.out.println("compacting journal cfs...");
        for (IInvokableInstance i : cluster)
        {
            try { i.nodetool("compact", "system_accord.journal"); }
            catch (Throwable t) { logger.error("", t); }
        }
    }

    private void flushJournal(Cluster cluster)
    {
        System.out.println("flushing journal...");
        safeForEach(cluster, () -> {
            if (AccordService.started())
                ((AccordService) AccordService.instance()).journal().closeCurrentSegmentForTestingIfNonEmpty();
        });
    }

    private void flushData(Cluster cluster)
    {
        System.out.println("flushing data...");
        safeForEach(cluster, name -> {
            Schema.instance.getColumnFamilyStoreInstance(Schema.instance.getTableMetadata(KEYSPACE, name).id).forceFlush(UNIT_TESTS);
        }, accordTableName);
    }

    private void flushCfk(Cluster cluster)
    {
        System.out.println("flushing cfk...");
        safeForEach(cluster, () -> {
            if (CommitLog.instance.isStarted())
                AccordKeyspace.AccordColumnFamilyStores.commandsForKey.forceFlush(UNIT_TESTS);
        });
    }

    private static class ChaosActive
    {
        final ClusterChaos kind;
        final Future<?> future;
        final long startedAt;

        private ChaosActive(ClusterChaos kind, Future<?> future)
        {
            this.kind = kind;
            this.future = future;
            this.startedAt = Clock.Global.nanoTime();
        }
    }

    private static Future<?> chaos(Cluster cluster, AtomicIntegerArray coordinatorIndexes, Set<Integer> candidates, ExecutorService chaosExecutor, Random random, ClusterChaos chaos, List<String> history)
    {
        List<Integer> snapshot = new ArrayList<>(candidates);
        int nodeIdx;
        {
            int i = random.nextInt(snapshot.size());
            Integer remove = snapshot.get(i);
            candidates.remove(remove);
            snapshot = new ArrayList<>(candidates);
            Invariants.require(snapshot.size() > 0);
            nodeIdx = remove;
        }

        for (int i = 0; i < coordinatorIndexes.length(); ++i)
        {
            if (nodeIdx == coordinatorIndexes.get(i))
            {
                int j = random.nextInt(snapshot.size());
                int replaceIdx = snapshot.get(j);
                coordinatorIndexes.set(j, replaceIdx);
            }
        }

        String describe = String.format("%s node %d...", chaos, nodeIdx);
        history.add(describe);
        System.out.println("========= BEGIN CHAOS ==========");
        System.out.println(describe);
        System.out.println(candidates);
        System.out.println("========= BEGIN CHAOS ==========");
        switch (chaos)
        {
            default: throw UnhandledEnum.unknown(chaos);
            case REBOOTSTRAP_INCOMPLETE:
            case REBOOTSTRAP_RESET:
            {
                IInvokableInstance node = cluster.get(nodeIdx);
                return node.asyncAcceptsOnInstance((Set<Integer> cnds) -> {
                    try
                    {
                        Node accordNode = AccordService.instance().node();
                        AccordService.getBlocking(accordNode.commandStores().rebootstrap(accordNode, chaos == REBOOTSTRAP_RESET ? LOG_CORRUPTED : LOG_INCOMPLETE));
                    }
                    finally
                    {
                        Invariants.require(cnds.add(nodeIdx));
                        System.out.println("========== END CHAOS ===========");
                        System.out.println(describe);
                        System.out.println("========== END CHAOS ===========");
                    }
                }).apply(candidates);
            }
            case REBOOTSTRAP_IF_BEHIND:
            {
                IInvokableInstance node = cluster.get(nodeIdx);
                return node.asyncAcceptsOnInstance((Set<Integer> cmds) -> {
                    try
                    {
                        AccordService.getBlocking(Catchup.rebootstrapIfBehind(AccordService.instance().node()));
                    }
                    finally
                    {
                        Invariants.require(cmds.add(nodeIdx));
                        System.out.println("========== END CHAOS ===========");
                        System.out.println(String.format("%s node %d...", chaos, nodeIdx));
                        System.out.println("========== END CHAOS ===========");
                    }
                }).apply(candidates);
            }
            case RESTART:
            case RESTART_AND_REBOOTSTRAP_INCOMPLETE:
            case RESTART_AND_REBOOTSTRAP_RESET:
            case RESTART_AND_REBOOTSTRAP_AFTER_TIMEOUT:
            {
                return chaosExecutor.submit(() -> {
                    IInvokableInstance node = cluster.get(nodeIdx);
                    try
                    {
                        node.shutdown().get();
                        switch (chaos)
                        {
                            case RESTART_AND_REBOOTSTRAP_AFTER_TIMEOUT:
                                node.config().set("accord.catchup_on_start_on_timeout", "REBOOTSTRAP");
                                node.config().set("accord.catchup_on_start_success_latency", "0s");
                                node.config().set("accord.catchup_on_start_fail_latency", "0s");
                                break;
                            case RESTART_AND_REBOOTSTRAP_INCOMPLETE:
                                node.config().set("accord.journal.replay", "REBOOTSTRAP_INCOMPLETE");
                                break;
                            case RESTART_AND_REBOOTSTRAP_RESET:
                                node.config().set("accord.journal.replay", "REBOOTSTRAP_RESET");
                                break;
                        }
                        node.startup();
                        return null;
                    }
                    catch (InterruptedException | ExecutionException e)
                    {
                        throw new RuntimeException(e);
                    }
                    finally
                    {
                        Invariants.require(candidates.add(nodeIdx));
                        node.config().set("accord.catchup_on_start_success_latency", "60s");
                        node.config().set("accord.catchup_on_start_fail_latency", "120s");
                        node.config().set("accord.catchup_on_start_on_timeout", "IGNORE");
                        node.config().set("accord.journal.replay", "PART_NON_DURABLE");
                        System.out.println("========== END CHAOS ===========");
                        System.out.println(String.format("%s node %d...", chaos, nodeIdx));
                        System.out.println("========== END CHAOS ===========");
                    }
                });
            }
        }
    }


    private static void printRates(long nowMillis, long batchStartNanos, long batchSize, EstimatedHistogram reads, EstimatedHistogram writes)
    {
        System.out.println(String.format("%tT.%tL rate: %.2f/s (%d total)", nowMillis, nowMillis, (((float)batchSize * 1000) / NANOSECONDS.toMillis(System.nanoTime() - batchStartNanos)), batchSize));
        System.out.println(String.format("%tT.%tL reads : %d %d %d %d %d %d", nowMillis, nowMillis, reads.percentile(.25)/1000, reads.percentile(.5)/1000, reads.percentile(.95)/1000, reads.percentile(.99)/1000, reads.percentile(.999)/1000, reads.percentile(1)/1000));
        System.out.println(String.format("%tT.%tL writes: %d %d %d %d %d %d", nowMillis, nowMillis, writes.percentile(.25)/1000, writes.percentile(.5)/1000, writes.percentile(.95)/1000, writes.percentile(.99)/1000, writes.percentile(.999)/1000, writes.percentile(1)/1000));
    }

    private static void printInstanceMetrics(long nowMillis, Cluster cluster)
    {
        safeForEach(cluster, () -> {
            refresh(AccordExecutorMetrics.INSTANCE.elapsedRunning);
            refresh(AccordExecutorMetrics.INSTANCE.elapsed);
            System.out.println(String.format("%tT.%tL (%d %d %d %d %d %d)ms (%d %d %d %d %d %d)ms (%d %d %d %d %.0f, %d %d %d)us %d %d %d", nowMillis, nowMillis,
                              getLatency(AccordCoordinatorMetrics.readMetrics.preacceptLatency, 0.5),
                              getLatency(AccordCoordinatorMetrics.readMetrics.executeLatency, 0.5),
                              getLatency(AccordCoordinatorMetrics.readMetrics.applyLatency, 0.5),
                              getLatency(AccordCoordinatorMetrics.readMetrics.preacceptLatency, 0.999),
                              getLatency(AccordCoordinatorMetrics.readMetrics.executeLatency, 0.999),
                              getLatency(AccordCoordinatorMetrics.readMetrics.applyLatency, 0.999),
                              getLatency(AccordCoordinatorMetrics.writeMetrics.preacceptLatency, 0.95),
                              getLatency(AccordCoordinatorMetrics.writeMetrics.executeLatency, 0.5),
                              getLatency(AccordCoordinatorMetrics.writeMetrics.applyLatency, 0.5),
                              getLatency(AccordCoordinatorMetrics.writeMetrics.preacceptLatency, 0.999),
                              getLatency(AccordCoordinatorMetrics.writeMetrics.executeLatency, 0.999),
                              getLatency(AccordCoordinatorMetrics.writeMetrics.applyLatency, 0.999),
                              getLatency(AccordExecutorMetrics.INSTANCE.elapsedRunning, 0.5),
                              getLatency(AccordExecutorMetrics.INSTANCE.elapsedRunning, 0.9),
                              getLatency(AccordExecutorMetrics.INSTANCE.elapsedRunning, 1.0),
                              getCount(AccordExecutorMetrics.INSTANCE.elapsedRunning),
                              getTotal(AccordExecutorMetrics.INSTANCE.elapsedRunning),
                              getLatency(AccordExecutorMetrics.INSTANCE.elapsed, 0.5),
                              getLatency(AccordExecutorMetrics.INSTANCE.elapsed, 0.9),
                              getLatency(AccordExecutorMetrics.INSTANCE.elapsed, 0.999),
                              AccordExecutorMetrics.INSTANCE.running.getValue(),
                              AccordExecutorMetrics.INSTANCE.waitingToRun.getValue(),
                              AccordExecutorMetrics.INSTANCE.preparingToRun.getValue()
            ));
            clear(AccordExecutorMetrics.INSTANCE.elapsedRunning);
            clear(AccordExecutorMetrics.INSTANCE.elapsed);
        });
    }

    private static void printVerbs(long nowMillis, Map<Verb, AtomicInteger> verbs)
    {
        class VerbCount
        {
            final Verb verb;
            final int count;

            VerbCount(Verb verb, int count)
            {
                this.verb = verb;
                this.count = count;
            }
        }
        List<VerbCount> verbCounts = new ArrayList<>();
        for (Map.Entry<Verb, AtomicInteger> e : verbs.entrySet())
        {
            int count = e.getValue().getAndSet(0);
            if (count != 0) verbCounts.add(new VerbCount(e.getKey(), count));
        }
        verbCounts.sort(Comparator.comparing(v -> -v.count));

        StringBuilder verbSummary = new StringBuilder();
        for (VerbCount vs : verbCounts)
        {
            {
                if (verbSummary.length() > 0)
                    verbSummary.append(", ");
                verbSummary.append(vs.verb);
                verbSummary.append(": ");
                verbSummary.append(vs.count);
            }
        }
        System.out.println(String.format("%tT.%tL verbs: %s", nowMillis, nowMillis, verbSummary));
    }

    private static void maybePrintSlowestTraces(Cluster cluster, LoadSettings settings)
    {
        if (settings.traceSlowest > 0f)
        {
            safeForEach(cluster, () -> {
                AccordTracing tracing = ((AccordAgent)AccordService.instance().agent()).tracing();

                tracing.forEach(Functions.alwaysTrue(), (txnId, state) -> {
                    state.forEach(event -> {
                        if (event.elapsedNanos() < MILLISECONDS.toNanos(100))
                            return;

                        for (Message message : event.messages())
                        {
                            long multiplier = message.atNanos < event.doneAtNanos() ? 1 : -1;
                            System.out.println(String.format("%s %s %s %s %s %s", txnId, event.kind, multiplier * (message.atNanos - event.atNanos)/1000000, message.nodeId, message.commandStoreId, message.message));
                        }
                    });
                });
                tracing.eraseAll();
            });
        }
    }

    private static void maybePrintLastTraces(Cluster cluster, AtomicBoolean pauseOrStop, LoadSettings settings, WaitQueue waitQueue)
    {
        if (settings.traceLast > 0)
        {
            pauseOrStop.set(true);
            Map<String, List<List<String>>> print = new HashMap<>();
            for (int i = 1 ; i <= cluster.size() ; ++i)
            {
                cluster.get(i).acceptOnInstance(out -> {
                    AccordService service = (AccordService)AccordService.instance();
                    AccordTracing tracing = ((AccordAgent)AccordService.instance().agent()).tracing();
                    PriorityQueue<SortedByElapsed> candidates = new PriorityQueue<>(Comparator.comparingLong(c -> -c.elapsedMicros));
                    tracing.forEach(Functions.alwaysTrue(), (txnId, events) -> {
                        events.forEach(event -> {
                            if (event.kind == Client)
                            {
                                long doneAtMicros = event.doneAtMicros();
                                long elapsedMicros = doneAtMicros - event.txnId().hlc();
                                if (elapsedMicros > 350000 && elapsedMicros < 390000)
                                    candidates.add(new SortedByElapsed(txnId, elapsedMicros));
                            }
                        });
                    });

                    AtomicInteger storeId = new AtomicInteger();
                    while (!candidates.isEmpty())
                    {
                        SortedByElapsed sortedCandidate = candidates.poll();
                        if (sortedCandidate.elapsedMicros < 300000)
                            return;

                        TxnId candidate = sortedCandidate.txnId;
                        storeId.lazySet(-1);
                        tracing.forEach(candidate, events -> {
                            events.forEach(event -> {
                                if (storeId.get() >= 0)
                                    return;
                                for (Message message : event.messages())
                                {
                                    if (message.nodeId < 0 && message.commandStoreId >= 0)
                                    {
                                        storeId.set(message.commandStoreId);
                                        break;
                                    }
                                }
                            });
                        });

                        if (storeId.get() >= 0)
                        {
                            CommandStore commandStore = service.node().commandStores().forId(storeId.get());
                            List<List<String>> result = AccordService.getBlocking(commandStore.submit(ExecutionContext.unsequenced(candidate, "LoadTest"), safeStore -> {
                                SafeCommand safeCommand = safeStore.unsafeGet(candidate);
                                PartialDeps deps = safeCommand.current().partialDeps();
                                if (deps == null)
                                    return null;
                                List<List<String>> infos = new ArrayList<>();
                                for (TxnId txnId : deps.txnIds())
                                {
                                    List<String> info = new ArrayList<>();
                                    info.add(txnId.toString());
                                    infos.add(info);
                                }
                                List<String> info = new ArrayList<>();
                                info.add(candidate.toString());
                                infos.add(info);
                                return infos;
                            }));

                            if (result != null)
                            {
                                for (List<String> info : result)
                                {
                                    TxnId txnId = TxnId.parse(info.get(0));
                                    AccordService.getBlocking(commandStore.execute(ExecutionContext.unsequenced(txnId, "LoadTest"), safeStore -> {
                                        SafeCommand safeCommand = safeStore.unsafeGet(txnId);
                                        if (safeCommand.current().executeAt != null)
                                            info.add(safeCommand.current().executeAt.toString());
                                    }));
                                }

                                out.put(candidate.toString(), result);
                                return;
                            }
                        }
                    }
                }, print);
            }

            for (int i = 1 ; i <= cluster.size() ; ++i)
            {
                cluster.get(i).acceptOnInstance(out -> {
                    AccordTracing tracing = ((AccordAgent)AccordService.instance().agent()).tracing();
                    for (Map.Entry<String, List<List<String>>> e : out.entrySet())
                    {
                        TxnId parentId = TxnId.parse(e.getKey());
                        for (List<String> infos : e.getValue())
                        {
                            TxnId depId = TxnId.parse(infos.get(0));
                            tracing.forEach(depId, events -> {
                                events.forEach(event -> {
                                    infos.add(event.kind + ": [" + (event.idMicros - parentId.hlc()) + "..." + (event.doneAtMicros() - parentId.hlc()) + "][" + (event.idMicros - depId.hlc()) + "..." + (event.doneAtMicros() - depId.hlc()) + "]");
                                    if (event.kind == Execute)
                                    {
                                        for (Message message : event.messages())
                                        {
                                            if (message.nodeId == parentId.node.id)
                                            {
                                                long atMicros = (event.idMicros + (message.atNanos - event.atNanos)/1000) - parentId.hlc();
                                                infos.add(atMicros + ": " + message.message);
                                            }
                                        }
                                    }
                                });
                            });
                        }
                    }
                }, print);
            }

            for (Map.Entry<String, List<List<String>>> e : print.entrySet())
            {
                System.out.println("======" + e.getKey() + "======");
                for (List<String> infos : e.getValue())
                    System.out.println(infos);
            }
            if (!print.isEmpty())
                System.out.println();
            pauseOrStop.set(false);
            waitQueue.signalAll();
        }
    }

    private static void refresh(Histogram histogram)
    {
        if (histogram instanceof ShardedHistogram)
            ((ShardedHistogram) histogram).refresh();
        if (histogram instanceof ShardedDecayingHistogram)
            ((ShardedDecayingHistogram) histogram).refresh();
    }

    private static long getLatency(Histogram histogram, double percentile)
    {
        return (long)(histogram.getSnapshot().getValue(percentile) / 1000);
    }

    private static long getCount(Histogram histogram)
    {
        return histogram.getSnapshot().size();
    }

    private static double getTotal(Histogram histogram)
    {
        Snapshot snapshot = histogram.getSnapshot();
        return (snapshot.getMean() * 0.0001d * snapshot.size());
    }

    private static void clear(Histogram histogram)
    {
        if (histogram instanceof ShardedHistogram)
            ((ShardedHistogram) histogram).clear();
        if (histogram instanceof ShardedDecayingHistogram)
            ((ShardedDecayingHistogram) histogram).clear();
    }

    private static long getLatency(Timer timer, double percentile)
    {
        if (timer instanceof SnapshottingTimer)
            return (long) (((SnapshottingTimer) timer).getPercentileSnapshot().getValue(percentile) / 1000);
        return (long)(timer.getSnapshot().getValue(0.999) / 1000);
    }

    private static long getSize(Timer timer)
    {
        if (timer instanceof SnapshottingTimer)
            return ((SnapshottingTimer) timer).getPercentileSnapshot().size();
        return timer.getSnapshot().size();
    }

    @Override
    protected Logger logger()
    {
        return logger;
    }

    static class SortedByElapsed
    {
        final TxnId txnId;
        final long elapsedMicros;

        SortedByElapsed(TxnId txnId, long elapsedMicros)
        {
            this.txnId = txnId;
            this.elapsedMicros = elapsedMicros;
        }
    }
}
