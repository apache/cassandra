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

package org.apache.cassandra.fuzz.accord.load;

import java.io.IOException;
import java.security.SecureRandom;
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
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
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

import accord.impl.progresslog.DefaultProgressLog;
import accord.local.Catchup;
import accord.local.CommandStore;
import accord.local.ExecutionContext;
import accord.local.Node;
import accord.local.SafeCommand;
import accord.local.durability.ShardDurability;
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
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.test.accord.AccordTestBase;
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
import org.apache.cassandra.service.accord.execution.CacheWedgeReport;
import org.apache.cassandra.tcm.CMSOperations;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.Throwables;
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
import static org.apache.cassandra.fuzz.accord.load.LoadSettings.ClusterChaos;
import static org.apache.cassandra.fuzz.accord.load.LoadSettings.ClusterChaos.REBOOTSTRAP_RESET;
import static org.apache.cassandra.service.accord.debug.AccordTracing.BucketMode.LEAKY;
import static org.apache.cassandra.service.accord.debug.AccordTracing.BucketMode.RING;
import static org.apache.cassandra.service.accord.debug.AccordTracing.BucketMode.SLOWEST;

public class AccordLoadTestBase extends AccordTestBase
{
    private static long CHAOS_WARN_NANOS = TimeUnit.MINUTES.toNanos(2L);
    private static long CHAOS_FAIL_NANOS = TimeUnit.MINUTES.toNanos(CassandraRelevantProperties.ACCORD_TEST_CHAOS_TIMEOUT.getLong(10L));
    private static final Logger logger = LoggerFactory.getLogger(AccordLoadTestBase.class);

    static
    {
        // this is a bit ugly, but to avoid specifying unique parameters for load tests that will cause strain on CI
        // simply allow us to override and disable paranoia/debug for this test to reduce overheads
        if (CassandraRelevantProperties.ACCORD_PARANOIA_PERMIT_TEST_OVERRIDE.getBoolean())
        {
            CassandraRelevantProperties.ACCORD_PARANOID.setBoolean(false);
            CassandraRelevantProperties.ACCORD_DEBUG.setBoolean(false);
            CassandraRelevantProperties.ACCORD_PARANOIA_CPU.setString(Invariants.Paranoia.NONE.name());
            CassandraRelevantProperties.ACCORD_PARANOIA_MEMORY.setString(Invariants.Paranoia.NONE.name());
            CassandraRelevantProperties.TEST_DEBUG_REF_COUNT.setBoolean(false);
        }
    }

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
        CassandraRelevantProperties.SIMULATOR_STARTED.setString(Long.toString(MILLISECONDS.toSeconds(currentTimeMillis())));
        try { SHARED_CLUSTER = createCluster(nodeCount, builder -> builder.withDCs(nodeCount).withConfig(configure)); }
        catch (IOException e) { throw new RuntimeException(e); }
        SHARED_CLUSTER.get(1).runOnInstance(() -> {
            ClusterMetadata metadata = ClusterMetadata.current();
            Map<String, Integer> rf = new HashMap<>();
            for (String dc : metadata.directory.knownDatacenters())
                rf.put(dc, 1);
            CMSOperations.instance.reconfigureCMS(rf);
        });
    }

    public void testLoad(final LoadSettings settings) throws Exception
    {
        Runnable stopClients = null;
        Cluster cluster = SHARED_CLUSTER;
        cluster.setUncaughtExceptionsFilter((instance, error) -> isExpectedDuringChaos(error));
        // make the repair-retry configuration visible in the log: with retries disabled a lost merkle tree response
        // silently strands the repair (and any rebootstrap waiting on it)
        cluster.get(1).runOnInstance(() -> LoggerFactory.getLogger(AccordLoadTestBase.class)
                                                       .info("repair retries: {} (merkle tree retries enabled: {})",
                                                             org.apache.cassandra.config.DatabaseDescriptor.getRepairRetrySpec(),
                                                             org.apache.cassandra.config.DatabaseDescriptor.getRepairRetrySpec().isMerkleTreeRetriesEnabled()));
        cluster.schemaChange("CREATE TABLE " + qualifiedAccordTableName + " (k int, v int, PRIMARY KEY(k)) WITH transactional_mode = 'full'");
        long seed = new SecureRandom().nextLong();
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
            final Random chaosRandom = new Random(seed);
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

            stopClients = () -> {
                stop.set(true);
                pauseOrStop.set(true);
                waitQueue.signalAll();
                chaosExecutor.shutdownNow();
                clientExecutor.shutdown();
                try
                {
                    if (!clientExecutor.awaitTermination(1, TimeUnit.MINUTES))
                    {
                        logger.warn("Clients did not stop within a minute; interrupting them");
                        clientExecutor.shutdownNow();
                    }
                }
                catch (InterruptedException e)
                {
                    clientExecutor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            };

            for (int client = 0 ; client < clientCount ; ++client)
            {
                rateLimiters.set(client, RateLimiter.create(clientRatePerSecond));
                final int clientIndex = client;
                coordinatorIndexes.set(client, client + 1);
                clients.add(clientExecutor.submit(() -> {
                    final Semaphore inFlight = new Semaphore(settings.clientConcurrency);
                    long sleep = 100;
                    try
                    {
                        while (!stop.get())
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
                                sleep = 10;
                            }
                            catch (Throwable t)
                            {
                                inFlight.release();
                                boolean fail = true;
                                if (t instanceof IllegalStateException)
                                {
                                    fail = !(t.getMessage().contains("Can't use shutdown node") || t.getMessage().contains("Accord service was not started"));
                                }
                                else if (t instanceof RejectedExecutionException)
                                {
                                    fail = false;
                                }
                                if (fail)
                                    Throwables.maybeFail(t);

                                Thread.sleep(sleep);
                                sleep = Math.min(1000, sleep * 2);
                            }
                        }
                    }
                    catch (Throwable t)
                    {
                        logger.error("Client failed exceptionally", t);
                        stop.set(true);
                        pauseOrStop.set(true);
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

                // Chaos liveness is checked on every iteration (i.e. at least every batchPeriodNanos), not only
                // when the operation counter trips: a stuck chaos operation stalls the workload, so gating this on
                // completed-operation count means a hang silently postpones its own detection indefinitely.
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
                            if (elapsedNanos >= CHAOS_WARN_NANOS)
                            {
                                String message = "Chaos " + chaos + " has been running for " + NANOSECONDS.toSeconds(elapsedNanos) + "s with seed " + seed;
                                if (elapsedNanos >= CHAOS_FAIL_NANOS) throw chaosStalled(message, cluster, chaos, chaosHistory);
                                else if (chaos.maybeWarn(elapsedNanos)) logger.warn("{}\n{}", message, stallReport(cluster, chaos, chaosHistory));
                            }
                        }
                    }
                }

                if ((nextChaosAt -= batchSize) <= 0)
                {
                    if (chaosActive.size() < settings.clusterChaosConcurrency)
                    {
                        if (remainingClusterChaos > 0)
                        {
                            String unhealthy = chaosGateReason(cluster, untouchedByChaos(cluster, chaosTouched),
                                                              1 + cluster.size() / 2);
                            if (unhealthy != null)
                            {
                                if (chaosHealthWaitSince == 0) chaosHealthWaitSince = System.nanoTime();
                                long waitedNanos = System.nanoTime() - chaosHealthWaitSince;
                                if (waitedNanos > CHAOS_FAIL_NANOS)
                                    throw new AssertionError("Cluster did not become healthy within "
                                                             + NANOSECONDS.toSeconds(waitedNanos) + "s, so the next chaos op was never started:"
                                                             + unhealthy + '\n' + stallReport(cluster, null, chaosHistory));

                                if (waitedNanos - chaosHealthWaitLogged > SECONDS.toNanos(15))
                                {
                                    chaosHealthWaitLogged = waitedNanos;
                                    logger.info("Deferring next chaos op ({}s): unhealthy:{}", NANOSECONDS.toSeconds(waitedNanos), unhealthy);
                                }
                                nextChaosAt += Math.min(settings.clusterChaosInterval, 4 * batchSize);
                            }
                            else
                            {
                            if (chaosHealthWaitSince != 0)
                            {
                                logger.info("Cluster healthy after deferring the next chaos op for {}s",
                                            NANOSECONDS.toSeconds(System.nanoTime() - chaosHealthWaitSince));
                                chaosHealthWaitSince = chaosHealthWaitLogged = 0;
                            }
                            --remainingClusterChaos;
                            nextChaosAt += settings.clusterChaosInterval;
                            ClusterChaos chaos = clusterChaos.get();
                            ChaosActive active = chaos(cluster, coordinatorIndexes, chaosCandidates, chaosExecutor, chaosRandom, chaos, chaosHistory);
                            chaosTouched.add(active.node);
                            chaosActive.add(active);
                            }
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
            if (stopClients != null)
                stopClients.run();
            throw t;
        }

        stopClients.run();
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
        final int node;
        long warnedAtNanos;

        private ChaosActive(ClusterChaos kind, Future<?> future, int node)
        {
            this.kind = kind;
            this.future = future;
            this.node = node;
            this.startedAt = Clock.Global.nanoTime();
        }

        /** rate-limits the slow-chaos report to one per CHAOS_WARN_NANOS */
        boolean maybeWarn(long elapsedNanos)
        {
            if (elapsedNanos - warnedAtNanos < CHAOS_WARN_NANOS)
                return false;
            warnedAtNanos = elapsedNanos;
            return true;
        }

        @Override
        public String toString()
        {
            return kind + " node " + node;
        }
    }

    private static ChaosActive chaos(Cluster cluster, AtomicIntegerArray coordinatorIndexes, Set<Integer> candidates, ExecutorService chaosExecutor, Random random, ClusterChaos chaos, List<String> history)
    {
        List<Integer> snapshot = new ArrayList<>(candidates);
        int nodeId;
        {
            int i = random.nextInt(snapshot.size());
            Integer remove = snapshot.get(i);
            candidates.remove(remove);
            snapshot = new ArrayList<>(candidates);
            Invariants.require(snapshot.size() > 0);
            nodeId = remove;
        }

        for (int i = 0; i < coordinatorIndexes.length(); ++i)
        {
            if (nodeId == coordinatorIndexes.get(i))
            {
                int j = random.nextInt(snapshot.size());
                int replaceIdx = snapshot.get(j);
                coordinatorIndexes.set(i, replaceIdx);
            }
        }

        String describe = String.format("%s node %d...", chaos, nodeId);
        history.add(describe);
        System.out.println("========= BEGIN CHAOS ==========");
        System.out.println(describe);
        System.out.println(candidates);
        System.out.println("========= BEGIN CHAOS ==========");
        Future<?> future;
        switch (chaos)
        {
            default: throw UnhandledEnum.unknown(chaos);
            case REBOOTSTRAP_INCOMPLETE:
            case REBOOTSTRAP_RESET:
            {
                IInvokableInstance node = cluster.get(nodeId);
                future = node.asyncAcceptsOnInstance((Set<Integer> cnds) -> {
                    try
                    {
                        AccordService.getBlocking(((AccordService) AccordService.instance()).rebootstrap(chaos == REBOOTSTRAP_RESET ? LOG_CORRUPTED : LOG_INCOMPLETE, true));
                    }
                    finally
                    {
                        Invariants.require(cnds.add(nodeId));
                        System.out.println("========== END CHAOS ===========");
                        System.out.println(describe);
                        System.out.println("========== END CHAOS ===========");
                    }
                }).apply(candidates);
                break;
            }
            case REBOOTSTRAP_IF_BEHIND:
            {
                IInvokableInstance node = cluster.get(nodeId);
                future = node.asyncAcceptsOnInstance((Set<Integer> cmds) -> {
                    try
                    {
                        AccordService.getBlocking(Catchup.rebootstrapIfBehind(AccordService.instance().node()));
                    }
                    finally
                    {
                        Invariants.require(cmds.add(nodeId));
                        System.out.println("========== END CHAOS ===========");
                        System.out.println(String.format("%s node %d...", chaos, nodeId));
                        System.out.println("========== END CHAOS ===========");
                    }
                }).apply(candidates);
                break;
            }
            case RESTART:
            case RESTART_AND_REBOOTSTRAP_INCOMPLETE:
            case RESTART_AND_REBOOTSTRAP_RESET:
            case RESTART_AND_REBOOTSTRAP_AFTER_TIMEOUT:
            {
                future = chaosExecutor.submit(() -> {
                    IInvokableInstance node = cluster.get(nodeId);
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
                        Invariants.require(candidates.add(nodeId));
                        node.config().set("accord.catchup_on_start_success_latency", "60s");
                        node.config().set("accord.catchup_on_start_fail_latency", "120s");
                        node.config().set("accord.catchup_on_start_on_timeout", "IGNORE");
                        node.config().set("accord.journal.replay", "PART_NON_DURABLE");
                        System.out.println("========== END CHAOS ===========");
                        System.out.println(String.format("%s node %d...", chaos, nodeId));
                        System.out.println("========== END CHAOS ===========");
                    }
                });
                break;
            }
        }

        return new ChaosActive(chaos, future, nodeId);
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
                                SafeCommand safeCommand = safeStore.unsafeTryGet(candidate);
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
                                        SafeCommand safeCommand = safeStore.unsafeTryGet(txnId);
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

    /** deferral bookkeeping for the health gate above */
    private long chaosHealthWaitSince, chaosHealthWaitLogged;
    /** every node a chaos op has been started on: these are the nodes we consider "subject to chaos" */
    private final Set<Integer> chaosTouched = new ConcurrentSkipListSet<>();

    private static Set<Integer> untouchedByChaos(Cluster cluster, Set<Integer> chaosTouched)
    {
        Set<Integer> untouched = new TreeSet<>();
        for (int i = 1; i <= cluster.size(); ++i)
            if (!chaosTouched.contains(i))
                untouched.add(i);
        return untouched;
    }

    /* ------------------------------------------------------------------ stall diagnostics
     * Authored by Claude
     *
     * A chaos operation that never returns leaves us nothing to work with in CI: the dtest log is not published, so
     * the only text that survives a failure is the exception itself - its message, its stack, and its suppressed
     * exceptions (the thread dump we do get today comes from AbstractCluster's thread-leak check, i.e. from an
     * exception). Everything we want to know about a stall therefore has to be attached to what we throw.
     *
     * We attach two things:
     *   1) a compact per-node state report (in the message), enough to tell "waiting on a data fetch" from "waiting
     *      to stop refusing" from "nothing is driving a dependency", which is the first fork in every one of these
     *      investigations so far; and
     *   2) the stacks of the threads that could be holding the stall, one suppressed Throwable per distinct stack,
     *      so any JUnit/surefire reporter renders them without needing the log.
     */

    /** each node is asked for its state on its own thread, and only briefly: a stalled node must not stall the report */
    private static final long STALL_REPORT_TIMEOUT_MS = 15_000L;
    /** keep the report small enough that a CI reporter will not truncate the important first lines */
    private static final int STALL_REPORT_MAX_BLOCKED_TXNS = 5;
    private static final int STALL_REPORT_MAX_DURABILITY_ROWS = 5;
    private static final int STALL_REPORT_MAX_LOG_LINES = 20;
    private static final int STALL_REPORT_MAX_THREAD_STACKS = 25;
    private static final long STALL_REPORT_WEDGE_MIN_AGE_NANOS = TimeUnit.SECONDS.toNanos(60);
    private static final int STALL_REPORT_MAX_WEDGED_TASKS = 3;
    private static final int STALL_REPORT_MAX_BLOCKED_THREADS = 15;

    /**
     * JVM-wide (all in-JVM instances share one JVM): monitor/ownable-synchronizer deadlocks, plus every thread that is
     * BLOCKED or parked on a lock another thread owns, with the owner. In the message rather than the suppressed
     * stacks, as only the message is reliably rendered by CI. Note it cannot see accord's own waits (cache entry
     * queues, ExclusiveExecutor's park/unpark handoff) - those are covered per node by {@link CacheWedgeReport}.
     */
    private static String jvmLockReport()
    {
        try
        {
            java.lang.management.ThreadMXBean mx = java.lang.management.ManagementFactory.getThreadMXBean();
            StringBuilder sb = new StringBuilder("jvm locks:");
            long[] deadlocked = mx.findDeadlockedThreads();
            if (deadlocked == null) sb.append(" no deadlocked threads");
            else
            {
                sb.append(" DEADLOCK among ").append(deadlocked.length).append(" threads:");
                for (java.lang.management.ThreadInfo info : mx.getThreadInfo(deadlocked, true, true))
                    if (info != null) sb.append("\n  ").append(info.toString().trim().replace("\n", "\n    "));
            }

            Map<Thread.State, Integer> states = new java.util.EnumMap<>(Thread.State.class);
            int listed = 0, contended = 0;
            StringBuilder blocked = new StringBuilder();
            for (java.lang.management.ThreadInfo info : mx.dumpAllThreads(true, true))
            {
                states.merge(info.getThreadState(), 1, Integer::sum);
                if (info.getLockOwnerName() == null)
                    continue;
                ++contended;
                if (++listed > STALL_REPORT_MAX_BLOCKED_THREADS)
                    continue;
                StackTraceElement[] stack = info.getStackTrace();
                blocked.append("\n  ").append(info.getThreadName()).append(' ').append(info.getThreadState())
                       .append(" on ").append(info.getLockName()).append(" held by ").append(info.getLockOwnerName())
                       .append(" at ").append(stack.length == 0 ? "?" : stack[0]);
            }
            sb.append("; thread states ").append(states).append("; ").append(contended).append(" waiting on an owned lock").append(blocked);
            return sb.append('\n').toString();
        }
        catch (Throwable t)
        {
            return "jvm locks: <failed: " + t + ">\n";
        }
    }

    private static AssertionError chaosStalled(String message, Cluster cluster, ChaosActive chaos, List<String> history)
    {
        // capture JVM lock state first: it is the cheapest, and the per-node report below takes executor locks
        String jvmLocks = jvmLockReport();
        AssertionError failure = new AssertionError(message + '\n' + jvmLocks + stallReport(cluster, chaos, history));
        for (Throwable stack : stuckThreadStacks())
            failure.addSuppressed(stack);
        return failure;
    }

    /**
     * The tail of this node's log, from the fixed-size RINGBUFFER appender in {@code logback-dtest-info-rolling.xml} (absent
     * configs simply produce a note). Runs on the instance: dtest instances each configure their own logger context.
     */
    private static String logTail(boolean allInfo)
    {
        try
        {
            ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
            ch.qos.logback.core.Appender<ch.qos.logback.classic.spi.ILoggingEvent> appender = root.getAppender("RINGBUFFER");
            if (!(appender instanceof ch.qos.logback.core.read.CyclicBufferAppender))
                return " <no RINGBUFFER appender configured; see logback-dtest-info-rolling.xml>";

            ch.qos.logback.core.read.CyclicBufferAppender<ch.qos.logback.classic.spi.ILoggingEvent> ring =
            (ch.qos.logback.core.read.CyclicBufferAppender<ch.qos.logback.classic.spi.ILoggingEvent>) appender;
            List<String> selected = new ArrayList<>();
            long nowMillis = System.currentTimeMillis();
            for (int i = 0, length = ring.getLength(); i < length ; ++i)
            {
                ch.qos.logback.classic.spi.ILoggingEvent event = ring.get(i);
                if (event == null)
                    continue;
                // WARN and above, plus accord's own INFO: the rest is flush/compaction/gossip noise
                String name = event.getLoggerName();
                if (!event.getLevel().isGreaterOrEqual(allInfo ? ch.qos.logback.classic.Level.INFO : ch.qos.logback.classic.Level.WARN)
                    && !name.startsWith("accord.") && !name.startsWith("org.apache.cassandra.service.accord"))
                    continue;
                // age, so we can tell a line from the stall from one emitted minutes before it; and the throwable,
                // since AccordAgent.handleException logs expected exceptions with an empty format string
                StringBuilder line = new StringBuilder("\n    -").append((nowMillis - event.getTimeStamp()) / 1000).append("s ")
                                     .append(event.getLevel()).append(' ').append(name.substring(name.lastIndexOf('.') + 1))
                                     .append(" - ").append(event.getFormattedMessage());
                for (ch.qos.logback.classic.spi.IThrowableProxy t = event.getThrowableProxy() ; t != null && line.length() < 600 ; t = t.getCause())
                {
                    line.append(" | ").append(t.getClassName()).append(": ").append(t.getMessage());
                    ch.qos.logback.classic.spi.StackTraceElementProxy[] frames = t.getStackTraceElementProxyArray();
                    if (frames != null && frames.length > 0) line.append(" @ ").append(frames[0].getStackTraceElement());
                }
                selected.add(line.toString());
            }
            StringBuilder sb = new StringBuilder(" ").append(selected.size()).append(" interesting of ").append(ring.getLength()).append(" buffered");
            int maxLines = allInfo ? 2 * STALL_REPORT_MAX_LOG_LINES : STALL_REPORT_MAX_LOG_LINES;
            for (String line : selected.subList(Math.max(0, selected.size() - maxLines), selected.size()))
                sb.append(line);
            return sb.toString();
        }
        catch (Throwable t)
        {
            return " <could not read the log tail: " + t + '>';
        }
    }

    /** runs on the instance: how far AccordService got, read reflectively as the state is private */
    private static String accordStartupPhase()
    {
        try
        {
            if (!AccordService.isSetup())
                return "AccordService not set up";
            Object instance = AccordService.unsafeInstance();
            StringBuilder sb = new StringBuilder(instance.getClass().getSimpleName());
            for (String field : new String[]{ "state", "rebootstrapOnStart" })
            {
                try
                {
                    java.lang.reflect.Field f = instance.getClass().getDeclaredField(field);
                    f.setAccessible(true);
                    sb.append(' ').append(field).append('=').append(f.get(instance));
                }
                catch (NoSuchFieldException ignore) {}
            }
            sb.append(" started()=").append(AccordService.started());
            return sb.toString();
        }
        catch (Throwable t)
        {
            return "phase unknown: " + t;
        }
    }

    private static final int STALL_REPORT_MAX_STARTUP_STACKS = 6;
    private static final int STALL_REPORT_MAX_STARTUP_FRAMES = 45;

    /**
     * Stacks of the threads that are doing, or waiting on, node {@code num}'s startup: the ClusterChaos thread that
     * called startup(), and any of the node's own threads (named {@code node<num>_}) that are in startup, journal
     * replay, accord or TCM code. Grouped by identical stack, JDK pool frames trimmed.
     */
    private static String startupStacks(int num)
    {
        try
        {
            String prefix = "node" + num + '_';
            Map<List<StackTraceElement>, List<String>> byStack = new java.util.LinkedHashMap<>();
            for (Map.Entry<Thread, StackTraceElement[]> e : Thread.getAllStackTraces().entrySet())
            {
                String name = e.getKey().getName();
                StackTraceElement[] stack = e.getValue();
                boolean chaosThread = name.startsWith("ClusterChaos");
                if (!chaosThread && !name.startsWith(prefix))
                    continue;
                if (!isStartupRelated(stack) || isIdleLoop(stack))
                    continue;
                byStack.computeIfAbsent(java.util.Arrays.asList(stack), ignore -> new ArrayList<>())
                       .add(name + '/' + e.getKey().getState());
            }
            // the startup path itself first (the chaos thread's startup() and the instance thread executing it),
            // then anything else of the node that is actually running, then the rest
            List<Map.Entry<List<StackTraceElement>, List<String>>> ranked = new ArrayList<>(byStack.entrySet());
            ranked.sort(Comparator.comparingInt(e -> startupRank(e.getKey(), e.getValue())));
            if (byStack.isEmpty())
                return "  startup stacks: <no thread of node" + num + " is in startup/replay/accord/tcm code>\n";

            StringBuilder sb = new StringBuilder("  startup stacks:");
            int printed = 0;
            for (Map.Entry<List<StackTraceElement>, List<String>> e : ranked)
            {
                if (++printed > STALL_REPORT_MAX_STARTUP_STACKS)
                {
                    sb.append("\n    ... and ").append(byStack.size() - STALL_REPORT_MAX_STARTUP_STACKS).append(" more distinct stacks");
                    break;
                }
                sb.append("\n    ").append(e.getValue());
                List<StackTraceElement> stack = e.getKey();
                int frames = 0;
                for (StackTraceElement frame : stack)
                {
                    String cls = frame.getClassName();
                    // the pool/thread plumbing below the work adds nothing
                    if (cls.startsWith("java.util.concurrent.ThreadPoolExecutor") || cls.startsWith("io.netty.util.concurrent.FastThreadLocalRunnable"))
                        break;
                    if (++frames > STALL_REPORT_MAX_STARTUP_FRAMES)
                    {
                        sb.append("\n        ...");
                        break;
                    }
                    sb.append("\n        at ").append(frame);
                }
            }
            return sb.append('\n').toString();
        }
        catch (Throwable t)
        {
            return "  startup stacks: <failed: " + t + ">\n";
        }
    }

    private static int startupRank(List<StackTraceElement> stack, List<String> threads)
    {
        for (StackTraceElement frame : stack)
            if (frame.getMethodName().toLowerCase().contains("startup"))
                return 0;
        for (String thread : threads)
            if (thread.endsWith("/RUNNABLE"))
                return 1;
        return 2;
    }

    /** a background loop parked waiting for work: present on every healthy node, so only noise in a stall report */
    private static boolean isIdleLoop(StackTraceElement[] stack)
    {
        for (int i = 0 ; i < stack.length ; ++i)
        {
            String cls = stack[i].getClassName();
            if (cls.startsWith("java.") || cls.startsWith("jdk.") || cls.startsWith("sun."))
                continue;
            // the first non-JDK frames say what is waiting; a known idle wait means nothing to see
            String below = i + 2 < stack.length ? stack[i + 2].getClassName() : "";
            return stack[i].getMethodName().equals("awaitExclusive")
                   || cls.equals("org.apache.cassandra.concurrent.InfiniteLoopExecutor")
                   || cls.startsWith("org.apache.cassandra.journal.Flusher")
                   || cls.startsWith("org.apache.cassandra.concurrent.SEPWorker")
                   || cls.startsWith("org.apache.cassandra.utils.concurrent.WaitQueue") && below.equals("org.apache.cassandra.tcm.log.LocalLog$Async$AsyncRunnable");
        }
        return true;
    }

    private static boolean isStartupRelated(StackTraceElement[] stack)
    {
        for (StackTraceElement frame : stack)
        {
            String cls = frame.getClassName(), method = frame.getMethodName();
            if (method.toLowerCase().contains("startup") || method.toLowerCase().contains("replay")
                || cls.startsWith("accord.") || cls.startsWith("org.apache.cassandra.service.accord.")
                || cls.startsWith("org.apache.cassandra.tcm.") || cls.startsWith("org.apache.cassandra.journal.")
                || cls.equals("org.apache.cassandra.service.CassandraDaemon") || cls.equals("org.apache.cassandra.service.StorageService"))
                return true;
        }
        return false;
    }

    private static String stallReport(Cluster cluster, ChaosActive chaos, List<String> history)
    {
        StringBuilder sb = new StringBuilder();
        try
        {
            sb.append("=== stall report for ").append(chaos).append(" ===\n");
            sb.append("chaos so far: ").append(history).append('\n');
            for (IInvokableInstance instance : cluster)
            {
                int num = instance.config().num();
                sb.append("node").append(num).append(": ");
                if (instance.isShutdown()) sb.append("shutdown\n");
                else
                {
                    String state = accordState(instance);
                    sb.append(state).append('\n');
                    // only the message reliably reaches CI, so for a node stuck starting up put the stacks of
                    // whatever is doing (or waiting on) its startup there too, not only in the suppressed throwables
                    if (state.startsWith("accord not started") || state.startsWith("<unavailable"))
                        sb.append(startupStacks(num));
                }
            }
        }
        catch (Throwable t)
        {
            sb.append("<stall report failed: ").append(t).append(">\n");
        }
        return sb.toString();
    }

    /** never throws, and never blocks for longer than {@link #STALL_REPORT_TIMEOUT_MS} */
    private static String accordState(IInvokableInstance instance)
    {
        try
        {
            Future<String> result = instance.asyncCallsOnInstance(() -> {
                if (!AccordService.isStarted())
                {
                    // a node that never finishes starting is itself the stall (e.g. a RESTART op whose startup
                    // hangs), so say how far it got and keep its whole startup log tail, not just accord's lines
                    return "accord not started (" + accordStartupPhase() + ")\n  log tail:" + logTail(true);
                }

                StringBuilder sb = new StringBuilder();
                Node node = AccordService.instance().node();
                sb.append("epoch=").append(node.epoch()).append(" minEpoch=").append(node.topology().minEpoch());

                int stores = 0, idle = 0;
                StringBuilder busy = new StringBuilder();
                for (CommandStore store : node.commandStores().all())
                {
                    ++stores;
                    // describeState() prints bootstraps only when there are some, so a store with neither refusals
                    // nor bootstraps cannot be holding a rebootstrap and is merely counted
                    String state = store.describeState();
                    if (state.contains("refuses=none") && !state.contains("bootstraps=")) ++idle;
                    else busy.append("\n    ").append(state);
                }
                sb.append(" stores=").append(stores).append(" (").append(idle).append(" idle)").append(busy);

                // the progress log is what should be driving a stuck dependency to completion; if the stall is a
                // transaction that nothing decides, it shows up here and (without the log) nowhere else. Report a
                // histogram of what everything is blocked on, then only the entries that have actually been retried
                // - an unretried entry is merely in flight, and there are hundreds of those under load.
                int tracked = 0, retried = 0;
                Map<String, Integer> blockedOn = new HashMap<>();
                StringBuilder top = new StringBuilder();
                for (CommandStore store : node.commandStores().all())
                {
                    DefaultProgressLog.ImmutableView view = ((DefaultProgressLog) store.unsafeProgressLog()).immutableView();
                    while (view.advance())
                    {
                        ++tracked;
                        blockedOn.merge(view.waitingIsBlockedUntil() + "/" + view.waitingProgress(), 1, Integer::sum);
                        if (view.waitingRetryCounter() == 0 && view.homeRetryCounter() == 0 && !view.contactEveryone())
                            continue;
                        if (++retried > STALL_REPORT_MAX_BLOCKED_TXNS)
                            continue;
                        top.append("\n    store").append(store.id()).append(' ').append(view.txnId())
                           .append(" blockedUntil=").append(view.waitingIsBlockedUntil())
                           .append(" waitingProgress=").append(view.waitingProgress())
                           .append(" waitingRetries=").append(view.waitingRetryCounter())
                           .append(" homePhase=").append(view.homePhase())
                           .append(" homeProgress=").append(view.homeProgress())
                           .append(" homeRetries=").append(view.homeRetryCounter())
                           .append(" contactEveryone=").append(view.contactEveryone());
                    }
                }
                sb.append("\n  progressLog: ").append(tracked).append(" tracked, ").append(retried)
                  .append(" retried, blocked on ").append(blockedOn).append(top);

                // and the durability service: a (re)bootstrap that cannot finish is usually waiting on a durability
                // requirement it never achieves, and only this view names the requirement and the retry count
                int durabilityRows = 0;
                StringBuilder durability = new StringBuilder();
                ShardDurability.ImmutableView view = ((AccordService) AccordService.instance()).shardDurability();
                while (view.advance())
                {
                    if (view.requestedBy() == null && view.retries() == 0)
                        continue;
                    if (++durabilityRows > STALL_REPORT_MAX_DURABILITY_ROWS)
                        continue;
                    durability.append("\n    ").append(view.shard().range)
                              .append(" retries=").append(view.retries())
                              .append(" min=").append(view.min())
                              .append(" active=").append(view.active())
                              .append(" waiting=").append(view.waiting())
                              .append(" requestedBy=").append(view.requestedBy());
                }
                sb.append("\n  durability: ").append(durabilityRows).append(" range(s) retrying/requested").append(durability);

                // a stall with every executor thread idle is invisible to a thread dump: tasks wait on cache entries,
                // not on monitors. Look for wait cycles / mis-counted waiters among the tasks queued on, or holding,
                // any cache entry, and list the oldest such tasks - one queued on an entry for a minute is a wedge
                // whether or not it closes a cycle.
                sb.append("\n  cache wedges:").append(CacheWedgeReport.describe(node, STALL_REPORT_WEDGE_MIN_AGE_NANOS, STALL_REPORT_MAX_WEDGED_TASKS));

                // finally this node's own log tail: CI discards the log file, and the lines that explain a stall
                // ("insufficient to satisfy NoLocal/MinorityQuorumAndWaitedForAll requested by Bootstrap ...") are
                // accord's INFO/WARN lines. Read here rather than in the harness because each instance configures
                // its own logger context (see the RINGBUFFER appender in logback-dtest-info.xml).
                sb.append("\n  log tail:").append(logTail(false));
                return sb.toString();
            }).call();
            return result.get(STALL_REPORT_TIMEOUT_MS, MILLISECONDS);
        }
        catch (Throwable t)
        {
            // a node whose executors cannot answer even this is itself the finding, so report it rather than fail
            return "<unavailable: " + t + '>';
        }
    }

    /**
     * The stacks of everything that might be holding the stall, grouped by identical stack and wrapped in synthetic
     * throwables so they survive into the CI report. In-JVM dtests run every node in this JVM, so this needs nothing
     * from the (possibly wedged) instances: {@code nodeN_} thread names identify them.
     */
    private static List<Throwable> stuckThreadStacks()
    {
        List<Throwable> dumps = new ArrayList<>();
        try
        {
            Map<List<StackTraceElement>, List<String>> byStack = new HashMap<>();
            for (Map.Entry<Thread, StackTraceElement[]> e : Thread.getAllStackTraces().entrySet())
            {
                if (!isInterestingDuringStall(e.getKey().getName(), e.getValue()))
                    continue;
                byStack.computeIfAbsent(java.util.Arrays.asList(e.getValue()), ignore -> new ArrayList<>())
                       .add(e.getKey().getName());
            }
            for (Map.Entry<List<StackTraceElement>, List<String>> e : byStack.entrySet())
            {
                if (dumps.size() >= STALL_REPORT_MAX_THREAD_STACKS)
                    break;
                Throwable dump = new Throwable("stalled threads: " + e.getValue());
                dump.setStackTrace(e.getKey().toArray(new StackTraceElement[0]));
                dumps.add(dump);
            }
        }
        catch (Throwable t)
        {
            dumps.add(new Throwable("could not collect thread stacks", t));
        }
        return dumps;
    }

    private static boolean isInterestingDuringStall(String threadName, StackTraceElement[] stack)
    {
        // the chaos operation itself (it runs on the node's isolatedExecutor), plus anything that shuts accord down
        if (threadName.startsWith("ClusterChaos") || threadName.contains("isolatedExecutor") || threadName.contains("ShutdownAccord"))
            return true;
        // ... and anything driving or blocking a (re)bootstrap: accord itself, repair/streaming (the data fetch), TCM
        for (StackTraceElement frame : stack)
        {
            String cls = frame.getClassName();
            if (cls.startsWith("accord.") || cls.startsWith("org.apache.cassandra.service.accord.")
                || cls.startsWith("org.apache.cassandra.repair.") || cls.startsWith("org.apache.cassandra.streaming.")
                || cls.startsWith("org.apache.cassandra.tcm."))
                return true;
        }
        return false;
    }

    /**
     * The condition for starting a new chaos op: as seen by *every* node that is up,
     * <ul>
     *   <li>every node not subject to chaos (i.e. that no chaos op has been started on) must be advertised
     *       UP by the failure detector and HEALTHY by the published accord node info, and</li>
     *   <li>at least {@code minHealthy} nodes - a simple majority - must be advertised so.</li>
     * </ul>
     * Both halves matter, and both are weaker than "the whole cluster is healthy": a node that chaos has
     * already restarted may still be catching up (it advertises itself UNREADABLE until its rebootstrap and
     * catchup finish), and we deliberately do *not* wait for that - the point of the gate is to keep a
     * quorum of decision-making replicas, not to serialise recovery. Note the consequence: this permits a
     * new op while a previous target is still UNREADABLE, i.e. it permits two UNREADABLE replicas of a
     * range, which is the condition HANDOVER-accord-rebootstrap-6.md section 1 identifies as removing all
     * consensus slack (every round then needs unanimity of the survivors). That is intentional: it is the
     * workload we want the product to survive, not one the harness hides.
     *
     * Both signals are consulted because accord treats both as "do not contact, record a failure"
     * (AbstractCoordination.contact): the failure detector decides gossip liveness, while the accord node
     * info carries the UNREADABLE bit a rebootstrapping node publishes about itself.
     *
     * @return null if the condition holds, else a description of what does not
     */
    private static String chaosGateReason(Cluster cluster, Set<Integer> mustBeHealthy, int minHealthy)
    {
        StringBuilder all = new StringBuilder();
        for (int i = 1; i <= cluster.size(); ++i)
        {
            IInvokableInstance observer = cluster.get(i);
            if (observer.isShutdown())
                continue;   // it cannot tell us anything; its own (un)health is counted by the others below

            String advertised;
            try
            {
                advertised = observer.callOnInstance(() -> {
                    ClusterMetadata metadata = ClusterMetadata.current();
                    StringBuilder healthy = new StringBuilder();
                    for (org.apache.cassandra.tcm.membership.NodeId tcmId : metadata.directory.peerIds())
                    {
                        org.apache.cassandra.locator.InetAddressAndPort ep = metadata.directory.endpoint(tcmId);
                        boolean up = ep.equals(org.apache.cassandra.utils.FBUtilities.getBroadcastAddressAndPort())
                                     || org.apache.cassandra.gms.FailureDetector.instance.isAlive(ep);
                        org.apache.cassandra.service.accord.topology.AccordNodeInfos.StampedNodeInfo info
                        = metadata.accordNodeInfos.getOrDefault(new Node.Id(tcmId.id()));
                        boolean accordHealthy = !info.isUnreadable()
                                                && info.status() == org.apache.cassandra.service.accord.topology.AccordNodeInfos.Status.NORMAL;
                        if (up && accordHealthy)
                            healthy.append(healthy.length() == 0 ? "" : ",").append(tcmId.id());
                    }
                    return healthy.toString();
                });
            }
            catch (Throwable t)
            {
                all.append(" [on node").append(i).append("] health check threw ").append(t.getClass().getSimpleName());
                continue;
            }

            Set<Integer> healthy = new TreeSet<>();
            for (String id : advertised.split(","))
                if (!id.isEmpty()) healthy.add(Integer.parseInt(id));

            Set<Integer> missing = new TreeSet<>(mustBeHealthy);
            missing.removeAll(healthy);
            if (!missing.isEmpty())
                all.append(" [on node").append(i).append("] not-yet-chaosed nodes not healthy: ").append(missing);
            if (healthy.size() < minHealthy)
                all.append(" [on node").append(i).append("] only ").append(healthy.size()).append('/')
                   .append(cluster.size()).append(" healthy (need ").append(minHealthy).append("): ").append(healthy);
        }
        return all.length() == 0 ? null : all.toString();
    }


    /**
     * Chaos shuts nodes down under the workload, so a CMS member asked for a <i>consistent</i> log fetch can
     * legitimately fail to assemble a SERIAL quorum. The requester is told (it gets a RequestFailure and tries another
     * CMS member), but the exception escapes {@code FetchCMSLog.Handler.doVerb} and {@code InboundSink} rethrows
     * anything not on its list of expected verb-handler failures, so the harness counts it as an uncaught exception and
     * fails an otherwise-passing run (4-13 per run were observed). Matching is by name and stack frame, as the throwable
     * comes from an instance classloader and so is not instanceof anything we can name here.
     */
    private static boolean isExpectedDuringChaos(Throwable error)
    {
        for (Throwable cause = error ; cause != null ; cause = cause.getCause())
        {
            if (!"org.apache.cassandra.exceptions.UnavailableException".equals(cause.getClass().getName()))
                continue;

            for (StackTraceElement frame : cause.getStackTrace())
            {
                if (frame.getClassName().startsWith("org.apache.cassandra.tcm.")
                    || frame.getClassName().equals("org.apache.cassandra.schema.DistributedMetadataLogKeyspace"))
                {
                    logger.info("Ignoring expected {} from a metadata log fetch: {}", cause.getClass().getSimpleName(), cause.getMessage());
                    return true;
                }
            }
        }
        return false;
    }
}
