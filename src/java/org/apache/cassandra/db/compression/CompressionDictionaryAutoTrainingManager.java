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

package org.apache.cassandra.db.compression;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.compress.IDictionaryCompressor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.Refs;

/**
 * Singleton that periodically retrains compression dictionaries for dictionary-compressed tables and
 * adopts a freshly trained dictionary only when it compresses better than the current one.
 * <p>
 * The scheduled task runs on every node, but only the first CMS member (by lowest
 * {@link NodeId}) performs the actual check on each cycle. This allows leadership
 * to migrate naturally as CMS membership changes.
 * <p>
 * On each check cycle, iterates over all live {@link ColumnFamilyStore} instances via
 * {@code ColumnFamilyStore.all()}, skipping tables that are not eligible (see
 * {@link #isEligibleTable(ColumnFamilyStore)}), that do not have dictionary compression enabled,
 * or that do not have {@code auto_training_enabled = true} in their compression options.
 * <p>
 * For an eligible table, a candidate dictionary is trained from a fresh sample of the current
 * SSTables. The candidate is then compared against the current dictionary on a second sample drawn
 * independently from the same SSTables: both dictionaries compress the same chunks, and the candidate
 * is adopted only if it improves the compression ratio by at least
 * {@link IDictionaryCompressor#DEFAULT_AUTO_TRAINING_IMPROVEMENT_THRESHOLD_VALUE}.
 * Note that the second sample is re-drawn, not held out: nothing excludes the chunks the candidate was
 * trained on, and when the sample budget covers every chunk the two samples coincide. The comparison is
 * therefore partly in-sample and to that extent flatters the candidate.
 * <p>
 * No per-table state is cached in memory. The current dictionary and its last training time are read
 * from the {@code system_distributed.compression_dictionaries} table on every check cycle, ensuring
 * correctness after manual training or node restarts.
 */
public class CompressionDictionaryAutoTrainingManager implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(CompressionDictionaryAutoTrainingManager.class);

    public static final CompressionDictionaryAutoTrainingManager instance = new CompressionDictionaryAutoTrainingManager();

    private volatile ScheduledExecutorPlus executor;
    private volatile ScheduledFuture<?> checkTask;
    private volatile boolean closed;

    @VisibleForTesting
    CompressionDictionaryAutoTrainingManager()
    {
    }

    /**
     * Starts the periodic compression ratio check on this node.
     * The task runs on every node but only performs work on the first CMS member.
     */
    public void start()
    {
        if (!DatabaseDescriptor.getCompressionDictionaryAutoTrainingEnabled())
        {
            logger.debug("Dictionary auto training management is disabled.");
            return;
        }

        int initialDelay = DatabaseDescriptor.getCompressionDictionaryAutoTrainingInitialDelay();
        int interval = DatabaseDescriptor.getCompressionDictionaryAutoTrainingInterval();
        start(initialDelay, interval, TimeUnit.SECONDS);
    }

    @VisibleForTesting
    synchronized void start(long initialDelay, long interval, TimeUnit unit)
    {
        if (checkTask != null)
            return;

        closed = false;
        executor = ExecutorFactory.Global.executorFactory().scheduled(false, "CompressionDictionaryAutoTraining");
        checkTask = executor.scheduleWithFixedDelay(this::checkAllTables,
                                                   initialDelay,
                                                   interval,
                                                   unit);

        logger.info("Auto-training manager started, checking every {} {}.", interval, unit);
    }

    @VisibleForTesting
    void checkAllTables()
    {
        if (closed)
            return;

        if (!isFirstCMSMember())
            return;

        for (ColumnFamilyStore cfs : getTables())
        {
            if (!isEligibleTable(cfs))
                continue;

            try
            {
                checkTable(cfs);
            }
            catch (Exception e)
            {
                logger.warn("Error during auto-training check cycle for {}.{}", cfs.getKeyspaceName(), cfs.getTableName(), e);
            }
        }
    }

    @VisibleForTesting
    Iterable<ColumnFamilyStore> getTables()
    {
        return ColumnFamilyStore.all();
    }

    /**
     * Returns {@code true} if this node is the first CMS member by lowest {@link NodeId}.
     * Evaluated on every cycle so leadership migrates naturally as CMS membership changes.
     */
    @VisibleForTesting
    boolean isFirstCMSMember()
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        List<NodeId> cmsMemberIds = new ArrayList<>(metadata.fullCMSMemberIds());
        cmsMemberIds.sort(Comparator.comparingInt(NodeId::id));

        return !cmsMemberIds.isEmpty() && cmsMemberIds.get(0).equals(metadata.myNodeId());
    }

    public static boolean isEligibleTable(ColumnFamilyStore cfs)
    {
        if (SchemaConstants.isSystemKeyspace(cfs.getKeyspaceName()))
            return false;

        if (cfs.isIndex())
            return false;

        if (cfs.metadata().isView())
            return false;

        return !cfs.metadata().isStaticCompactTable();
    }

    @VisibleForTesting
    void checkTable(ColumnFamilyStore cfs)
    {
        if (!cfs.metadata().params.compression.isDictionaryCompressionEnabled())
            return;

        CompressionDictionaryManager manager = getCompressionDictionaryManager(cfs);
        if (manager == null || !manager.isEnabled() || !manager.isAutoTrainingEnabled())
            return;

        if (manager.isTrainingRunning())
        {
            logger.info("There is ongoing compression dictionary training for table {}.{}, " +
                        "skipping this round of auto-training.",
                        cfs.getKeyspaceName(),
                        cfs.getTableName());
            return;
        }

        CompressionDictionary latest = retrieveLatestDictionary(cfs);

        if (latest == null)
        {
            logger.debug("There is no existing compression dictionary for table {}.{}, " +
                         "nothing to compare against, skipping this round of auto-training.",
                         cfs.getKeyspaceName(),
                         cfs.getTableName());
            return;
        }

        if (!canConsume(cfs, latest))
        {
            logger.debug("The latest compression dictionary for table {}.{} is of kind {}, which the current " +
                         "compressor cannot consume, so there is nothing comparable to evaluate against, " +
                         "skipping this round of auto-training.",
                         cfs.getKeyspaceName(), cfs.getTableName(), latest.kind());
            return;
        }

        ColumnFamilyStore.RefViewFragment trainingFragment = resolveViewFragment(cfs);

        // proactively checking if there is a fragment to run auto-training over,
        // if not then further logic does not make any sense
        if (trainingFragment == null)
        {
            logger.debug("There is no SSTable view fragment to auto-train on for table {}.{}",
                         cfs.getKeyspaceName(), cfs.getTableName());
            return;
        }

        // scheduleTraining hands trainingFragment to the training task, which releases it as its last
        // act - and scheduleTraining blocks until that task has completed. The evaluation pass below
        // reads the very same SSTables, so it must hold its own independent references; sharing one
        // fragment would have it opening data channels on readers nothing references anymore, which a
        // concurrent compaction is then free to tidy underneath it.
        ColumnFamilyStore.RefViewFragment evaluationFragment = referenceAgain(trainingFragment);

        if (evaluationFragment == null)
        {
            logger.debug("Could not acquire a second set of SSTable references to evaluate on for table {}.{}, " +
                         "skipping this round of auto-training.",
                         cfs.getKeyspaceName(), cfs.getTableName());
            trainingFragment.close();
            return;
        }

        try (evaluationFragment)
        {
            CompressionDictionary autoTrainedDictionary = scheduleTraining(cfs, trainingFragment, manager);
            if (autoTrainedDictionary == null)
                return;

            if (newDictionaryBetterThanLatest(cfs,
                                              latest,
                                              autoTrainedDictionary,
                                              evaluationFragment,
                                              manager.createTrainingConfig(Map.of())))
            {
                manager.handleNewDictionary(autoTrainedDictionary);
            }
        }
    }

    /**
     * Takes a second, independent set of references to the SSTables of {@code fragment}, so that two
     * consumers with different lifetimes can each own their references.
     *
     * @return a new fragment over the same SSTables, or {@code null} if any of them could no longer be referenced
     */
    private static ColumnFamilyStore.RefViewFragment referenceAgain(ColumnFamilyStore.RefViewFragment fragment)
    {
        Refs<SSTableReader> refs = Refs.tryRef(fragment.sstables);
        return refs == null ? null : new ColumnFamilyStore.RefViewFragment(fragment.sstables, fragment.memtables, refs);
    }

    @VisibleForTesting
    CompressionDictionaryManager getCompressionDictionaryManager(ColumnFamilyStore cfs)
    {
        return cfs.compressionDictionaryManager();
    }

    /**
     * Returns {@code true} if the table's current compressor can consume {@code dictionary}, i.e. the dictionary
     * is of a kind this compressor understands.
     */
    @VisibleForTesting
    static boolean canConsume(ColumnFamilyStore cfs, CompressionDictionary dictionary)
    {
        CompressionParams params = cfs.metadata().params.compression;
        if (!params.isDictionaryCompressionEnabled())
            return false;

        return ((IDictionaryCompressor<?>) params.getSstableCompressor()).canConsumeDictionary(dictionary);
    }

    @VisibleForTesting
    CompressionDictionary retrieveLatestDictionary(ColumnFamilyStore cfs)
    {
        return SystemDistributedKeyspace.retrieveLatestCompressionDictionary(cfs.getKeyspaceName(),
                                                                             cfs.getTableName(),
                                                                             cfs.metadata().id.toLongString());
    }

    @VisibleForTesting
    ColumnFamilyStore.RefViewFragment resolveViewFragment(ColumnFamilyStore cfs)
    {
        CompressionDictionaryTrainingConfig config = getCompressionDictionaryManager(cfs).createTrainingConfig(Map.of());
        return new RecencyBiasViewFragmentResolver(cfs, config).resolveViewFragment();
    }

    @VisibleForTesting
    boolean newDictionaryBetterThanLatest(ColumnFamilyStore cfs,
                                          CompressionDictionary latest,
                                          CompressionDictionary autoTrainedDictionary,
                                          ColumnFamilyStore.RefViewFragment refViewFragment,
                                          CompressionDictionaryTrainingConfig config)
    {
        // The trainer must be started (moved into SAMPLING state) before the evaluator draws its held-out sample:
        // createEvaluator() samples into the trainer in its constructor, and SSTableChunkSampler rejects samples
        // unless the trainer is SAMPLING. Hence start(config) precedes createEvaluator().
        ICompressionDictionaryTrainer trainer = createTrainer(cfs);
        trainer.start(config);
        try (trainer; CompressionRatioEvaluator evaluator = createEvaluator(cfs, refViewFragment, trainer, config))
        {
            double compressionRatioLatestDict = evaluator.evaluate(latest);
            double compressionRatioAutoTrainedDict = evaluator.evaluate(autoTrainedDictionary);

            double improvement = (compressionRatioLatestDict - compressionRatioAutoTrainedDict) / compressionRatioLatestDict;

            boolean isImprovement = improvement >= config.autoTrainingImprovementThreshold;

            String result = "Candidate will not be promoted.";
            if (isImprovement)
                result = "Candidate will be promoted.";

            logger.info("Auto-training candidate for {}.{}: baseline ratio={}, candidate ratio={}, improvement={}%, adoption threshold={}%. {}",
                        cfs.getKeyspaceName(), cfs.getTableName(), compressionRatioLatestDict, compressionRatioAutoTrainedDict,
                        String.format("%.3f", improvement * 100),
                        String.format("%.3f", config.autoTrainingImprovementThreshold * 100),
                        result);

            CompressionDictionaryAutoTrainingHistory.instance.record(cfs.getKeyspaceName(),
                                                                     cfs.getTableName(),
                                                                     autoTrainedDictionary.kind(),
                                                                     compressionRatioLatestDict,
                                                                     compressionRatioAutoTrainedDict,
                                                                     improvement,
                                                                     config.autoTrainingImprovementThreshold,
                                                                     isImprovement);

            return isImprovement;
        }
        catch (Throwable t)
        {
            logger.warn("Unable to evaluate compression ratios of the latest and potential compression dictionary for {}.{}",
                        cfs.getKeyspaceName(),
                        cfs.getTableName(),
                        t);
        }

        return false;
    }

    @VisibleForTesting
    ICompressionDictionaryTrainer createTrainer(ColumnFamilyStore cfs)
    {
        return ICompressionDictionaryTrainer.create(cfs.getKeyspaceName(),
                                                    cfs.getTableName(),
                                                    cfs.metadata().params.compression);
    }

    @VisibleForTesting
    CompressionRatioEvaluator createEvaluator(ColumnFamilyStore cfs,
                                              ColumnFamilyStore.RefViewFragment refViewFragment,
                                              ICompressionDictionaryTrainer trainer,
                                              CompressionDictionaryTrainingConfig config) throws Throwable
    {
        return new CompressionRatioEvaluator(cfs.getKeyspaceName(),
                                             cfs.getTableName(),
                                             refViewFragment,
                                             trainer,
                                             config);
    }

    private static class TrainedTableConsumer implements Consumer<CompressionDictionary>
    {
        public volatile CompressionDictionary trainedDictionary;

        @Override
        public void accept(CompressionDictionary compressionDictionary)
        {
            this.trainedDictionary = compressionDictionary;
        }
    }

    /**
     * Triggers dictionary training via {@link CompressionDictionaryManager#train(boolean, Map)}.
     * <p>
     * The train method is synchronized and will throw if training is already in progress
     * (e.g., triggered manually via nodetool) or if the min frequency hasn't elapsed
     * (race between our check and an intervening manual training). Both cases are
     * non-fatal and logged at debug level — the next check cycle will re-evaluate.
     *
     * @return dictionary trained dictionary, but not persisted yet
     */
    @VisibleForTesting
    CompressionDictionary scheduleTraining(ColumnFamilyStore cfs,
                                           ColumnFamilyStore.RefViewFragment refViewFragment,
                                           CompressionDictionaryManager manager)
    {
        try
        {
            TrainedTableConsumer trainedTableConsumer = new TrainedTableConsumer();
            // we need to wait until training has completed in order to progress
            // TODO optionally we can timeout etc.
            Future<?> trainingFuture = manager.train(false, Collections.emptyMap(), refViewFragment, trainedTableConsumer);
            if (trainingFuture != null)
            {
                trainingFuture.awaitUninterruptibly();
                return trainedTableConsumer.trainedDictionary;
            }
            else
                throw new RuntimeException("Failed to start a trainer.");
        }
        catch (IllegalStateException e)
        {
            // Training already in progress — will retry on next cycle
            logger.debug("Skipping auto-training for {}.{}: {}", cfs.getKeyspaceName(), cfs.getTableName(), e.getMessage());
        }
        catch (IllegalArgumentException e)
        {
            // Min frequency not elapsed (race with manual training) — will retry on next cycle
            logger.debug("Skipping auto-training for {}.{}: {}", cfs.getKeyspaceName(), cfs.getTableName(), e.getMessage());
        }
        catch (Exception e)
        {
            logger.warn("Failed to trigger auto-training for {}.{}", cfs.getKeyspaceName(), cfs.getTableName(), e);
        }

        return null;
    }

    @Override
    public synchronized void close()
    {
        if (closed)
            return;

        logger.debug("Stopping auto-training manager.");

        closed = true;
        if (checkTask != null)
        {
            checkTask.cancel(false);
            checkTask = null;
        }
        if (executor != null)
        {
            try
            {
                ExecutorUtils.shutdownNowAndWait(1L, TimeUnit.MINUTES, executor);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                logger.warn("Interrupted while stopping the auto-training executor.");
            }
            catch (TimeoutException e)
            {
                logger.warn("Auto-training executor did not stop within a minute.");
            }
            executor = null;
        }
        logger.debug("Auto-training manager stopped.");
    }

    public static class CompressionRatioEvaluator implements AutoCloseable
    {
        private final String keyspaceName;
        private final String tableName;
        private final ColumnFamilyStore.RefViewFragment refViewFragment;
        private final ICompressionDictionaryTrainer trainer;
        private final CompressionDictionaryTrainingConfig config;

        private CompressionRatioEvaluator(String keyspaceName,
                                          String tableName,
                                          ColumnFamilyStore.RefViewFragment refViewFragment,
                                          ICompressionDictionaryTrainer trainer,
                                          CompressionDictionaryTrainingConfig config) throws Throwable
        {
            this.keyspaceName = keyspaceName;
            this.tableName = tableName;
            this.refViewFragment = refViewFragment;
            this.trainer = trainer;
            this.config = config;

            performSampling();
        }

        public void performSampling() throws Throwable
        {
            try
            {
                logger.info("Sampling chunks from {} SSTables for {}.{}", refViewFragment.sstables.size(), keyspaceName, tableName);

                // Sample chunks from SSTables and add to trainer
                SSTableChunkSampler.sampleFromSSTables(refViewFragment.sstables, trainer, config);

                logger.info("Completed sampling for {}.{}, now computing compression ratio", keyspaceName, tableName);
            }
            catch (Exception e)
            {
                logger.error("Failed to sample from SSTables for {}.{}", keyspaceName, tableName, e);
                throw e;
            }
        }

        public double evaluate(CompressionDictionary dictionary)
        {
            double ratio = trainer.computeSampleCompressionRatio(dictionary.rawDictionary());
            // round to at most 5 decimal places
            return Double.isFinite(ratio) ? Math.round(ratio * 100_000.0) / 100_000.0 : ratio;
        }

        @Override
        public void close() throws Exception
        {
            try
            {
                trainer.close();
            }
            catch (Throwable t)
            {
                logger.debug("Unable to close sample compression ratio trainer.", t);
            }
            // refViewFragment is owned by the caller, which closes it; the evaluator only borrows it.
        }
    }
}
