/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.compaction;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.ScannerList;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.schema.CompactionParams;

public class UnifiedCompactionContainer implements CompactionStrategyContainer
{
    private final CompactionStrategyFactory factory;
    private final CompactionParams params;
    private final CompactionParams metadataParams;
    private final UnifiedCompactionStrategy strategy;

    AtomicBoolean enabled;

    UnifiedCompactionContainer(CompactionStrategyFactory factory,
                               BackgroundCompactions backgroundCompactions,
                               CompactionParams params,
                               CompactionParams metadataParams,
                               boolean enabled)
    {
        this.factory = factory;
        this.params = params;
        this.metadataParams = metadataParams;
        this.strategy = new UnifiedCompactionStrategy(factory, backgroundCompactions, params.options());
        this.enabled = new AtomicBoolean(enabled);

        factory.getCompactionLogger().strategyCreated(this.strategy);

        if (this.strategy.getOptions().isLogAll())
            factory.getCompactionLogger().enable();
        else
            factory.getCompactionLogger().disable();

        startup();
    }

    @Override
    public void enable()
    {
        this.enabled.set(true);
    }

    @Override
    public void disable()
    {
        this.enabled.set(false);
    }

    @Override
    public boolean isEnabled()
    {
        return enabled.get() && strategy.isActive;
    }

    @Override
    public boolean isActive()
    {
        return strategy.isActive;
    }

    public static CompactionStrategyContainer create(@Nullable CompactionStrategyContainer previous,
                                                     CompactionStrategyFactory strategyFactory,
                                                     CompactionParams compactionParams,
                                                     CompactionStrategyContainer.ReloadReason reason)
    {
        boolean enabled = CompactionStrategyFactory.enableCompactionOnReload(previous, compactionParams, reason);
        BackgroundCompactions backgroundCompactions;
        // inherit compactions history from previous UCS container
        if (previous instanceof UnifiedCompactionContainer)
            backgroundCompactions = ((UnifiedCompactionContainer) previous).getBackgroundCompactions();

        // for other cases start from scratch
        // We don't inherit from legacy compactions right now because there are multiple strategies and we'd need
        // to merge their BackgroundCompactions to support that. Merging per se is not tricky, but the bigger problem
        // is aggregate cleanup. We'd need to unsubscribe from compaction tasks by legacy strategies and subscribe
        // by the new UCS to remove inherited ongoing compactions when they complete.
        // We might want to revisit this issue later to improve UX.
        else
            backgroundCompactions = new BackgroundCompactions(strategyFactory.getCfs());
        CompactionParams metadataParams = createMetadataParams(previous, compactionParams, reason);

        if (previous != null)
            previous.shutdown();

        return new UnifiedCompactionContainer(strategyFactory,
                                              backgroundCompactions,
                                              compactionParams,
                                              metadataParams,
                                              enabled);
    }

    @Override
    public CompactionStrategyContainer reload(@Nonnull CompactionStrategyContainer previous,
                                              CompactionParams compactionParams,
                                              ReloadReason reason)
    {
        return create(previous, factory, compactionParams, reason);
    }

    private static CompactionParams createMetadataParams(@Nullable CompactionStrategyContainer previous,
                                                         CompactionParams compactionParams,
                                                         ReloadReason reason)
    {
        CompactionParams metadataParams;
        if (reason == CompactionStrategyContainer.ReloadReason.METADATA_CHANGE)
            // metadataParams are aligned with compactionParams. We do not access TableParams.compaction to avoid racing with
            // concurrent ALTER TABLE metadata change.
            metadataParams = compactionParams;
        else if (previous != null)
            metadataParams = previous.getMetadataCompactionParams();
        else
            metadataParams = null;

        return metadataParams;
    }

    @Override
    public CompactionParams getCompactionParams()
    {
        return params;
    }

    @Override
    public CompactionParams getMetadataCompactionParams()
    {
        return metadataParams;
    }

    @Override
    public List<CompactionStrategy> getStrategies()
    {
        return ImmutableList.of(strategy);
    }

    @Override
    public List<CompactionStrategy> getStrategies(boolean isRepaired, @Nullable UUID pendingRepair)
    {
        return getStrategies();
    }

    @Override
    public void repairSessionCompleted(UUID sessionID)
    {
        // We are not tracking SSTables, so nothing to do here.
    }

    /**
     * UCC does not need to use this method with {@link ColumnFamilyStore#mutateRepaired}
     * @return null
     */
    @Override
    public ReentrantReadWriteLock.WriteLock getWriteLock()
    {
        return null;
    }

    @Override
    public CompactionLogger getCompactionLogger()
    {
        return strategy.compactionLogger;
    }

    @Override
    public void pause()
    {
        strategy.pause();
    }

    @Override
    public void resume()
    {
        strategy.resume();
    }

    @Override
    public void startup()
    {
        strategy.startup();
    }

    @Override
    public void shutdown()
    {
        strategy.shutdown();
    }

    @Override
    public Collection<AbstractCompactionTask> getNextBackgroundTasks(int gcBefore)
    {
        return strategy.getNextBackgroundTasks(gcBefore);
    }

    @Override
    public CompactionTasks getMaximalTasks(int gcBefore, boolean splitOutput)
    {
        return strategy.getMaximalTasks(gcBefore, splitOutput);
    }

    @Override
    public CompactionTasks getUserDefinedTasks(Collection<SSTableReader> sstables, int gcBefore)
    {
        return strategy.getUserDefinedTasks(sstables, gcBefore);
    }

    @Override
    public int getEstimatedRemainingTasks()
    {
        return strategy.getEstimatedRemainingTasks();
    }

    @Override
    public AbstractCompactionTask createCompactionTask(LifecycleTransaction txn, int gcBefore, long maxSSTableBytes)
    {
        return strategy.createCompactionTask(txn, gcBefore, maxSSTableBytes);
    }

    @Override
    public int getTotalCompactions()
    {
        return strategy.getTotalCompactions();
    }

    @Override
    public List<CompactionStrategyStatistics> getStatistics()
    {
        return strategy.getStatistics();
    }

    @Override
    public long getMaxSSTableBytes()
    {
        return strategy.getMaxSSTableBytes();
    }

    @Override
    public int[] getSSTableCountPerLevel()
    {
        return strategy.getSSTableCountPerLevel();
    }

    @Override
    public int getLevelFanoutSize()
    {
        return strategy.getLevelFanoutSize();
    }

    @Override
    public ScannerList getScanners(Collection<SSTableReader> sstables, Collection<Range<Token>> ranges)
    {
        return strategy.getScanners(sstables, ranges);
    }

    @Override
    public String getName()
    {
        return strategy.getName();
    }

    @Override
    public Set<SSTableReader> getSSTables()
    {
        return strategy.getSSTables();
    }

    @Override
    public Collection<Collection<SSTableReader>> groupSSTablesForAntiCompaction(Collection<SSTableReader> sstablesToGroup)
    {
        return strategy.groupSSTablesForAntiCompaction(sstablesToGroup);
    }

    @Override
    public SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor,
                                                       long keyCount,
                                                       long repairedAt,
                                                       UUID pendingRepair,
                                                       boolean isTransient,
                                                       MetadataCollector collector,
                                                       SerializationHeader header,
                                                       Collection<Index.Group> indexGroups,
                                                       LifecycleNewTracker lifecycleNewTracker)
    {
        return strategy.createSSTableMultiWriter(descriptor,
                                                 keyCount,
                                                 repairedAt,
                                                 pendingRepair,
                                                 isTransient,
                                                 collector,
                                                 header,
                                                 indexGroups,
                                                 lifecycleNewTracker);
    }

    @Override
    public boolean supportsEarlyOpen()
    {
        return strategy.supportsEarlyOpen();
    }

    BackgroundCompactions getBackgroundCompactions()
    {
        return strategy.backgroundCompactions;
    }

    @Override
    public void onInProgress(CompactionProgress progress)
    {
        strategy.onInProgress(progress);
    }

    @Override
    public void onCompleted(UUID id)
    {
        strategy.onCompleted(id);
    }

    @Override
    public void handleNotification(INotification notification, Object sender)
    {
        // TODO - this is a no-op because the strategy is stateless but we could detect here
        // sstables that are added either because of streaming or because of nodetool refresh
    }
}
