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

package org.apache.cassandra.db.compaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.schema.TableMetadata;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class BackgroundCompactionsTest
{
    private final String keyspace = "ks";
    private final String table = "table";

    @Mock
    private ColumnFamilyStore cfs;

    @Mock
    private AbstractCompactionStrategy strategy;

    @Mock
    private CompactionStrategyManager strategyManager;

    @Mock
    private CompactionLogger compactionLogger;

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization(); // because of all the static initialization in CFS
    }

    @Before
    public void setUp()
    {
        MockitoAnnotations.initMocks(this);

        TableMetadata metadata = TableMetadata.builder(keyspace, table)
                                              .addPartitionKeyColumn("pk", AsciiType.instance)
                                              .build();

        when(cfs.metadata()).thenReturn(metadata);
        when(cfs.getKeyspaceName()).thenReturn(keyspace);
        when(cfs.getTableName()).thenReturn(table);
        when(cfs.getCompactionStrategyManager()).thenReturn(strategyManager);
        when(strategyManager.compactionLogger()).thenReturn(compactionLogger);
        when(compactionLogger.enabled()).thenReturn(true);
    }

    private CompactionAggregate mockAggregate(long key, int numCompactions, int numCompacting)
    {
        if (numCompacting > numCompactions)
            throw new IllegalArgumentException("Cannot have more compactions in progress than total compactions");

        CompactionAggregate ret = Mockito.mock(CompactionAggregate.class);
        when(ret.getKey()).thenReturn(key);

        List<CompactionPick> compactions = new ArrayList<>(numCompactions);
        for (int i = 0; i < numCompactions; i++)
            compactions.add(Mockito.mock(CompactionPick.class));

        when(ret.numEstimatedCompactions()).thenReturn(numCompactions);
        when(ret.getActive()).thenReturn(compactions);
        when(ret.getInProgress()).thenReturn(compactions.subList(0, numCompacting));
        when(ret.toString()).thenReturn(String.format("Key: %d, compactions: %d/%d", key, numCompactions, numCompacting));

        return ret;
    }

    @Test
    public void testNoCompaction()
    {
        BackgroundCompactions backgroundCompactions = new BackgroundCompactions(strategy, cfs);
        assertEquals(0, backgroundCompactions.getEstimatedRemainingTasks());
        assertEquals(0, backgroundCompactions.getTotalCompactions());

        CompactionStrategyStatistics statistics = backgroundCompactions.getStatistics();
        assertNotNull(statistics);
        assertTrue(statistics.aggregates().isEmpty());
        assertEquals(keyspace, statistics.keyspace());
        assertEquals(table, statistics.table());
        assertEquals(strategy.getClass().getSimpleName(), statistics.strategy());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullPendingCompactions()
    {
        BackgroundCompactions backgroundCompactions = new BackgroundCompactions(strategy, cfs);
        backgroundCompactions.setPending(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDuplicatePendingCompactions()
    {
        BackgroundCompactions backgroundCompactions = new BackgroundCompactions(strategy, cfs);

        List<CompactionAggregate> pending = new ArrayList<>(0);
        for (int i = 0; i < 5; i++)
            pending.add(mockAggregate(1, 1, 0));

        // Two compactions with the same key are invalid
        backgroundCompactions.setPending(pending);
    }

    @Test
    public void testPendingCompactions()
    {
        BackgroundCompactions backgroundCompactions = new BackgroundCompactions(strategy, cfs);

        List<CompactionAggregate> pending = new ArrayList<>(0);
        for (int i = 0; i < 5; i++)
            pending.add(mockAggregate(i, 1, 0));

        backgroundCompactions.setPending(pending);

        Mockito.verify(compactionLogger, times(1)).statistics(eq(strategy), eq("pending"), any(CompactionStrategyStatistics.class));
        Mockito.verify(compactionLogger, times(1)).pending(eq(strategy), eq(pending.size()));

        assertEquals(pending.size(), backgroundCompactions.getEstimatedRemainingTasks());
        assertEquals(pending.size(), backgroundCompactions.getTotalCompactions());

        // Remove the previous pending compactions, none should be kept since they don't have in progress compactions
        backgroundCompactions.setPending(ImmutableList.of());
        assertEquals(0, backgroundCompactions.getEstimatedRemainingTasks());
        assertEquals(0, backgroundCompactions.getTotalCompactions());
    }

    @Test
    public void testCompactionFromPending()
    {
        // Add some pending compactions, and then submit one of them, the most common case

        BackgroundCompactions backgroundCompactions = new BackgroundCompactions(strategy, cfs);

        List<CompactionAggregate> pending = new ArrayList<>(0);
        for (int i = 0; i < 5; i++)
            pending.add(mockAggregate(i, 1, 0));

        backgroundCompactions.setPending(pending);

        Mockito.verify(compactionLogger, times(1)).statistics(eq(strategy), eq("pending"), any(CompactionStrategyStatistics.class));
        Mockito.verify(compactionLogger, times(1)).pending(eq(strategy), eq(pending.size()));

        assertEquals(pending.size(), backgroundCompactions.getEstimatedRemainingTasks());
        assertEquals(pending.size(), backgroundCompactions.getTotalCompactions());

        UUID uuid = UUID.randomUUID();
        CompactionAggregate aggregate = pending.get(0);
        CompactionPick compaction = Mockito.mock(CompactionPick.class);
        when(aggregate.getSelected()).thenReturn(compaction);
        when(aggregate.getMatching(any(TreeMap.class))).thenReturn(aggregate);
        when(aggregate.getActive()).thenReturn(ImmutableList.of(compaction)); // ensure the aggregate already has the compaction

        backgroundCompactions.setSubmitted(uuid, aggregate);

        Mockito.verify(compaction, times(1)).setSubmitted(eq(uuid));
        Mockito.verify(compactionLogger, times(1)).statistics(eq(strategy), eq("submitted"), any(CompactionStrategyStatistics.class));

        when(pending.get(0).numEstimatedCompactions()).thenReturn(0);
        assertEquals(pending.size() - 1, backgroundCompactions.getEstimatedRemainingTasks());
        assertEquals(pending.size(), backgroundCompactions.getTotalCompactions());

        CompactionProgress progress = Mockito.mock(CompactionProgress.class);
        when(progress.operationId()).thenReturn(uuid);

        backgroundCompactions.setInProgress(progress);
        Mockito.verify(compaction, times(1)).setProgress(eq(progress));

        assertEquals(pending.size() - 1, backgroundCompactions.getEstimatedRemainingTasks());
        assertEquals(pending.size(), backgroundCompactions.getTotalCompactions());

        // Remove the previous pending compactions, the one submitted should be kept
        backgroundCompactions.setPending(ImmutableList.of());
        assertEquals(0, backgroundCompactions.getEstimatedRemainingTasks());
        assertEquals(1, backgroundCompactions.getTotalCompactions());

        backgroundCompactions.setCompleted(uuid);

        Mockito.verify(compaction, times(1)).setCompleted();
        Mockito.verify(compactionLogger, times(1)).statistics(eq(strategy), eq("completed"), any(CompactionStrategyStatistics.class));

        assertEquals(0, backgroundCompactions.getEstimatedRemainingTasks());
        assertEquals(0, backgroundCompactions.getTotalCompactions());
    }

    @Test
    public void testCompactionWithMatchingPending()
    {
        // Add some pending compactions, and then submit a compaction from an aggregate that is not in the pending
        // but for which there is a matching aggregate, this would happen if two threads raced and created equivalent
        // but not identical pending aggregates

        BackgroundCompactions backgroundCompactions = new BackgroundCompactions(strategy, cfs);

        List<CompactionAggregate> pending = new ArrayList<>(0);
        for (int i = 0; i < 5; i++)
            pending.add(mockAggregate(i, 1, 0));

        backgroundCompactions.setPending(pending);

        Mockito.verify(compactionLogger, times(1)).statistics(eq(strategy), eq("pending"), any(CompactionStrategyStatistics.class));
        Mockito.verify(compactionLogger, times(1)).pending(eq(strategy), eq(pending.size()));

        assertEquals(pending.size(), backgroundCompactions.getEstimatedRemainingTasks());
        assertEquals(pending.size(), backgroundCompactions.getTotalCompactions());

        UUID uuid = UUID.randomUUID();
        CompactionAggregate aggregate = mockAggregate(0, 1, 0);
        CompactionPick compaction = Mockito.mock(CompactionPick.class);
        when(aggregate.getSelected()).thenReturn(compaction);
        when(aggregate.getMatching(any(TreeMap.class))).thenReturn(pending.get(0));
        when(pending.get(0).getActive()).thenReturn(ImmutableList.of()); // ensure the matching aggregate does not have the compaction
        when(pending.get(0).withAdditionalCompactions(any(Collection.class))).thenReturn(pending.get(0));

        backgroundCompactions.setSubmitted(uuid, aggregate);

        Mockito.verify(compaction, times(1)).setSubmitted(eq(uuid));
        Mockito.verify(compactionLogger, times(1)).statistics(eq(strategy), eq("submitted"), any(CompactionStrategyStatistics.class));

        when(pending.get(0).numEstimatedCompactions()).thenReturn(0);
        assertEquals(pending.size() - 1, backgroundCompactions.getEstimatedRemainingTasks());
        assertEquals(pending.size(), backgroundCompactions.getTotalCompactions());

        CompactionProgress progress = Mockito.mock(CompactionProgress.class);
        when(progress.operationId()).thenReturn(uuid);

        backgroundCompactions.setInProgress(progress);
        Mockito.verify(compaction, times(1)).setProgress(eq(progress));

        assertEquals(pending.size() - 1, backgroundCompactions.getEstimatedRemainingTasks());
        assertEquals(pending.size(), backgroundCompactions.getTotalCompactions());

        // Remove the previous pending compactions, the one submitted should be kept
        backgroundCompactions.setPending(ImmutableList.of());
        assertEquals(0, backgroundCompactions.getEstimatedRemainingTasks());
        assertEquals(1, backgroundCompactions.getTotalCompactions());

        backgroundCompactions.setCompleted(uuid);

        Mockito.verify(compaction, times(1)).setCompleted();
        Mockito.verify(compactionLogger, times(1)).statistics(eq(strategy), eq("completed"), any(CompactionStrategyStatistics.class));

        assertEquals(0, backgroundCompactions.getEstimatedRemainingTasks());
        assertEquals(0, backgroundCompactions.getTotalCompactions());
    }

    @Test
    public void testCompactionNotInPending()
    {
        // Submit a compaction that is not part of a pending aggregate, this normally happens for tombstone compactions,
        // in this case the pending aggregates are empty but a tombstone compaction is submitted

        BackgroundCompactions backgroundCompactions = new BackgroundCompactions(strategy, cfs);

        backgroundCompactions.setPending(ImmutableList.of());

        Mockito.verify(compactionLogger, times(1)).statistics(eq(strategy), eq("pending"), any(CompactionStrategyStatistics.class));
        Mockito.verify(compactionLogger, times(1)).pending(eq(strategy), eq(0));

        assertEquals(0, backgroundCompactions.getEstimatedRemainingTasks());
        assertEquals(0, backgroundCompactions.getTotalCompactions());

        UUID uuid = UUID.randomUUID();
        CompactionAggregate aggregate = mockAggregate(-1, 0, 0);
        CompactionPick compaction = Mockito.mock(CompactionPick.class);
        when(aggregate.getSelected()).thenReturn(compaction);
        when(aggregate.getMatching(any(TreeMap.class))).thenReturn(null);

        backgroundCompactions.setSubmitted(uuid, aggregate);

        Mockito.verify(compaction, times(1)).setSubmitted(eq(uuid));
        Mockito.verify(compactionLogger, times(1)).statistics(eq(strategy), eq("submitted"), any(CompactionStrategyStatistics.class));

        assertEquals(0, backgroundCompactions.getEstimatedRemainingTasks());
        assertEquals(1, backgroundCompactions.getTotalCompactions());

        CompactionProgress progress = Mockito.mock(CompactionProgress.class);
        when(progress.operationId()).thenReturn(uuid);

        backgroundCompactions.setInProgress(progress);
        Mockito.verify(compaction, times(1)).setProgress(eq(progress));

        assertEquals(0, backgroundCompactions.getEstimatedRemainingTasks());
        assertEquals(1, backgroundCompactions.getTotalCompactions());

        // Remove the previous pending compactions, the one submitted should be kept
        backgroundCompactions.setPending(ImmutableList.of());
        assertEquals(0, backgroundCompactions.getEstimatedRemainingTasks());
        assertEquals(1, backgroundCompactions.getTotalCompactions());

        backgroundCompactions.setCompleted(uuid);

        Mockito.verify(compaction, times(1)).setCompleted();
        Mockito.verify(compactionLogger, times(1)).statistics(eq(strategy), eq("completed"), any(CompactionStrategyStatistics.class));

        assertEquals(0, backgroundCompactions.getEstimatedRemainingTasks());
        assertEquals(0, backgroundCompactions.getTotalCompactions());
    }

    @Test
    public void testReplacePending()
    {
        // Add som pending aggregates, then replace them with aggregates with different keys, verify that only
        // those with compactions are kept, partially overlap the keys between the old and new aggregates

        BackgroundCompactions backgroundCompactions = new BackgroundCompactions(strategy, cfs);

        List<CompactionAggregate> pending = new ArrayList<>(0);
        int key = 0;
        for (int i = 0; i < 5; i++)
        {
            pending.add(mockAggregate(key++, 1, 0)); // these aggregates have no compactions
        }

        // this aggregates have a compaction
        for (int i = 0; i < 5; i++)
        {
            CompactionAggregate aggregateWithComps = mockAggregate(key++, 1, 1);
            when(aggregateWithComps.withOnlyTheseCompactions(any(Collection.class))).thenReturn(aggregateWithComps);
            when(aggregateWithComps.getMatching(any(TreeMap.class))).thenCallRealMethod();
            pending.add(aggregateWithComps);
        }

        backgroundCompactions.setPending(pending);

        assertEquals(pending.size(), backgroundCompactions.getEstimatedRemainingTasks());
        assertEquals(pending.size(), backgroundCompactions.getTotalCompactions());

        pending.clear();

        key -= 2; //overlap the aggregates by 2 keys

        for (int i = 0; i < 5; i++)
        {
            // those that overlap the key need to report 2 compactions because they take the one from the old aggregate
            // when addCompacting is called
            CompactionAggregate aggregate = mockAggregate(key++, i < 2 ? 2 : 1, 0);
            when(aggregate.withAdditionalCompactions(any(Collection.class))).thenReturn(aggregate);
            pending.add(aggregate);
        }

        backgroundCompactions.setPending(pending);

        // the extra compactions are those from the old aggregates with a compaction regardless of whether
        // the keys overlapped or not (when the keys overlap the new one has a compaction added, when they do
        // not the old aggregate is used)
        assertEquals(pending.size() + 5, backgroundCompactions.getEstimatedRemainingTasks());
        assertEquals(pending.size() + 5, backgroundCompactions.getTotalCompactions());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetSubmittedNoId()
    {
        BackgroundCompactions backgroundCompactions = new BackgroundCompactions(strategy, cfs);
        backgroundCompactions.setSubmitted(null, Mockito.mock(CompactionAggregate.class));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetSubmittedNoAggregate()
    {
        BackgroundCompactions backgroundCompactions = new BackgroundCompactions(strategy, cfs);
        backgroundCompactions.setSubmitted(UUID.randomUUID(), null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetSubmittedDuplicateId()
    {
        UUID uuid = UUID.randomUUID();
        CompactionAggregate aggregate = mockAggregate(1, 1, 0);
        when(aggregate.getSelected()).thenReturn(CompactionPick.EMPTY);

        BackgroundCompactions backgroundCompactions = new BackgroundCompactions(strategy, cfs);
        backgroundCompactions.setSubmitted(uuid, aggregate);
        backgroundCompactions.setSubmitted(uuid, aggregate);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetInProgressNoProgress()
    {
        BackgroundCompactions backgroundCompactions = new BackgroundCompactions(strategy, cfs);
        backgroundCompactions.setInProgress(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetCompletedNoId()
    {
        BackgroundCompactions backgroundCompactions = new BackgroundCompactions(strategy, cfs);
        backgroundCompactions.setCompleted(null);
    }
}