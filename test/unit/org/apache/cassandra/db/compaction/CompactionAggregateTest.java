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
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.Test;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Pair;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CompactionAggregateTest
{
    @Test
    public void testContainsSameInstance()
    {
        // Create a CompactionAggregate
        SSTableReader sstableReader = mock(SSTableReader.class);
        CompactionAggregate agg = CompactionAggregate.createForTombstones(sstableReader);
        // Non-existnig compaction
        Pair<Boolean, CompactionPick> res = agg.containsSameInstance(mock(CompactionPick.class));
        
        assertFalse(res.left);
        assertNull(res.right);
        
        // Existing compaction and same instance
        CompactionPick existing = agg.getSelected();
        res = agg.containsSameInstance(existing);
        
        assertTrue(res.left); // same instance
        assertNotNull(res.right);
        assertEquals(existing, res.right);
        assertSame(existing, res.right);

        // Existing compaction but different instance
        CompactionPick otherInstance = existing.withParent(existing.parent());
        res = agg.containsSameInstance(otherInstance);
        
        assertFalse(res.left); // different instance
        assertNotNull(res.right);
        assertEquals(otherInstance, res.right);
        assertNotSame(otherInstance, res.right);
    }

    @Test
    public void testWithReplacedCompaction()
    {
        // Create a CompactionAggregate with two compactions
        CompactionPick anotherCompaction = Mockito.mock(CompactionPick.class);
        when(anotherCompaction.sstables()).thenReturn(ImmutableSet.of());
        SSTableReader sstableReader = mock(SSTableReader.class);
        CompactionAggregate agg = CompactionAggregate.createForTombstones(sstableReader)
                                                     .withAdditionalCompactions(ImmutableList.of(anotherCompaction));

        // Setup existing and replacement CompactionPick
        CompactionPick existing = agg.getSelected();
        CompactionPick replacement = existing.withParent(existing.parent() + 1);

        // Initial conditions
        assertEquals(2, agg.compactions.size());
        assertFalse(agg.compactions.contains(replacement));

        // No existing CompactionPick to replace - replacement is added
        CompactionAggregate res = agg.withReplacedCompaction(replacement, null);
        
        assertEquals(3, res.compactions.size());
        assertTrue(res.compactions.contains(replacement));

        // Existing CompactionPick is replaced
        res = agg.withReplacedCompaction(replacement, existing);
        
        assertEquals(2, res.compactions.size());
        assertFalse(res.compactions.contains(existing));
        assertTrue(res.compactions.contains(replacement));
    }

    @Test
    public void testAggregatedStatistics()
    {
        UnifiedCompactionStrategy.Arena arena = Mockito.mock(UnifiedCompactionStrategy.Arena.class);
        UnifiedCompactionStrategy.Level level = Mockito.mock(UnifiedCompactionStrategy.Level.class);
        int picksCount = 15;
        int perPick = 4;
        long readTput = 1_000_000;
        long writeTput = 800_000;
        long sstableSize = picksCount * perPick * readTput;
        int totalTime = 0;
        int inProgressCount = 0;
        ArrayList<CompactionPick> compactions = new ArrayList<>();
        for (int i = 0; i < picksCount; ++i)
        {
            CompactionPick pickMock = Mockito.mock(CompactionPick.class);
            final ImmutableSet<CompactionSSTable> pickSSTables = mockSSTables(4, sstableSize);
            when(pickMock.sstables()).thenReturn(pickSSTables);
            final boolean inProgress = (i & 1) == 0;
            when(pickMock.inProgress()).thenReturn(inProgress);
            when(pickMock.submitted()).thenReturn(inProgress);
            final boolean completed = i % 10 == 9;
            when(pickMock.completed()).thenReturn(completed);
            if (inProgress && !completed)
            {
                CompactionProgress progress = Mockito.mock(CompactionTask.CompactionOperation.class);
                when(pickMock.progress()).thenReturn(progress);
                final int timeInSecs = picksCount - i;
                when(progress.durationInNanos()).thenReturn(TimeUnit.SECONDS.toNanos(timeInSecs));
                when(progress.readThroughput()).thenCallRealMethod();
                when(progress.writeThroughput()).thenCallRealMethod();
                when(progress.uncompressedBytesRead()).thenReturn(timeInSecs * readTput);
                when(progress.uncompressedBytesWritten()).thenReturn(timeInSecs * writeTput);
                totalTime += timeInSecs;
                ++inProgressCount;
            }
            compactions.add(pickMock);
        }
        List<CompactionSSTable> sstables = compactions.stream().flatMap(pick -> pick.sstables().stream()).collect(Collectors.toList());
        CompactionPick selectedPick = compactions.remove(0);
        ImmutableSet<CompactionSSTable> expired = mockSSTables(7, sstableSize);
        sstables.addAll(expired);
        when(selectedPick.expired()).thenReturn(expired);
        for (CompactionPick pending : compactions)
            when(pending.expired()).thenReturn(ImmutableSet.of());
        CompactionAggregate agg = CompactionAggregate.createUnified(sstables, 4, selectedPick, compactions, arena, level);
        CompactionAggregateStatistics stats = agg.getStatistics();
        final long incompletePicks = picksCount - compactions.stream().filter(CompactionPick::completed).count();
        assertEquals(incompletePicks, stats.numCompactions());
        assertEquals(inProgressCount, stats.numCompactionsInProgress());
        assertEquals(incompletePicks * perPick * sstableSize, stats.tot());
        assertEquals(totalTime * readTput, stats.read());
        assertEquals(totalTime * writeTput, stats.written());
        assertEquals(inProgressCount * readTput, stats.readThroughput(), 0);
        assertEquals(inProgressCount * writeTput, stats.writeThroughput(), 0);
        assertEquals(7, stats.numExpiredSSTables);
        assertEquals(7 * sstableSize, stats.totalBytesToDrop);
    }

    private static ImmutableSet<CompactionSSTable> mockSSTables(int perPick, long sstableSize)
    {
        Set<CompactionSSTable> sstables = Sets.newHashSet();
        for (int i = 0; i < perPick; ++i)
        {
            CompactionSSTable sstableMock = Mockito.mock(CompactionSSTable.class, Mockito.withSettings().stubOnly());
            when(sstableMock.uncompressedLength()).thenReturn(sstableSize);
            sstables.add(sstableMock);
        }
        return ImmutableSet.copyOf(sstables);
    }
}
