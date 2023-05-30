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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.agrona.collections.IntArrayList;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.unified.Controller;
import org.apache.cassandra.db.compaction.unified.UnifiedCompactionTask;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Splitter;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Interval;
import org.apache.cassandra.utils.Overlaps;
import org.apache.cassandra.utils.Pair;
import org.hamcrest.Matchers;
import org.mockito.Mockito;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;

public class UnifiedCompactionStrategyGetSelectionTest extends BaseCompactionStrategyTest
{
    @Test
    public void testGetSelection_Modifier1_Reservations0()
    {
        long startSize = 1L << 30;
        for (int levels = 1; levels < 5; ++levels)
            for (int compactors = 1; compactors <= 32; compactors *= 2)
            {
                testGetSelection(generateCompactions(levels, 8 + compactors * 4, startSize, 1.0), 0, compactors, levels, 100L << 30, random.nextInt(20) + 1);
            }
    }

    @Test
    public void testGetSelection_Modifier1_Reservations1()
    {
        long startSize = 1L << 30;
        for (int levels = 1; levels < 5; ++levels)
            for (int compactors = 1; compactors <= 32; compactors *= 2)
            {
                testGetSelection(generateCompactions(levels, 8 + compactors * 4, startSize, 1.0), 1, compactors, levels, 100L << 30, random.nextInt(20) + 1);
            }
    }

    @Test
    public void testGetSelection_Modifier1_ReservationsMax()
    {
        long startSize = 1L << 30;
        for (int levels = 1; levels < 5; ++levels)
            for (int compactors = 1; compactors <= 32; compactors *= 2)
            {
                testGetSelection(generateCompactions(levels, 8 + compactors * 4, startSize, 1.0), Integer.MAX_VALUE, compactors, levels, 100L << 30, random.nextInt(20) + 1);
            }
    }

    @Test
    public void testGetSelection_Modifier0_Reservations0()
    {
        long startSize = 1L << 30;
        for (int levels = 1; levels < 5; ++levels)
            for (int compactors = 1; compactors <= 32; compactors *= 2)
            {
                testGetSelection(generateCompactions(levels, 8 + compactors * 4, startSize, 0.0), 0, compactors, levels, 100L << 30, random.nextInt(20) + 1);
            }
    }

    @Test
    public void testGetSelection_Modifier0_Reservations1()
    {
        long startSize = 1L << 30;
        for (int levels = 1; levels < 5; ++levels)
            for (int compactors = 1; compactors <= 32; compactors *= 2)
            {
                testGetSelection(generateCompactions(levels, 8 + compactors * 4, startSize, 0.0), 1, compactors, levels, 100L << 30, random.nextInt(20) + 1);
            }
    }

    @Test
    public void testGetSelection_Modifier0_ReservationsMax()
    {
        long startSize = 1L << 30;
        for (int levels = 1; levels < 5; ++levels)
            for (int compactors = 1; compactors <= 32; compactors *= 2)
            {
                testGetSelection(generateCompactions(levels, 8 + compactors * 4, startSize, 0.0), Integer.MAX_VALUE, compactors, levels, 100L << 30, random.nextInt(20) + 1);
            }
    }

    @Test
    public void testGetSelection_Modifier0pt5_Reservations0()
    {
        long startSize = 1L << 30;
        for (int levels = 1; levels < 5; ++levels)
            for (int compactors = 1; compactors <= 32; compactors *= 2)
            {
                testGetSelection(generateCompactions(levels, 8 + compactors * 4, startSize, 0.5), 0, compactors, levels, 20L << 30, random.nextInt(20) + 1);
            }
    }

    @Test
    public void testGetSelection_Modifier0pt5_Reservations1()
    {
        long startSize = 1L << 30;
        for (int levels = 1; levels < 5; ++levels)
            for (int compactors = 1; compactors <= 32; compactors *= 2)
            {
                testGetSelection(generateCompactions(levels, 8 + compactors * 4, startSize, 0.5), 1, compactors, levels, 20L << 30, random.nextInt(20) + 1);
            }
    }

    @Test
    public void testGetSelection_Modifier0pt5_ReservationsMax()
    {
        long startSize = 1L << 30;
        for (int levels = 1; levels < 5; ++levels)
            for (int compactors = 1; compactors <= 32; compactors *= 2)
            {
                testGetSelection(generateCompactions(levels, 8 + compactors * 4, startSize, 0.5), Integer.MAX_VALUE, compactors, levels, 20L << 30, random.nextInt(20) + 1);
            }
    }

    private List<CompactionAggregate.UnifiedAggregate> generateCompactions(int levels, int perLevel, long startSize, double sizeModifier)
    {
        double growth = Math.pow(2, 1 - sizeModifier);
        List<CompactionAggregate.UnifiedAggregate> list = new ArrayList<>();
        long size = startSize;
        List<CompactionSSTable> fakeSet = ImmutableList.of(Mockito.mock(CompactionSSTable.class));
        for (int i = 0; i < levels; ++i)
        {
            for (int j = 0; j < perLevel; ++j)
            {
                int overlap = (int) Math.max(0, random.nextGaussian() * 5 + 15);
                CompactionPick pick = CompactionPick.create(UUID.randomUUID(),
                                                            i,
                                                            fakeSet,
                                                            Collections.emptySet(),
                                                            random.nextInt(20) == 0 ? -1 : 1,
                                                            size,
                                                            size);
                CompactionAggregate.UnifiedAggregate aggregate = Mockito.mock(CompactionAggregate.UnifiedAggregate.class, Mockito.withSettings().stubOnly());
                when(aggregate.getSelected()).thenReturn(pick);
                when(aggregate.maxOverlap()).thenReturn(overlap);
                when(aggregate.toString()).thenAnswer(inv -> toString((CompactionAggregate) inv.getMock()));
                list.add(aggregate);
            }
            size *= growth;
        }
        return list;
    }

    @Test
    public void testGetSelection_Modifier0_Reservations0_Repeats()
    {
        long startSize = 1L << 30;
        for (int levels = 1; levels < 5; ++levels)
            for (int compactors = 1; compactors <= 32; compactors *= 2)
            {
                testGetSelection(generateCompactionsWithRepeats(levels, 8 + compactors * 4, startSize, 0.0), 0, compactors, levels, 100L << 30, random.nextInt(20) + 1, false);
            }
    }

    private List<CompactionAggregate.UnifiedAggregate> generateCompactionsWithRepeats(int levels, int perLevel, long startSize, double sizeModifier)
    {
        double growth = Math.pow(2, 1 - sizeModifier);
        List<CompactionAggregate.UnifiedAggregate> list = new ArrayList<>();
        long size = startSize;
        List<List<CompactionSSTable>> sets = IntStream.range(0, levels * perLevel)
                                                      .mapToObj(i -> ImmutableList.of(Mockito.mock(CompactionSSTable.class)))
                                                      .collect(Collectors.toList());
        ImmutableList.of(Mockito.mock(CompactionSSTable.class));
        for (int i = 0; i < levels; ++i)
        {
            for (int j = 0; j < perLevel; ++j)
            {
                int overlap = (int) Math.max(0, random.nextGaussian() * 5 + 15);
                CompactionPick pick = CompactionPick.create(UUID.randomUUID(),
                                                            i,
                                                            sets.get(getRepeatIndex(levels * perLevel, i * perLevel + j)),
                                                            Collections.emptySet(),
                                                            random.nextInt(20) == 0 ? -1 : 1,
                                                            size,
                                                            size);
                CompactionAggregate.UnifiedAggregate aggregate = Mockito.mock(CompactionAggregate.UnifiedAggregate.class, Mockito.withSettings().stubOnly());
                when(aggregate.getSelected()).thenReturn(pick);
                when(aggregate.maxOverlap()).thenReturn(overlap);
                when(aggregate.toString()).thenAnswer(inv -> toString((CompactionAggregate) inv.getMock()));
                list.add(aggregate);
            }
            size *= growth;
        }
        return list;
    }

    private int getRepeatIndex(int size, int index)
    {
        double d = random.nextGaussian();
        if (d <= 0.5 || d > 1)
            return index;
        else
            return (int) (d * size - 1);    // high likelihood of hitting the same index
    }

    static String toString(CompactionAggregate a)
    {
        CompactionAggregate.UnifiedAggregate u = (CompactionAggregate.UnifiedAggregate) a;
        CompactionPick p = u.getSelected();
        return String.format("level %d size %s overlap %d%s", levelOf(p), FBUtilities.prettyPrintMemory(p.totSizeInBytes()), u.maxOverlap(), p.hotness() < 0 ? " adaptive" : "");
    }

    public void testGetSelection(List<CompactionAggregate.UnifiedAggregate> compactions, int reservations, int totalCount, int levelCount, long spaceAvailable, int adaptiveLimit)
    {
        testGetSelection(compactions, reservations, totalCount, levelCount, spaceAvailable, adaptiveLimit, true);  // do not reject repeated sstables when we only mock one
    }

    public void testGetSelection(List<CompactionAggregate.UnifiedAggregate> compactions, int reservations, int totalCount, int levelCount, long spaceAvailable, int adaptiveLimit, boolean ignoreRepeats)
    {
        System.out.println(String.format("Starting testGetSelection: reservations %d, totalCount %d, levelCount %d, spaceAvailable %s, adaptiveLimit %d",
                                         reservations,
                                         totalCount,
                                         levelCount,
                                         FBUtilities.prettyPrintMemory(spaceAvailable),
                                         adaptiveLimit));

        Controller controller = Mockito.mock(Controller.class, Mockito.withSettings().stubOnly());
        when(controller.random()).thenAnswer(inv -> ThreadLocalRandom.current());
        when(controller.prioritize(anyList())).thenCallRealMethod();
        when(controller.getReservedThreadsPerLevel()).thenReturn(reservations);
        when(controller.getOverheadSizeInBytes(any())).thenAnswer(inv -> ((CompactionPick) inv.getArgument(0)).totSizeInBytes());
        when(controller.isRecentAdaptive(any())).thenAnswer(inv -> ((CompactionPick) inv.getArgument(0)).hotness() < 0);  // hotness is used to mock adaptive
        when(controller.overlapInclusionMethod()).thenReturn(ignoreRepeats ? Overlaps.InclusionMethod.TRANSITIVE : Overlaps.InclusionMethod.NONE);

        int[] perLevel = new int[levelCount];
        long remainder = totalCount - levelCount * (long) reservations; // long to deal with MAX_VALUE
        int allowedExtra = remainder >= 0 ? (int) remainder : totalCount % levelCount == 0 ? 0 : 1; // one remainder per level if reservations cannot be matched
        if (remainder < 0)
        {
            remainder = totalCount % levelCount;
            reservations = totalCount / levelCount;
        }

        List<CompactionAggregate> running = new ArrayList<>();

        while (!compactions.isEmpty())
        {
            Arrays.fill(perLevel, 0);
            long spaceTaken = 0;
            int adaptiveUsed = 0;
            for (CompactionAggregate aggregate : running)
            {
                CompactionPick compaction = aggregate.getSelected();
                final int level = levelOf(compaction);
                ++perLevel[level];
                spaceTaken += compaction.totSizeInBytes();
                if (controller.isRecentAdaptive(compaction))
                    ++adaptiveUsed;
            }

            List<CompactionAggregate> result = UnifiedCompactionStrategy.getSelection(compactions,
                                                                                      controller,
                                                                                      totalCount,
                                                                                      levelCount,
                                                                                      perLevel,
                                                                                      spaceAvailable - spaceTaken,
                                                                                      adaptiveLimit - adaptiveUsed);

            System.out.println("Selected " + result.size() + ": " + result.stream()
                                                                          .map(a -> toString(a))
                                                                          .collect(Collectors.joining(", ")));
            if (result.isEmpty())
            {
                Assert.assertFalse(running.isEmpty());
                // if running is not empty, run through to remove something from it and try again
            }


            compactions.removeAll(result);
            running.addAll(result);

            Arrays.fill(perLevel, 0);
            spaceTaken = 0;
            adaptiveUsed = 0;
            for (CompactionAggregate aggregate : running)
            {
                CompactionPick compaction = aggregate.getSelected();
                final int level = levelOf(compaction);
                ++perLevel[level];
                spaceTaken += compaction.totSizeInBytes();
                if (controller.isRecentAdaptive(compaction))
                    ++adaptiveUsed;
            }

            // Check that restrictions are honored
            Assert.assertThat(running.size(), Matchers.lessThanOrEqualTo(totalCount));
            Assert.assertThat(spaceTaken, Matchers.lessThanOrEqualTo(spaceAvailable));
            Assert.assertThat(adaptiveUsed, Matchers.lessThanOrEqualTo(adaptiveLimit));
            long remainderUsed = 0;
            for (int i = 0; i < levelCount; ++i)
            {
                Assert.assertThat(perLevel[i], Matchers.lessThanOrEqualTo(reservations + allowedExtra));
                if (perLevel[i] > reservations)
                    remainderUsed += perLevel[i] - reservations;
            }
            Assert.assertThat(remainderUsed, Matchers.lessThanOrEqualTo(remainder));

            // Check that we do select what we can select
            if (running.size() < totalCount)
            {
                for (int i = 0; i < levelCount; ++i)
                {
                    if (perLevel[i] < reservations || (perLevel[i] < reservations + allowedExtra && remainderUsed < remainder))
                    {
                        List<CompactionAggregate.UnifiedAggregate> failures = getSelectablePicks(compactions,
                                                                                                 ignoreRepeats
                                                                                                   ? Collections.emptySet()
                                                                                                   : running.stream().flatMap(a -> a.getSelected().sstables().stream()).collect(Collectors.toSet()),
                                                                                                 spaceAvailable - spaceTaken,
                                                                                                 adaptiveUsed == adaptiveLimit,
                                                                                                 controller,
                                                                                                 i);
                        Assert.assertThat(failures, Matchers.hasSize(0));
                    }
                }
            }

            // Check priorities were respected
            for (CompactionAggregate c : result)
            {
                CompactionPick p = c.getSelected();
                int level = levelOf(p);
                for (CompactionAggregate.UnifiedAggregate other : getSelectablePicks(compactions,
                                                                                     ignoreRepeats
                                                                                       ? Collections.emptySet()
                                                                                       : running.stream().flatMap(a -> a.getSelected().sstables().stream()).collect(Collectors.toSet()),
                                                                                     spaceAvailable - spaceTaken + p.totSizeInBytes(),
                                                                                     controller.isRecentAdaptive(p) ? false : adaptiveUsed == adaptiveLimit,
                                                                                     controller,
                                                                                     level))
                {
                    final CompactionAggregate.UnifiedAggregate unifiedAggregate = (CompactionAggregate.UnifiedAggregate) c;
                    Assert.assertThat(other.maxOverlap(), Matchers.lessThanOrEqualTo(unifiedAggregate.maxOverlap()));
                    if (other.maxOverlap() == unifiedAggregate.maxOverlap())
                        Assert.assertThat(other.bucketIndex(), Matchers.lessThanOrEqualTo(unifiedAggregate.bucketIndex()));
                }
            }


            // randomly simulate some of them completing
            int toRemove = (running.size() + 1) / 2; // round up, to remove one for size == 1
            for (int i = 0; i < toRemove; ++i)
                running.remove(random.nextInt(running.size()));
        }
    }

    private static <T extends CompactionAggregate> List<T> getSelectablePicks(List<T> compactions, Set<CompactionSSTable> rejectIfContained, long spaceRemaining, boolean adaptiveAtLimit, Controller controller, int level)
    {
        List<T> failures = new ArrayList<>();
        for (T compaction : compactions)
        {
            CompactionPick x = compaction.getSelected();
            if (!isSelectable(rejectIfContained, spaceRemaining, adaptiveAtLimit, controller, level, x))
                continue;

            failures.add(compaction);
        }
        return failures;
    }

    private static boolean isSelectable(Set<CompactionSSTable> rejectIfContained, long spaceRemaining, boolean adaptiveAtLimit, Controller controller, int level, CompactionPick x)
    {
        if (levelOf(x) != level) return false;
        if (x.totSizeInBytes() > spaceRemaining) return false;
        if (adaptiveAtLimit && controller.isRecentAdaptive(x)) return false;
        if (!Collections.disjoint(x.sstables(), rejectIfContained)) return false;
        return true;
    }

    private static int levelOf(CompactionPick x)
    {
        return (int) x.parent();
    }
}
