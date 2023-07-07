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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.db.compaction.unified.Controller;
import org.apache.cassandra.db.compaction.unified.Reservations;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Overlaps;
import org.hamcrest.Matchers;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class UnifiedCompactionStrategyGetSelectionTest extends BaseCompactionStrategyTest
{
    @Parameterized.Parameter(0)
    public double modifier;

    @Parameterized.Parameter(1)
    public int reservations;

    @Parameterized.Parameter(2)
    public Reservations.Type reservationsType;

    @Parameterized.Parameter(3)
    public int levels;

    @Parameterized.Parameter(4)
    public int compactors;

    static final long START_SIZE = 1L << 30;


    @Parameterized.Parameters(name = "Type {2} Reservations {1} Modifier {0} Levels {3} Compactors {4}")
    public static List<Object[]> params()
    {
        ArrayList<Object[]> params = new ArrayList<>();
        for (Reservations.Type reservationsType : Reservations.Type.values())
            for (int reservations : new int[]{ 0, 1, Integer.MAX_VALUE })
                for (double modifier : new double[]{ 0.0, 0.5, 1.0 })
                    for (int levels = 1; levels < 5; ++levels)
                        for (int compactors : new int[] {1, 4, 12, 30})
                            params.add(new Object[]{ modifier, reservations, reservationsType, levels, compactors });
        return params;
    }

    @Test
    public void testGetSelection()
    {
        testGetSelection(generateCompactions(levels, 8 + compactors * 4, START_SIZE, modifier),
                         reservations,
                         reservationsType,
                         compactors,
                         levels,
                         100L << 30,
                         random.nextInt(20) + 1);
    }

    boolean ignoreRepeats()
    {
        return true;
    }

    List<CompactionSSTable> getSSTablesSet(List<List<CompactionSSTable>> sets, int levels, int perLevel, int level, int sstableInLevel)
    {
        return sets.get(0);
    }

    List<List<CompactionSSTable>> prepareSSTablesSets(int levels, int perLevel)
    {
        // We are reusing the same set, the construction will ignore it
        final List<CompactionSSTable> fakeSet = ImmutableList.of(Mockito.mock(CompactionSSTable.class));
        return ImmutableList.of(fakeSet);
    }

    List<CompactionAggregate.UnifiedAggregate> generateCompactions(int levels, int perLevel, long startSize, double sizeModifier)
    {
        double growth = Math.pow(2, 1 - sizeModifier);
        List<CompactionAggregate.UnifiedAggregate> list = new ArrayList<>();
        List<List<CompactionSSTable>> sets = prepareSSTablesSets(levels, perLevel);
        long size = startSize;
        for (int i = 0; i < levels; ++i)
        {
            for (int j = 0; j < perLevel; ++j)
            {
                int overlap = (int) Math.max(0, random.nextGaussian() * 5 + 15);
                CompactionPick pick = CompactionPick.create(UUID.randomUUID(),
                                                            i,
                                                            getSSTablesSet(sets, levels, perLevel, i, j),
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

    static String toString(CompactionAggregate a)
    {
        CompactionAggregate.UnifiedAggregate u = (CompactionAggregate.UnifiedAggregate) a;
        CompactionPick p = u.getSelected();
        return String.format("level %d size %s overlap %d%s", levelOf(p), FBUtilities.prettyPrintMemory(p.totSizeInBytes()), u.maxOverlap(), p.hotness() < 0 ? " adaptive" : "");
    }

    public void testGetSelection(List<CompactionAggregate.UnifiedAggregate> compactions,
                                 int reservations,
                                 Reservations.Type reservationType,
                                 int totalCount,
                                 int levelCount,
                                 long spaceAvailable,
                                 int adaptiveLimit)
    {
        System.out.println(String.format("Starting testGetSelection: reservations %d, totalCount %d, levelCount %d, spaceAvailable %s, adaptiveLimit %d",
                                         reservations,
                                         totalCount,
                                         levelCount,
                                         FBUtilities.prettyPrintMemory(spaceAvailable),
                                         adaptiveLimit));
        boolean ignoreRepeats = ignoreRepeats();

        Controller controller = Mockito.mock(Controller.class, Mockito.withSettings().stubOnly());
        when(controller.random()).thenAnswer(inv -> ThreadLocalRandom.current());
        when(controller.prioritize(anyList())).thenCallRealMethod();
        when(controller.getReservedThreads()).thenReturn(reservations);
        when(controller.getReservationsType()).thenReturn(reservationType);
        when(controller.getOverheadSizeInBytes(any())).thenAnswer(inv -> ((CompactionPick) inv.getArgument(0)).totSizeInBytes());
        when(controller.isRecentAdaptive(any())).thenAnswer(inv -> ((CompactionPick) inv.getArgument(0)).hotness() < 0);  // hotness is used to mock adaptive
        when(controller.overlapInclusionMethod()).thenReturn(ignoreRepeats ? Overlaps.InclusionMethod.TRANSITIVE : Overlaps.InclusionMethod.NONE);

        int[] perLevel = new int[levelCount];
        int maxReservations = totalCount / levelCount;
        boolean oneExtra = maxReservations < reservations;
        reservations = Math.min(reservations, maxReservations);
        int remainder = totalCount - levelCount * reservations;

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
            boolean extrasExhausted = verifyReservations(reservationType, reservations, levelCount, perLevel, remainder, oneExtra);

            // Check that we do select what we can select
            if (running.size() < totalCount)
            {
                for (int i = 0; i < levelCount; ++i)
                {
                    if (hasRoomInLevel(reservationType, reservations, remainder, oneExtra, extrasExhausted, perLevel, i))
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

    private static boolean verifyReservations(Reservations.Type type, int reservations, int levelCount, int[] perLevel, int remainder, boolean oneExtra)
    {
        switch (type)
        {
            case PER_LEVEL:
                return verifyReservationsPerLevel(reservations, levelCount, perLevel, remainder, oneExtra);
            case LEVEL_OR_BELOW:
                return verifyReservationsLevelOrBelow(reservations, levelCount, perLevel, remainder, oneExtra);
            default:
                throw new AssertionError();
        }
    }
    private static boolean verifyReservationsPerLevel(int reservations, int levelCount, int[] perLevel, int remainder, boolean oneExtra)
    {
        int remainderUsed = 0;
        int allowedExtra = oneExtra ? 1 : remainder;
        for (int i = 0; i < levelCount; ++i)
        {
            Assert.assertThat(perLevel[i], Matchers.lessThanOrEqualTo(reservations + allowedExtra));
            if (perLevel[i] > reservations)
                remainderUsed += perLevel[i] - reservations;
        }
        Assert.assertThat(remainderUsed, Matchers.lessThanOrEqualTo(remainder));
        return remainderUsed >= remainder;
    }

    private static boolean verifyReservationsLevelOrBelow(int reservations, int levelCount, int[] perLevel, long remainder, boolean oneExtra)
    {
        long sum = 0;
        long allowed = oneExtra ? 0 : remainder;
        int count = 0;
        for (int i = levelCount - 1; i >= 0; --i)
        {
            sum += perLevel[i];
            allowed += reservations;
            if (++count <= remainder && oneExtra)
                ++allowed;
            Assert.assertThat(sum, Matchers.lessThanOrEqualTo(allowed));
        }
        Assert.assertThat(sum, Matchers.lessThanOrEqualTo(remainder + levelCount * reservations));
        assertEquals(allowed, remainder + levelCount * reservations);   // if failed, the problem is in the test
        return sum >= remainder + levelCount * reservations;
    }

    private static boolean isAcceptableLevelOrBelow(int reservations, int levelCount, int[] perLevel, long remainder, boolean oneExtra)
    {
        long sum = 0;
        long allowed = oneExtra ? 0 : remainder;
        int count = 0;
        for (int i = levelCount - 1; i >= 0; --i)
        {
            sum += perLevel[i];
            allowed += reservations;
            if (++count <= remainder && oneExtra)
                ++allowed;
            if (sum > allowed)
                return false;
        }
        return true;
    }

    private static boolean hasRoomInLevel(Reservations.Type type, int reservations, int remainder, boolean oneExtra, boolean extrasExhausted, int perLevel[], int level)
    {
        switch (type)
        {
            case PER_LEVEL:
                return hasRoomInLevelPerLevel(reservations, remainder, oneExtra, extrasExhausted, perLevel, level);
            case LEVEL_OR_BELOW:
                return hasRoomInLevelOrAbove(reservations, remainder, oneExtra, extrasExhausted, perLevel, level);
            default:
                throw new AssertionError();
        }
    }

    private static boolean hasRoomInLevelPerLevel(int reservations, int remainder, boolean oneExtra, boolean extrasExhausted, int perLevel[], int level)
    {
        int allowedExtra = extrasExhausted ? 0 : (oneExtra ? 1 : remainder);
        return perLevel[level] < reservations + allowedExtra;
    }

    private static boolean hasRoomInLevelOrAbove(int reservations, int remainder, boolean oneExtra, boolean extrasExhausted, int perLevel[], int level)
    {
        if (extrasExhausted)
            return false;
        ++perLevel[level];
        boolean result = isAcceptableLevelOrBelow(reservations, perLevel.length, perLevel, remainder, oneExtra);
        --perLevel[level];
        return result;
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
