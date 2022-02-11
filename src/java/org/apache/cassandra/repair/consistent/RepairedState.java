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

package org.apache.cassandra.repair.consistent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

import static org.apache.cassandra.service.ActiveRepairService.UNREPAIRED_SSTABLE;

/**
 * Tracks the repaired state of token ranges per table, and is effectively an
 * in memory representation of the on-disk local incremental repair state.
 *
 * The main purpose of this class is to provide metrics via nodetool repair_admin. To make sure those metrics
 * are accurate, it also determines when a completed IR session can be deleted, which is explained in a bit
 * more detail in LocalSessions#cleanup, by the call to isSuperseded.
 */
public class RepairedState
{
    static class Level
    {
        final List<Range<Token>> ranges;
        final long repairedAt;

        private static final Comparator<Level> timeComparator = Comparator.comparingLong(l -> -l.repairedAt);

        Level(Collection<Range<Token>> ranges, long repairedAt)
        {
            this.ranges = Range.normalize(ranges);
            this.repairedAt = repairedAt;
        }

        Level subtract(Collection<Range<Token>> ranges)
        {
            if (ranges.isEmpty())
                return this;

            Set<Range<Token>> difference = Range.subtract(this.ranges, ranges);
            if (difference.isEmpty())
                return null;

            return new Level(difference, repairedAt);
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Level level = (Level) o;
            return repairedAt == level.repairedAt &&
                   Objects.equals(ranges, level.ranges);
        }

        public int hashCode()
        {
            return Objects.hash(ranges, repairedAt);
        }

        @Override
        public String toString()
        {
            return "Level{" +
                   "ranges=" + ranges +
                   ", repairedAt=" + repairedAt +
                   '}';
        }
    }

    public static class Section
    {
        public final Range<Token> range;
        public final long repairedAt;
        private static final Comparator<Section> tokenComparator = (l, r) -> l.range.left.compareTo(r.range.left);

        Section(Range<Token> range, long repairedAt)
        {
            this.range = range;
            this.repairedAt = repairedAt;
        }

        Section makeSubsection(Range<Token> subrange)
        {
            Preconditions.checkArgument(range.contains(subrange));
            return new Section(subrange, repairedAt);
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Section section = (Section) o;
            return repairedAt == section.repairedAt &&
                   Objects.equals(range, section.range);
        }

        public int hashCode()
        {

            return Objects.hash(range, repairedAt);
        }

        @Override
        public String toString()
        {
            return "Section{" +
                   "range=" + range +
                   ", repairedAt=" + repairedAt +
                   '}';
        }
    }

    public static class Stats
    {
        public static final Stats EMPTY = new Stats(UNREPAIRED_SSTABLE, UNREPAIRED_SSTABLE, Collections.emptyList());

        public final long minRepaired;
        public final long maxRepaired;
        public final List<Section> sections;

        public Stats(long minRepaired, long maxRepaired, List<Section> sections)
        {
            this.minRepaired = minRepaired;
            this.maxRepaired = maxRepaired;
            this.sections = sections;
        }


    }

    static class State
    {
        final ImmutableList<Level> levels;
        final ImmutableList<Range<Token>> covered;
        final ImmutableList<Section> sections;

        State(List<Level> levels, List<Range<Token>> covered, List<Section> sections)
        {
            this.levels = ImmutableList.copyOf(levels);
            this.covered = ImmutableList.copyOf(covered);
            this.sections = ImmutableList.copyOf(sections);
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            State state = (State) o;
            return Objects.equals(levels, state.levels) &&
                   Objects.equals(covered, state.covered);
        }

        public int hashCode()
        {
            return Objects.hash(levels, covered);
        }

        @Override
        public String toString()
        {
            return "State{" +
                   "levels=" + levels +
                   ", covered=" + covered +
                   '}';
        }
    }

    private volatile State state = new State(Collections.emptyList(), Collections.emptyList(), Collections.emptyList());

    State state()
    {
        return state;
    }

    public synchronized void add(Collection<Range<Token>> ranges, long repairedAt)
    {
        addAll(Collections.singletonList(new Level(ranges, repairedAt)));
    }

    public void addAll(List<Level> newLevels)
    {
        State lastState = state;
        List<Level> tmp = new ArrayList<>(lastState.levels.size() + newLevels.size());
        tmp.addAll(lastState.levels);
        tmp.addAll(newLevels);
        tmp.sort(Level.timeComparator);

        List<Level> levels = new ArrayList<>(tmp.size());
        List<Range<Token>> covered = new ArrayList<>();

        for (Level level : tmp)
        {
            Level subtracted = level.subtract(covered);
            if (subtracted == null)
                continue;

            levels.add(subtracted);

            covered.addAll(subtracted.ranges);
            covered = Range.normalize(covered);
        }

        List<Section> sections = new ArrayList<>();
        for (Level level : levels)
        {
            for (Range<Token> range : level.ranges)
            {
                sections.add(new Section(range, level.repairedAt));
            }
        }
        sections.sort(Section.tokenComparator);
        state = new State(levels, covered, sections);
	}

    public long minRepairedAt(Collection<Range<Token>> ranges)
    {
        State current = state;

        Set<Range<Token>> remainingRanges = new HashSet<>(ranges);
        long minTime = Long.MAX_VALUE;
        for (Section section : current.sections)
        {
            if (section.range.intersects(remainingRanges))
            {
                minTime = Math.min(minTime, section.repairedAt);
                remainingRanges = Range.subtract(remainingRanges, Collections.singleton(section.range));
            }

            if (remainingRanges.isEmpty())
                break;
        }
        // if there are still ranges we don't have data for, part of the requested ranges is unrepaired
        return remainingRanges.isEmpty() ? minTime : UNREPAIRED_SSTABLE;
    }

    static List<Section> getRepairedStats(List<Section> sections, Collection<Range<Token>> ranges)
    {
        if (ranges.isEmpty())
            return Collections.emptyList();

        Set<Range<Token>> remaining = Sets.newHashSet(Range.normalize(ranges));
        List<Section> results = new ArrayList<>();

        for (Section section : sections)
        {
            if (remaining.isEmpty())
                break;

            Set<Range<Token>> sectionRanges = Range.rangeSet(section.range);
            for (Range<Token> range : remaining)
            {
                if (sectionRanges.isEmpty())
                    break;

                Set<Range<Token>> intersection = new HashSet<>();
                sectionRanges.forEach(r -> intersection.addAll(r.intersectionWith(range)));

                if (intersection.isEmpty())
                    continue;

                intersection.forEach(r -> results.add(section.makeSubsection(r)));
                sectionRanges = Range.subtract(sectionRanges, intersection);
            }

            remaining = Range.subtract(remaining, Collections.singleton(section.range));
        }

        remaining.forEach(r -> results.add(new Section(r, UNREPAIRED_SSTABLE)));
        results.sort(Section.tokenComparator);

        return results;
    }

    public Stats getRepairedStats(Collection<Range<Token>> ranges)
    {
        List<Section> sections = getRepairedStats(state.sections, ranges);

        if (sections.isEmpty())
            return new Stats(UNREPAIRED_SSTABLE, UNREPAIRED_SSTABLE, Collections.emptyList());

        long minTime = Long.MAX_VALUE;
        long maxTime = Long.MIN_VALUE;

        for (Section section : sections)
        {
            minTime = Math.min(minTime, section.repairedAt);
            maxTime = Math.max(maxTime, section.repairedAt);
        }

        return new Stats(minTime, maxTime, sections);
    }
}
