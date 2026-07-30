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

package org.apache.cassandra.service.accord.execution;

import java.util.ArrayDeque;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.Invariants;

import static org.apache.cassandra.service.accord.execution.Task.MAX_TRANCHE;

/**
 * Tasks are separated into tranches based on their position.
 * <p>
 * When we want to wait for all submitted tasks and their consequences to complete, we create a new tranche
 * and track the number of tasks still extant for all prior tranches - once these reach zero we can signal
 * the completion of the work.
 */
final class Tranches
{
    private static final Logger logger = LoggerFactory.getLogger(Tranches.class);

    class WithDeferred implements Runnable
    {
        final Runnable run;
        final ArrayDeque<Runnable> deferred;

        WithDeferred(Runnable run, Runnable deferred)
        {
            this.run = run;
            this.deferred = new ArrayDeque<>(1);
            this.deferred.add(deferred);
        }

        WithDeferred(Runnable run, ArrayDeque<Runnable> deferred)
        {
            this.run = run;
            this.deferred = deferred;
        }

        @Override
        public void run()
        {
            Runnable register = deferred.poll();
            if (!deferred.isEmpty())
            {
                Invariants.require(runs[firstIndex] != null);
                Invariants.require(!(runs[firstIndex] instanceof WithDeferred));
                runs[firstIndex] = new WithDeferred(runs[firstIndex], deferred);
            }
            registerWait(register);
            try
            {
                run.run();
            }
            catch (Throwable t)
            {
                owner.agent.onException(t);
            }
        }
    }

    final AccordExecutor owner;

    int firstTranche;
    int firstIndex;
    long[] mins = new long[8];
    int[] counts = new int[8];
    Runnable[] runs = new Runnable[8];

    // the next tranche, that all new work is being collected against
    // (and will move to the array accounting once a new wait is registered)
    long nextMin;
    int nextTranche;
    int nextCount;

    Tranches(AccordExecutor owner)
    {
        this.owner = owner;
    }

    private int trancheToIndex(int tranche)
    {
        int offset = trancheToIndexOffset(tranche);
        Invariants.require(offset < size(), "tranche %d is no longer tracked (first=%d next=%d)", tranche, firstTranche, nextTranche);
        return firstIndex + offset;
    }

    private int trancheToIndexOffset(int tranche)
    {
        int offset = tranche - firstTranche;
        if (offset < 0)
            offset += MAX_TRANCHE + 1;
        return offset;
    }

    int size()
    {
        return trancheToIndexOffset(nextTranche);
    }

    int capacity()
    {
        return counts.length;
    }

    int addNew(long position)
    {
        Invariants.require(position >= nextMin);
        ++nextCount;
        return nextTranche;
    }

    void addInherited(int tranche, long position)
    {
        if (tranche == nextTranche)
        {
            Invariants.require(position >= nextMin);
            ++nextCount;
        }
        else
        {
            int index = trancheToIndex(tranche);
            Invariants.require(counts[index] > 0);
            Invariants.require(mins[index] <= position);
            ++counts[index];
        }
    }

    void complete(int tranche)
    {
        if (tranche == nextTranche)
        {
            Invariants.require(nextCount > 0);
            --nextCount;
        }
        else
        {
            if (counts[trancheToIndex(tranche)] == 1)
                owner.drainUnqueuedNewWorkExclusive(); // make sure we don't have any pending

            // recompute as drainUnqueued may reentrantly modify the backing array
            int trancheIndex = trancheToIndex(tranche);
            Invariants.require(counts[trancheIndex] > 0);
            if (--counts[trancheIndex] == 0 && tranche == firstTranche)
            {
                do
                {
                    advance();
                }
                while (firstTranche != nextTranche && counts[firstIndex] == 0);
            }

            if (firstIndex >= counts.length / 2)
                compact();
        }
    }

    public void finishAll(long nextPosition)
    {
        while (firstTranche != nextTranche)
        {
            logger.warn("{} processed all pending tasks (<{}) but found {} waiting for {}", this,
                                       nextPosition, counts[firstIndex], size() == 1 ? nextMin : mins[firstIndex + 1]);
            advance();
        }
    }

    private void advance()
    {
        Runnable run = runs[firstIndex];
        counts[firstIndex] = 0;
        runs[firstIndex] = null;
        ++firstIndex;
        if (firstTranche == MAX_TRANCHE) firstTranche = 0;
        else ++firstTranche;
        try
        {
            run.run();
        }
        catch (Throwable t)
        {
            owner.agent.onException(t);
        }
    }

    public void registerWait(Runnable run)
    {
        int newNextTranche = (nextTranche + 1) % (MAX_TRANCHE + 1);
        if (newNextTranche == firstTranche)
        {
            Runnable cur = runs[firstIndex];
            if (cur instanceof WithDeferred) ((WithDeferred) cur).deferred.add(run);
            else runs[firstIndex] = new WithDeferred(runs[firstIndex], run);
            return;
        }

        if ((firstIndex + size()) == capacity())
            growOrCompact();

        int index = firstIndex + size();
        mins[index] = nextMin;
        counts[index] = nextCount;
        runs[index] = run;
        nextMin = owner.minPosition = owner.nextPosition;
        nextCount = 0;
        nextTranche = newNextTranche;
    }

    private void compact()
    {
        int size = size();
        if (size <= capacity() / 4 && capacity() > 8) resize(capacity() / 2);
        else
        {
            compact(mins, counts, runs);
            Arrays.fill(runs, size, runs.length, null);
            Arrays.fill(mins, size, mins.length, 0);
            Arrays.fill(counts, size, counts.length, 0);
        }
    }

    private void growOrCompact()
    {
        if (size() > capacity() / 2) resize(capacity() * 2);
        else compact();
    }

    private void resize(int newSize)
    {
        Invariants.require(newSize > 0);
        long[] newMins = new long[newSize];
        int[] newCounts = new int[newSize];
        Runnable[] newRuns = new Runnable[newSize];
        compact(newMins, newCounts, newRuns);
        mins = newMins;
        counts = newCounts;
        runs = newRuns;
    }

    private void compact(long[] newMins, int[] newCounts, Runnable[] newRuns)
    {
        int size = size();
        System.arraycopy(mins, firstIndex, newMins, 0, size);
        System.arraycopy(counts, firstIndex, newCounts, 0, size);
        System.arraycopy(runs, firstIndex, newRuns, 0, size);
        firstIndex = 0;
    }
}
