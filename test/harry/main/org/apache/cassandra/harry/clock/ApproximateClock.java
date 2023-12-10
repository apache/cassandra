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

package org.apache.cassandra.harry.clock;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.harry.core.Configuration;
import org.apache.cassandra.harry.model.OpSelectors;

/**
 * Monotonic clock, that guarantees that any LTS can be converted to a unique RTS, given
 * the number of LTS does not exceed the number of RTS (e.g., we do not draw LTS more frequently
 * than once per microsecond).
 * <p>
 * This conversion works as follows:
 * <p>
 * * every `period`, we record the current timestamp and maximum seen LTS, and keep history of up to
 * `historySize` LTS/timestamp combinations
 * * when queried to retrieve the LTS, we find a timestamp, relative to which we can calculate RTS.
 * After that, we calculate a difference between the largest LTS that is still smaller than the converted
 * one, and add this difference to the timestamp.
 * <p>
 * This way, later LTS can only be mapped to later RTS, and any LTS that was drawn previously, will be
 * uniquely mapped relative to some timestamp, with the order matching the LTS order.
 */
public class ApproximateClock implements OpSelectors.Clock
{
    public static final long START_VALUE = 0;
    public static final long DEFUNCT = Long.MIN_VALUE;
    public static final long REBASE_IN_PROGRESS = Long.MIN_VALUE + 1;

    // TODO: there's a theoretical possibility of a bug; when we have several consecutive epochs without
    // change in LTS, current implementation will return the latest epoch instead of the earliest one.
    // This is not a big deal in terms of monotonicity but can cause some problems when validating TTL.
    // The simples fix would be to find the smallest matching epoch.
    private final ScheduledExecutorService executor;
    private final int historySize;
    private final CopyOnWriteArrayList<Long> ltsHistory;
    private final long startTimeMicros;
    private volatile int idx;
    private final AtomicLong lts;

    private final long periodMicros;

    private final long epoch;
    private final TimeUnit epochTimeUnit;

    public ApproximateClock(long period, TimeUnit timeUnit)
    {
        this(10000, period, timeUnit);
    }

    public ApproximateClock(int historySize, long epoch, TimeUnit epochTimeUnit)
    {
        this(TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()),
             historySize, new CopyOnWriteArrayList<>(), START_VALUE, 0, epoch, epochTimeUnit);
        rebase();
    }

    ApproximateClock(long startTimeMicros,
                     int historySize,
                     CopyOnWriteArrayList<Long> history,
                     long lts,
                     int idx,
                     long epoch,
                     TimeUnit epochTimeUnit)
    {
        this.startTimeMicros = startTimeMicros;
        this.historySize = historySize;
        this.ltsHistory = history;
        this.lts = new AtomicLong(lts);
        this.idx = idx;
        this.periodMicros = epochTimeUnit.toMicros(epoch);
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setName("ApproximateMonotonicClock-ScheduledTasks");
            t.setDaemon(true);
            return t;
        });
        this.executor.scheduleAtFixedRate(this::rebase, epoch, epoch, epochTimeUnit);
        this.epoch = epoch;
        this.epochTimeUnit = epochTimeUnit;
    }

    @VisibleForTesting
    public static ApproximateClock forDebug(long startTimeMicros, int historySize, long lts, int idx, long period, TimeUnit timeUnit, long... values)
    {
        CopyOnWriteArrayList<Long> history = new CopyOnWriteArrayList<>();
        for (int i = 0; i < values.length; i++)
            history.set(i, values[i]);

        assert values.length == idx; // sanity check
        return new ApproximateClock(startTimeMicros, historySize, history, lts, idx, period, timeUnit);
    }

    public long get(long idx)
    {
        return ltsHistory.get((int) (idx % historySize));
    }

    private void rebase()
    {
        int arrayIdx = idx % historySize;
        long rebaseLts = lts.get();
        if (rebaseLts == DEFUNCT)
            throw new IllegalStateException();

        while (!lts.compareAndSet(rebaseLts, REBASE_IN_PROGRESS))
            rebaseLts = lts.get();

        ltsHistory.add(arrayIdx, rebaseLts == START_VALUE ? START_VALUE : (rebaseLts + 1));

        // If we happen to exhaust counter, we just need to make operations "wider".
        // It is unsafe to proceed, so we defunct the clock.
        //
        // We could make a clock implementation that would sleep on `get`, but it will
        // be more expensive, since we'll have to check for overflow each time before
        // returning anything.
        if (idx > 1 && get(idx) - get(idx - 1) > periodMicros)
        {
            lts.set(DEFUNCT);
            executor.shutdown();
            throwCounterExhaustedException();
        }

        idx = idx + 1;
        if (!lts.compareAndSet(REBASE_IN_PROGRESS, rebaseLts))
            throw new IllegalStateException("No thread should have changed LTS during rebase. " + lts.get());
    }

    @Override
    public long nextLts()
    {
        long current = lts.get();
        while (true)
        {
            if (current >= 0)
            {
                if (lts.compareAndSet(current, current + 1))
                    return current;

                current = lts.get();
                continue;
            }

            if (current == REBASE_IN_PROGRESS)
            {
                LockSupport.parkNanos(1);
                current = lts.get();
                continue;
            }

            if (current == DEFUNCT)
                throwCounterExhaustedException();

            throw new IllegalStateException("This should have been unreachable: " + current);
        }
    }

    public long peek()
    {
        while (true)
        {
            long ret = lts.get();

            if (ret == REBASE_IN_PROGRESS)
            {
                LockSupport.parkNanos(1);
                continue;
            }

            if (ret == DEFUNCT)
                throwCounterExhaustedException();

            return ret;
        }
    }

    public Configuration.ClockConfiguration toConfig()
    {
        int idx = this.idx;
        long[] history = new long[Math.min(idx, historySize)];
        for (int i = 0; i < history.length; i++)
            history[i] = ltsHistory.get(i);
        return new Configuration.DebugApproximateClockConfiguration(startTimeMicros,
                                                                    ltsHistory.size(),
                                                                    history,
                                                                    lts.get(),
                                                                    idx,
                                                                    epoch,
                                                                    epochTimeUnit);
    }

    public long lts(final long rts)
    {
        final int historyIdx = idx - 1;
        for (int i = 0; i < historySize - 1 && historyIdx - i >= 0; i++)
        {
            long periodStartRts = startTimeMicros + periodMicros * (historyIdx - i);
            if (rts >= periodStartRts)
            {
                long periodStartLts = get(historyIdx - i);
                return periodStartLts + rts - periodStartRts;
            }
        }
        throw new IllegalStateException("RTS is too old to convert to LTS: " + rts + "\n " + ltsHistory);
    }

    public long rts(final long lts)
    {
        assert lts <= peek() : String.format("Queried for LTS we haven't yet issued %d. Max is %d.", lts, peek());

        final int historyIdx = idx - 1;
        for (int i = 0; i < historySize - 1 && historyIdx - i >= 0; i++)
        {
            long periodStartLts = get(historyIdx - i);
            if (lts >= periodStartLts)
            {
                long periodStartRts = startTimeMicros + periodMicros * (historyIdx - i);
                return periodStartRts + lts - periodStartLts;
            }
        }

        throw new IllegalStateException("LTS is too old to convert to RTS: " + lts + "\n " + dumpHistory());
    }

    private String dumpHistory()
    {
        String s = "";
        int idx = this.idx;
        for (int i = 0; i < Math.min(idx, historySize); i++)
        {
            s += ltsHistory.get(i) + ",";
        }
        return s.substring(0, Math.max(0, s.length() - 1));
    }

    public String toString()
    {
        return String.format("withDebugClock(%dL,\n\t%d,\n\t%d,\n\t%d,\n\t%d,\n\t%s,\n\t%s)",
                             startTimeMicros,
                             historySize,
                             lts.get(),
                             idx,
                             epoch,
                             epochTimeUnit,
                             dumpHistory());
    }

    private void throwCounterExhaustedException()
    {
        long diff = get(idx) - get(idx - 1);
        throw new RuntimeException(String.format("Counter was exhausted. Drawn %d out of %d lts during the period.",
                                                 diff, periodMicros));
    }
}
