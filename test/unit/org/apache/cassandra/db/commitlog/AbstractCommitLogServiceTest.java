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

package org.apache.cassandra.db.commitlog;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.AbstractCommitLogService.SyncRunnable;
import org.apache.cassandra.utils.FreeRunningClock;

import static org.apache.cassandra.concurrent.Interruptible.State.NORMAL;
import static org.apache.cassandra.concurrent.Interruptible.State.SHUTTING_DOWN;
import static org.apache.cassandra.db.commitlog.AbstractCommitLogService.DEFAULT_MARKER_INTERVAL_MILLIS;

public class AbstractCommitLogServiceTest
{
    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setCommitLogSync(Config.CommitLogSync.periodic);
        DatabaseDescriptor.setCommitLogSyncPeriod(10 * 1000);
    }

    @Test
    public void testConstructorSyncIsQuantized()
    {
        long syncTimeMillis = 10 * 1000;
        FakeCommitLogService commitLogService = new FakeCommitLogService(syncTimeMillis);
        Assert.assertEquals(toNanos(DEFAULT_MARKER_INTERVAL_MILLIS), commitLogService.markerIntervalNanos);
        Assert.assertEquals(toNanos(syncTimeMillis), commitLogService.syncIntervalNanos);
    }

    @Test
    public void testConstructorSyncEqualsMarkerDefault()
    {
        long syncTimeMillis = 100;
        FakeCommitLogService commitLogService = new FakeCommitLogService(syncTimeMillis);
        Assert.assertEquals(toNanos(DEFAULT_MARKER_INTERVAL_MILLIS), commitLogService.markerIntervalNanos);
        Assert.assertEquals(toNanos(syncTimeMillis), commitLogService.syncIntervalNanos);
        Assert.assertEquals(commitLogService.markerIntervalNanos, commitLogService.syncIntervalNanos);
    }

    @Test
    public void testConstructorSyncShouldRoundUp()
    {
        long syncTimeMillis = 151;
        long expectedMillis = 200;
        FakeCommitLogService commitLogService = new FakeCommitLogService(syncTimeMillis);
        Assert.assertEquals(toNanos(DEFAULT_MARKER_INTERVAL_MILLIS), commitLogService.markerIntervalNanos);
        Assert.assertEquals(toNanos(expectedMillis), commitLogService.syncIntervalNanos);
    }

    @Test
    public void testConstructorSyncShouldRoundDown()
    {
        long syncTimeMillis = 121;
        long expectedMillis = 100;
        FakeCommitLogService commitLogService = new FakeCommitLogService(syncTimeMillis);
        Assert.assertEquals(toNanos(DEFAULT_MARKER_INTERVAL_MILLIS), commitLogService.markerIntervalNanos);
        Assert.assertEquals(toNanos(expectedMillis), commitLogService.syncIntervalNanos);
    }

    @Test
    public void testConstructorSyncTinyValue()
    {
        long syncTimeMillis = 10;
        long expectedNanos = toNanos(syncTimeMillis);
        FakeCommitLogService commitLogService = new FakeCommitLogService(syncTimeMillis);
        Assert.assertEquals(expectedNanos, commitLogService.markerIntervalNanos);
        Assert.assertEquals(expectedNanos, commitLogService.syncIntervalNanos);
    }

    private static long toNanos(long millis)
    {
        return TimeUnit.MILLISECONDS.toNanos(millis);
    }

    private static class FakeCommitLogService extends AbstractCommitLogService
    {
        FakeCommitLogService(long syncIntervalMillis)
        {
            super(new FakeCommitLog(), "This is not a real commit log", syncIntervalMillis, true);
            lastSyncedAt = 0;
        }

        protected void maybeWaitForSync(CommitLogSegment.Allocation alloc)
        {
            // nop
        }
    }

    @Test
    public void testSync() throws InterruptedException
    {
        long syncTimeMillis = AbstractCommitLogService.DEFAULT_MARKER_INTERVAL_MILLIS * 2;
        FreeRunningClock clock = new FreeRunningClock();
        FakeCommitLogService commitLogService = new FakeCommitLogService(syncTimeMillis);
        SyncRunnable syncRunnable = commitLogService.new SyncRunnable(clock);
        FakeCommitLog commitLog = (FakeCommitLog) commitLogService.commitLog;

        // at time 0
        syncRunnable.run(NORMAL);
        Assert.assertEquals(1, commitLog.markCount.get());
        Assert.assertEquals(0, commitLog.syncCount.get());

        // at time DEFAULT_MARKER_INTERVAL_MILLIS
        clock.advance(DEFAULT_MARKER_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
        syncRunnable.run(NORMAL);
        Assert.assertEquals(2, commitLog.markCount.get());
        Assert.assertEquals(0, commitLog.syncCount.get());

        // at time DEFAULT_MARKER_INTERVAL_MILLIS * 2
        clock.advance(DEFAULT_MARKER_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
        syncRunnable.run(NORMAL);
        Assert.assertEquals(2, commitLog.markCount.get());
        Assert.assertEquals(1, commitLog.syncCount.get());

        // at time DEFAULT_MARKER_INTERVAL_MILLIS * 3, but with shutdown!
        clock.advance(DEFAULT_MARKER_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
        syncRunnable.run(SHUTTING_DOWN);
        Assert.assertEquals(2, commitLog.markCount.get());
        Assert.assertEquals(2, commitLog.syncCount.get());
    }

    private static class FakeCommitLog extends CommitLog
    {
        private final AtomicInteger markCount = new AtomicInteger();
        private final AtomicInteger syncCount = new AtomicInteger();

        FakeCommitLog()
        {
            super(null);
        }

        @Override
        public void sync(boolean flush)
        {
            if (flush)
                syncCount.incrementAndGet();
            else
                markCount.incrementAndGet();
        }
    }

    @Test
    public void maybeLogFlushLag_MustLog()
    {
        long syncTimeMillis = 10;
        SyncRunnable syncRunnable = new FakeCommitLogService(syncTimeMillis).new SyncRunnable(new FreeRunningClock());
        long pollStarted = 1;
        long now = Integer.MAX_VALUE;
        Assert.assertTrue(syncRunnable.maybeLogFlushLag(pollStarted, now));
        Assert.assertEquals(now - pollStarted, syncRunnable.getTotalSyncDuration());
    }

    @Test
    public void maybeLogFlushLag_NoLog()
    {
        long syncTimeMillis = 10;
        SyncRunnable syncRunnable = new FakeCommitLogService(syncTimeMillis).new SyncRunnable(new FreeRunningClock());
        long pollStarted = 1;
        long now = pollStarted + (syncTimeMillis - 1);
        Assert.assertFalse(syncRunnable.maybeLogFlushLag(pollStarted, now));
        Assert.assertEquals(now - pollStarted, syncRunnable.getTotalSyncDuration());
    }

    /**
     * Mostly tests that {@link SyncRunnable#totalSyncDuration} is handled correctly
     */
    @Test
    public void maybeLogFlushLag_MultipleOperations()
    {
        long syncTimeMillis = 10;
        SyncRunnable syncRunnable = new FakeCommitLogService(syncTimeMillis).new SyncRunnable(new FreeRunningClock());

        long pollStarted = 1;
        long now = pollStarted + (syncTimeMillis - 1);

        int runCount = 12;
        for (int i = 1; i <= runCount; i++)
        {
            Assert.assertFalse(syncRunnable.maybeLogFlushLag(pollStarted, now));
            Assert.assertEquals(i * (now - pollStarted), syncRunnable.getTotalSyncDuration());
        }

        now = pollStarted + Integer.MAX_VALUE;
        Assert.assertTrue(syncRunnable.maybeLogFlushLag(pollStarted, now));
        Assert.assertEquals(now - pollStarted, syncRunnable.getTotalSyncDuration());
    }
}
