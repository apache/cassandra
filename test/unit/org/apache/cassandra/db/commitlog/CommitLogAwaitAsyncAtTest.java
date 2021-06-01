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

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FreeRunningClock;
import org.apache.cassandra.utils.MonotonicClock;
import org.mockito.Mockito;

public class CommitLogAwaitAsyncAtTest
{
    @BeforeClass
    public static void beforeClass() throws ConfigurationException
    {
        DatabaseDescriptor.daemonInitialization();
    }

    /**
     * syncTime (awaitSyncAt param) is in the past, now value overflowed, awaitSyncAt should not block,
     * no clock advance calls.
     */
    @Test
    public void notBlockIfSyncTimeIsInPast() throws InterruptedException
    {
        testResumingAwaitSyncAt(Long.MIN_VALUE + 10,
                              Long.MAX_VALUE - 10,
                                0);
    }

    /**
     * syncTime (awaitSyncAt param) is in the future, awaitSyncAt should block, unblocking is caused by the flush
     */
    @Test
    public void flushShouldUnblockAwaitSync() throws InterruptedException
    {
        testResumingAwaitSyncAt(Long.MAX_VALUE - 10,
                              Long.MAX_VALUE - 5,
                                1000);
    }

    /**
     * Creates a CommitLogService instance and a new thread that calls awaitSyncAt. Awaits for at most a minute
     * for the call to return.
     * Uses artificial clock to progress through the commit flush. One clock advance is performed after the service and
     * the thread are started.
     *
     * @param nowNanos test start time nanoseconds
     * @param syncAtNanos awaitSyncAt parameter nanoseconds
     * @param advanceMillis clock step in milliseconds
     */
    private void testResumingAwaitSyncAt(long nowNanos, long syncAtNanos, long advanceMillis) throws InterruptedException
    {
        FreeRunningClock clock = new FreeRunningClock(nowNanos);
        AbstractCommitLogService service = getCommitLogService(clock);

        Thread awaitForSync = new Thread(CommitLogAwaitAsyncAtTest.class.getSimpleName() + " commit log waiting thread")
        {
            @Override
            public void run()
            {
                service.awaitSyncAt(syncAtNanos, null);
            }
        };
        awaitForSync.start();

        service.start();

        // move clock once with advance millis
        clock.advance(advanceMillis, TimeUnit.MILLISECONDS);

        // wait at most 1 minute for awaitSyncAt to unblock
        awaitForSync.join(60 * 1000);
        if (awaitForSync.isAlive())
            Assert.fail("awaitSyncAt should be unblocked by now, check commit log code for bugs in nanoseconds" +
                        "comparisons");
    }

    private AbstractCommitLogService getCommitLogService(MonotonicClock clock) {
        CommitLog commitLog = Mockito.mock(CommitLog.class);
        return new AbstractCommitLogService(commitLog, "testService", 100, clock)
        {
            @Override
            protected void maybeWaitForSync(CommitLogSegment.Allocation alloc)
            {
            }
        };
    }
}
