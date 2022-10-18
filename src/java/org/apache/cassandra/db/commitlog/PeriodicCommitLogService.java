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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.MonotonicClock;

import static org.apache.cassandra.config.CassandraRelevantProperties.SYNC_LAG_FACTOR;

class PeriodicCommitLogService extends AbstractCommitLogService
{
    private static final long blockWhenSyncLagsNanos = (long) (TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getCommitLogSyncPeriod()) * SYNC_LAG_FACTOR.getDouble());

    public PeriodicCommitLogService(final CommitLog commitLog, MonotonicClock clock)
    {
        super(commitLog, "PERIODIC-COMMIT-LOG-SYNCER", DatabaseDescriptor.getCommitLogSyncPeriod(), clock,
              !(commitLog.configuration.useCompression() || commitLog.configuration.useEncryption()));
    }

    protected void maybeWaitForSync(CommitLogSegment.Allocation alloc)
    {
        long expectedSyncTime = clock.now() - blockWhenSyncLagsNanos;
        if (lastSyncedAt - expectedSyncTime < 0)
        {
            pending.incrementAndGet();
            awaitSyncAt(expectedSyncTime, commitLog.metrics.waitingOnCommit.time());
            pending.decrementAndGet();
        }
    }
}
