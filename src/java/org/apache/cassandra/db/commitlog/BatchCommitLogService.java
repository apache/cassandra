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

import static org.apache.cassandra.config.CassandraRelevantProperties.BATCH_COMMIT_LOG_SYNC_INTERVAL;

class BatchCommitLogService extends AbstractCommitLogService
{
    /**
     * Batch mode does not rely on the sync thread in {@link AbstractCommitLogService} to wake up for triggering
     * the disk sync. Instead we trigger it explicitly in {@link #maybeWaitForSync(CommitLogSegment.Allocation)}.
     * This value here is largely irrelevant, but should high enough so the sync thread is not continually waking up.
     */
    private static final int POLL_TIME_MILLIS = BATCH_COMMIT_LOG_SYNC_INTERVAL.getInt();

    public BatchCommitLogService(CommitLog commitLog)
    {
        super(commitLog, "COMMIT-LOG-WRITER", POLL_TIME_MILLIS);
    }

    protected void maybeWaitForSync(CommitLogSegment.Allocation alloc)
    {
        // wait until record has been safely persisted to disk
        pending.incrementAndGet();
        requestExtraSync();
        alloc.awaitDiskSync(commitLog.metrics.waitingOnCommit);
        pending.decrementAndGet();
    }
}
