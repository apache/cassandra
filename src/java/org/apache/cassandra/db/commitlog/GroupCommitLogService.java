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

import org.apache.cassandra.config.DatabaseDescriptor;

/**
 * A commitlog service that will block returning an ACK back to the a coordinator/client
 * for a minimum amount of time as we wait until the the commit log segment is flushed.
 */
public class GroupCommitLogService extends AbstractCommitLogService
{
    public GroupCommitLogService(CommitLog commitLog)
    {
        super(commitLog, "GROUP-COMMIT-LOG-WRITER", (int) DatabaseDescriptor.getCommitLogSyncGroupWindow());
    }

    protected void maybeWaitForSync(CommitLogSegment.Allocation alloc)
    {
        // wait until record has been safely persisted to disk
        pending.incrementAndGet();
        // wait for commitlog_sync_group_window
        alloc.awaitDiskSync(commitLog.metrics.waitingOnCommit);
        pending.decrementAndGet();
    }
}

