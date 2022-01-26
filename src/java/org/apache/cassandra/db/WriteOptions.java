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

package org.apache.cassandra.db;

import java.util.Collections;

import org.apache.cassandra.db.view.ViewManager;
import org.apache.cassandra.streaming.StreamOperation;

public enum WriteOptions
{
    /**
     * Default write options for client initiated requests.
     */
    DEFAULT(null, true, true, null, true),

    /**
     * Does not persist commit log but updates indexes and is droppable. Used for tests and
     * when commit log persistence is not required.
     */
    DEFAULT_WITHOUT_COMMITLOG(false, true, true, null, true),

    /**
     * Same as default, except it does not perform paired view replication since it's delayed write
     */
    FOR_READ_REPAIR(null, true, true, null, false),

    /**
     * Streaming with CDC always needs to write to commit log. It's also not droppable since it's not a client-initiated request.
     *
     * The difference from this to {@link this#FOR_STREAMING} is that we can safely skip updating views when updating
     * sstables through the commit log, since we can ensure view sstables will be streamed from other replicas.
     */
    FOR_BOOTSTRAP_STREAMING(true, true, false, false, false),

    /**
     * Streaming with CDC always needs to write to commit log. It's also not droppable since it's not a client-initiated request.
     */
    FOR_STREAMING(true, true, false, null, false),

    /**
     * Streaming with MVs does not need to write to commit log, since it can be recovered on crash. It's also not
     * droppable since it's not a client-initiated request.
     */
    FOR_STREAMING_WITH_MV(false, true, false, null, false),

    /**
     * Commit log replay obviously does not need to write to the commit log.
     * It's also not droppable since it's not a client-initiated request.
     */
    FOR_COMMITLOG_REPLAY(false, true, false, null, false),

    /**
     * Paxos commit must write to commit log, independent of keyspace settings.
     *
     * It can also used paired view replication, since we can ensure it's the first time the
     * mutation is written.
     */
    FOR_PAXOS_COMMIT(true, true, true, null, true),

    /**
     * View rebuild uses paired view replication since all nodes will build the view simultaneously
     */
    FOR_VIEW_BUILD(true, true, false, null, true),

    /**
     * For use on SecondaryIndexTest
     */
    SKIP_INDEXES_AND_COMMITLOG(false, false, false, null, false),

    /**
     * Batchlog replay uses default settings but does not perform paired view replications for view writes.
     */
    FOR_BATCH_REPLAY(null, true, true, null, false),

    /**
     * Batchlog replay but done by CNDB, so without using the commit log.
     */
    FOR_BATCH_CNDB_REPLAY(false, true, true, null, false),
    /**
     * Hint replay uses default settings but does not perform paired view replications for view writes.
     */
    FOR_HINT_REPLAY(null, true, true, null, false);


    /**
     * Disable index updates (used by CollationController "defragmenting")
     */
    public final boolean updateIndexes;
    /**
     * Should this update Materialized Views? Used by {@link this#FOR_BOOTSTRAP_STREAMING} to skip building
     * views when receiving from streaming.
     *
     * When unset, it will only perform view updates when {@link this#updateIndexes} is true and there are views
     * in the table being written to.
     */
    public final Boolean updateViews;
    /**
     * Throws WriteTimeoutException if write does not acquire lock within write_request_timeout_in_ms
     */
    public final boolean isDroppable;
    /**
     * Whether paired view replication should be used for view writes.
     *
     * This is only the case for {@link this#DEFAULT}, {@link this#DEFAULT_WITHOUT_COMMITLOG}
     * and {@link this#FOR_VIEW_BUILD}.
     */
    public final boolean usePairedViewReplication;
    /**
     * Whether the write should be appened to the commit log. A null value means default keyspace settings are used.
     */
    private final Boolean writeCommitLog;

    WriteOptions(Boolean writeCommitLog, boolean updateIndexes, boolean isDroppable, Boolean updateViews,
            boolean usePairedViewReplication)
    {
        this.writeCommitLog = writeCommitLog;
        this.updateIndexes = updateIndexes;
        this.isDroppable = isDroppable;
        this.usePairedViewReplication = usePairedViewReplication;
        this.updateViews = updateViews;
    }

    public static WriteOptions forStreaming(StreamOperation streamOperation, boolean cdcEnabled)
    {
        if (cdcEnabled)
            return streamOperation == StreamOperation.BOOTSTRAP
                   ? FOR_BOOTSTRAP_STREAMING
                   : FOR_STREAMING;

        return FOR_STREAMING_WITH_MV;
    }

    public boolean shouldWriteCommitLog(String keyspaceName)
    {
        return writeCommitLog != null
               ? writeCommitLog
               : Keyspace.open(keyspaceName).getMetadata().params.durableWrites;
    }

    public boolean requiresViewUpdate(ViewManager viewManager, Mutation mutation)
    {
        return updateViews != null ? updateViews :
               updateIndexes && viewManager.updatesAffectView(Collections.singleton(mutation), false);
    }
}
