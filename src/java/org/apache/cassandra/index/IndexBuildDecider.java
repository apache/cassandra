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

package org.apache.cassandra.index;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
import org.apache.cassandra.utils.FBUtilities;

public interface IndexBuildDecider
{
    IndexBuildDecider instance = CassandraRelevantProperties.CUSTOM_INDEX_BUILD_DECIDER.getString() == null ?
                                 new IndexBuildDecider() {} :
                                 FBUtilities.construct(CassandraRelevantProperties.CUSTOM_INDEX_BUILD_DECIDER.getString(), "custom index build decider");

    enum Decision
    {
        /**
         * index will be built synchronously
         */
        SYNC,
        /**
         * index will be built asynchronously
         */
        ASYNC,
        /**
         * index build will be skipped
         */
        NONE;

        public boolean skipped()
        {
            return this == NONE;
        }
    }

    /**
     * CNDB overrides this method to skip building indexes for sstables.
     *
     * @return decision for index initial build {@link Index#getInitializationTask()}
     */
    default Decision onInitialBuild()
    {
        return Decision.SYNC;
    }

    /**
     * CNDB overrides this method to mark index queryable if there is no sstables on writer.
     *
     * @return true if index should be queryable after {@link Index#getInitializationTask()}
     */
    default boolean isIndexQueryableAfterInitialBuild(ColumnFamilyStore cfs)
    {
        return true;
    }

    /**
     * CNDB overrides this method to skip building indexes on writer when sstables are reloaded from remote storage
     *
     * @return decision for index initial build when receiving {@link SSTableListChangedNotification}
     */
    default Decision onSSTableListChanged(SSTableListChangedNotification notification)
    {
        return notification.operationType.equals(OperationType.REMOTE_RELOAD) ? Decision.ASYNC : Decision.NONE;
    }

    /**
     * CNDB overrides this method to skip building indexes on writer when sstables are reloaded from remote storage
     *
     * @return decision for index initial build when receiving {@link SSTableAddedNotification}
     */
    default Decision onSSTableAdded(SSTableAddedNotification notification)
    {
        // SSTables associated to a memtable come from a flush, so their contents have already been indexed
        if (notification.memtable().isPresent())
            return Decision.NONE;

        return notification.operationType == OperationType.REMOTE_RELOAD ? Decision.ASYNC : Decision.SYNC;
    }
}
