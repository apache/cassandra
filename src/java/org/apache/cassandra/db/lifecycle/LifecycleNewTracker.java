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


package org.apache.cassandra.db.lifecycle;

import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.SSTable;

/**
 * An interface for tracking new sstables added to a LifecycleTransaction, possibly through some proxy.
 */
public interface LifecycleNewTracker
{
    /**
     * Called when a new table is about to be created, so that this table can be tracked by a transaction.
     * @param table - the new table to be tracked
     */
    void trackNew(SSTable table);


    /**
     * Called when a new table is no longer required, so that this table can be untracked by a transaction.
     * @param table - the table to be untracked
     */
    void untrackNew(SSTable table);

    /**
     * @return the type of operation tracking these sstables
     */
    OperationType opType();
}
