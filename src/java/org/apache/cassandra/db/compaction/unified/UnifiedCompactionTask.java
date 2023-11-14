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

package org.apache.cassandra.db.compaction.unified;

import java.util.Set;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.db.compaction.ShardManager;
import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;

/**
 * The sole purpose of this class is to currently create a {@link ShardedCompactionWriter}.
 */
public class UnifiedCompactionTask extends CompactionTask
{
    private final ShardManager shardManager;
    private final Controller controller;

    public UnifiedCompactionTask(ColumnFamilyStore cfs,
                                 UnifiedCompactionStrategy strategy,
                                 LifecycleTransaction txn,
                                 long gcBefore,
                                 ShardManager shardManager)
    {
        super(cfs, txn, gcBefore);
        this.controller = strategy.getController();
        this.shardManager = shardManager;
    }

    @Override
    public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs,
                                                          Directories directories,
                                                          LifecycleTransaction txn,
                                                          Set<SSTableReader> nonExpiredSSTables)
    {
        double density = shardManager.calculateCombinedDensity(nonExpiredSSTables);
        int numShards = controller.getNumShards(density * shardManager.shardSetCoverage());
        return new ShardedCompactionWriter(cfs, directories, txn, nonExpiredSSTables, keepOriginals, shardManager.boundaries(numShards));
    }
}