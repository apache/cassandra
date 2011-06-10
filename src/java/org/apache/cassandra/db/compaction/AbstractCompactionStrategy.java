/**
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

package org.apache.cassandra.db.compaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.utils.DefaultInteger;
import org.apache.cassandra.utils.Pair;


/**
 * Pluggable compaction strategy determines how SSTables get merged.
 *
 * There are two main goals:
 *  - perform background compaction constantly as needed; this typically makes a tradeoff between
 *    i/o done by compaction, and merging done at read time.
 *  - perform a full (maximum possible) compaction if requested by the user
 */
public abstract class AbstractCompactionStrategy
{
    protected final ColumnFamilyStore cfs;
    protected final Map<String, String> options;

    protected AbstractCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        this.cfs = cfs;
        this.options = options;
    }

    /**
     * @return a list of compaction tasks that should run in the background to get the sstable
     * count down to desired parameters. Will not be null, but may be empty.
     * @param gcBefore throw away tombstones older than this
     */
    public abstract List<AbstractCompactionTask> getBackgroundTasks(final int gcBefore);

    /**
     * @return a list of compaction tasks that should be run to compact this columnfamilystore
     * as much as possible.  Will not be null, but may be empty.
     * @param gcBefore throw away tombstones older than this
     */
    public abstract List<AbstractCompactionTask> getMaximalTasks(final int gcBefore);

    /**
     * @return a compaction task corresponding to the requested sstables.
     * Will not be null. (Will throw if user requests an invalid compaction.)
     * @param gcBefore throw away tombstones older than this
     */
    public abstract AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, final int gcBefore);

    /**
     * @return the number of background tasks estimated to still be needed for this columnfamilystore
     */
    public abstract int getEstimatedRemainingTasks();
}
