package org.apache.cassandra.db.compaction;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import com.google.common.collect.Iterables;

import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableWriter;

public class LeveledCompactionTask extends CompactionTask
{
    private final int sstableSizeInMB;

    private final CountDownLatch latch = new CountDownLatch(1);

    public LeveledCompactionTask(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, final int gcBefore, int sstableSizeInMB)
    {
        super(cfs, sstables, gcBefore);
        this.sstableSizeInMB = sstableSizeInMB;
    }

    @Override
    public int execute(CompactionManager.CompactionExecutorStatsCollector collector) throws IOException
    {
        try
        {
            int n = super.execute(collector);
            return n;
        }
        finally
        {
            latch.countDown();
        }
    }

    public boolean isDone()
    {
        return latch.getCount() == 0;
    }

    @Override
    protected boolean newSSTableSegmentThresholdReached(SSTableWriter writer, long position)
    {
        return position > sstableSizeInMB * 1024L * 1024L;
    }

    @Override
    protected boolean isCompactionInteresting(Set<SSTableReader> toCompact)
    {
        return true;
    }

    @Override
    protected boolean partialCompactionsAcceptable()
    {
        return false;
    }

    @Override
    protected void cancel()
    {
        latch.countDown();
    }
}
