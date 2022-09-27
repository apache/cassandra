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

package org.apache.cassandra.db.compaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;

public class ActiveCompactions implements ActiveCompactionsTracker
{
    // a synchronized identity set of running tasks to their compaction info
    private final Set<CompactionInfo.Holder> compactions = Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<>()));

    public List<CompactionInfo.Holder> getCompactions()
    {
        return new ArrayList<>(compactions);
    }

    public void beginCompaction(CompactionInfo.Holder ci)
    {
        compactions.add(ci);
    }

    public void finishCompaction(CompactionInfo.Holder ci)
    {
        compactions.remove(ci);
        CompactionManager.instance.getMetrics().bytesCompacted.inc(ci.getCompactionInfo().getTotal());
        CompactionManager.instance.getMetrics().totalCompactionsCompleted.mark();
    }

    /**
     * Get the estimated number of bytes remaining to write per sstable directory
     */
    public Map<File, Long> estimatedRemainingWriteBytes()
    {
        synchronized (compactions)
        {
            Map<File, Long> writeBytesPerSSTableDir = new HashMap<>();
            for (CompactionInfo.Holder holder : compactions)
            {
                CompactionInfo compactionInfo = holder.getCompactionInfo();
                List<File> directories = compactionInfo.getTargetDirectories();
                if (directories == null || directories.isEmpty())
                    continue;
                long remainingWriteBytesPerDataDir = compactionInfo.estimatedRemainingWriteBytes() / directories.size();
                for (File directory : directories)
                    writeBytesPerSSTableDir.merge(directory, remainingWriteBytesPerDataDir, Long::sum);
            }
            return writeBytesPerSSTableDir;
        }
    }

    /**
     * Iterates over the active compactions and tries to find CompactionInfos with the given compactionType for the given sstable
     *
     * Number of entries in compactions should be small (< 10) but avoid calling in any time-sensitive context
     */
    public Collection<CompactionInfo> getCompactionsForSSTable(SSTableReader sstable, OperationType compactionType)
    {
        List<CompactionInfo> toReturn = null;
        synchronized (compactions)
        {
            for (CompactionInfo.Holder holder : compactions)
            {
                CompactionInfo compactionInfo = holder.getCompactionInfo();
                if (compactionInfo.getSSTables().contains(sstable) && compactionInfo.getTaskType() == compactionType)
                {
                    if (toReturn == null)
                        toReturn = new ArrayList<>();
                    toReturn.add(compactionInfo);
                }
            }
        }
        return toReturn;
    }
}
