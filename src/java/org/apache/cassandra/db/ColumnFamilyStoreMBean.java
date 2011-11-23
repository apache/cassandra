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

package org.apache.cassandra.db;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.config.ConfigurationException;

/**
 * The MBean interface for ColumnFamilyStore
 */
public interface ColumnFamilyStoreMBean
{
    /**
     * @return the name of the column family
     */
    public String getColumnFamilyName();
    
    /**
     * Returns the total amount of data stored in the memtable, including
     * column related overhead.
     * 
     * @return The size in bytes.
     */
    public long getMemtableDataSize();
    
    /**
     * Returns the total number of columns present in the memtable.
     * 
     * @return The number of columns.
     */
    public long getMemtableColumnsCount();
    
    /**
     * Returns the number of times that a flush has resulted in the
     * memtable being switched out.
     *
     * @return the number of memtable switches
     */
    public int getMemtableSwitchCount();

    /**
     * Triggers an immediate memtable flush.
     */
    public Object forceFlush() throws IOException;

    /**
     * @return a histogram of the number of sstable data files accessed per read: reading this property resets it
     */
    public long[] getRecentSSTablesPerReadHistogram();

    /**
     * @return a histogram of the number of sstable data files accessed per read
     */
    public long[] getSSTablesPerReadHistogram();

    /**
     * @return the number of read operations on this column family
     */
    public long getReadCount();

    /**
     * @return total read latency (divide by getReadCount() for average)
     */
    public long getTotalReadLatencyMicros();

    /**
     * @return an array representing the latency histogram
     */
    public long[] getLifetimeReadLatencyHistogramMicros();

    /**
     * @return an array representing the latency histogram
     */
    public long[] getRecentReadLatencyHistogramMicros();

    /**
     * @return average latency per read operation since the last call
     */
    public double getRecentReadLatencyMicros();

    /**
     * @return the number of write operations on this column family
     */
    public long getWriteCount();
    
    /**
     * @return total write latency (divide by getReadCount() for average)
     */
    public long getTotalWriteLatencyMicros();

    /**
     * @return an array representing the latency histogram
     */
    public long[] getLifetimeWriteLatencyHistogramMicros();

    /**
     * @return an array representing the latency histogram
     */
    public long[] getRecentWriteLatencyHistogramMicros();

    /**
     * @return average latency per write operation since the last call
     */
    public double getRecentWriteLatencyMicros();

    /**
     * @return the estimated number of tasks pending for this column family
     */
    public int getPendingTasks();

    /**
     * @return the number of SSTables on disk for this CF
     */
    public int getLiveSSTableCount();

    /**
     * @return disk space used by SSTables belonging to this CF
     */
    public long getLiveDiskSpaceUsed();

    /**
     * @return total disk space used by SSTables belonging to this CF, including obsolete ones waiting to be GC'd
     */
    public long getTotalDiskSpaceUsed();

    /**
     * force a major compaction of this column family
     */
    public void forceMajorCompaction() throws ExecutionException, InterruptedException;

    /**
     * invalidate the key cache; for use after invalidating row cache
     */
    public void invalidateKeyCache();

    /**
     * invalidate the row cache; for use after bulk loading via BinaryMemtable
     */
    public void invalidateRowCache();


    /**
     * return the size of the smallest compacted row
     * @return
     */
    public long getMinRowSize();

    /**
     * return the size of the largest compacted row
     * @return
     */
    public long getMaxRowSize();

    /**
     * return the mean size of the rows compacted
     * @return
     */
    public long getMeanRowSize();

    public long getBloomFilterFalsePositives();

    public long getRecentBloomFilterFalsePositives();

    public double getBloomFilterFalseRatio();

    public double getRecentBloomFilterFalseRatio();

    public long getBloomFilterDiskSpaceUsed();

    /**
     * Gets the minimum number of sstables in queue before compaction kicks off
     */
    public int getMinimumCompactionThreshold();

    /**
     * Sets the minimum number of sstables in queue before compaction kicks off
     */
    public void setMinimumCompactionThreshold(int threshold);

    /**
     * Gets the maximum number of sstables in queue before compaction kicks off
     */
    public int getMaximumCompactionThreshold();

    /**
     * Sets the maximum number of sstables in queue before compaction kicks off
     */
    public void setMaximumCompactionThreshold(int threshold);

    /**
     * Disable automatic compaction.
     */
    public void disableAutoCompaction();

    public long estimateKeys();

    public long[] getEstimatedRowSizeHistogram();
    public long[] getEstimatedColumnCountHistogram();
    public double getCompressionRatio();

    /**
     * Returns a list of the names of the built column indexes for current store
     * @return list of the index names
     */
    public List<String> getBuiltIndexes();

    public int getRowCacheSavePeriodInSeconds();
    public void setRowCacheSavePeriodInSeconds(int rcspis);

    public int getKeyCacheSavePeriodInSeconds();
    public void setKeyCacheSavePeriodInSeconds(int kcspis);

    public int getRowCacheKeysToSave();
    public void setRowCacheKeysToSave(int keysToSave);

    /**
     * Scan through Keyspace/ColumnFamily's data directory
     * determine which SSTables should be loaded and load them
     */
    public void loadNewSSTables();

    /**
     * @return the number of SSTables in L0.  Always return 0 if Leveled compaction is not enabled.
     */
    public int getUnleveledSSTables();
}
