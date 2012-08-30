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

import java.util.List;
import java.util.Map;
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
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#memtableDataSize
     * @return The size in bytes.
     */
    @Deprecated
    public long getMemtableDataSize();

    /**
     * Returns the total number of columns present in the memtable.
     *
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#memtableColumnsCount
     * @return The number of columns.
     */
    @Deprecated
    public long getMemtableColumnsCount();

    /**
     * Returns the number of times that a flush has resulted in the
     * memtable being switched out.
     *
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#memtableSwitchCount
     * @return the number of memtable switches
     */
    @Deprecated
    public int getMemtableSwitchCount();

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#recentSSTablesPerReadHistogram
     * @return a histogram of the number of sstable data files accessed per read: reading this property resets it
     */
    @Deprecated
    public long[] getRecentSSTablesPerReadHistogram();

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#sstablesPerReadHistogram
     * @return a histogram of the number of sstable data files accessed per read
     */
    @Deprecated
    public long[] getSSTablesPerReadHistogram();

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#readLatency
     * @return the number of read operations on this column family
     */
    @Deprecated
    public long getReadCount();

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#readLatency
     * @return total read latency (divide by getReadCount() for average)
     */
    @Deprecated
    public long getTotalReadLatencyMicros();

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#readLatency
     * @return an array representing the latency histogram
     */
    @Deprecated
    public long[] getLifetimeReadLatencyHistogramMicros();

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#readLatency
     * @return an array representing the latency histogram
     */
    @Deprecated
    public long[] getRecentReadLatencyHistogramMicros();

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#readLatency
     * @return average latency per read operation since the last call
     */
    @Deprecated
    public double getRecentReadLatencyMicros();

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#writeLatency
     * @return the number of write operations on this column family
     */
    @Deprecated
    public long getWriteCount();

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#writeLatency
     * @return total write latency (divide by getReadCount() for average)
     */
    @Deprecated
    public long getTotalWriteLatencyMicros();

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#writeLatency
     * @return an array representing the latency histogram
     */
    @Deprecated
    public long[] getLifetimeWriteLatencyHistogramMicros();

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#writeLatency
     * @return an array representing the latency histogram
     */
    @Deprecated
    public long[] getRecentWriteLatencyHistogramMicros();

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#writeLatency
     * @return average latency per write operation since the last call
     */
    @Deprecated
    public double getRecentWriteLatencyMicros();

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#pendingTasks
     * @return the estimated number of tasks pending for this column family
     */
    @Deprecated
    public int getPendingTasks();

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#liveSSTableCount
     * @return the number of SSTables on disk for this CF
     */
    @Deprecated
    public int getLiveSSTableCount();

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#liveDiskSpaceUsed
     * @return disk space used by SSTables belonging to this CF
     */
    @Deprecated
    public long getLiveDiskSpaceUsed();

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#totalDiskSpaceUsed
     * @return total disk space used by SSTables belonging to this CF, including obsolete ones waiting to be GC'd
     */
    @Deprecated
    public long getTotalDiskSpaceUsed();

    /**
     * force a major compaction of this column family
     */
    public void forceMajorCompaction() throws ExecutionException, InterruptedException;

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#minRowSize
     * @return the size of the smallest compacted row
     */
    @Deprecated
    public long getMinRowSize();

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#maxRowSize
     * @return the size of the largest compacted row
     */
    @Deprecated
    public long getMaxRowSize();

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#meanRowSize
     * @return the size of the smallest compacted row
     */
    @Deprecated
    public long getMeanRowSize();

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#bloomFilterFalsePositives
     */
    @Deprecated
    public long getBloomFilterFalsePositives();

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#recentBloomFilterFalsePositives
     */
    @Deprecated
    public long getRecentBloomFilterFalsePositives();

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#bloomFilterFalseRatio
     */
    @Deprecated
    public double getBloomFilterFalseRatio();

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#recentBloomFilterFalseRatio
     */
    @Deprecated
    public double getRecentBloomFilterFalseRatio();

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#bloomFilterDiskSpaceUsed
     */
    @Deprecated
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
     * Sets the maximum and maximum number of SSTables in queue before compaction kicks off
     */
    public void setCompactionThresholds(int minThreshold, int maxThreshold);

    /**
     * Sets the maximum number of sstables in queue before compaction kicks off
     */
    public void setMaximumCompactionThreshold(int threshold);

    /**
     * Sets the compaction strategy by class name
     * @param className the name of the compaction strategy class
     */
    public void setCompactionStrategyClass(String className) throws ConfigurationException;

    /**
     * Gets the compaction strategy class name
     */
    public String getCompactionStrategyClass();

    /**
     * Get the compression parameters
     */
    public Map<String,String> getCompressionParameters();

    /**
     * Set the compression parameters
     * @param opts map of string names to values
     */
    public void setCompressionParameters(Map<String,String> opts) throws ConfigurationException;

    /**
     * Disable automatic compaction.
     */
    public void disableAutoCompaction();

    public long estimateKeys();

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#estimatedRowSizeHistogram
     */
    @Deprecated
    public long[] getEstimatedRowSizeHistogram();
    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#estimatedColumnCountHistogram
     */
    @Deprecated
    public long[] getEstimatedColumnCountHistogram();
    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#compressionRatio
     */
    @Deprecated
    public double getCompressionRatio();

    /**
     * Returns a list of the names of the built column indexes for current store
     * @return list of the index names
     */
    public List<String> getBuiltIndexes();

    /**
     * Returns a list of filenames that contain the given key on this node
     * @param key
     * @return list of filenames containing the key
     */
    public List<String> getSSTablesForKey(String key);

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
