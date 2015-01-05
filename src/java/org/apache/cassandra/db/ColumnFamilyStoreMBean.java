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

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;

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
     * force a major compaction of this column family
     */
    public void forceMajorCompaction() throws ExecutionException, InterruptedException;

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
    public void setCompactionStrategyClass(String className);

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
    public void setCompressionParameters(Map<String,String> opts);

    /**
     * Set new crc check chance
     */
    public void setCrcCheckChance(double crcCheckChance);

    public boolean isAutoCompactionDisabled();

    public long estimateKeys();


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

    /**
     * @return sstable count for each level. null unless leveled compaction is used.
     *         array index corresponds to level(int[0] is for level 0, ...).
     */
    public int[] getSSTableCountPerLevel();

    /**
     * Get the ratio of droppable tombstones to real columns (and non-droppable tombstones)
     * @return ratio
     */
    public double getDroppableTombstoneRatio();

    /**
     * @return the size of SSTables in "snapshots" subdirectory which aren't live anymore
     */
    public long trueSnapshotsSize();

    /**
     * begin sampling for a specific sampler with a given capacity.  The cardinality may
     * be larger than the capacity, but depending on the use case it may affect its accuracy
     */
    public void beginLocalSampling(String sampler, int capacity);

    /**
     * @return top <i>count</i> items for the sampler since beginLocalSampling was called
     */
    public CompositeData finishLocalSampling(String sampler, int count) throws OpenDataException;
}
