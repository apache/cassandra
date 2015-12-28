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
package org.apache.cassandra.db.partitions;

import org.apache.cassandra.cache.IRowCacheEntry;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.ISerializer;

/**
 * A partition stored in the partition cache.
 *
 * Note that in practice, the only implementation of this is {@link CachedBTreePartition},
 * we keep this interface mainly to make it clear what we need from partition in the cache
 * (that we don't otherwise)
 */
public interface CachedPartition extends Partition, IRowCacheEntry
{
    public static final ISerializer<CachedPartition> cacheSerializer = new CachedBTreePartition.Serializer();

    /**
     * The number of {@code Row} objects in this cached partition.
     *
     * Please note that this is <b>not</b> the number of <em>live</em> rows since
     * some of the row may only contains deleted (or expired) information.
     *
     * @return the number of row in the partition.
     */
    public int rowCount();

    /**
     * The number of rows that were live at the time the partition was cached.
     *
     * See {@link org.apache.cassandra.db.ColumnFamilyStore#isFilterFullyCoveredBy} to see why we need this.
     *
     * @return the number of rows in this partition that were live at the time the
     * partition was cached (this can be different from the number of live rows now
     * due to expiring cells).
     */
    public int cachedLiveRows();

    /**
     * The number of rows in this cached partition that have at least one non-expiring
     * non-deleted cell.
     *
     * Note that this is generally not a very meaningful number, but this is used by
     * {@link org.apache.cassandra.db.filter.DataLimits#hasEnoughLiveData} as an optimization.
     *
     * @return the number of row that have at least one non-expiring non-deleted cell.
     */
    public int rowsWithNonExpiringCells();

    /**
     * The last row in this cached partition (in order words, the row with the
     * biggest clustering that the partition contains).
     *
     * @return the last row of the partition, or {@code null} if the partition is empty.
     */
    public Row lastRow();

    /**
     * The number of {@code cell} objects that are not tombstone in this cached partition.
     *
     * Please note that this is <b>not</b> the number of <em>live</em> cells since
     * some of the cells might be expired.
     *
     * @return the number of non tombstone cells in the partition.
     */
    public int nonTombstoneCellCount();

    /**
     * The number of cells in this cached partition that are neither tombstone nor expiring.
     *
     * Note that this is generally not a very meaningful number, but this is used by
     * {@link org.apache.cassandra.db.filter.DataLimits#hasEnoughLiveData} as an optimization.
     *
     * @return the number of cells that are neither tombstones nor expiring.
     */
    public int nonExpiringLiveCells();
}
