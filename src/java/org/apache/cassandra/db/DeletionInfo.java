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

import java.util.Iterator;

import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.utils.memory.ByteBufferCloner;

/**
 * A combination of a top-level (partition) tombstone and range tombstones describing the deletions
 * within a partition.
 * <p>
 * Note that in practice {@link MutableDeletionInfo} is the only concrete implementation of this, however
 * different parts of the code will return either {@code DeletionInfo} or {@code MutableDeletionInfo} based
 * on whether it can/should be mutated or not.
 * <p>
 * <b>Warning:</b> do not ever cast a {@code DeletionInfo} into a {@code MutableDeletionInfo} to mutate it!!!
 * TODO: it would be safer to have 2 actual implementation of DeletionInfo, one mutable and one that isn't (I'm
 * just lazy right this minute).
 */
public interface DeletionInfo extends IMeasurableMemory
{
    // Note that while MutableDeletionInfo.live() is mutable, we expose it here as a non-mutable DeletionInfo so sharing is fine.
    public static final DeletionInfo LIVE = MutableDeletionInfo.live();

    /**
     * Returns whether this DeletionInfo is live, that is deletes no columns.
     */
    public boolean isLive();

    public DeletionTime getPartitionDeletion();

    // Use sparingly, not the most efficient thing
    public Iterator<RangeTombstone> rangeIterator(boolean reversed);

    public Iterator<RangeTombstone> rangeIterator(Slice slice, boolean reversed);

    public RangeTombstone rangeCovering(Clustering<?> name);

    public void collectStats(EncodingStats.Collector collector);

    public int dataSize();

    public boolean hasRanges();

    public int rangeCount();

    public long maxTimestamp();

    /**
     * Whether this deletion info may modify the provided one if added to it.
     */
    public boolean mayModify(DeletionInfo delInfo);

    public MutableDeletionInfo mutableCopy();

    public DeletionInfo clone(ByteBufferCloner cloner);
}
