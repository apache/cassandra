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

package org.apache.cassandra.db.rows;

import java.nio.ByteBuffer;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.IDataSize;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.CassandraUInt;
import org.apache.cassandra.utils.memory.ByteBufferCloner;
import org.apache.cassandra.utils.memory.Cloner;

/**
 * The data of a cell, without its column and path.
 */
public interface CellData<V, C extends CellData<?, ?>> extends IDataSize, CellLivenessInfo
{
    public static final int NO_TTL = 0;
    public static final long NO_DELETION_TIME = Long.MAX_VALUE;
    public static final int NO_DELETION_TIME_UNSIGNED_INTEGER = CassandraUInt.MAX_VALUE_UINT;
    public static final long MAX_DELETION_TIME = CassandraUInt.MAX_VALUE_LONG - 2;
    public static final int MAX_DELETION_TIME_UNSIGNED_INTEGER = CassandraUInt.fromLong(MAX_DELETION_TIME);

    // Since C14227 we only support Uints, negative ldts (corruption, overflow) get converted to this
    public static final long INVALID_DELETION_TIME = CassandraUInt.MAX_VALUE_LONG - 1;
    // Do not use. Only for legacy ser/deser pre CASSANDRA-14227 and backwards compatible CAP policies
    public static final int MAX_DELETION_TIME_2038_LEGACY_CAP = Integer.MAX_VALUE - 1;

    public static int deletionTimeLongToUnsignedInteger(long deletionTime)
    {
        return deletionTime == NO_DELETION_TIME ? NO_DELETION_TIME_UNSIGNED_INTEGER : CassandraUInt.fromLong(deletionTime);
    }

    public static long deletionTimeUnsignedIntegerToLong(int deletionTimeUnsignedInteger)
    {
        return deletionTimeUnsignedInteger == NO_DELETION_TIME_UNSIGNED_INTEGER ? NO_DELETION_TIME : CassandraUInt.toLong(deletionTimeUnsignedInteger);
    }

    public static long getVersionedMaxDeletiontionTime()
    {
        if (DatabaseDescriptor.getStorageCompatibilityMode().disabled())
            // The whole cluster is 2016, we're out of the 2038/2106 mixed cluster scenario. Shortcut to avoid the 'minClusterVersion' volatile read
            return MAX_DELETION_TIME;
        else
            return MessagingService.instance().versions.minClusterVersion >= MessagingService.VERSION_50
                   ? MAX_DELETION_TIME
                   : MAX_DELETION_TIME_2038_LEGACY_CAP;
    }

    /**
     * Whether the cell is a counter cell or not.
     *
     * @return whether the cell is a counter cell or not.
     */
    boolean isCounterCell();

    V value();

    ValueAccessor<V> accessor();

    default int valueSize()
    {
        return accessor().size(value());
    }

    default ByteBuffer buffer()
    {
        return accessor().toBuffer(value());
    }

    /**
     * The cell timestamp.
     * <p>
     * @return the cell timestamp.
     */
    long timestamp();

    /**
     * The cell ttl.
     *
     * @return the cell ttl, or {@code NO_TTL} if the cell isn't an expiring one.
     */
    int ttl();

    /**
     * The cell local deletion time.
     *
     * @return the cell local deletion time, or {@code NO_DELETION_TIME} if the cell is neither
     * a tombstone nor an expiring one.
     */
    default long localDeletionTime()
    {
        return deletionTimeUnsignedIntegerToLong(localDeletionTimeAsUnsignedInt());
    }

    int localDeletionTimeAsUnsignedInt();

    /**
     * Whether the cell is a tombstone or not.
     *
     * @return whether the cell is a tombstone or not.
     */
    default boolean isTombstone()
    {
        return localDeletionTimeAsUnsignedInt() != NO_DELETION_TIME_UNSIGNED_INTEGER && ttl() == NO_TTL;
    }

    /**
     * Whether the cell is an expiring one or not.
     * <p>
     * Note that this only correspond to whether the cell liveness info
     * have a TTL or not, but doesn't tells whether the cell is already expired
     * or not. You should use {@link #isLive} for that latter information.
     *
     * @return whether the cell is an expiring one or not.
     */
    default boolean isExpiring()
    {
        return ttl() != NO_TTL;
    }

    /**
     * Whether the cell is live or not given the current time.
     *
     * @param nowInSec the current time in seconds. This is used to
     * decide if an expiring cell is expired or live.
     * @return whether the cell is live or not at {@code nowInSec}.
     */
    default boolean isLive(long nowInSec)
    {
        return localDeletionTimeAsUnsignedInt() == NO_DELETION_TIME_UNSIGNED_INTEGER
               || (ttl() != NO_TTL && nowInSec < localDeletionTime());
    }

    default boolean hasInvalidDeletions()
    {
        return ttl() < 0 || localDeletionTime() < 0 || (isExpiring() && localDeletionTimeAsUnsignedInt() == NO_DELETION_TIME_UNSIGNED_INTEGER);
    }

    long unsharedHeapSize();
    long unsharedHeapSizeExcludingData();

    default int dataSizeWithoutPath()
    {
        // NOTE: TypeSizes.sizeof(localDeletionTime()) - method call like this is not eliminated by JIT in case of a megamorphic call
        return TypeSizes.LONG_SIZE // timestamp()
               + TypeSizes.INT_SIZE // ttl()
               + TypeSizes.LONG_SIZE // localDeletionTime()
               + valueSize();
    }

    C withUpdatedTimestampAndLocalDeletionTime(long newTimestamp, long newLocalDeletionTime);

    C updateAllTimestamp(long newTimestamp);

    /**
     * Used to apply the same optimization as in {@link Cell.Serializer#deserialize} when
     * the column is not queried but eventhough it's used for digest calculation.
     * @return a cell with an empty buffer as value
     */
    C withSkippedValue();

    C clone(Cloner cloner);

    C clone(ByteBufferCloner cloner);

    C purge(DeletionPurger purger, long nowInSec);
    C purgeDataOlderThan(long timestamp);

    C markCounterLocalToBeCleared();

    /**
     * Returns a cell with the same column and path as this one, but with new data (timestamps and value).
     * Note that this can and will return a cell/CellData of a different type.
     */
    C withNewData(long timestamp, long localDeletionTime, int ttl, ByteBuffer value);

    /**
     * Binds a CellData object to the given column and cell path to turn it into a Cell.
     */
    Cell<V> toCell(ColumnMetadata column, CellPath cellPath);
}
