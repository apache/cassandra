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

import java.io.IOException;
import java.nio.ByteBuffer;

import com.google.common.base.Objects;

import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.CassandraUInt;
import org.apache.cassandra.utils.ObjectSizes;

import static java.lang.Math.min;

/**
 * Information on deletion of a storage engine object.
 */
public class DeletionTime implements Comparable<DeletionTime>, IMeasurableMemory
{
    public static final long EMPTY_SIZE = ObjectSizes.measure(new DeletionTime(0, 0));

    /**
     * A special DeletionTime that signifies that there is no top-level (row) tombstone.
     */
    public static final DeletionTime LIVE = new DeletionTime(Long.MIN_VALUE, Long.MAX_VALUE);

    private static final Serializer serializer = new Serializer();
    private static final Serializer legacySerializer = new LegacySerializer();

    private final long markedForDeleteAt;
    final int localDeletionTimeUnsignedInteger;

    public static DeletionTime build(long markedForDeleteAt, long localDeletionTime)
    {
        // Negative ldts can only be a result of a corruption or when scrubbing legacy sstables with overflown int ldts
        return localDeletionTime < 0 || localDeletionTime > Cell.MAX_DELETION_TIME
                    ? new InvalidDeletionTime(markedForDeleteAt)
                    : new DeletionTime(markedForDeleteAt, localDeletionTime);
    }

    // Do not use. This is a perf optimization where some data structures known to hold valid uints are allowed to use it.
    // You should use 'build' instead to not workaround validations, corruption detections, etc
    static DeletionTime buildUnsafeWithUnsignedInteger(long markedForDeleteAt, int localDeletionTimeUnsignedInteger)
    {
        return CassandraUInt.compare(Cell.MAX_DELETION_TIME_UNSIGNED_INTEGER, localDeletionTimeUnsignedInteger) < 0
                ? new InvalidDeletionTime(markedForDeleteAt)
                : new DeletionTime(markedForDeleteAt, localDeletionTimeUnsignedInteger);
    }

    private DeletionTime(long markedForDeleteAt, long localDeletionTime)
    {
        this.markedForDeleteAt = markedForDeleteAt;
        this.localDeletionTimeUnsignedInteger = Cell.deletionTimeLongToUnsignedInteger(localDeletionTime);
    }

    private DeletionTime(long markedForDeleteAt, int localDeletionTimeUnsignedInteger)
    {
        this.markedForDeleteAt = markedForDeleteAt;
        this.localDeletionTimeUnsignedInteger = localDeletionTimeUnsignedInteger;
    }

    /**
     * A timestamp (typically in microseconds since the unix epoch, although this is not enforced) after which
     * data should be considered deleted. If set to Long.MIN_VALUE, this implies that the data has not been marked
     * for deletion at all.
     */
    public long markedForDeleteAt()
    {
        return markedForDeleteAt;
    }

    /**
     * The local server timestamp, in seconds since the unix epoch, at which this tombstone was created. This is
     * only used for purposes of purging the tombstone after gc_grace_seconds have elapsed.
     */
    public long localDeletionTime()
    {
        return Cell.deletionTimeUnsignedIntegerToLong(localDeletionTimeUnsignedInteger);
    }

    /**
     * Returns whether this DeletionTime is live, that is deletes no columns.
     */
    public boolean isLive()
    {
        return markedForDeleteAt() == Long.MIN_VALUE && localDeletionTime() == Long.MAX_VALUE;
    }

    public void digest(Digest digest)
    {
        // localDeletionTime is basically a metadata of the deletion time that tells us when it's ok to purge it.
        // It's thus intrinsically a local information and shouldn't be part of the digest (which exists for
        // cross-nodes comparisons).
        digest.updateWithLong(markedForDeleteAt());
    }

    /**
     * check if this deletion time is valid - localDeletionTime can never be negative
     * @return true if it is valid
     */
    public boolean validate()
    {
        return true;
    }

    @Override
    public boolean equals(Object o)
    {
        if(!(o instanceof DeletionTime))
            return false;
        DeletionTime that = (DeletionTime)o;
        return markedForDeleteAt() == that.markedForDeleteAt() && localDeletionTime() == that.localDeletionTime();
    }

    @Override
    public final int hashCode()
    {
        return Objects.hashCode(markedForDeleteAt(), localDeletionTime());
    }

    @Override
    public String toString()
    {
        return String.format("deletedAt=%d, localDeletion=%d", markedForDeleteAt(), localDeletionTime());
    }

    public int compareTo(DeletionTime dt)
    {
        if (markedForDeleteAt() < dt.markedForDeleteAt())
            return -1;
        else if (markedForDeleteAt() > dt.markedForDeleteAt())
            return 1;
        else return CassandraUInt.compare(localDeletionTimeUnsignedInteger, dt.localDeletionTimeUnsignedInteger);
    }

    public boolean supersedes(DeletionTime dt)
    {
        return markedForDeleteAt() > dt.markedForDeleteAt() || (markedForDeleteAt() == dt.markedForDeleteAt() && localDeletionTime() > dt.localDeletionTime());
    }

    public boolean deletes(LivenessInfo info)
    {
        return deletes(info.timestamp());
    }

    public boolean deletes(Cell<?> cell)
    {
        return deletes(cell.timestamp());
    }

    public boolean deletes(long timestamp)
    {
        return timestamp <= markedForDeleteAt();
    }

    public int dataSize()
    {
        return 12;
    }

    public long unsharedHeapSize()
    {
        if (this == LIVE)
            return 0;

        return EMPTY_SIZE;
    }
    
    public static Serializer getSerializer(Version version)
    {
        if (version.hasUIntDeletionTime())
            return serializer;
        else
            return legacySerializer;
    }

    // Serializer for Usigned Integer ldt
    public static class Serializer implements ISerializer<DeletionTime>
    {
        public void serialize(DeletionTime delTime, DataOutputPlus out) throws IOException
        {
            out.writeInt(delTime.localDeletionTimeUnsignedInteger);
            out.writeLong(delTime.markedForDeleteAt());
        }

        public DeletionTime deserialize(DataInputPlus in) throws IOException
        {
            int localDeletionTimeUnsignedInteger = in.readInt();
            long mfda = in.readLong();
            return mfda == Long.MIN_VALUE && localDeletionTimeUnsignedInteger == Cell.NO_DELETION_TIME_UNSIGNED_INTEGER
                 ? LIVE
                 : new DeletionTime(mfda, localDeletionTimeUnsignedInteger);
        }

        public DeletionTime deserialize(ByteBuffer buf, int offset)
        {
            int localDeletionTimeUnsignedInteger = buf.getInt(offset);
            long mfda = buf.getLong(offset + 4);
            return mfda == Long.MIN_VALUE && localDeletionTimeUnsignedInteger == Cell.NO_DELETION_TIME_UNSIGNED_INTEGER
                   ? LIVE
                   : new DeletionTime(mfda, localDeletionTimeUnsignedInteger);
        }

        public void skip(DataInputPlus in) throws IOException
        {
            in.skipBytesFully(4 + 8);
        }

        public long serializedSize(DeletionTime delTime)
        {
            return TypeSizes.sizeof(Integer.MAX_VALUE)
                   + TypeSizes.sizeof(delTime.markedForDeleteAt());
        }
    }

    // Serializer for Int TTL/localDeletionTime for legacy versions
    public static class LegacySerializer extends Serializer
    {
        public void serialize(DeletionTime delTime, DataOutputPlus out) throws IOException
        {
            int ldt = delTime.localDeletionTime() == Cell.NO_DELETION_TIME ? Integer.MAX_VALUE : (int) min(delTime.localDeletionTime(), (long)Integer.MAX_VALUE - 1);
            out.writeInt(ldt);
            out.writeLong(delTime.markedForDeleteAt);
        }

        public DeletionTime deserialize(DataInputPlus in) throws IOException
        {
            int ldt = in.readInt();
            long mfda = in.readLong();
            return mfda == Long.MIN_VALUE && ldt == Integer.MAX_VALUE
                 ? LIVE
                 : DeletionTime.build(mfda, ldt);
        }

        public DeletionTime deserialize(ByteBuffer buf, int offset)
        {
            int ldt = buf.getInt(offset);
            long mfda = buf.getLong(offset + 4);
            return mfda == Long.MIN_VALUE && ldt == Integer.MAX_VALUE
                   ? LIVE
                   : new DeletionTime(mfda, ldt);
        }

        public void skip(DataInputPlus in) throws IOException
        {
            in.skipBytesFully(4 + 8);
        }

        public long serializedSize(DeletionTime delTime)
        {
            return TypeSizes.sizeof(Integer.MAX_VALUE)
                   + TypeSizes.sizeof(delTime.markedForDeleteAt());
        }
    }

    // When scrubbing legacy sstables (overflown) or upon sstable corruption we could have negative ldts
    public static class InvalidDeletionTime extends DeletionTime
    {
        private InvalidDeletionTime(long markedForDeleteAt)
        {
            // We're calling the super constructor with int ldt to force invalid values through
            // and workaround any validation
            super(markedForDeleteAt, Cell.INVALID_DELETION_TIME);
        }

        @Override
        public long localDeletionTime()
        {
            return Cell.INVALID_DELETION_TIME;
        }

        @Override
        public boolean validate()
        {
            return false;
        }
    }
}
