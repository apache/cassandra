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
package org.apache.cassandra.hints;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.primitives.Ints;

import javax.annotation.Nullable;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_MAX_HINT_TTL;
import static org.apache.cassandra.db.TypeSizes.sizeof;
import static org.apache.cassandra.db.TypeSizes.sizeofUnsignedVInt;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

/**
 * Encapsulates the hinted mutation, its creation time, and the gc grace seconds param for each table involved.
 *
 * - Why do we need to track hint creation time?
 * - We must exclude updates for tables that have been truncated after hint's creation, otherwise the result is data corruption.
 *
 * - Why do we need to track gc grace seconds?
 * - Hints can stay in storage for a while before being applied, and without recording gc grace seconds (+ creation time),
 *   if we apply the mutation blindly, we risk resurrecting a deleted value, a tombstone for which had been already
 *   compacted away while the hint was in storage.
 *
 *   We also look at the smallest current value of the gcgs param for each affected table when applying the hint, and use
 *   creation time + min(recorded gc gs, current gcgs + current gc grace) as the overall hint expiration time.
 *   This allows now to safely reduce gc gs on tables without worrying that an applied old hint might resurrect any data.
 */
public final class Hint
{
    public static final Serializer serializer = new Serializer();
    static final int maxHintTTL = CASSANDRA_MAX_HINT_TTL.getInt();

    final Mutation mutation;
    final long creationTime;  // time of hint creation (in milliseconds)
    final int gcgs; // the smallest gc gs of all involved tables (in seconds)

    private Hint(Mutation mutation, long creationTime, int gcgs)
    {
        this.mutation = mutation;
        this.creationTime = creationTime;
        this.gcgs = gcgs;
    }

    /**
     * @param mutation the hinted mutation
     * @param creationTime time of this hint's creation (in milliseconds since epoch)
     */
    public static Hint create(Mutation mutation, long creationTime)
    {
        return new Hint(mutation, creationTime, mutation.smallestGCGS());
    }

    /*
     * @param mutation the hinted mutation
     * @param creationTime time of this hint's creation (in milliseconds since epoch)
     * @param gcgs the smallest gcgs of all tables involved at the time of hint creation (in seconds)
     */
    public static Hint create(Mutation mutation, long creationTime, int gcgs)
    {
        return new Hint(mutation, creationTime, gcgs);
    }

    /**
     * Applies the contained mutation unless it's expired, filtering out any updates for truncated tables
     */
    Future<?> applyFuture()
    {
        if (isLive())
        {
            // filter out partition update for tables that have been truncated since hint's creation
            Mutation filtered = mutation;
            for (TableId id : mutation.getTableIds())
                if (creationTime <= SystemKeyspace.getTruncatedAt(id))
                    filtered = filtered.without(id);

            if (!filtered.isEmpty())
                return filtered.applyFuture();
        }

        return ImmediateFuture.success(null);
    }

    void apply()
    {
        try
        {
            applyFuture().get();
        }
        catch (Exception e)
        {
            Throwables.throwIfUnchecked(e.getCause());
            throw new RuntimeException(e.getCause());
        }
    }

    /**
     * @return the overall ttl of the hint - the minimum of all mutation's tables' gc gs now and at the time of creation
     */
    int ttl()
    {
        return Math.min(gcgs, mutation.smallestGCGS());
    }

    /**
     * @return calculates whether or not it is safe to apply the hint without risking to resurrect any deleted data
     */
    public boolean isLive()
    {
        return isLive(creationTime, currentTimeMillis(), ttl());
    }

    static boolean isLive(long creationTime, long now, int hintTTL)
    {
        return expirationInMillis(creationTime, hintTTL) > now;
    }

    @VisibleForTesting
    long expirationInMillis()
    {
        int smallestGCGS = Math.min(gcgs, mutation.smallestGCGS());
        return expirationInMillis(creationTime, smallestGCGS);
    }

    private static long expirationInMillis(long creationTime, int hintTTL)
    {
        return creationTime + TimeUnit.SECONDS.toMillis(Math.min(hintTTL, maxHintTTL));
    }

    static final class Serializer implements IVersionedSerializer<Hint>
    {
        public long serializedSize(Hint hint, int version)
        {
            long size = sizeof(hint.creationTime);
            size += sizeofUnsignedVInt(hint.gcgs);
            size += hint.mutation.serializedSize(version);
            return size;
        }

        public void serialize(Hint hint, DataOutputPlus out, int version) throws IOException
        {
            out.writeLong(hint.creationTime);
            out.writeUnsignedVInt32(hint.gcgs);
            Mutation.serializer.serialize(hint.mutation, out, version);
        }

        public Hint deserialize(DataInputPlus in, int version) throws IOException
        {
            long creationTime = in.readLong();
            int gcgs = in.readUnsignedVInt32();
            return new Hint(Mutation.serializer.deserialize(in, version), creationTime, gcgs);
        }

        public long getHintCreationTime(ByteBuffer hintBuffer, int version)
        {
            return hintBuffer.getLong(0);
        }

        /**
         * Will short-circuit Mutation deserialization if the hint is definitely dead. If a Hint instance is
         * returned, there is a chance it's live, if gcgs on one of the table involved got reduced between
         * hint creation and deserialization, but this does not impact correctness - an extra liveness check will
         * also be performed on the receiving end.
         *
         * @return null if the hint is definitely dead, a Hint instance if it's likely live
         */
        @Nullable
        Hint deserializeIfLive(DataInputPlus in, long now, long size, int version) throws IOException
        {
            long creationTime = in.readLong();
            int gcgs = in.readUnsignedVInt32();
            int bytesRead = sizeof(creationTime) + sizeofUnsignedVInt(gcgs);

            if (isLive(creationTime, now, gcgs))
                return new Hint(Mutation.serializer.deserialize(in, version), creationTime, gcgs);

            in.skipBytesFully(Ints.checkedCast(size) - bytesRead);
            return null;
        }

        /**
         * Will short-circuit ByteBuffer allocation if the hint is definitely dead. If a ByteBuffer instance is
         * returned, there is a chance it's live, if gcgs on one of the table involved got reduced between
         * hint creation and deserialization, but this does not impact correctness - an extra liveness check will
         * also be performed on the receiving end.
         *
         * @return null if the hint is definitely dead, a ByteBuffer instance if it's likely live
         */
        @Nullable
        ByteBuffer readBufferIfLive(DataInputPlus in, long now, int size, int version) throws IOException
        {
            int maxHeaderSize = Math.min(sizeof(Long.MAX_VALUE) + VIntCoding.MAX_SIZE, size);
            byte[] header = new byte[maxHeaderSize];
            in.readFully(header);

            try (DataInputBuffer input = new DataInputBuffer(header))
            {
                long creationTime = input.readLong();
                int gcgs = input.readUnsignedVInt32();

                if (!isLive(creationTime, now, gcgs))
                {
                    in.skipBytesFully(size - maxHeaderSize);
                    return null;
                }
            }

            byte[] bytes = new byte[size];
            System.arraycopy(header, 0, bytes, 0, header.length);
            in.readFully(bytes, header.length, size - header.length);
            return ByteBuffer.wrap(bytes);
        }
    }
}
