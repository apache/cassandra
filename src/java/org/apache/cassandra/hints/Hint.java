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
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.*;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

import static org.apache.cassandra.db.TypeSizes.sizeof;
import static org.apache.cassandra.db.TypeSizes.sizeofUnsignedVInt;

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

    final Mutation mutation;
    final long creationTime;  // time of hint creation (in milliseconds)
    final int gcgs; // the smallest gc gs of all involved tables

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

    /**
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
    void apply()
    {
        if (!isLive())
            return;

        // filter out partition update for table that have been truncated since hint's creation
        Mutation filtered = mutation;
        for (UUID id : mutation.getColumnFamilyIds())
            if (creationTime <= SystemKeyspace.getTruncatedAt(id))
                filtered = filtered.without(id);

        if (!filtered.isEmpty())
            filtered.apply();
    }

    /**
     * @return calculates whether or not it is safe to apply the hint without risking to resurrect any deleted data
     */
    boolean isLive()
    {
        int smallestGCGS = Math.min(gcgs, mutation.smallestGCGS());
        long expirationTime = creationTime + TimeUnit.SECONDS.toMillis(smallestGCGS);
        return expirationTime > System.currentTimeMillis();
    }

    static final class Serializer implements IVersionedSerializer<Hint>
    {
        public long serializedSize(Hint hint, int version)
        {
            long size = sizeof(hint.creationTime);
            size += sizeofUnsignedVInt(hint.gcgs);
            size += Mutation.serializer.serializedSize(hint.mutation, version);
            return size;
        }

        public void serialize(Hint hint, DataOutputPlus out, int version) throws IOException
        {
            out.writeLong(hint.creationTime);
            out.writeUnsignedVInt(hint.gcgs);
            Mutation.serializer.serialize(hint.mutation, out, version);
        }

        public Hint deserialize(DataInputPlus in, int version) throws IOException
        {
            long creationTime = in.readLong();
            int gcgs = (int) in.readUnsignedVInt();
            return new Hint(Mutation.serializer.deserialize(in, version), creationTime, gcgs);
        }
    }
}
