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

package org.apache.cassandra.io.sstable;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime.ReusableDeletionTime;
import org.apache.cassandra.dht.ReusableDecoratedKey;
import org.apache.cassandra.io.util.ResizableByteBuffer;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.io.util.RandomAccessReader;

public final class PartitionDescriptor extends ResizableByteBuffer
{
    private long position;
    private final ReusableDeletionTime deletionTime = ReusableDeletionTime.live();
    private final ReusableDecoratedKey decoratedKey;

    public PartitionDescriptor(ReusableDecoratedKey decoratedKey)
    {
        this.decoratedKey = decoratedKey;
        this.decoratedKey.shadowKey(buffer(), bytes(), length());
    }

    /**
     *  Loads the following structure:
     *  <pre>
     *   struct partition_header header {
     *     be16 key_length; // e.g. 8 if long
     *     byte key[key_length];
     *     struct deletion_time deletion_time {
     *       be32 local_deletion_time;
     *       be64 marked_for_delete_at;
     *     };
     *   };
     *   </pre>
     */
    void load(RandomAccessReader dataReader, DeletionTime.Serializer serializer) throws IOException
    {
        position = dataReader.getPosition();

        boolean resize = loadShortLength(dataReader);
        if (!resize)
        {
            decoratedKey.shadowKey(length());
        }
        else
        {
            decoratedKey.shadowKey(buffer(), bytes(), length());
        }
        serializer.deserialize(dataReader, deletionTime);
    }

    public long position()
    {
        return position;
    }

    public DeletionTime deletionTime()
    {
        return deletionTime;
    }

    public DecoratedKey key()
    {
        return decoratedKey;
    }

    public ByteBuffer keyBuffer() {
        return super.buffer();
    }

    public int keyLength() {
        return super.length();
    }

    public byte[] keyBytes() {
        return super.bytes();
    }

    public void resetPartition()
    {
        resetBuffer();
        decoratedKey.reset();
        deletionTime.resetLive();
        position = 0;
    }

    @Override
    public String toString()
    {
        return "PartitionDescriptor{ key=" + decoratedKey +
               ", position=" + position +
               ", deletionTime=" + deletionTime +
               '}';
    }
}
