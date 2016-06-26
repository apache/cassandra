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

package org.apache.cassandra.index.sasi.utils;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.index.sasi.*;
import org.apache.cassandra.index.sasi.disk.*;
import org.apache.cassandra.utils.*;

public class KeyConverter implements KeyFetcher
{
    public final static KeyConverter instance = new KeyConverter();

    KeyConverter()
    {}

    @Override
    public DecoratedKey getPartitionKey(long offset)
    {
        return dk(offset);
    }

    @Override
    public Clustering getClustering(long offset)
    {
        return ck(offset);
    }

    @Override

    public RowKey getRowKey(long partitionOffset, long rowOffset)
    {
        return new RowKey(getPartitionKey(partitionOffset), getClustering(rowOffset), new ClusteringComparator(LongType.instance));
    }

    public static DecoratedKey dk(long partitionOffset)
    {
        ByteBuffer buf = ByteBuffer.allocate(8);
        buf.putLong(partitionOffset);
        buf.flip();
        Long hashed = MurmurHash.hash2_64(buf, buf.position(), buf.remaining(), 0);
        return new BufferDecoratedKey(new Murmur3Partitioner.LongToken(hashed), buf);
    }

    public static Clustering ck(long offset)
    {
        return Clustering.make(ByteBufferUtil.bytes(offset));
    }
}