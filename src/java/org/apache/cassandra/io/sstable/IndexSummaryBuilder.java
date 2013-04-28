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

import java.util.ArrayList;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.utils.ByteBufferUtil;

public class IndexSummaryBuilder
{
    private static final Logger logger = LoggerFactory.getLogger(IndexSummaryBuilder.class);

    private final ArrayList<Long> positions;
    private final ArrayList<byte[]> keys;
    private long keysWritten = 0;

    public IndexSummaryBuilder(long expectedKeys)
    {
        long expectedEntries = expectedKeys / DatabaseDescriptor.getIndexInterval();
        if (expectedEntries > Integer.MAX_VALUE)
        {
            // that's a _lot_ of keys, and a very low interval
            int effectiveInterval = (int) Math.ceil((double) Integer.MAX_VALUE / expectedKeys);
            expectedEntries = expectedKeys / effectiveInterval;
            assert expectedEntries <= Integer.MAX_VALUE : expectedEntries;
            logger.warn("Index interval of {} is too low for {} expected keys; using interval of {} instead",
                        DatabaseDescriptor.getIndexInterval(), expectedKeys, effectiveInterval);
        }
        positions = new ArrayList<Long>((int)expectedEntries);
        keys = new ArrayList<byte[]>((int)expectedEntries);
    }

    public IndexSummaryBuilder maybeAddEntry(DecoratedKey decoratedKey, long indexPosition)
    {
        if (keysWritten % DatabaseDescriptor.getIndexInterval() == 0)
        {
            keys.add(ByteBufferUtil.getArray(decoratedKey.key));
            positions.add(indexPosition);
        }
        keysWritten++;

        return this;
    }

    public IndexSummary build(IPartitioner partitioner)
    {
        byte[][] keysArray = new byte[keys.size()][];
        for (int i = 0; i < keys.size(); i++)
            keysArray[i] = keys.get(i);

        return new IndexSummary(partitioner, keysArray, Longs.toArray(positions));
    }
}
