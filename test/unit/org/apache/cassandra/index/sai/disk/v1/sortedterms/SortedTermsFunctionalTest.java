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

package org.apache.cassandra.index.sai.disk.v1.sortedterms;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.SegmentMemoryLimiter;

import static org.junit.Assert.assertEquals;

public class SortedTermsFunctionalTest extends AbstractSortedTermsTester
{
    private PrimaryKey.Factory primaryKeyFactory;

    @Before
    public void setup() throws Exception
    {
        super.setup();
        primaryKeyFactory = new PrimaryKey.Factory(indexDescriptor.clusteringComparator);
    }

    @Test
    public void primaryKeySearchSingleSegment() throws Exception
    {
        doPrimaryKeySearch();
    }

    @Test
    public void primaryKeySearchMultipleSegments() throws Exception
    {
        memoryLimiter.setLimitBytes(100);

        doPrimaryKeySearch();
    }

    private void doPrimaryKeySearch() throws Exception
    {
        List<PrimaryKey> keys = new ArrayList<>();

        writeTerms(writer ->
        {
            for (int x = 0; x < 4000; x++)
            {
                PrimaryKey key = makeKey(x);
                keys.add(key);

                writer.add(key::asComparableBytes);
            }
        });

        withTrieSearcher(searcher ->
        {
            for (long index = 0; index < keys.size(); index++)
            {
                PrimaryKey key = keys.get((int)index);
                long result = searcher.prefixSearch(key::asComparableBytes);
                assertEquals(index, result);
            }
        });
    }

    PrimaryKey makeKey(int partitionKey)
    {
        ByteBuffer key = Int32Type.instance.decompose(partitionKey);
        return primaryKeyFactory.createPartitionKeyOnly(Murmur3Partitioner.instance.decorateKey(key));
    }
}
