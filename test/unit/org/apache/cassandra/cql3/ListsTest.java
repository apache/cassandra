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

package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.Iterators;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.Lists.PrecisionTime;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;

public class ListsTest extends CQLTester
{
    private static final long DEFAULT_MILLIS = 424242424242L;
    private static final int DEFAULT_NANOS = PrecisionTime.MAX_NANOS;

    @Test
    public void testPrecisionTime_getNext_simple()
    {
        PrecisionTime.set(DEFAULT_MILLIS, DEFAULT_NANOS);

        long millis = DEFAULT_MILLIS - 100;
        int count = 1;
        PrecisionTime next = PrecisionTime.getNext(millis, count);
        Assert.assertEquals(millis, next.millis);
        Assert.assertEquals(DEFAULT_NANOS - count, next.nanos);

        next = PrecisionTime.getNext(millis, count);
        Assert.assertEquals(millis, next.millis);
        Assert.assertEquals(DEFAULT_NANOS - (count * 2), next.nanos);
    }

    @Test
    public void testPrecisionTime_getNext_Mulitple()
    {
        PrecisionTime.set(DEFAULT_MILLIS, DEFAULT_NANOS);

        long millis = DEFAULT_MILLIS - 100;
        int count = DEFAULT_NANOS / 2;
        PrecisionTime next = PrecisionTime.getNext(millis, count);
        Assert.assertEquals(millis, next.millis);
        Assert.assertEquals(DEFAULT_NANOS - count, next.nanos);
    }

    @Test
    public void testPrecisionTime_getNext_RollOverNanos()
    {
        final int remainingNanos = 0;
        PrecisionTime.set(DEFAULT_MILLIS, remainingNanos);

        long millis = DEFAULT_MILLIS;
        int count = 1;
        PrecisionTime next = PrecisionTime.getNext(millis, count);
        Assert.assertEquals(millis - 1, next.millis);
        Assert.assertEquals(DEFAULT_NANOS - count, next.nanos);

        next = PrecisionTime.getNext(millis, count);
        Assert.assertEquals(millis - 1, next.millis);
        Assert.assertEquals(DEFAULT_NANOS - (count * 2), next.nanos);
    }

    @Test
    public void testPrecisionTime_getNext_BorkedClock()
    {
        final int remainingNanos = 1;
        PrecisionTime.set(DEFAULT_MILLIS, remainingNanos);

        long millis = DEFAULT_MILLIS + 100;
        int count = 1;
        PrecisionTime next = PrecisionTime.getNext(millis, count);
        Assert.assertEquals(DEFAULT_MILLIS, next.millis);
        Assert.assertEquals(remainingNanos - count, next.nanos);

        // this should roll the clock
        next = PrecisionTime.getNext(millis, count);
        Assert.assertEquals(DEFAULT_MILLIS - 1, next.millis);
        Assert.assertEquals(DEFAULT_NANOS - count, next.nanos);
    }

    @Test
    public void testPrepender_SmallList()
    {
        List<ByteBuffer> terms = new ArrayList<>();
        terms.add(ByteBufferUtil.bytes(1));
        terms.add(ByteBufferUtil.bytes(2));
        terms.add(ByteBufferUtil.bytes(3));
        terms.add(ByteBufferUtil.bytes(4));
        terms.add(ByteBufferUtil.bytes(5));
        testPrepender_execute(terms);
    }

    @Test
    public void testPrepender_HugeList()
    {
        List<ByteBuffer> terms = new ArrayList<>();
        // create a large enough array, then remove some off the end, just to make it an odd size
        for (int i = 0; i < PrecisionTime.MAX_NANOS * 4 - 287; i++)
            terms.add(ByteBufferUtil.bytes(i));
        testPrepender_execute(terms);
    }

    private void testPrepender_execute(List<ByteBuffer> terms)
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, l list<text>)");
        CFMetaData metaData = currentTableMetadata();

        ColumnDefinition columnDefinition = metaData.getColumnDefinition(ByteBufferUtil.bytes("l"));
        Term term = new Lists.Value(terms);
        Lists.Prepender prepender = new Lists.Prepender(columnDefinition, term);

        ByteBuffer keyBuf = ByteBufferUtil.bytes("key");
        DecoratedKey key = Murmur3Partitioner.instance.decorateKey(keyBuf);
        UpdateParameters parameters = new UpdateParameters(metaData, null, null, System.currentTimeMillis(), 1000, Collections.emptyMap());
        Clustering clustering = Clustering.make(ByteBufferUtil.bytes(1));
        parameters.newRow(clustering);
        prepender.execute(key, parameters);

        Row row = parameters.buildRow();
        Assert.assertEquals(terms.size(), Iterators.size(row.cells().iterator()));

        int idx = 0;
        UUID last = null;
        for (Cell cell : row.cells())
        {
            UUID uuid = UUIDGen.getUUID(cell.path().get(0));

            if (last != null)
                Assert.assertTrue(last.compareTo(uuid) < 0);
            last = uuid;

            Assert.assertEquals(String.format("different values found: expected: '%d', found '%d'", ByteBufferUtil.toInt(terms.get(idx)), ByteBufferUtil.toInt(cell.value())),
                                terms.get(idx), cell.value());
            idx++;
        }
    }
}
