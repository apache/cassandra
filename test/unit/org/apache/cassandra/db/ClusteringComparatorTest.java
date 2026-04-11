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
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Test;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;

public class ClusteringComparatorTest
{
    @Test
    public void compareLong()
    {
        Iterable<AbstractType<?>> types;
        ClusteringComparator comparator = new ClusteringComparator(LongType.instance);
        for (int i=0;i<1000; i++) {
            long l1 = ThreadLocalRandom.current().nextLong();
            long l2 = ThreadLocalRandom.current().nextLong();
            int cmp = comparator.compare(
            Clustering.make(ByteBufferUtil.bytes(l1)),
            Clustering.make(ByteBufferUtil.bytes(l2)));
            assertEquals(Long.compare(l1, l2), cmp == 0?0:cmp<0?-1:1);

        }
    }

    @Test
    public void compareRawLong() throws IOException
    {
        AbstractType<?>[] types = {LongType.instance};
        for (int i=0;i<1000; i++) {
            long l1 = ThreadLocalRandom.current().nextLong();
            long l2 = ThreadLocalRandom.current().nextLong();

            int compare = Long.compare(l1, l2);
            int compareCluster = ClusteringComparator.compare(types, clusteringOfLongAsBuffer(types, l1),
                                                        clusteringOfLongAsBuffer(types, l2));
            assertEquals("FFS: l1=" + l1 + ", l2=" + l2,
                         compare,
                         compareCluster == 0?0:compareCluster<0?-1:1);
            assertEquals("FFS: v1=" + l1 + ", v2=" + l2,
                         compare > 0,
                         compareCluster > 0);
            assertEquals("FFS: v1=" + l1 + ", v2=" + l2,
                         compare < 0,
                         compareCluster < 0);
            assertEquals("FFS: v1=" + l1 + ", v2=" + l2,
                         compare == 0,
                         compareCluster == 0);
        }
    }
    @Test
    public void compareRawInt() throws IOException
    {
        AbstractType<?>[] types = { Int32Type.instance};
        for (int i=0;i<1000; i++) {
            int i1 = ThreadLocalRandom.current().nextInt();
            int i2 = ThreadLocalRandom.current().nextInt();

            int compare = Integer.compare(i1, i2);
            int compareCluster = ClusteringComparator.compare(types, clusteringOfIntAsBuffer(types, i1),
                                                       clusteringOfIntAsBuffer(types, i2));
            assertEquals("FFS: v1=" + i1 + ", v2=" + i2,
                         compare > 0,
                         compareCluster > 0);
            assertEquals("FFS: v1=" + i1 + ", v2=" + i2,
                         compare < 0,
                         compareCluster < 0);
            assertEquals("FFS: v1=" + i1 + ", v2=" + i2,
                         compare == 0,
                         compareCluster == 0);
        }
    }

    private static ByteBuffer clusteringOfLongAsBuffer(AbstractType<?>[] types, long v1) throws IOException
    {
        Clustering<ByteBuffer> clustering = Clustering.make(ByteBufferUtil.bytes(v1));
        DataOutputBuffer out = new DataOutputBuffer();
        Clustering.serializer.serialize(clustering, out, 0, List.of(types));
        return out.asNewBuffer();
    }

    private static ByteBuffer clusteringOfIntAsBuffer(AbstractType<?>[] types, int v1) throws IOException
    {
        Clustering<ByteBuffer> clustering = Clustering.make(ByteBufferUtil.bytes(v1));
        DataOutputBuffer out = new DataOutputBuffer();
        Clustering.serializer.serialize(clustering, out, 0, List.of(types));
        return out.asNewBuffer();
    }
}