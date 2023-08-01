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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.NativePool;
import org.assertj.core.api.Assertions;

@RunWith(Parameterized.class)
public class ClusteringHeapSizeTest
{
    private final Clustering<?> clustering;

    public ClusteringHeapSizeTest(Clustering<?> clustering)
    {
        this.clustering = clustering;
    }

    @Test
    public void unsharedHeap()
    {
        long measureDeep = ObjectSizes.measureDeep(clustering);
        long unsharedHeapSize = clustering.unsharedHeapSize();

        double allowedDiff = 0.1; // 10% is seen as "close enough"
        Assertions.assertThat(unsharedHeapSize)
                  .describedAs("Real size is %d", measureDeep)
                  .isBetween((long) (measureDeep * (1 - allowedDiff)), (long) (measureDeep * (1 + allowedDiff)));
    }

    @Test
    public void unsharedHeapSizeExcludingDataLTEUnsharedHeapSize()
    {
        long max = clustering.unsharedHeapSize();
        long min = clustering.unsharedHeapSizeExcludingData();
        Assertions.assertThat(min).isLessThanOrEqualTo(max);
    }

    @Test
    public void testSingletonClusteringHeapSize()
    {
        Clustering<?> clustering = this.clustering.accessor().factory().staticClustering();
        Assertions.assertThat(clustering.unsharedHeapSize())
                  .isEqualTo(0);
        Assertions.assertThat(clustering.unsharedHeapSizeExcludingData())
                  .isEqualTo(0);

        clustering = this.clustering.accessor().factory().clustering();
        Assertions.assertThat(clustering.unsharedHeapSize())
                  .isEqualTo(0);
        Assertions.assertThat(clustering.unsharedHeapSizeExcludingData())
                  .isEqualTo(0);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        byte[] rawBytes = { 0, 1, 2, 3, 4, 5, 6 };
        NativePool pool = new NativePool(1024, 1024, 1, () -> ImmediateFuture.success(true));
        OpOrder order = new OpOrder();

        ArrayClustering array = ArrayClustering.make(rawBytes);
        BufferClustering buffer = BufferClustering.make(ByteBuffer.wrap(rawBytes));
        return Arrays.asList(new Object[][] {
        { array },
        { buffer },
        { new NativeClustering(pool.newAllocator(null), order.getCurrent(), array)}
        });
    }
}