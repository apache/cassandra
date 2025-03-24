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

package org.apache.cassandra.utils.memory;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.db.ArrayClustering;
import org.apache.cassandra.db.BufferClustering;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.rows.ArrayCell;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.arbitrary;
import static org.quicktheories.generators.SourceDSL.integers;

@RunWith(Parameterized.class)
public class ContextClonerTest
{
    private static final ColumnMetadata cellDef = ColumnMetadata.regularColumn("ks", "cf", "c",
                                                                               Int32Type.instance,
                                                                               ColumnMetadata.NO_UNIQUE_ID);

    private static final ColumnMetadata complexCellDef = ColumnMetadata.regularColumn("ks", "cf", "c",
                                                                                      MapType.getInstance(Int32Type.instance,
                                                                                                          Int32Type.instance,
                                                                                                          true),
                                                                                      ColumnMetadata.NO_UNIQUE_ID);
    @Parameterized.Parameters(name="allocationType={0}")
    public static Iterable<? extends Object> data()
    {
        return Arrays.asList(Config.MemtableAllocationType.values());
    }

    @Parameterized.Parameter
    public Config.MemtableAllocationType allocationType;

    boolean offheapAllocation = false;

    MemtablePool pool;

    @Before
    public void before()
    {
        long capacity = Integer.MAX_VALUE;
        switch(allocationType)
        {
            case unslabbed_heap_buffers:
                pool = new HeapPool(capacity, 1.0f, () -> ImmediateFuture.success(false));
                break;
            case unslabbed_heap_buffers_logged:
                pool = new HeapPool.Logged(capacity, 1.0f, () -> ImmediateFuture.success(false));
                break;
            case heap_buffers:
                pool = new SlabPool(capacity, 0, 1.0f, () -> ImmediateFuture.success(false));
                break;
            case offheap_buffers:
                pool = new SlabPool(0, capacity, 1.0f, () -> ImmediateFuture.success(false));
                offheapAllocation = true;
                break;
            case offheap_objects:
                pool = new NativePool(0, capacity, 1.0f, () -> ImmediateFuture.success(false));
                offheapAllocation = true;
                break;
            default: throw new UnsupportedOperationException();

        }
    }

    @Test
    public void test()
    {
        qt().forAll(arbitrary().pick(Type.values()),
                    integers().between(10, 20),
                    integers().between(0, 10),
                    integers().between(-20, 20)).checkAssert(this::test);
    }

    public void test(Type type, int valueSize, int pathSize, int estimationError) {
        MemtableAllocator memtableAllocator = pool.newAllocator("test");
        OpOrder opOrder = new OpOrder();
        opOrder.start();
        Cloner cloner = memtableAllocator.cloner(opOrder.getCurrent());
        if (allocationType == Config.MemtableAllocationType.unslabbed_heap_buffers ||
            allocationType == Config.MemtableAllocationType.unslabbed_heap_buffers_logged)
        {
            Assert.assertFalse(cloner.isContextAwareCloningSupported());
            return;
        }
        else
        {
            Assert.assertTrue(cloner.isContextAwareCloningSupported());
        }

        int cellValueSize = valueSize;
        Cell<?> cell = cell(type, cellValueSize, pathSize);
        int cellEstimation = cloner.estimateCloneSize(cell);
        Assert.assertThat(cellEstimation, greaterThanOrEqualTo(cellValueSize + pathSize));

        int clusteringValueSize = valueSize;
        int clusteringColumns = 2;
        Clustering<?> clustering = clustering(type, clusteringValueSize, clusteringColumns);
        int clusteringEstimation = cloner.estimateCloneSize(clustering);
        Assert.assertThat(clusteringEstimation, greaterThanOrEqualTo(clusteringValueSize * clusteringColumns));

        int estimationSize = clusteringEstimation + cellEstimation;
        Cloner contextCloner = cloner.createContextAwareCloner(estimationSize + estimationError);
        contextCloner.clone(clustering);
        contextCloner.clone(cell);
        contextCloner.adjustUnused();
        Assert.assertEquals(estimationSize,
                            offheapAllocation ? memtableAllocator.offHeap().owns()
                                              : memtableAllocator.onHeap().owns());
    }

    Cell<?> cell(Type type, int valueSize, int pathSize)
    {
        CellPath path = (pathSize > 0) ? CellPath.create(ByteBuffer.allocate(pathSize)) : null;
        ColumnMetadata cellDefToUse = (pathSize > 0) ? complexCellDef : cellDef;
        switch(type)
        {
            case ARRAY:
                byte[] cellValueArray = new byte[valueSize];
                return new ArrayCell(cellDefToUse, 0L, Cell.NO_TTL, Cell.NO_DELETION_TIME, cellValueArray, path);
            case BUFFER:
                ByteBuffer cellValueBuffer = ByteBuffer.allocate(valueSize);
                return new BufferCell(cellDefToUse, 0L, Cell.NO_TTL, Cell.NO_DELETION_TIME, cellValueBuffer, path);
        }
        throw new UnsupportedOperationException();
    }

    Clustering<?> clustering(Type type, int columnSize, int numberOfColumns)
    {
        switch(type)
        {
            case ARRAY:
                byte[][] arrayValues = new byte[columnSize][numberOfColumns];
                return new ArrayClustering(arrayValues);
            case BUFFER:
                ByteBuffer[] bufferValues = new ByteBuffer[numberOfColumns];
                for (int i = 0; i < numberOfColumns; i++)
                {
                    bufferValues[i] = ByteBuffer.allocate(columnSize);
                }
                return new BufferClustering(bufferValues);
        }
        throw new UnsupportedOperationException();
    }

    enum Type
    {
        ARRAY, BUFFER;
    }
}
