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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.rows.ArrayCell;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.NativeCell;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.NativeAllocator;
import org.apache.cassandra.utils.memory.NativePool;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

@RunWith(Parameterized.class)
public class CellSpecTest
{
    private final Cell<?> cell;

    @SuppressWarnings("unused")
    public CellSpecTest(String ignoreOnlyUsedForBetterTestName, Cell<?> cell)
    {
        this.cell = cell;
    }

    @Test
    public void unsharedHeapSizeExcludingData()
    {
        long empty = ObjectSizes.measure(cell);
        long expected;
        if (cell instanceof NativeCell)
        {
            // NativeCell stores the contents off-heap, so the cost on-heap is just the object's empty case
            expected = empty;
        }
        else
        {
            // size should be: empty + valuePtr + path.unsharedHeapSizeExcludingData() if present
            expected = empty + valuePtrSize(cell.value());
            if (cell.path() != null)
                expected += cell.path().unsharedHeapSizeExcludingData();
        }

        Assertions.assertThat(cell.unsharedHeapSizeExcludingData())
                  .isEqualTo(expected);
    }

    private static long valuePtrSize(Object value)
    {
        if (value instanceof ByteBuffer)
            return ObjectSizes.sizeOfEmptyHeapByteBuffer();
        else if (value instanceof byte[])
            return ObjectSizes.sizeOfEmptyByteArray();
        throw new IllegalArgumentException("Unsupported type: " + value.getClass());
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        TableMetadata table = TableMetadata.builder("testing", "testing")
                                           .addPartitionKeyColumn("pk", BytesType.instance)
                                           .build();

        byte[] rawBytes = { 0, 1, 2, 3, 4, 5, 6 };
        ByteBuffer bbBytes = ByteBuffer.wrap(rawBytes);
        NativePool pool = new NativePool(1024, 1024, 1, () -> CompletableFuture.completedFuture(true));
        NativeAllocator allocator = pool.newAllocator();
        OpOrder order = new OpOrder();

        List<Cell<?>> tests = new ArrayList<>();
        BiConsumer<ColumnMetadata, CellPath> fn = (column, path) -> {
            tests.add(new ArrayCell(column, 1234, 1, 1, rawBytes, path));
            tests.add(new BufferCell(column, 1234, 1, 1, bbBytes, path));
            tests.add(new NativeCell(allocator, order.getCurrent(), column, 1234, 1, 1, bbBytes, path));
        };
        // simple
        fn.accept(ColumnMetadata.regularColumn(table, bytes("simple"), BytesType.instance), null);

        // complex
        // seems NativeCell does not allow CellPath.TOP, or CellPath.BOTTOM
        fn.accept(ColumnMetadata.regularColumn(table, bytes("complex"), ListType.getInstance(BytesType.instance, true)), CellPath.create(bytes(UUIDGen.getTimeUUID())));

        return tests.stream().map(a -> new Object[] {a.getClass().getSimpleName() + ":" + (a.path() == null ? "simple" : "complex"), a}).collect(Collectors.toList());
    }

}
