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

package org.apache.cassandra.db.partitions;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class RowTrackingIteratorTest
{
    private static final int EMPTY_ROW = -1;

    Partition[] testData = new Partition[]
    {
        partition(key(1), staticRow(3), rows(1, 2, 3)),
        partition(key(11), staticRow(), rows(10)),
        partition(key(21), staticRow(), rows()),
        partition(key(31), staticRow(), rows(20, 21, 22))
    };

    @Mock
    Consumer<DecoratedKey> partitionVerifier;
    @Mock
    Consumer<Row> staticRowVerifier;
    @Mock
    Consumer<Row> rowVerifier;

    @Captor
    private ArgumentCaptor<DecoratedKey> partitionCaptor;
    @Captor
    private ArgumentCaptor<Row> staticRowCaptor;
    @Captor
    private ArgumentCaptor<Row> rowCaptor;

    @Test
    public void testRowTrackingIterator()
    {
        initMocks(this);
        PartitionIterator partitionIterator = makePartitionIterator(testData);

        PartitionIterator testedIterator = PartitionIterators.filteredRowTrackingIterator(partitionIterator,
                                                                                          partitionVerifier,
                                                                                          staticRowVerifier,
                                                                                          rowVerifier);

        // iterate over partitions and rows
        while (testedIterator.hasNext())
        {
            RowIterator rowIterator = testedIterator.next();
            while (rowIterator.hasNext())
            {
                rowIterator.next();
            }
        }

        // verify mock expectations
        verify(partitionVerifier, times(4)).accept(partitionCaptor.capture());
        verify(staticRowVerifier, times(1)).accept(staticRowCaptor.capture());
        verify(rowVerifier, times(7)).accept(rowCaptor.capture());

        int staticRowIndex = 0;
        int rowIndex = 0;
        for (int partitionIndex = 0; partitionIndex < testData.length; partitionIndex++)
        {
            Partition partition = testData[partitionIndex];
            assertEquals(partition.key, partitionCaptor.getAllValues().get(partitionIndex));
            if (!partition.staticRow.isEmpty())
            {
                // in the test we're identifying rows by the number of columns they bear
                assertEquals(partition.staticRow.columnCount(), staticRowCaptor.getAllValues().get(staticRowIndex++).columnCount());
            }
            for (int idx = 0; idx < partition.rows.size(); idx++)
            {
                // in the test we're identifying rows by the number of columns they bear
                assertEquals(partition.rows.get(idx).columnCount(), rowCaptor.getAllValues().get(rowIndex++).columnCount());
            }
        }

        verifyNoMoreInteractions(partitionVerifier, staticRowVerifier, rowVerifier);
    }

    // below are helper methods to make the test data more readable
    private static class Partition
    {
        private final DecoratedKey key;
        private final Row staticRow;
        private final List<Row> rows;

        public Partition(int key, int staticRowDiscriminator, int[] rows)
        {
            this.key = new Murmur3Partitioner().decorateKey(ByteBufferUtil.bytes(key));
            Row staticRow = mock(Row.class);
            if (staticRowDiscriminator != EMPTY_ROW)
            {
                when(staticRow.columnCount()).thenReturn(staticRowDiscriminator);
            }
            else
            {
                when(staticRow.isEmpty()).thenReturn(true);
            }
            this.staticRow = staticRow;
            this.rows = Arrays.stream(rows).boxed().map(rowDiscriminator -> {
                Row row = mock(Row.class);
                when(row.columnCount()).thenReturn(rowDiscriminator);
                return row;
            }).collect(Collectors.toList());
        }
    }

    private Partition partition(int key, int staticRowDiscriminator, int... rows)
    {
        return new Partition(key, staticRowDiscriminator, rows);
    }

    private static int[] rows(int... rowDiscriminators)
    {
        return rowDiscriminators;
    }

    private static int key(int keyDiscriminator)
    {
        return keyDiscriminator;
    }

    private static int staticRow(int staticRowDiscriminator)
    {
        return staticRowDiscriminator;
    }

    private static int staticRow()
    {
        return EMPTY_ROW;
    }

    private static PartitionIterator makePartitionIterator(Partition[] partitions)
    {
        return new PartitionIterator()
        {
            int i = 0;
            public boolean hasNext()
            {
                return i < partitions.length;
            }
            public RowIterator next()
            {
                return makeRowIterator(partitions[i++]);
            }
            public void close()
            {
            }
        };
    }

    private static RowIterator makeRowIterator(Partition partition)
    {
        Iterator<Row> rows = partition.rows.iterator();

        RowIterator iter = mock(RowIterator.class);
        when(iter.partitionKey()).thenReturn(partition.key);
        when(iter.staticRow()).thenReturn(partition.staticRow);
        when(iter.hasNext()).thenAnswer(invocation -> rows.hasNext());
        when(iter.next()).thenAnswer(invocation -> rows.next());

        return iter;
    }
}