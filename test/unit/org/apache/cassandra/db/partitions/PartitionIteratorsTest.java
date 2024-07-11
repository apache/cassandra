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

import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SinglePartitionReadQuery;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PartitionIteratorsTest
{
    private SinglePartitionReadQuery mockReadQuery()
    {
        SinglePartitionReadQuery readQuery = mock(SinglePartitionReadQuery.class);
        TableMetadata metadata = mock(TableMetadata.class);
        when(readQuery.metadata()).thenReturn(metadata);
        DecoratedKey key = mock(DecoratedKey.class);
        when(readQuery.partitionKey()).thenReturn(key);
        ClusteringIndexFilter filter = mock(ClusteringIndexFilter.class);
        when(readQuery.clusteringIndexFilter()).thenReturn(filter);
        return readQuery;
    }

    @Test
    public void testPartitionIteratorIsClosed()
    {
        PartitionIterator partitionIterator = mock(PartitionIterator.class);
        SinglePartitionReadQuery readQuery = mockReadQuery();

        Row r = mock(Row.class);
        when(r.isRow()).thenReturn(true);

        RowIterator ri = mock(RowIterator.class);
        Iterator<Row> i1 = List.of(r).iterator();
        when(ri.next()).thenAnswer(i -> i1.next());
        when(ri.hasNext()).thenAnswer(i -> i1.hasNext());

        when(partitionIterator.hasNext()).thenReturn(true, false);
        when(partitionIterator.next()).thenReturn(ri);

        try (RowIterator rowIterator = PartitionIterators.getOnlyElement(partitionIterator, readQuery))
        {
            while (rowIterator.hasNext()) rowIterator.next();
        }

        verify(partitionIterator).close();
    }

    @Test
    public void testPartitionIteratorIsClosedInCaseOfError()
    {
        PartitionIterator partitionIterator = mock(PartitionIterator.class);
        SinglePartitionReadQuery readQuery = mockReadQuery();

        Row r = mock(Row.class);
        when(r.isRow()).thenReturn(true);

        RowIterator ri = mock(RowIterator.class);
        Iterator<Row> i1 = List.of(r).iterator();
        when(ri.next()).thenAnswer(i -> i1.next());
        when(ri.hasNext()).thenAnswer(i -> i1.hasNext());

        when(partitionIterator.hasNext()).thenReturn(true, false);
        when(partitionIterator.next()).thenThrow(new RuntimeException("expected"));

        try (RowIterator rowIterator = PartitionIterators.getOnlyElement(partitionIterator, readQuery))
        {
            while (rowIterator.hasNext()) rowIterator.next();
            fail();
        }
        catch (RuntimeException e)
        {
            // expected
        }

        verify(partitionIterator).close();
    }
}