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

package org.apache.cassandra.service.pager;

import java.util.function.Supplier;

import org.junit.Test;

import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AbstractQueryPagerTest
{
    @Test
    public void retryAfterExecutionFailure() throws Exception
    {
        TableMetadata metadata = TableMetadata.builder("ks", "tab")
                                              .addPartitionKeyColumn("k", Int32Type.instance)
                                              .build();
        for (int execution = 0; execution < 3; execution++)
        {
            ReadQuery query = mock(ReadQuery.class);
            when(query.metadata()).thenReturn(metadata);
            when(query.limits()).thenReturn(DataLimits.cqlLimits(10));
            AbstractQueryPager<ReadQuery> pager = new TestPager(query);
            IllegalStateException failure = new IllegalStateException("read failed");
            Supplier<AutoCloseable> fetch;

            // TRIGGER: CASSANDRA-11745, execution throws before a page iterator exists to close.
            switch (execution)
            {
                case 0:
                    when(query.execute(ConsistencyLevel.ONE, null, null)).thenThrow(failure).thenReturn(EmptyIterators.partition());
                    fetch = () -> pager.fetchPage(PageSize.inRows(1), ConsistencyLevel.ONE, null, null);
                    break;
                case 1:
                    when(query.executeInternal(null)).thenThrow(failure).thenReturn(EmptyIterators.partition());
                    fetch = () -> pager.fetchPageInternal(PageSize.inRows(1), null);
                    break;
                default:
                    when(query.executeLocally(null)).thenThrow(failure).thenReturn(EmptyIterators.unfilteredPartition(metadata));
                    fetch = () -> pager.fetchPageUnfiltered(metadata, PageSize.inRows(1), null);
                    break;
            }

            // HARNESS: Exercise each execution path on the same pager before and after failure.
            assertThatThrownBy(fetch::get).isSameAs(failure);
            assertFalse(pager.isExhausted());
            // ORACLE: Retrying must succeed; previously startNextPage asserted that a page was still open.
            try (AutoCloseable page = fetch.get())
            {
                assertFalse(pager.isExhausted());
            }
            assertTrue(pager.isExhausted());
        }
    }

    private static class TestPager extends AbstractQueryPager<ReadQuery>
    {
        private TestPager(ReadQuery query)
        {
            super(query, ProtocolVersion.CURRENT);
        }

        protected ReadQuery nextPageReadQuery(PageSize pageSize, DataLimits limits)
        {
            return query;
        }

        protected void recordLast(DecoratedKey key, Row row)
        {
        }

        protected boolean isPreviouslyReturnedPartition(DecoratedKey key)
        {
            return false;
        }

        public PagingState state()
        {
            return null;
        }

        public QueryPager withUpdatedLimit(DataLimits limits)
        {
            throw new UnsupportedOperationException();
        }
    }
}
