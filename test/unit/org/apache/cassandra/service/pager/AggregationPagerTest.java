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

import org.junit.Test;

import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.SinglePartitionReadQuery;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AggregationPagerTest extends QueryPagerTest
{
    @Test
    public void toStringTest()
    {
        TableMetadata metadata = TableMetadata.builder("ks", "tab")
                                              .addPartitionKeyColumn("k", Int32Type.instance)
                                              .addClusteringColumn("c", Int32Type.instance)
                                              .addColumn(ColumnMetadata.regularColumn("ks", "tab", "v", Int32Type.instance))
                                              .build();

        DataLimits limits = DataLimits.cqlLimits(31, 29);

        Clustering clustering = Clustering.make(bytes(11));
        Row row = mock(Row.class);
        when(row.clustering()).thenReturn(clustering);
        when(row.isRow()).thenReturn(true);

        PagingState state = new PagingState(ByteBufferUtil.bytes(1), PagingState.RowMark.create(metadata, row, ProtocolVersion.CURRENT), 19, 17);

        SinglePartitionReadQuery singlePartitionReadQuery = mock(SinglePartitionReadQuery.class);
        when(singlePartitionReadQuery.metadata()).thenReturn(metadata);
        when(singlePartitionReadQuery.limits()).thenReturn(limits);
        when(singlePartitionReadQuery.partitionKey()).thenReturn(metadata.partitioner.decorateKey(ByteBufferUtil.bytes(1)));
        QueryPager singlePartitionPager = new SinglePartitionPager(singlePartitionReadQuery, state, ProtocolVersion.CURRENT);

        AggregationQueryPager aggregationQueryPager = new AggregationQueryPager(singlePartitionPager, PageSize.inBytes(512), limits);
        Assertions.assertThat(aggregationQueryPager.toString())
                  .contains("limits=" + limits)
                  .contains("subPageSize=512 bytes")
                  .contains("subPager=" + singlePartitionPager)
                  .contains("lastReturned=c=11");
    }


}
