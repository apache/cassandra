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
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.PartitionRangeReadQuery;
import org.apache.cassandra.db.ReadCommand;
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

public class PartitionRangePagerTest extends AbstractPartitionsPagerTest
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

        PartitionRangeReadQuery partitionRangeReadQuery = mock(PartitionRangeReadQuery.class);
        when(partitionRangeReadQuery.metadata()).thenReturn(metadata);
        when(partitionRangeReadQuery.limits()).thenReturn(limits);
        QueryPager partitionRangeQueryPager = new PartitionRangeQueryPager(partitionRangeReadQuery, state, ProtocolVersion.CURRENT);
        Assertions.assertThat(partitionRangeQueryPager.toString())
                  .contains(limits.toString())
                  .contains("remaining=19")
                  .contains("remainingInPartition=17")
                  .contains("lastReturnedRow=c=11")
                  .contains("lastCounter=null")
                  .contains("lastKey=DecoratedKey(00000001, 00000001)")
                  .contains("lastReturnedKey=DecoratedKey(00000001, 00000001)")
                  .contains("exhausted=false");
    }

    @Override
    protected ReadCommand makePartitionsSliceQuery(int limit, int perPartitionLimit, ColumnFamilyStore cfs, String startKeyInc, String endKeyExcl, String startClustInc, String endClustExcl)
    {
        return rangeSliceQuery(limit, perPartitionLimit, PageSize.NONE, cfs, startKeyInc, endKeyExcl, startClustInc, endClustExcl).build();
    }

    @Override
    protected ReadCommand makePartitionsNamesQuery(int limit, int perPartitionLimit, ColumnFamilyStore cfs, String startKeyInc, String endKeyExcl, String... clusts)
    {
        return rangeNamesQuery(limit, perPartitionLimit, PageSize.NONE, cfs, startKeyInc, endKeyExcl, clusts);
    }
}
