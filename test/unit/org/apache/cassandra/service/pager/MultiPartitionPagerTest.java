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

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.assertj.core.api.Assertions;

import static java.util.Arrays.asList;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MultiPartitionPagerTest extends AbstractPartitionsPagerTest
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

        SinglePartitionReadCommand.Group singlePartitionReadQueryGroup = SinglePartitionReadCommand.Group.create(metadata,
                                                                                                                 FBUtilities.nowInSeconds(),
                                                                                                                 ColumnFilter.all(metadata),
                                                                                                                 RowFilter.NONE, limits,
                                                                                                                 asList(metadata.partitioner.decorateKey(bytes(1)), metadata.partitioner.decorateKey(bytes(2))),
                                                                                                                 new ClusteringIndexSliceFilter(Slices.ALL, false));
        QueryPager multiPartitionPager = new MultiPartitionPager<>(singlePartitionReadQueryGroup, state, ProtocolVersion.CURRENT);
        Assertions.assertThat(multiPartitionPager.toString())
                  .contains("queries.length=2")
                  .contains("limit=" + limits)
                  .contains("remaining=19")
                  .contains("current=0");
    }

    SinglePartitionReadCommand.Group makeGroup(int limit, int perPartitionLimit, String firstKey, String lastKey, Function<String, SinglePartitionReadCommand> partitionQuerySupplier)
    {
        int keyStartIdx = tokenOrderedKeys.indexOf(firstKey);
        assert keyStartIdx >= 0 && keyStartIdx < tokenOrderedKeys.size();
        int keyEndIdx = tokenOrderedKeys.indexOf(lastKey);
        assert keyEndIdx >= 0 && keyEndIdx < tokenOrderedKeys.size();

        List<SinglePartitionReadCommand> cmds = IntStream.range(keyStartIdx, keyEndIdx)
                                                         .mapToObj(keyIdx -> partitionQuerySupplier.apply(tokenOrderedKeys.get(keyIdx)))
                                                         .collect(Collectors.toList());

        return SinglePartitionReadCommand.Group.create(cmds, DataLimits.cqlLimits(limit, perPartitionLimit));
    }

    @Override
    protected ReadQuery makePartitionsSliceQuery(int limit, int perPartitionLimit, ColumnFamilyStore cfs, String startKeyInc, String endKeyExcl, String startClustInc, String endClustExcl)
    {
        return makeGroup(limit, perPartitionLimit, startKeyInc, endKeyExcl, key -> sliceQuery(cfs, key, startClustInc, endClustExcl).build());
    }

    @Override
    protected ReadQuery makePartitionsNamesQuery(int limit, int perPartitionLimit, ColumnFamilyStore cfs, String startKeyInc, String endKeyExcl, String... clusts)
    {
        return makeGroup(limit, perPartitionLimit, startKeyInc, endKeyExcl, key -> namesQuery(cfs, key, clusts).build());
    }
}
