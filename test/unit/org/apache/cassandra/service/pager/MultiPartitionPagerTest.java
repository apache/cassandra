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

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.DataLimits;

public class MultiPartitionPagerTest extends AbstractPartitionsPagerTest
{
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
        return makeGroup(limit, perPartitionLimit, startKeyInc, endKeyExcl, key -> sliceQuery(cfs, key, startClustInc, endClustExcl));
    }

    @Override
    protected ReadQuery makePartitionsNamesQuery(int limit, int perPartitionLimit, ColumnFamilyStore cfs, String startKeyInc, String endKeyExcl, String... clusts)
    {
        return makeGroup(limit, perPartitionLimit, startKeyInc, endKeyExcl, key -> namesQuery(cfs, key, clusts));
    }
}
