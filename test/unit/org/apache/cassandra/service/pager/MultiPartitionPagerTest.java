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

import java.util.stream.IntStream;

import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.AbstractReadCommandBuilder.MultiPartitionBuilder;
import org.apache.cassandra.db.ColumnFamilyStore;

public class MultiPartitionPagerTest extends PartitionsPagerTests
{
    @Override
    protected MultiPartitionBuilder makeBuilder(int limit, int perPartitionLimit, PageSize pageSize, ColumnFamilyStore cfs, String startKeyInc, String endKeyExcl)
    {
        int keyStartIdx = tokenOrderedKeys.indexOf(startKeyInc);
        assert keyStartIdx >= 0 && keyStartIdx < tokenOrderedKeys.size();
        int keyEndIdx = tokenOrderedKeys.indexOf(endKeyExcl);
        assert keyEndIdx >= 0 && keyEndIdx < tokenOrderedKeys.size();

        MultiPartitionBuilder builder = new MultiPartitionBuilder(cfs);
        builder.withNowInSeconds(nowInSec);
        IntStream.range(keyStartIdx, keyEndIdx).mapToObj(keyIdx -> tokenOrderedKeys.get(keyIdx)).forEachOrdered(builder::addPartition);
        setLimits(builder, limit, perPartitionLimit, pageSize);
        return builder;
    }

}
