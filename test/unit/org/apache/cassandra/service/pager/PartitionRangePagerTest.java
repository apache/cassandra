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

import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.AbstractReadCommandBuilder.PartitionRangeBuilder;
import org.apache.cassandra.db.ColumnFamilyStore;

public class PartitionRangePagerTest extends PartitionsPagerTests
{
    @Override
    protected PartitionRangeBuilder makeBuilder(int limit, int perPartitionLimit, PageSize pageSize, ColumnFamilyStore cfs, String startKeyInc, String endKeyExcl)
    {
        PartitionRangeBuilder builder = new PartitionRangeBuilder(cfs);
        builder.fromKeyIncl(startKeyInc);
        builder.toKeyExcl(endKeyExcl);
        builder.withNowInSeconds(nowInSec);
        setLimits(builder, limit, perPartitionLimit, pageSize);
        return builder;
    }

}
