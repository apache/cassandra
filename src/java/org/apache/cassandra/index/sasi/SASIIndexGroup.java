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

package org.apache.cassandra.index.sasi;

import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SingletonIndexGroup;
import org.apache.cassandra.index.sasi.plan.SASIIndexQueryPlan;

public class SASIIndexGroup extends SingletonIndexGroup
{
    public SASIIndexGroup(SASIIndex index)
    {
        super(index);
    }

    @Override
    public Index.QueryPlan queryPlanFor(RowFilter rowFilter)
    {
        return SASIIndexQueryPlan.create((SASIIndex) getIndex(), rowFilter);
    }
}
