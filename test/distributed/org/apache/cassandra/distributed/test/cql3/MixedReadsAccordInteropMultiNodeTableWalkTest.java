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

package org.apache.cassandra.distributed.test.cql3;

import accord.utils.Property;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.service.consensus.TransactionalMode;

public class MixedReadsAccordInteropMultiNodeTableWalkTest extends AccordInteropMultiNodeTableWalkBase
{
    public MixedReadsAccordInteropMultiNodeTableWalkTest()
    {
        super(TransactionalMode.mixed_reads);
    }

    @Override
    protected void preCheck(Cluster cluster, Property.StatefulBuilder builder)
    {
        super.preCheck(cluster, builder);
        // if a failing seed is detected, populate here
        // Example: builder.withSeed(42L);
        // CQL operations may have opertors such as +, -, and / (example 4 + 4), to "apply" them to get a constant value
        // CQL_DEBUG_APPLY_OPERATOR = true;
    }
}
