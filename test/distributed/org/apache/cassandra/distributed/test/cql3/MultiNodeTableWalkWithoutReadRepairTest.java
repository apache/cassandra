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
import org.apache.cassandra.service.reads.repair.ReadRepairStrategy;

public class MultiNodeTableWalkWithoutReadRepairTest extends MultiNodeTableWalkBase
{
    public MultiNodeTableWalkWithoutReadRepairTest()
    {
        super(ReadRepairStrategy.NONE);
    }

    @Override
    protected void preCheck(Cluster cluster, Property.StatefulBuilder builder)
    {
        // if a failing seed is detected, populate here
        // Example: builder.withSeed(42L);
        // CQL operations may have opertors such as +, -, and / (example 4 + 4), to "apply" them to get a constant value
         CQL_DEBUG_APPLY_OPERATOR = true;

//        builder.withSeed(3447985801999894413L).withExamples(1); // -= list with value
//        builder.withSeed(-2687302456481776596L).withExamples(1); // Fixed by CASSANDRA-20449: row mismatch

        // All seeds here are when the allowed types were larger, the test limited to collections + primitives above
//         builder.withSeed(2772645953614578715L).withExamples(1); // Fixed: += on map type
//        builder.withSeed(3447991911091931691L).withExamples(1); // Fixed: NPE deflating keys
//        builder.withSeed(-7718863789281418201L).withExamples(1); // Fixed: eval of Optional.empty (missed updating static code path)
//        builder.withSeed(880881107305966245L).withExamples(1); // Fixed: -= not handling BB
//        builder.withSeed(-2989523031488301677L).withExamples(1); // Fixed: += map with value present


         // All seeds here were with supported types including UDTs, UDTs were excluded for the time being so these seeds won't repo
//        builder.withSeed(-814430092055554935L).withExamples(1); // Fixed: UDT empty bytes == null, which is not expected by the model (as it is different than other code paths)
//        builder.withSeed(-5039578131309477040L).withExamples(1); // Fixed: Missing update due to UDT empty
//        builder.withSeed(-5326848375356006181L).withExamples(1); // Fixed (previous fix used UNTOUCHED rather than NULL, this case requires NULL): Static UDT empty bytes missing in model
//        builder.withSeed(3447991384254834118L).withExamples(1); // UDT empty returns empty bytes rather than null like previous seeds; is select token not checking that the cell is a tombstone?
    }
}
