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


        // This got wrapped up into CASSANDRA-20189.  With RR=NONE a query was found to produce incorrect result, and when you run with RR=BLOCKING this corrupts the data causing a failure down the line
        // The pattern seen is that SAI is touching multiple unrepaired columns and would misclassify matches.
//        builder.withSeed(-4289657656513111232L).withExamples(1); // CASSANDRA-20189: Avoid possible consistency violations for SAI intersection queries over repaired index matches and multiple non-indexed column matches

        builder.withExamples(Integer.MAX_VALUE);
        // All seeds are against SHA=00bf3edc2c86d2f6f435d1263063f9dfd633bc84

        // CASSANDRA-20260: Partition restricted SAI query returned rows that didn't match the clustering index column predicate
//        builder.withSeed(-3940244592355929823L).withExamples(1);

        // NOTE: all select commands other than "fullTableScan" were commented out, so this seed will not repo
        // without first modifying the test
        // Git SHA=a67755db0ac5c62f560d61d5095f4fc418db0eca
        builder.withSeed(7684880116377589440L).withExamples(1);
    }
}
