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

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.Property;
import org.apache.cassandra.cql3.ast.CreateIndexDDL;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.service.reads.repair.ReadRepairStrategy;
import org.apache.cassandra.utils.LoggingCommand;

import static accord.utils.Property.commands;
import static accord.utils.Property.stateful;
import static org.apache.cassandra.cql3.KnownIssue.AF_MULTI_NODE_MULTI_COLUMN_AND_NODE_LOCAL_WRITES;

public class MultiNodeTableWalkWithMutationTrackingTest extends MultiNodeTableWalkBase
{
    private static final Logger logger = LoggerFactory.getLogger(MultiNodeTableWalkWithMutationTrackingTest.class);
    
    public MultiNodeTableWalkWithMutationTrackingTest()
    {
        super(ReadRepairStrategy.NONE, ReplicationType.tracked);
    }

    @Override
    protected void preCheck(Cluster cluster, Property.StatefulBuilder builder)
    {
        // if a failing seed is detected, populate here
        // Example: builder.withSeed(42L);
        // CQL operations may have opertors such as +, -, and / (example 4 + 4), to "apply" them to get a constant value
        // CQL_DEBUG_APPLY_OPERATOR = true;
        // When mutations look to be lost as seen by more complex SELECTs, it can be useful to just SELECT the partition/row right after to write to see if it was safe at the time.
        // READ_AFTER_WRITE = true;
    }

    // TODO: Remove this override entirely when range reads and indexing are working properly together.
    @Override
    protected List<CreateIndexDDL.Indexer> supportedIndexers()
    {
        return Collections.singletonList(CreateIndexDDL.SAI);
    }

    @Override
    protected void clusterConfig(IInstanceConfig c)
    {
        super.clusterConfig(c);
        IGNORED_ISSUES.remove(AF_MULTI_NODE_MULTI_COLUMN_AND_NODE_LOCAL_WRITES);
    }

    @Test
    public void test() throws IOException
    {
        try (Cluster cluster = createCluster())
        {
            Property.StatefulBuilder statefulBuilder = stateful().withExamples(10).withSteps(400);
            preCheck(cluster, statefulBuilder);

            // TODO: Uncomment the commented bits below to test range queries w/ the seeds above.
            statefulBuilder.check(commands(() -> rs -> createState(rs, cluster))
                                  .add(StatefulASTBase::insert)
//                                  .add(StatefulASTBase::fullTableScan)
//                                  .addIf(State::allowUsingTimestamp, StatefulASTBase::validateUsingTimestamp)
                                  .addIf(State::hasPartitions, this::selectExisting)
//                                  .addAllIf(State::supportTokens, this::selectToken, this::selectTokenRange, StatefulASTBase::selectMinTokenRange)
                                  .addIf(State::hasEnoughMemtable, StatefulASTBase::flushTable)
                                  .addIf(State::hasEnoughSSTables, StatefulASTBase::compactTable)
//                                  .addIf(State::allowNonPartitionQuery, this::nonPartitionQuery)
//                                  .addIf(State::allowNonPartitionMultiColumnQuery, this::multiColumnQuery)
                                  .addIf(State::allowPartitionQuery, this::partitionRestrictedQuery)
                                  .addIf(State::allowPartitionMultiColumnQuery, this::multiColumnPartitionQuery)
                                  .destroyState(State::close)
                                  .commandsTransformer(LoggingCommand.factory())
                                  .onSuccess(onSuccess(logger))
                                  .build());
        }
    }
}
