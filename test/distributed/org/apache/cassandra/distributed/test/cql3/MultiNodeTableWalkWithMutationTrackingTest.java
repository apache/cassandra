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
        // The following seeds fail with full coverage, including table scans, token restrictions, and range queries.

        // Unexpected results for query: SELECT * FROM ks1.tbl WHERE v1 = ({f0: false, f1: 0x375365d533e5ff, f2: -5629}, [00000000-0000-1200-9b00-000000000000]) LIMIT 991 ALLOW FILTERING
        // No rows returned, 105 steps
//        builder.withSeed(3448511221048049990L).withExamples(1);

        // SELECT * FROM ks1.tbl WHERE v4 > {-4237118076428244729, -1815831816430314156} ALLOW FILTERING -- v4 frozen<set<bigint>>, on node2, fetch size 5000
        // Timeout!, 22 steps
//        builder.withSeed(3448511767874561358L).withExamples(1);

        // Unexpected results for query: SELECT * FROM ks1.tbl WHERE v3 > '브ﭶ熒讘ꯄ謏??䎸锭商Ử豫羀펛葕䝆㛔' LIMIT 785 ALLOW FILTERING
        // No rows returned, 52 steps
//        builder.withSeed(3448512096918920638L).withExamples(1);

        // Unexpected results for query: SELECT * FROM ks1.tbl WHERE pk0 <= -14 LIMIT 659 ALLOW FILTERING
        // No rows returned, 117 steps
//        builder.withSeed(3448512193316910104L).withExamples(1);

        // Unexpected results for query: SELECT * FROM ks1.tbl WHERE v0 >= [{0}, {0, 514}, {-1715, 3, 1215135}] PER PARTITION LIMIT 140 LIMIT 10 ALLOW FILTERING
        // Missing rows, likely related to CASSANDRA-20954
//        builder.withSeed(3448512636059630802L).withExamples(1);

        // Unexpected results for query: SELECT * FROM ks1.tbl WHERE s0 > [[00000000-0000-1700-a700-000000000000, 00000000-0000-1a00-9100-000000000000, 00000000-0000-1500-a800-000000000000]] PER PARTITION LIMIT 184 LIMIT 491 ALLOW FILTERING
        // No rows returned, likely related to CASSANDRA-20954
//        builder.withSeed(3448154736661599106L).withExamples(1);

        // CQL operations may have opertors such as +, -, and / (example 4 + 4), to "apply" them to get a constant value
        // CQL_DEBUG_APPLY_OPERATOR = true;
        // When mutations look to be lost as seen by more complex SELECTs, it can be useful to just SELECT the partition/row right after to write to see if it was safe at the time.
        // READ_AFTER_WRITE = true;
    }

    @Override
    protected List<CreateIndexDDL.Indexer> supportedIndexers()
    {
        return Collections.emptyList();
    }

    @Override
    protected void clusterConfig(IInstanceConfig c)
    {
        super.clusterConfig(c);
        c.set("mutation_tracking_enabled", "true");
    }

    @Test
    public void test() throws IOException
    {
        try (Cluster cluster = createCluster())
        {
            Property.StatefulBuilder statefulBuilder = stateful().withExamples(10).withSteps(400);
            preCheck(cluster, statefulBuilder);
            statefulBuilder.check(commands(() -> rs -> createState(rs, cluster))
                                  .add(StatefulASTBase::insert)
                                  .add(StatefulASTBase::fullTableScan)
                                  .addIf(State::allowUsingTimestamp, StatefulASTBase::validateUsingTimestamp)
                                  .addIf(State::hasPartitions, this::selectExisting)
                                  .addAllIf(State::supportTokens, this::selectToken, this::selectTokenRange, StatefulASTBase::selectMinTokenRange)
                                  .addIf(State::hasEnoughMemtable, StatefulASTBase::flushTable)
                                  .addIf(State::hasEnoughSSTables, StatefulASTBase::compactTable)
                                  .addIf(State::allowNonPartitionQuery, this::nonPartitionQuery)
                                  .addIf(State::allowNonPartitionMultiColumnQuery, this::multiColumnQuery)
                                  .addIf(State::allowPartitionQuery, this::partitionRestrictedQuery)
                                  .destroyState(State::close)
                                  .commandsTransformer(LoggingCommand.factory())
                                  .onSuccess(onSuccess(logger))
                                  .build());
        }
    }
}
