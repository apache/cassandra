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

package org.apache.cassandra.fuzz.harry.examples;

import java.util.Arrays;
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.fuzz.harry.integration.model.IntegrationTestBase;
import org.apache.cassandra.harry.checker.ModelChecker;
import org.apache.cassandra.harry.ddl.ColumnSpec;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.gen.rng.JdkRandomEntropySource;
import org.apache.cassandra.harry.model.Model;
import org.apache.cassandra.harry.operations.Query;
import org.apache.cassandra.harry.sut.SystemUnderTest;
import org.apache.cassandra.harry.sut.TokenPlacementModel;
import org.apache.cassandra.harry.sut.injvm.InJvmSutBase;
import org.apache.cassandra.harry.tracker.DataTracker;
import org.apache.cassandra.harry.tracker.DefaultDataTracker;
import org.apache.cassandra.harry.visitors.ReplayingVisitor;

public class RepairBurnTest extends IntegrationTestBase
{
    @BeforeClass
    public static void before() throws Throwable
    {
        init(3,
             (cfg) -> InJvmSutBase.defaultConfig().accept(cfg.with(Feature.NETWORK, Feature.GOSSIP)));
    }

    private final long seed = 1L;

    @Test
    public void repairBurnTest() throws Throwable
    {
        SchemaSpec schema = new SchemaSpec("repair_burn_test",
                                           "test_overrides",
                                           Arrays.asList(
                                           ColumnSpec.pk("pk1", ColumnSpec.asciiType(4, 10)),
                                           ColumnSpec.pk("pk2", ColumnSpec.int64Type),
                                           ColumnSpec.pk("pk3", ColumnSpec.int64Type),
                                           ColumnSpec.pk("pk4", ColumnSpec.asciiType(2, 10))),
                                           Arrays.asList(
                                           ColumnSpec.ck("ck1", ColumnSpec.asciiType(2, 0)),
                                           ColumnSpec.ck("ck2", ColumnSpec.asciiType(2, 0)),
                                           ColumnSpec.ck("ck3", ColumnSpec.int64Type),
                                           ColumnSpec.ck("ck4", ColumnSpec.asciiType(4, 100)),
                                           ColumnSpec.ck("ck5", ColumnSpec.asciiType(8, 100))
                                           ),
                                           Arrays.asList(
                                           ColumnSpec.regularColumn("regular1", ColumnSpec.asciiType(8, 100)),
                                           ColumnSpec.regularColumn("regular2", ColumnSpec.asciiType(8, 100)),
                                           ColumnSpec.regularColumn("regular3", ColumnSpec.asciiType(8, 100))
                                           ),
                                           Arrays.asList(
                                           ColumnSpec.staticColumn("static1", ColumnSpec.asciiType(8, 100)),
                                           ColumnSpec.staticColumn("static2", ColumnSpec.asciiType(8, 100)),
                                           ColumnSpec.staticColumn("static3", ColumnSpec.asciiType(8, 100))
                                           ));

        sut.schemaChange("CREATE KEYSPACE " + schema.keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
        sut.schemaChange(schema.compile().cql());

        ModelChecker<HistoryBuilder, Void> modelChecker = new ModelChecker<>();
        DataTracker tracker = new DefaultDataTracker();

        TokenPlacementModel.ReplicationFactor rf = new TokenPlacementModel.SimpleReplicationFactor(3);

        int maxPartitionSize = 10;
        int partitions = 1000;

        modelChecker.init(new HistoryBuilder(seed, maxPartitionSize, 10, schema, rf))
                    .step((history, rng) -> {
                        history.visitPartition(rng.nextInt(partitions),
                                               (ps) -> {
                                                   Object[][] clusterings = new Object[maxPartitionSize][];
                                                   for (int i = 0; i < clusterings.length; i++)
                                                   {
                                                       Object[] v = schema.ckGenerator.inflate(rng.next());
                                                       for (int j = 0; j < v.length; j++)
                                                       {
                                                           if (rng.nextBoolean() && v[j] instanceof String)
                                                           {
                                                               v[j] = "";
                                                               return;
                                                           }
                                                       }
                                                       clusterings[i] = v;
                                                   }
                                                   ps.overrideClusterings(clusterings);
                                               })
                               .insert(rng.nextInt(maxPartitionSize));
                    })
                    .step((history, rng) -> {
                        history.visitPartition(rng.nextInt(partitions))
                               .deleteRow(rng.nextInt(maxPartitionSize));
                    })
                    .exitCondition((history) -> {
                        if (history.size() < 10_000)
                            return false;

                        ReplayingVisitor visitor = history.visitor(tracker, sut, SystemUnderTest.ConsistencyLevel.NODE_LOCAL);
                        visitor.replayAll();

                        cluster.get(1).nodetool("repair", "--full");

                        Model model = history.quiescentLocalChecker(tracker, sut);

                        for (Long pd : history.visitedPds())
                            model.validate(Query.selectAllColumns(history.schema(), pd, false));

                        return true;
                    })
                    .run(Integer.MAX_VALUE, seed, new JdkRandomEntropySource(new Random(seed)));
    }
}