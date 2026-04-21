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

package org.apache.cassandra.distributed.test.tracking;

import java.util.ArrayList;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.log.FuzzTestBase;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.dsl.ReplayingHistoryBuilder;
import org.apache.cassandra.harry.execution.InJvmDTestVisitExecutor;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.SchemaGenerators;
import org.apache.cassandra.replication.MutationJournal;

import static org.apache.cassandra.harry.checker.TestHelper.withRandom;

public class MutationTrackingBounceTest extends FuzzTestBase
{
    private static final int POPULATION = 1000;

    @Test
    public void bounceTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1).start())
        {
            bounceTest(cluster, 1, 1);
        }
    }

    @Test
    public void bounceTestMultiNode() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(3).start())
        {
            bounceTest(cluster, 3, 1);
        }
    }

    @Ignore("https://issues.apache.org/jira/browse/CASSANDRA-21256")
    @Test
    public void doubleBounceTestMultiNode() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(3).start())
        {
            bounceTest(cluster, 3, 2);
        }
    }

    private void bounceTest(Cluster cluster, int rf, int bounces) throws Throwable
    {
        int tables = 10;
        int writesPerKey = 2;
        int pks = 100;
        withRandom(rng -> {
            cluster.schemaChange(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': %d} " +
                                               "AND replication_type='tracked'",
                                               KEYSPACE, rf));

            List<HistoryBuilder> builders = new ArrayList<>();
            for (int i = 0; i < tables; i++)
            {
                Generator<SchemaSpec> schemaGen = SchemaGenerators.trivialSchema(KEYSPACE, () -> "mutation_tracking_bounce_" + (builders.size() + 1), POPULATION,
                                                                                 SchemaSpec.optionsBuilder());

                SchemaSpec schema = schemaGen.generate(rng);
                cluster.schemaChange(schema.compile());
                builders.add(new ReplayingHistoryBuilder(schema.valueGenerators,
                                                         hb -> InJvmDTestVisitExecutor.builder()
                                                                                      .consistencyLevel(ConsistencyLevel.QUORUM)
                                                                                      .build(schema, hb, cluster)));
            }

            int counter = 0;
            for (int pk = 0; pk < pks; pk++)
            {
                for (HistoryBuilder history : builders)
                    for (int i = 0; i < writesPerKey; i++)
                        history.insert(pk);

                    if (++counter % 10 == 0)
                        cluster.get(1).runOnInstance(() -> MutationJournal.instance().closeCurrentSegmentForTestingIfNonEmpty());
                }

            for (int bounce = 0; bounce < bounces; bounce++)
            {
                ClusterUtils.stopUnchecked(cluster.get(1));
                cluster.get(1).startup();
            }

            for (int pk = 0; pk < pks; pk++)
                for (HistoryBuilder history : builders)
                    for (int i = 0; i < 10; i++)
                        history.selectPartition(pk);

            cluster.get(1).runOnInstance(new MutationTrackingBounce_ValidateRunnable(tables * pks * writesPerKey));
        });
    }
}
