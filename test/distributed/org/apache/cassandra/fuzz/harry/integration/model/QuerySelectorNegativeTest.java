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

package org.apache.cassandra.fuzz.harry.integration.model;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.harry.core.Configuration;
import org.apache.cassandra.harry.core.Run;
import org.apache.cassandra.harry.corruptor.AddExtraRowCorruptor;
import org.apache.cassandra.harry.corruptor.ChangeValueCorruptor;
import org.apache.cassandra.harry.corruptor.HideRowCorruptor;
import org.apache.cassandra.harry.corruptor.HideValueCorruptor;
import org.apache.cassandra.harry.corruptor.QueryResponseCorruptor;
import org.apache.cassandra.harry.corruptor.ShowValueCorruptor;
import org.apache.cassandra.harry.ddl.SchemaGenerators;
import org.apache.cassandra.harry.model.Model;
import org.apache.cassandra.harry.model.QuiescentChecker;
import org.apache.cassandra.harry.visitors.MutatingVisitor;
import org.apache.cassandra.harry.visitors.MutatingRowVisitor;
import org.apache.cassandra.harry.operations.Query;
import org.apache.cassandra.harry.operations.QueryGenerator;
import org.apache.cassandra.harry.visitors.Visitor;

import static org.apache.cassandra.harry.corruptor.QueryResponseCorruptor.SimpleQueryResponseCorruptor;

@RunWith(Parameterized.class)
public class QuerySelectorNegativeTest extends IntegrationTestBase
{
    private final int CYCLES = 1000;

    private final Random rnd = new Random();

    private final QueryResponseCorruptorFactory corruptorFactory;

    public QuerySelectorNegativeTest(QueryResponseCorruptorFactory corruptorFactory)
    {
        this.corruptorFactory = corruptorFactory;
    }

    @Parameterized.Parameters
    public static Collection<QueryResponseCorruptorFactory> source()
    {
        return Arrays.asList((run) -> new SimpleQueryResponseCorruptor(run.schemaSpec,
                                                                       run.clock,
                                                                       ChangeValueCorruptor::new),
                             (run) -> new SimpleQueryResponseCorruptor(run.schemaSpec,
                                                                       run.clock,
                                                                       HideValueCorruptor::new),
                             (run) -> new SimpleQueryResponseCorruptor(run.schemaSpec,
                                                                       run.clock,
                                                                       ShowValueCorruptor::new),
                             (run) -> new SimpleQueryResponseCorruptor(run.schemaSpec,
                                                                       run.clock,
                                                                       HideRowCorruptor::new),
                             (run) -> new AddExtraRowCorruptor(run.schemaSpec,
                                                               run.clock,
                                                               run.tracker,
                                                               run.descriptorSelector)
        );
    }

    interface QueryResponseCorruptorFactory
    {
        QueryResponseCorruptor create(Run run);
    }

    @Test
    public void selectRows()
    {
        Map<Query.QueryKind, Integer> stats = new HashMap<>();
        Supplier<Configuration.ConfigurationBuilder> gen = sharedConfiguration();

        int rounds = SchemaGenerators.DEFAULT_RUNS;
        int failureCounter = 0;
        outer:
        for (int counter = 0; counter < rounds; counter++)
        {
            beforeEach();
            Configuration config = gen.get()
                                      .setClusteringDescriptorSelector(sharedCDSelectorConfiguration()
                                                                       .setOperationsPerLtsDistribution(new Configuration.ConstantDistributionConfig(2))
                                                                       .setMaxPartitionSize(2000)
                                                                       .build())
                                      .build();
            Run run = config.createRun();
            run.sut.schemaChange(run.schemaSpec.compile().cql());
            System.out.println(run.schemaSpec.compile().cql());

            Visitor visitor = new MutatingVisitor(run, MutatingRowVisitor::new);
            Model model = new QuiescentChecker(run);

            QueryResponseCorruptor corruptor = this.corruptorFactory.create(run);

            for (int i = 0; i < CYCLES; i++)
                visitor.visit();

            while (true)
            {
                long verificationLts = rnd.nextInt(1000);
                QueryGenerator queryGen = new QueryGenerator(run.schemaSpec,
                                                             run.pdSelector,
                                                             run.descriptorSelector,
                                                             run.rng);

                QueryGenerator.TypedQueryGenerator querySelector = new QueryGenerator.TypedQueryGenerator(run.rng, queryGen);
                Query query = querySelector.inflate(verificationLts, counter);

                model.validate(query);

                if (!corruptor.maybeCorrupt(query, run.sut))
                    continue;

                try
                {
                    model.validate(query);
                    Assert.fail("Should've failed");
                }
                catch (Throwable t)
                {
                    // expected
                    failureCounter++;
                    stats.compute(query.queryKind, (Query.QueryKind kind, Integer cnt) -> cnt == null ? 1 : (cnt + 1));
                    continue outer;
                }
            }
        }

        Assert.assertTrue(String.format("Seen only %d failures", failureCounter),
                          failureCounter > (rounds * 0.8));
    }
}