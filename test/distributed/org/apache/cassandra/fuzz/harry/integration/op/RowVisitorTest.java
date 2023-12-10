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

package org.apache.cassandra.fuzz.harry.integration.op;

import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.harry.core.MetricReporter;
import org.apache.cassandra.harry.core.Run;
import org.apache.cassandra.harry.ddl.SchemaGenerators;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.gen.distribution.Distribution;
import org.apache.cassandra.harry.model.OpSelectors;
import org.apache.cassandra.harry.clock.OffsetClock;
import org.apache.cassandra.harry.sut.SystemUnderTest;
import org.apache.cassandra.harry.tracker.DataTracker;
import org.apache.cassandra.harry.visitors.GeneratingVisitor;
import org.apache.cassandra.harry.visitors.MutatingRowVisitor;
import org.apache.cassandra.harry.visitors.MutatingVisitor;
import org.apache.cassandra.cql3.CQLTester;

import static org.apache.cassandra.harry.model.OpSelectors.DefaultDescriptorSelector.DEFAULT_OP_SELECTOR;

public class RowVisitorTest extends CQLTester
{
    @Before
    public void beforeTest() throws Throwable {
        super.beforeTest();
        schemaChange(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", SchemaGenerators.DEFAULT_KEYSPACE_NAME));
    }

    @Test
    public void rowWriteGeneratorTest()
    {
        Supplier<SchemaSpec> specGenerator = SchemaGenerators.progression(SchemaGenerators.DEFAULT_SWITCH_AFTER);
        EntropySource rand = EntropySource.forTests(6371747244598697093L);

        OpSelectors.PureRng rng = new OpSelectors.PCGFast(1);

        OpSelectors.PdSelector pdSelector = new OpSelectors.DefaultPdSelector(rng, 10, 10);

        for (int i = 0; i < SchemaGenerators.DEFAULT_RUNS; i++)
        {
            SchemaSpec schema = specGenerator.get();
            createTable(schema.compile().cql());

            OpSelectors.DescriptorSelector descriptorSelector = new OpSelectors.DefaultDescriptorSelector(rng,
                                                                                                          new OpSelectors.ColumnSelectorBuilder().forAll(schema)
                                                                                                                                                 .build(),
                                                                                                          DEFAULT_OP_SELECTOR,
                                                                                                          new Distribution.ScaledDistribution(1, 30),
                                                                                                          100);

            Run run = new Run(rng,
                              new OffsetClock(10000),
                              pdSelector,
                              descriptorSelector,
                              schema,
                              DataTracker.NO_OP,
                              SystemUnderTest.NO_OP,
                              MetricReporter.NO_OP);

            long[] descriptors = new long[4];
            for (int j = 0; j < descriptors.length; j++)
                descriptors[j] = rand.next();

            // some improvement wont hurt here
            new GeneratingVisitor(run, new MutatingVisitor(run, MutatingRowVisitor::new)).visit();
        }
    }
}