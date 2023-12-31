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

package org.apache.cassandra.fuzz.harry.integration.generators;

import java.util.Random;
import java.util.concurrent.CompletableFuture;

import com.google.common.collect.Iterators;
import org.junit.Test;

import org.apache.cassandra.harry.HarryHelper;
import org.apache.cassandra.harry.core.Configuration;
import org.apache.cassandra.harry.core.Run;
import org.apache.cassandra.harry.ddl.ColumnSpec;
import org.apache.cassandra.harry.ddl.SchemaGenerators;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.Surjections;
import org.apache.cassandra.harry.gen.distribution.Distribution;
import org.apache.cassandra.harry.model.NoOpChecker;
import org.apache.cassandra.harry.model.OpSelectors;
import org.apache.cassandra.harry.sut.SystemUnderTest;
import org.apache.cassandra.harry.visitors.MutatingVisitor;
import org.apache.cassandra.harry.visitors.MutatingRowVisitor;
import org.apache.cassandra.harry.visitors.SingleValidator;
import org.apache.cassandra.harry.util.TestRunner;
import org.apache.cassandra.harry.visitors.Visitor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.distributed.impl.RowUtil;

public class DataGeneratorsIntegrationTest extends CQLTester
{
    @Test
    public void testTimestampTieResolution()
    {
        Random rng = new Random(1);
        String ks = "test_timestamp_tie_resolution";
        createKeyspace(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", ks));
        int counter = 0;
        for (ColumnSpec.DataType<?> dataType : new ColumnSpec.DataType[]{ ColumnSpec.int8Type,
                                                                          ColumnSpec.int16Type,
                                                                          ColumnSpec.int32Type,
                                                                          ColumnSpec.int64Type,
                                                                          ColumnSpec.asciiType,
                                                                          ColumnSpec.floatType,
                                                                          ColumnSpec.doubleType })
        {


            String tbl = "table_" + (counter++);
            createTable(String.format("CREATE TABLE %s.%s (pk int PRIMARY KEY, v %s)",
                                      ks, tbl,
                                      dataType));
            for (int i = 0; i < 10_000; i++)
            {
                long d1 = dataType.generator().adjustEntropyDomain(rng.nextLong());
                long d2 = dataType.generator().adjustEntropyDomain(rng.nextLong());
                for (long d : new long[]{ d1, d2 })
                {
                    execute(String.format("INSERT INTO %s.%s (pk, v) VALUES (?,?) USING TIMESTAMP 1", ks, tbl),
                            i, dataType.generator().inflate(d));
                }

                if (dataType.compareLexicographically(d1, d2) > 0)
                    assertRows(execute(String.format("SELECT v FROM %s.%s WHERE pk=?", ks, tbl), i),
                               row(dataType.generator().inflate(d1)));
                else
                    assertRows(execute(String.format("SELECT v FROM %s.%s WHERE pk=?", ks, tbl), i),
                               row(dataType.generator().inflate(d2)));
            }
        }
    }

    @Test
    public void queryParseabilityTest() throws Throwable
    {
        Generator<SchemaSpec> gen = new SchemaGenerators.Builder(KEYSPACE).partitionKeyColumnCount(2, 4)
                                                                          .clusteringColumnCount(1, 4)
                                                                          .regularColumnCount(1, 4)
                                                                          .staticColumnCount(1, 4)
                                                                          .generator();

        TestRunner.test(gen,
                        (schema) -> {
                            try
                            {
                                schema.validate();
                            }
                            catch (AssertionError e)
                            {
                                return;
                            }
                            createTable(schema.compile().cql());


                            Configuration.ConfigurationBuilder builder = HarryHelper.defaultConfiguration()
                                                                                    .setDataTracker(new Configuration.NoOpDataTrackerConfiguration())
                                                                                    .setSchemaProvider(new Configuration.FixedSchemaProviderConfiguration(schema))
                                                                                    .setSUT(CqlTesterSut::new);

                            for (OpSelectors.OperationKind kind : OpSelectors.OperationKind.values())
                            {
                                Run run = builder
                                          .setClusteringDescriptorSelector((rng, schema_) -> {
                                              return new OpSelectors.DefaultDescriptorSelector(rng,
                                                                                               OpSelectors.columnSelectorBuilder().forAll(schema_).build(),
                                                                                               OpSelectors.OperationSelector.weighted(Surjections.weights(100), kind),
                                                                                               new Distribution.ConstantDistribution(2),
                                                                                               100);
                                          })
                                          .build()
                                          .createRun();

                                Visitor visitor = new MutatingVisitor(run, MutatingRowVisitor::new);
                                for (int lts = 0; lts < 100; lts++)
                                    visitor.visit();

                                SingleValidator validator = new SingleValidator(100, run, NoOpChecker::new);
                                for (int lts = 0; lts < 10; lts++)
                                    validator.visit(lts);
                            }
                        });
    }

    public class CqlTesterSut implements SystemUnderTest
    {
        public boolean isShutdown()
        {
            return false;
        }

        public void shutdown()
        {
        }

        public void schemaChange(String statement)
        {
            createTable(statement);
        }

        public Object[][] execute(String statement, ConsistencyLevel cl, Object... bindings)
        {
            try
            {
                UntypedResultSet res = DataGeneratorsIntegrationTest.this.execute(statement, bindings);
                if (res == null)
                    return new Object[][]{};

                return Iterators.toArray(RowUtil.toIter(res), Object[].class);
            }
            catch (Throwable throwable)
            {
                throw new RuntimeException(throwable);
            }
        }

        public CompletableFuture<Object[][]> executeAsync(String statement, ConsistencyLevel cl, Object... bindings)
        {
            return CompletableFuture.completedFuture(execute(statement, cl, bindings));
        }
    }
}