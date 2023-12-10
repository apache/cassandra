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
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.cassandra.harry.core.Configuration;
import org.apache.cassandra.harry.core.Run;
import org.apache.cassandra.harry.ddl.SchemaGenerators;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.runner.Runner;
import org.apache.cassandra.harry.runner.UpToLtsRunner;
import org.apache.cassandra.harry.visitors.MutatingRowVisitor;
import org.apache.cassandra.harry.visitors.MutatingVisitor;
import org.apache.cassandra.harry.visitors.SingleValidator;

public abstract class ModelTestBase extends IntegrationTestBase
{
    private final int ITERATIONS = 20_000;

    void negativeTest(Function<Run, Boolean> corrupt, BiConsumer<Throwable, Run> validate) throws Throwable
    {
        Supplier<SchemaSpec> supplier = SchemaGenerators.progression(SchemaGenerators.DEFAULT_SWITCH_AFTER);
        for (int i = 0; i < SchemaGenerators.DEFAULT_RUNS; i++)
        {
            SchemaSpec schema = supplier.get();
            negativeTest(corrupt, validate, i, schema);
        }
    }

    void negativeIntegrationTest(Configuration.RunnerConfiguration runnerConfig) throws Throwable
    {
        Supplier<SchemaSpec> supplier = SchemaGenerators.progression(1);
        for (int i = 0; i < SchemaGenerators.DEFAULT_RUNS; i++)
        {
            SchemaSpec schema = supplier.get();
            Configuration.ConfigurationBuilder builder = configuration(i, schema);

            builder.setClock(new Configuration.ApproximateClockConfiguration((int) TimeUnit.MINUTES.toMillis(10),
                                                                             1, TimeUnit.SECONDS))
                   .setCreateSchema(false)
                   .setDropSchema(false)
                   .setRunner(runnerConfig);

            Configuration config = builder.build();
            Runner runner = config.createRunner();
            
            Run run = runner.getRun();
            beforeEach();
            run.sut.schemaChange(run.schemaSpec.compile().cql());
            runner.run();
        }
    }

    protected abstract Configuration.ModelConfiguration modelConfiguration();

    protected SingleValidator validator(Run run)
    {
        return new SingleValidator(100, run , modelConfiguration());
    }

    public Configuration.ConfigurationBuilder configuration(long seed, SchemaSpec schema)
    {
        return sharedConfiguration(seed, schema);
    }

    void negativeTest(Function<Run, Boolean> corrupt, BiConsumer<Throwable, Run> validate, int counter, SchemaSpec schemaSpec) throws Throwable
    {
        Configuration config = configuration(counter, schemaSpec)
                               .setCreateSchema(true)
                               .setKeyspaceDdl(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': %d};",
                                                             schemaSpec.keyspace, cluster.size()))
                               .setDropSchema(true)
                               .build();

        Run run = config.createRun();

        new Runner.ChainRunner(run, config,
                               Arrays.asList(writer(ITERATIONS, 2, TimeUnit.MINUTES),
                                             (r,  c) -> new Runner.SingleVisitRunner(r, c, Collections.singletonList(this::validator)) {
                                                 @Override
                                                 public void runInternal()
                                                 {
                                                     if (!corrupt.apply(run))
                                                     {
                                                         System.out.println("Could not corrupt");
                                                         return;
                                                     }
                                                     try
                                                     {
                                                         super.runInternal();
                                                         throw new ShouldHaveThrownException();
                                                     }
                                                     catch (Throwable t)
                                                     {
                                                         validate.accept(t, run);
                                                     }
                                                 }
                                             })).run();
    }

    public static Configuration.RunnerConfiguration writer(long iterations, int runtime, TimeUnit timeUnit)
    {
        return (run, config) -> {
            return new UpToLtsRunner(run, config,
                                     Collections.singletonList((r_) -> new MutatingVisitor(r_, MutatingRowVisitor::new)),
                                     iterations,
                                     runtime, timeUnit);
        };
    }

    public static class ShouldHaveThrownException extends AssertionError
    {

    }
}

