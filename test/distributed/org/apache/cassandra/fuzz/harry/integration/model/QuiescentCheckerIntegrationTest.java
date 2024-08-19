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

import org.junit.Test;

import org.apache.cassandra.harry.core.Configuration;
import org.apache.cassandra.harry.core.Configuration.AllPartitionsValidatorConfiguration;
import org.apache.cassandra.harry.core.Configuration.ConcurrentRunnerConfig;
import org.apache.cassandra.harry.core.Configuration.LoggingVisitorConfiguration;
import org.apache.cassandra.harry.core.Configuration.RecentPartitionsValidatorConfiguration;
import org.apache.cassandra.harry.core.Configuration.SequentialRunnerConfig;
import org.apache.cassandra.harry.core.Configuration.SingleVisitRunnerConfig;
import org.apache.cassandra.harry.core.Run;
import org.apache.cassandra.harry.corruptor.AddExtraRowCorruptor;
import org.apache.cassandra.harry.corruptor.ChangeValueCorruptor;
import org.apache.cassandra.harry.corruptor.HideRowCorruptor;
import org.apache.cassandra.harry.corruptor.HideValueCorruptor;
import org.apache.cassandra.harry.corruptor.QueryResponseCorruptor;
import org.apache.cassandra.harry.corruptor.QueryResponseCorruptor.SimpleQueryResponseCorruptor;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.model.Model;
import org.apache.cassandra.harry.operations.Query;
import org.apache.cassandra.harry.runner.StagedRunner.StagedRunnerConfig;
import org.apache.cassandra.harry.visitors.QueryLogger;
import org.apache.cassandra.harry.visitors.SingleValidator;

public class QuiescentCheckerIntegrationTest extends ModelTestBase
{
    private final long CORRUPT_LTS = 0L;

    @Override
    protected SingleValidator validator(Run run)
    {
        return new SingleValidator(100, run, modelConfiguration()) {
            public void visit()
            {
                visit(CORRUPT_LTS);
            }
        };
    }

    @Test
    public void testNormalCondition() throws Throwable
    {
        negativeTest((run) -> true,
                     (t, run) -> {
                         if (!(t instanceof ShouldHaveThrownException))
                             throw new AssertionError(String.format("Throwable was supposed to be null. Schema: %s",
                                                                    run.schemaSpec.compile().cql()),
                                                      t);
                     });
    }

    @Test
    public void normalConditionIntegrationTest() throws Throwable
    {
        Model.ModelFactory factory = modelConfiguration();
                
        SequentialRunnerConfig sequential = 
                new SequentialRunnerConfig(Arrays.asList(new LoggingVisitorConfiguration(new Configuration.MutatingRowVisitorConfiguration()),
                                                         new RecentPartitionsValidatorConfiguration(10, 10, factory::make, () -> QueryLogger.NO_OP),
                                                         new AllPartitionsValidatorConfiguration(10, factory::make, () -> QueryLogger.NO_OP)),
                                           1, TimeUnit.MINUTES);
        negativeIntegrationTest(sequential);
    }

    @Test
    public void normalConditionStagedIntegrationTest() throws Throwable
    {
        Model.ModelFactory factory = modelConfiguration();

        ConcurrentRunnerConfig concurrent = 
                new ConcurrentRunnerConfig(Arrays.asList(new Configuration.VisitorPoolConfiguration("Writer", 4, new Configuration.MutatingVisitorConfiguation(new Configuration.MutatingRowVisitorConfiguration()))),
                                           30, TimeUnit.SECONDS);
        SingleVisitRunnerConfig sequential = 
                new SingleVisitRunnerConfig(Collections.singletonList(new RecentPartitionsValidatorConfiguration(1024, 0, factory::make, () -> QueryLogger.NO_OP)));
        
        StagedRunnerConfig staged = new StagedRunnerConfig(Arrays.asList(concurrent, sequential), 2, TimeUnit.MINUTES);

        negativeIntegrationTest(staged);
    }

    @Test
    public void testDetectsMissingRow() throws Throwable
    {
        negativeTest((run) -> {
                         SimpleQueryResponseCorruptor corruptor = new SimpleQueryResponseCorruptor(run.schemaSpec,
                                                                                                   run.clock,
                                                                                                   HideRowCorruptor::new);

                         Query query = Query.selectAllColumns(run.schemaSpec,
                                                              run.pdSelector.pd(CORRUPT_LTS, run.schemaSpec),
                                                              false);

                         return corruptor.maybeCorrupt(query, run.sut);
                     },
                     (t, run) -> {
                         // TODO: We can actually pinpoint the difference
                         String expected = "Expected results to have the same number of results, but expected result iterator has more results";
                         String expected2 = "Found a row in the model that is not present in the resultset";

                         if (t.getMessage().contains(expected) || t.getMessage().contains(expected2))
                             return;

                         throw new AssertionError(String.format("Exception string mismatch.\nExpected error: %s.\nActual error: %s", expected, t.getMessage()),
                                                  t);
                     });
    }

    @Test
    public void testDetectsExtraRow() throws Throwable
    {
        negativeTest((run) -> {
                         QueryResponseCorruptor corruptor = new AddExtraRowCorruptor(run.schemaSpec,
                                                                                     run.clock,
                                                                                     run.tracker,
                                                                                     run.descriptorSelector);

                         return corruptor.maybeCorrupt(Query.selectAllColumns(run.schemaSpec,
                                                                              run.pdSelector.pd(CORRUPT_LTS, run.schemaSpec),
                                                                              false),
                                                       run.sut);
                     },
                     (t, run) -> {
                         String expected = "Found a row in the model that is not present in the resultset";
                         String expected2 = "Expected results to have the same number of results, but actual result iterator has more results";
                         String expected3 = "Found a row while model predicts statics only";
                         if (t.getMessage().contains(expected) || t.getMessage().contains(expected2) || t.getMessage().contains(expected3))
                             return;

                         throw new AssertionError(String.format("Exception string mismatch.\nExpected error: %s.\nActual error: %s", expected, t.getMessage()),
                                                  t);
                     });
    }


    @Test
    public void testDetectsRemovedColumn() throws Throwable
    {
        negativeTest((run) -> {
                         SimpleQueryResponseCorruptor corruptor = new SimpleQueryResponseCorruptor(run.schemaSpec,
                                                                                                   run.clock,
                                                                                                   HideValueCorruptor::new);

                         return corruptor.maybeCorrupt(Query.selectAllColumns(run.schemaSpec,
                                                                              run.pdSelector.pd(CORRUPT_LTS, run.schemaSpec),
                                                                              false),
                                                       run.sut);
                     },
                     (t, run) -> {
                         String expected = "doesn't match the one predicted by the model";
                         String expected2 = "don't match ones predicted by the model";
                         String expected3 = "Found a row in the model that is not present in the resultset";
                         if (t.getMessage().contains(expected) || t.getMessage().contains(expected2) || t.getMessage().contains(expected3))
                             return;

                         throw new AssertionError(String.format("Exception string mismatch.\nExpected error: %s.\nActual error: %s", expected, t.getMessage()),
                                                  t);
                     });
    }


    @Test
    public void testDetectsOverwrittenRow() throws Throwable
    {
        negativeTest((run) -> {
                         SimpleQueryResponseCorruptor corruptor = new SimpleQueryResponseCorruptor(run.schemaSpec,
                                                                                                   run.clock,
                                                                                                   ChangeValueCorruptor::new);

                         return corruptor.maybeCorrupt(Query.selectAllColumns(run.schemaSpec,
                                                                              run.pdSelector.pd(CORRUPT_LTS, run.schemaSpec),
                                                                              false),
                                                       run.sut);
                     },
                     (t, run) -> {
                         String expected = "Returned row state doesn't match the one predicted by the model";
                         String expected2 = "Timestamps in the row state don't match ones predicted by the model";

                         if (t.getMessage() != null &&
                             (t.getMessage().contains(expected) || t.getMessage().contains(expected2)))
                             return;

                         throw new AssertionError(String.format("Exception string mismatch.\nExpected error: %s.\nActual error: %s", expected, t.getMessage()),
                                                  t);
                     });
    }

    @Override
    protected Configuration.ModelConfiguration modelConfiguration()
    {
        return new Configuration.QuiescentCheckerConfig();
    }

    public Configuration.ConfigurationBuilder configuration(long seed, SchemaSpec schema)
    {
        return super.configuration(seed, schema)
                    .setClusteringDescriptorSelector(sharedCDSelectorConfiguration()
                                                     .setOperationsPerLtsDistribution(new Configuration.ConstantDistributionConfig(2))
                                                     .setMaxPartitionSize(100)
                                                     .build());
    }
}