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

package org.apache.cassandra.distributed.fuzz;

import java.io.File;
import java.util.concurrent.TimeUnit;

import harry.core.Configuration;
import harry.model.OpSelectors;
import harry.model.clock.OffsetClock;
import harry.model.sut.PrintlnSut;

public class HarryHelper
{
    public static void init()
    {
        System.setProperty("log4j2.disableJmx", "true"); // setting both ways as changes between versions
        System.setProperty("log4j2.disable.jmx", "true");
        System.setProperty("log4j.shutdownHookEnabled", "false");
        System.setProperty("cassandra.allow_simplestrategy", "true"); // makes easier to share OSS tests without RF limits
        System.setProperty("cassandra.minimum_replication_factor", "0"); // makes easier to share OSS tests without RF limits

        System.setProperty("cassandra.disable_tcactive_openssl", "true");
        System.setProperty("relocated.shaded.io.netty.transport.noNative", "true");
        System.setProperty("org.apache.cassandra.disable_mbean_registration", "true");

        InJvmSut.init();
        QueryingNoOpChecker.init();
        Configuration.registerSubtypes(PrintlnSut.PrintlnSutConfiguration.class);
        Configuration.registerSubtypes(Configuration.NoOpMetricReporterConfiguration.class);
        Configuration.registerSubtypes(Configuration.RecentPartitionsValidatorConfiguration.class);
    }
    
    public static Configuration configuration(String... args) throws Exception
    {
        File configFile = harry.runner.HarryRunner.loadConfig(args);
        Configuration configuration = Configuration.fromFile(configFile);
        System.out.println("Using configuration generated from: " + configFile);
        return configuration;
    }

    public static Configuration.ConfigurationBuilder defaultConfiguration() throws Exception
    {
        return new Configuration.ConfigurationBuilder()
               .setClock(() -> new OffsetClock(100000))
               .setCreateSchema(true)
               .setTruncateTable(false)
               .setDropSchema(false)
               .setSchemaProvider(new Configuration.DefaultSchemaProviderConfiguration())
               .setClock(new Configuration.ApproximateMonotonicClockConfiguration(7300, 1, TimeUnit.SECONDS))
               .setClusteringDescriptorSelector(defaultClusteringDescriptorSelectorConfiguration().build())
               .setPartitionDescriptorSelector(new Configuration.DefaultPDSelectorConfiguration(100, 10))
               .setSUT(new PrintlnSut.PrintlnSutConfiguration())
               .setDataTracker(new Configuration.DefaultDataTrackerConfiguration())
               .setRunner((run, configuration) -> {
                   throw new IllegalArgumentException("Runner is not configured by default.");
               })
               .setMetricReporter(new Configuration.NoOpMetricReporterConfiguration());
    }

    public static Configuration.CDSelectorConfigurationBuilder defaultClusteringDescriptorSelectorConfiguration()
    {
        return new Configuration.CDSelectorConfigurationBuilder()
               .setNumberOfModificationsDistribution(new Configuration.ConstantDistributionConfig(2))
               .setRowsPerModificationDistribution(new Configuration.ConstantDistributionConfig(2))
               .setMaxPartitionSize(100)
               .setOperationKindWeights(new Configuration.OperationKindSelectorBuilder()
                                        .addWeight(OpSelectors.OperationKind.DELETE_ROW, 1)
                                        .addWeight(OpSelectors.OperationKind.DELETE_COLUMN, 1)
                                        .addWeight(OpSelectors.OperationKind.DELETE_RANGE, 1)
                                        .addWeight(OpSelectors.OperationKind.DELETE_SLICE, 1)
                                        .addWeight(OpSelectors.OperationKind.DELETE_PARTITION, 1)
                                        .addWeight(OpSelectors.OperationKind.DELETE_COLUMN_WITH_STATICS, 1)
                                        .addWeight(OpSelectors.OperationKind.INSERT_WITH_STATICS, 20)
                                        .addWeight(OpSelectors.OperationKind.INSERT, 20)
                                        .addWeight(OpSelectors.OperationKind.UPDATE_WITH_STATICS, 20)
                                        .addWeight(OpSelectors.OperationKind.UPDATE, 20)
                                        .build());
    }
}
