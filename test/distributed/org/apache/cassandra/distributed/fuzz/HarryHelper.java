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
import java.util.concurrent.atomic.AtomicInteger;

import harry.core.Configuration;
import harry.ddl.ColumnSpec;
import harry.ddl.SchemaGenerators;
import harry.ddl.SchemaSpec;
import harry.generators.Surjections;
import harry.model.OpSelectors;
import harry.model.clock.OffsetClock;
import harry.model.sut.PrintlnSut;

import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_ALLOW_SIMPLE_STRATEGY;
import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_MINIMUM_REPLICATION_FACTOR;
import static org.apache.cassandra.config.CassandraRelevantProperties.DISABLE_TCACTIVE_OPENSSL;
import static org.apache.cassandra.config.CassandraRelevantProperties.LOG4J2_DISABLE_JMX;
import static org.apache.cassandra.config.CassandraRelevantProperties.LOG4J2_DISABLE_JMX_LEGACY;
import static org.apache.cassandra.config.CassandraRelevantProperties.LOG4J_SHUTDOWN_HOOK_ENABLED;
import static org.apache.cassandra.config.CassandraRelevantProperties.ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION;
import static org.apache.cassandra.config.CassandraRelevantProperties.IO_NETTY_TRANSPORT_NONATIVE;

public class HarryHelper
{
    public static void init()
    {
        // setting both ways as changes between versions
        LOG4J2_DISABLE_JMX.setBoolean(true);
        LOG4J2_DISABLE_JMX_LEGACY.setBoolean(true);
        LOG4J_SHUTDOWN_HOOK_ENABLED.setBoolean(false);
        CASSANDRA_ALLOW_SIMPLE_STRATEGY.setBoolean(true); // makes easier to share OSS tests without RF limits
        CASSANDRA_MINIMUM_REPLICATION_FACTOR.setInt(0); // makes easier to share OSS tests without RF limits
        DISABLE_TCACTIVE_OPENSSL.setBoolean(true);
        IO_NETTY_TRANSPORT_NONATIVE.setBoolean(true);
        ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION.setBoolean(true);

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

    private static AtomicInteger counter = new AtomicInteger();

    public static Surjections.Surjection<SchemaSpec> schemaSpecGen(String keyspace, String prefix)
    {
        return new SchemaGenerators.Builder(keyspace, () -> prefix + counter.getAndIncrement())
               .partitionKeySpec(1, 2,
                                 ColumnSpec.int8Type,
                                 ColumnSpec.int16Type,
                                 ColumnSpec.int32Type,
                                 ColumnSpec.int64Type,
                                 ColumnSpec.doubleType,
                                 ColumnSpec.asciiType,
                                 ColumnSpec.textType)
               .clusteringKeySpec(1, 2,
                                  ColumnSpec.int8Type,
                                  ColumnSpec.int16Type,
                                  ColumnSpec.int32Type,
                                  ColumnSpec.int64Type,
                                  ColumnSpec.doubleType,
                                  ColumnSpec.asciiType,
                                  ColumnSpec.textType,
                                  ColumnSpec.ReversedType.getInstance(ColumnSpec.int64Type),
                                  ColumnSpec.ReversedType.getInstance(ColumnSpec.doubleType),
                                  ColumnSpec.ReversedType.getInstance(ColumnSpec.asciiType),
                                  ColumnSpec.ReversedType.getInstance(ColumnSpec.textType))
               .regularColumnSpec(1, 5,
                                  ColumnSpec.int8Type,
                                  ColumnSpec.int16Type,
                                  ColumnSpec.int32Type,
                                  ColumnSpec.int64Type,
                                  ColumnSpec.floatType,
                                  ColumnSpec.doubleType,
                                  ColumnSpec.asciiType(4, 128))
               .staticColumnSpec(0, 5,
                                 ColumnSpec.int8Type,
                                 ColumnSpec.int16Type,
                                 ColumnSpec.int32Type,
                                 ColumnSpec.int64Type,
                                 ColumnSpec.floatType,
                                 ColumnSpec.doubleType,
                                 ColumnSpec.asciiType(4, 128))
               .surjection();
    }

    public static Configuration.ConfigurationBuilder defaultConfiguration() throws Exception
    {
        return new Configuration.ConfigurationBuilder()
               .setClock(() -> new OffsetClock(100000))
               .setCreateSchema(true)
               .setTruncateTable(false)
               .setDropSchema(false)
               .setSchemaProvider((seed, sut) -> schemaSpecGen("harry", "tbl_").inflate(seed))
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
               .setNumberOfModificationsDistribution(new Configuration.ConstantDistributionConfig(1))
               .setRowsPerModificationDistribution(new Configuration.ConstantDistributionConfig(1))
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

    public static Configuration.CDSelectorConfigurationBuilder singleRowPerModification()
    {
        return new Configuration.CDSelectorConfigurationBuilder()
               .setNumberOfModificationsDistribution(new Configuration.ConstantDistributionConfig(1))
               .setRowsPerModificationDistribution(new Configuration.ConstantDistributionConfig(1))
               .setMaxPartitionSize(100)
               .setOperationKindWeights(new Configuration.OperationKindSelectorBuilder()
                                        .addWeight(OpSelectors.OperationKind.INSERT_WITH_STATICS, 100)
                                        .build());
    }
}

