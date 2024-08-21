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

package org.apache.cassandra.harry;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.harry.clock.OffsetClock;
import org.apache.cassandra.harry.core.Configuration;
import org.apache.cassandra.harry.ddl.ColumnSpec;
import org.apache.cassandra.harry.ddl.SchemaGenerators;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.dsl.ReplayingHistoryBuilder;
import org.apache.cassandra.harry.gen.Surjections;
import org.apache.cassandra.harry.model.OpSelectors;
import org.apache.cassandra.harry.runner.HarryRunner;
import org.apache.cassandra.harry.sut.SystemUnderTest;
import org.apache.cassandra.harry.sut.TokenPlacementModel;
import org.apache.cassandra.harry.tracker.DefaultDataTracker;

import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_ALLOW_SIMPLE_STRATEGY;
import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_MINIMUM_REPLICATION_FACTOR;
import static org.apache.cassandra.config.CassandraRelevantProperties.DISABLE_TCACTIVE_OPENSSL;
import static org.apache.cassandra.config.CassandraRelevantProperties.IO_NETTY_TRANSPORT_NONATIVE;
import static org.apache.cassandra.config.CassandraRelevantProperties.LOG4J2_DISABLE_JMX;
import static org.apache.cassandra.config.CassandraRelevantProperties.LOG4J2_DISABLE_JMX_LEGACY;
import static org.apache.cassandra.config.CassandraRelevantProperties.LOG4J_SHUTDOWN_HOOK_ENABLED;
import static org.apache.cassandra.config.CassandraRelevantProperties.ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION;

public class HarryHelper
{
    public static final String KEYSPACE = "harry";

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
    }
    
    public static Configuration configuration(String... args) throws Exception
    {
        File configFile = HarryRunner.loadConfig(args);
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
               .setClusteringDescriptorSelector(defaultClusteringDescriptorSelectorConfiguration().build())
               .setPartitionDescriptorSelector(new Configuration.DefaultPDSelectorConfiguration(1, 1))
               .setDataTracker(new Configuration.DefaultDataTrackerConfiguration())
               .setRunner((run, configuration) -> {
                   throw new IllegalArgumentException("Runner is not configured by default.");
               })
               .setMetricReporter(new Configuration.NoOpMetricReporterConfiguration());
    }

    public static ReplayingHistoryBuilder dataGen(SystemUnderTest sut, TokenPlacementModel.ReplicationFactor rf, SystemUnderTest.ConsistencyLevel writeCl)
    {
        return dataGen(1, sut, rf, writeCl);
    }

    public static ReplayingHistoryBuilder dataGen(long seed, SystemUnderTest sut, TokenPlacementModel.ReplicationFactor rf, SystemUnderTest.ConsistencyLevel writeCl)
    {
        SchemaSpec schema = schemaSpecGen("harry", "tbl_").inflate(seed);
        return new ReplayingHistoryBuilder(seed, 100, 1, new DefaultDataTracker(), sut, schema, rf, writeCl);
    }

    public static Configuration.CDSelectorConfigurationBuilder defaultClusteringDescriptorSelectorConfiguration()
    {
        return new Configuration.CDSelectorConfigurationBuilder()
               .setOperationsPerLtsDistribution(new Configuration.ConstantDistributionConfig(1))
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
               .setOperationsPerLtsDistribution(new Configuration.ConstantDistributionConfig(1))
               .setMaxPartitionSize(100)
               .setOperationKindWeights(new Configuration.OperationKindSelectorBuilder()
                                        .addWeight(OpSelectors.OperationKind.INSERT_WITH_STATICS, 100)
                                        .build());
    }
}

