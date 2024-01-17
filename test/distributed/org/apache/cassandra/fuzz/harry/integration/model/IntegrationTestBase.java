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

import java.util.function.Consumer;
import java.util.function.Supplier;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.harry.core.Configuration;
import org.apache.cassandra.harry.ddl.SchemaGenerators;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.clock.OffsetClock;
import org.apache.cassandra.harry.model.OpSelectors;
import org.apache.cassandra.harry.sut.injvm.InJvmSut;
import org.apache.cassandra.harry.sut.injvm.InJvmSutBase;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.test.TestBaseImpl;

public class IntegrationTestBase extends TestBaseImpl
{
    protected static final Logger logger = LoggerFactory.getLogger(IntegrationTestBase.class);
    protected static Cluster cluster;
    protected static InJvmSut sut;

    @BeforeClass
    public static void before() throws Throwable
    {
        init(1, InJvmSutBase.defaultConfig());
    }

    protected static void init(int nodes, Consumer<IInstanceConfig> cfg) throws Throwable
    {
        cluster = Cluster.build()
                         .withNodes(nodes)
                         .withConfig(cfg)
                         .createWithoutStarting();
        cluster.setUncaughtExceptionsFilter(t -> {
            logger.error("Caught exception, reporting during shutdown. Ignoring.", t);
            return true;
        });
        cluster.startup();
        cluster = init(cluster);
        sut = new InJvmSut(cluster);
    }
    @AfterClass
    public static void afterClass()
    {
        sut.shutdown();
    }

    @Before
    public void beforeEach()
    {
        cluster.schemaChange("DROP KEYSPACE IF EXISTS harry");
        cluster.schemaChange("CREATE KEYSPACE harry WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
    }

    private static long seed = 0;
    public static Supplier<Configuration.ConfigurationBuilder> sharedConfiguration()
    {
        Supplier<SchemaSpec> specGenerator = SchemaGenerators.progression(SchemaGenerators.DEFAULT_SWITCH_AFTER);
        return () -> {
            SchemaSpec schemaSpec = specGenerator.get();
            return sharedConfiguration(seed, schemaSpec);
        };
    }

    public static Configuration.CDSelectorConfigurationBuilder sharedCDSelectorConfiguration()
    {
        return new Configuration.CDSelectorConfigurationBuilder()
               .setOperationsPerLtsDistribution(new Configuration.ConstantDistributionConfig(2))
               .setMaxPartitionSize(100)
               .setOperationKindWeights(new Configuration.OperationKindSelectorBuilder()
                                        .addWeight(OpSelectors.OperationKind.DELETE_ROW, 1)
                                        .addWeight(OpSelectors.OperationKind.DELETE_COLUMN, 1)
                                        .addWeight(OpSelectors.OperationKind.DELETE_RANGE, 1)
                                        .addWeight(OpSelectors.OperationKind.DELETE_SLICE, 1)
                                        .addWeight(OpSelectors.OperationKind.DELETE_PARTITION, 1)
                                        .addWeight(OpSelectors.OperationKind.DELETE_COLUMN_WITH_STATICS, 5)
                                        .addWeight(OpSelectors.OperationKind.INSERT_WITH_STATICS, 20)
                                        .addWeight(OpSelectors.OperationKind.INSERT, 20)
                                        .addWeight(OpSelectors.OperationKind.UPDATE_WITH_STATICS, 25)
                                        .addWeight(OpSelectors.OperationKind.UPDATE, 25)
                                        .build());
    }

    public static Configuration.ConfigurationBuilder sharedConfiguration(long seed, SchemaSpec schema)
    {
        return new Configuration.ConfigurationBuilder().setSeed(seed)
                                                       .setClock(() -> new OffsetClock(100000))
                                                       .setCreateSchema(true)
                                                       .setTruncateTable(false)
                                                       .setDropSchema(true)
                                                       .setSchemaProvider((seed1, sut) -> schema)
                                                       .setClusteringDescriptorSelector(sharedCDSelectorConfiguration().build())
                                                       .setPartitionDescriptorSelector(new Configuration.DefaultPDSelectorConfiguration(2, 200))
                                                       .setSUT(() -> sut);
    }
}