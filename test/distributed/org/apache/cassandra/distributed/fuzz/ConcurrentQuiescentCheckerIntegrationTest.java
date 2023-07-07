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
import java.util.function.Supplier;

import org.junit.Test;

import harry.core.Configuration;
import harry.core.Run;
import harry.ddl.SchemaSpec;
import harry.generators.Surjections;
import harry.model.sut.injvm.InJvmSut;
import harry.runner.LockingDataTracker;
import harry.runner.Runner;
import harry.visitors.MutatingVisitor;
import harry.visitors.QueryLogger;
import harry.visitors.RandomPartitionValidator;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static harry.core.Configuration.VisitorPoolConfiguration.pool;
import static java.util.Arrays.asList;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class ConcurrentQuiescentCheckerIntegrationTest extends TestBaseImpl
{

    private static Supplier<SchemaSpec> schemaSupplier()
    {
        Surjections.Surjection<SchemaSpec> schemaGen = HarryHelper.schemaSpecGen("harry", "table_");
        return new Supplier<SchemaSpec>()
        {
            int schemaIdx = 0;
            public SchemaSpec get()
            {
                while (true)
                {
                    SchemaSpec schema = schemaGen.inflate(schemaIdx++);
                    try
                    {
                        schema.validate();
                        return schema;
                    }
                    catch (AssertionError e)
                    {
                        continue;
                    }
                }
            }
        };
    }

    @Test
    public void testWithConcurrentQuiescentChecker() throws Throwable
    {
        final int writeThreads = 2;
        final int readThreads = 2;

        try (ICluster<IInvokableInstance> cluster = builder().withNodes(3)
                                                       .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL))
                                                       .start())
        {
            Supplier<SchemaSpec> schemaSupplier = schemaSupplier();

            for (int i = 0; i < 3; i++)
            {
                SchemaSpec schema = schemaSupplier.get();
                Configuration config = HarryHelper
                                       .defaultConfiguration()
                                       .setSeed(1L)
                                       .setClock(new Configuration.ApproximateMonotonicClockConfiguration((int) TimeUnit.MINUTES.toSeconds(20), 1, TimeUnit.SECONDS))
                                       .setSUT(() -> new InJvmSut(cluster))
                                       .setKeyspaceDdl("CREATE KEYSPACE IF NOT EXISTS " + schema.keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};")
                                       .setSchemaProvider((seed, sut) -> schema)
                                       .setCreateSchema(true)
                                       .setDropSchema(true)
                                       .setDataTracker(LockingDataTracker::new)
                                       .build();

                Run run = config.createRun();
                try
                {
                    new Runner.ConcurrentRunner(config.createRun(), config,
                                                asList(pool("Writer", writeThreads, MutatingVisitor::new),
                                                       pool("Reader", readThreads, (run_) -> new RandomPartitionValidator(run_, new Configuration.QuiescentCheckerConfig(), QueryLogger.NO_OP))),
                                                1, TimeUnit.MINUTES)
                    .run();
                }
                catch (Throwable e)
                {
                    Configuration.toFile(new File(String.format("failure-%d.yaml", config.seed)),
                                         config.unbuild()
                                               .setClock(run.clock.toConfig())
                                               .setDataTracker(run.tracker.toConfig())
                                               .build());
                    throw e;
                }
            }
        }
    }
}