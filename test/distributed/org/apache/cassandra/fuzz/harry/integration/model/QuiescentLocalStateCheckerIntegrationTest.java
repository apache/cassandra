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

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.harry.core.Configuration;
import org.apache.cassandra.harry.ddl.SchemaGenerators;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.sut.TokenPlacementModel;
import org.apache.cassandra.harry.sut.injvm.InJvmSut;
import org.apache.cassandra.harry.sut.injvm.InJvmSutBase;
import org.apache.cassandra.harry.sut.injvm.QuiescentLocalStateChecker;
import org.apache.cassandra.harry.runner.Runner;
import org.apache.cassandra.harry.visitors.AllPartitionsValidator;
import org.apache.cassandra.harry.visitors.MutatingVisitor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;

public class QuiescentLocalStateCheckerIntegrationTest extends ModelTestBase
{
    @BeforeClass
    public static void before() throws Throwable
    {
        cluster = init(Cluster.build()
                              .withNodes(2)
                              .withConfig(InJvmSutBase.defaultConfig().andThen((cfg) -> cfg.with(Feature.GOSSIP)))
                              .start());
        sut = new InJvmSut(cluster);
    }

    @Test
    public void testQuiescentLocalStateChecker() throws Throwable
    {
        Supplier<SchemaSpec> supplier = SchemaGenerators.progression(1);
        for (int i = 0; i < SchemaGenerators.GENERATORS_COUNT; i++)
        {
            SchemaSpec schema = supplier.get();

            Configuration config = configuration(i, schema)
                                   .setKeyspaceDdl("CREATE KEYSPACE IF NOT EXISTS " + schema.keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};")
                                   .setCreateSchema(true)
                                   .setDropSchema(true)
                                   .build();

            Runner.chain(config,
                         Runner.sequential(MutatingVisitor::new, 2, TimeUnit.SECONDS),
                         Runner.single(AllPartitionsValidator.factoryForTests(1, QuiescentLocalStateChecker.factory(new TokenPlacementModel.SimpleReplicationFactor(1)))))
                  .run();
            break;
        }
    }

    @Override
    protected Configuration.ModelConfiguration modelConfiguration()
    {
        return new Configuration.QuiescentCheckerConfig();
    }
}