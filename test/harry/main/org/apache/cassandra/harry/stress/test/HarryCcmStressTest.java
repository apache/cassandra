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

package org.apache.cassandra.harry.stress.test;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import org.apache.cassandra.harry.ColumnSpec;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.checker.TestHelper;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.Generators;
import org.apache.cassandra.harry.stress.ExternalClusterSut;
import org.apache.cassandra.harry.stress.HarryStress;
import org.apache.cassandra.harry.stress.RotationStrategy;
import org.apache.cassandra.harry.stress.VisitGenerator;
import org.apache.cassandra.harry.stress.distribution.Distributions;
import org.apache.cassandra.service.consensus.TransactionalMode;

public class HarryCcmStressTest
{
    public static final Logger LOGGER = LoggerFactory.getLogger(HarryStressTest.class);

    @Test
    public void stressTest() throws Throwable
    {
        main();
    }

    public static Generator<SchemaSpec>  trivialSchema(String ks, String table, SchemaSpec.Options options)
    {
        return (rng) -> {
            List<ColumnSpec<?>> pks = new ArrayList<>();
            for (int i = 0; i < 2; i++)
                pks.add(ColumnSpec.pk("pk" + i, ColumnSpec.asciiType, Generators.ascii(10, 20)));

            List<ColumnSpec<?>> cks = new ArrayList<>();
            for (int i = 0; i < 2; i++)
                cks.add(ColumnSpec.ck("ck" + i, ColumnSpec.asciiType, Generators.ascii(10, 20), false));

            List<ColumnSpec<?>> regularColumns = new ArrayList<>();
            for (int i = 0; i < 2; i++)
                regularColumns.add(ColumnSpec.regularColumn("regular" + i, ColumnSpec.asciiType, Generators.ascii(10, 20)));

            List<ColumnSpec<?>> staticColumns = new ArrayList<>();
            for (int i = 0; i < 2; i++)
                staticColumns.add(ColumnSpec.staticColumn("static" + i, ColumnSpec.asciiType, Generators.ascii(10, 200)));

            return new SchemaSpec(ks, table,
                                  pks, cks, regularColumns, staticColumns,
                                  options);
        };
    }

    public static void main(String... args)
    {
        TestHelper.withRandom(1, rng -> {
            SchemaSpec schema = trivialSchema("ks" + System.currentTimeMillis(), "tbl",
                                              SchemaSpec.optionsBuilder()
                                                        .withTransactionalMode(TransactionalMode.full)
                                                        .addWriteTimestamps(false)
                                                        .build())
                                .generate(rng);
            // TODO: move
            {
                Session sut = new Cluster.Builder()
                              .addContactPoint("127.0.0.1")
                              .withPort(9042)
                              .withQueryOptions(new QueryOptions().setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.QUORUM))
                              .build()
                              .connect();
                sut.execute(String.format("CREATE KEYSPACE %s WITH replication = { 'class' : 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'datacenter1': '1' };",
                                          ////            cluster.schemaChange(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};",
//                                               "ks"));

                                          schema.keyspace));
//                sut.execute(String.format("create keyspace %s with replication = {'class':'SimpleStrategy', 'replication_factor':1}",
//                                          schema.keyspace));

                sut.execute(schema.compile());
            }

            HarryStress visitGenerator = new HarryStress(schema,
                                                         Distributions.fixed(1),
                                                         column -> Distributions.fixed(100),
                                                         Generators.pick(VisitGenerator.VisitType.values()),
                                                         Distributions.fixed(1),
                                                         new VisitGenerator.RandomOpKindGenFactory(),
                                                         new RotationStrategy.RandomRotationStrategy(100),
                                                         null, 30,
                                                         () -> {
                                                             Session sut = new Cluster.Builder()
                                                                           .addContactPoint("127.0.0.1")
                                                                           .withPort(9042)
                                                                           .withQueryOptions(new QueryOptions().setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.QUORUM))
                                                                           .build()
                                                                           .connect();

                                                             return (statement, run) -> {
                                                                 while (true)
                                                                 {
                                                                     try
                                                                     {
                                                                         Object[][] rs = ExternalClusterSut.resultSetToObjectArray(sut.execute(statement.cql(),
                                                                                                                                      statement.bindings()));
                                                                         if (run != null)
                                                                            run.run();
                                                                         return rs;
                                                                     }
                                                                     catch (Throwable t)
                                                                     {
                                                                         t.printStackTrace();
                                                                         LOGGER.error("Failed to execute statement, sleeping before retrying", t);
                                                                         sleepUninterruptibly(100);
                                                                     }
                                                                 }
                                                             };
                                                         },
                                                         2,
                                                         10_000,
                                                         0,
                                                         Long.MAX_VALUE,
                                                         0);

            visitGenerator.start(Long.MAX_VALUE, Long.MAX_VALUE);
        });
    }

    private static void sleepUninterruptibly(long millis)
    {
        try
        {
            Thread.sleep(millis);
        }
        catch (InterruptedException e)
        {
            return;
        }
    }
}

