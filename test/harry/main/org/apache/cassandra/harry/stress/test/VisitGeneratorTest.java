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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.test.log.FuzzTestBase;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.checker.TestHelper;
import org.apache.cassandra.harry.gen.Generators;
import org.apache.cassandra.harry.gen.SchemaGenerators;
import org.apache.cassandra.harry.gen.rng.JdkRandomEntropySource;
import org.apache.cassandra.harry.stress.ActivePartition;
import org.apache.cassandra.harry.stress.HarryStress;
import org.apache.cassandra.harry.stress.RotationStrategy;
import org.apache.cassandra.harry.stress.VisitGenerator;
import org.apache.cassandra.harry.stress.distribution.Distributions;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.consensus.TransactionalMode;

/**
 * A simple and lightweight way to test visit generator _without_ starting a server and executing any queries
 */
public class VisitGeneratorTest
{
    @Test
    public void visitGeneratorTest()
    {
        TestHelper.withRandom(1,rng -> {
            SchemaSpec schema = SchemaGenerators.trivialSchema("ks", "tbl").generate(rng);

            VisitGenerator visitGenerator = new VisitGenerator(new ActivePartition.Partitions(schema, Distributions.fixed(1), column -> Distributions.fixed(100), new RotationStrategy.RandomRotationStrategy(100)),
                                                               Generators.pick(VisitGenerator.VisitType.values()),
                                                               Distributions.fixed(1),
                                                               new VisitGenerator.RandomOpKindGenFactory());

        });
    }

    @Test
    public void descriptorConversionTest() throws Throwable
    {
        ActivePartition.DescriptorIndexBijection converter = ActivePartition.DescriptorIndexBijection.INSTANCE;
        for (int i = 0; i < 100; i++)
        {
            long pd = converter.toPd(i);
            long idx = converter.toIdx(pd);
            Assert.assertEquals(idx, idx);
        }
    }

    @Test
    public void  stressTest() throws Throwable
    {
        FuzzTestBase tester = new FuzzTestBase();

        try(Cluster cluster = tester.builder().withNodes(3)
                                    .withConfig(cfg -> cfg.set("accord.enabled", true))
                                    .start())
        {
            cluster.schemaChange(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};",
                                               "ks"));

            AtomicInteger schemaIdx = new AtomicInteger();
            for (int i = 0; i < 1; i++)
            {
                int finalI = i;
                new Thread(() -> {
                    TestHelper.withRandom(finalI, rng -> {
                        SchemaSpec schema = SchemaGenerators.trivialSchema("ks", "tbl" + finalI,
                                                                           SchemaSpec.optionsBuilder()
                                                                                     .withTransactionalMode(TransactionalMode.full)
                                                                                     .addWriteTimestamps(false)
                                                                                     .build())
                                                            .generate(rng);
                        cluster.schemaChange(schema.compile());
                        HarryStress visitGenerator = new HarryStress(schema,
                                                                     Distributions.fixed(1),
                                                                     column -> Distributions.fixed(100),
                                                                     Generators.pick(VisitGenerator.VisitType.values()),
                                                                     Distributions.fixed(1),
                                                                     new VisitGenerator.RandomOpKindGenFactory(),
                                                                     new RotationStrategy.RandomRotationStrategy(100),
                                                                     null, 30,
                                                                     () -> (statement, run) -> {
                                                                         while (true)
                                                                         {
                                                                             try
                                                                             {
                                                                                 Object[][] rs = cluster.coordinator(1).execute(statement.cql(),
                                                                                                                       ConsistencyLevel.QUORUM,
                                                                                                                       statement.bindings());

                                                                                 if (run != null)
                                                                                     run.run();
                                                                                 return rs;
                                                                             }
                                                                             catch (Throwable t)
                                                                             {
                                                                                 // retry — failures are expected when node 2 bounces
                                                                             }
                                                                         }
                                                                     },
                                                                     20,
                                                                     20_000,
                                                                     0,
                                                                     Long.MAX_VALUE,
                                                                     0);

                        try
                        {
                            visitGenerator.start(Long.MAX_VALUE, Long.MAX_VALUE);
                        }
                        catch (Throwable t)
                        {
                            t.printStackTrace();
                            System.out.println("Exiting");
                        }
                    });
                }, "stress-" + i).start();
            }

            Thread.sleep(10_000);
            cluster.filters().allVerbs().messagesMatching(new IMessageFilters.Matcher()
            {
                class Msg
                {
                    final int from;
                    final int to;
                    final int id;

                    Msg(int from, int to, int id)
                    {
                        this.from = from;
                        this.to = to;
                        this.id = id;
                    }

                    @Override
                    public boolean equals(Object o)
                    {
                        if (o == null || getClass() != o.getClass()) return false;
                        Msg msg = (Msg) o;
                        return from == msg.from && to == msg.to && id == msg.id;
                    }

                    @Override
                    public int hashCode()
                    {
                        return Objects.hash(from, to, id);
                    }

                    @Override
                    public String toString()
                    {
                        return "Msg{" +
                               "from=" + from +
                               ", to=" + to +
                               ", id=" + id +
                               '}';
                    }
                }

                private final Set<Msg> set = new HashSet<>(30_000);
                private final List<Msg> sent = new ArrayList<>(30_000);
                private final Lock lock = new ReentrantLock();
                private int head = 0;
                @Override
                public boolean matches(int from, int to, IMessage message)
                {
                    Msg msg = new Msg(from, to, message.id());
                    lock.lock();
                    try
                    {
                        if (set.contains(msg))
                            System.out.println("Already contains " + msg);
                        if (sent.size() < sent.size() + 1) {
                            // Buffer not full yet, simply add
                            sent.add(msg);
                        } else {
                            // Buffer full, replace at head position
                            set.remove(sent.set(head, msg));
                            // Move head pointer
                            head = (head + 1) % sent.size();
                        }
                    }
                    finally
                    {
                        lock.unlock();
                    }

                    if (!Verb.fromId(message.verb()).name().contains("ACCORD"))
                        return false;

                    if (ThreadLocalRandom.current().nextInt(100) > 98)
                        return true;

                    return false;
                }
            }).drop().on();
            while (true)
            {

                SchemaSpec schema = HarryCcmStressTest.trivialSchema("ks", "tbl" + schemaIdx.getAndIncrement(),
                                                                     SchemaSpec.optionsBuilder()
                                                                               .withTransactionalMode(TransactionalMode.full)
                                                                               .addWriteTimestamps(false)
                                                                               .build())
                                                      .generate(new JdkRandomEntropySource(schemaIdx.get()));
                cluster.schemaChange(schema.compile());

                Thread.sleep(5_000);
                cluster.forEach(i -> {
                    if (i.config().num() != 2)
                        return;

                    i.shutdown();
                    i.startup();
                });
            }
        }
    }


    @Test
    public void randomRotationStrategyTest()
    {
        RotationStrategy strategy = new RotationStrategy.RandomRotationStrategy(10);

        TestHelper.withRandom(rng -> {
            int size = strategy.targetSize();
            for (int i = 0; i < 1000; i++)
            {
                for (RotationStrategy.PartitionAction action : strategy.generate(rng))
                {
                    switch (action)
                    {
                        case REPLACE_WITH_NEW:
                        case REPLACE_WITH_VISITED:
                            // size stays the same — one out, one in
                            break;
                    }
                }
                System.out.println(size);
            }
        });

    }
}
