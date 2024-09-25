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

package org.apache.cassandra.distributed.test.log;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.op.Visit;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.dsl.HistoryBuilderHelper;
import org.apache.cassandra.harry.execution.CQLVisitExecutor;
import org.apache.cassandra.harry.execution.InJvmDTestVisitExecutor;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.Generators;
import org.apache.cassandra.harry.gen.SchemaGenerators;
import org.apache.cassandra.locator.InetAddressAndPort;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.harry.checker.TestHelper.withRandom;
import static org.junit.Assert.fail;
import static org.psjava.util.AssertStatus.assertTrue;

public class BounceGossipTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(BounceGossipTest.class);

    @Test
    public void bounceTest() throws Exception
    {
        Generator<SchemaSpec> schemaGen = SchemaGenerators.schemaSpecGen("bounce_gossip_test", "bounce_test", 1000);
        try (Cluster cluster = init(builder().withNodes(3)
                                             .withConfig(config -> config.with(NETWORK, GOSSIP)
                                                                         .set(Constants.KEY_DTEST_FULL_STARTUP, true))
                                             .start()))
        {

            ExecutorService es = executorFactory().pooled("harry", 1);
            AtomicBoolean stop = new AtomicBoolean(false);

            withRandom(rng -> {
                SchemaSpec schema = schemaGen.generate(rng);
                Generators.TrackingGenerator<Integer> pkGen = Generators.tracking(Generators.int32(0, Math.min(schema.valueGenerators.pkPopulation(), 1000)));
                Generator<Integer> ckGen = Generators.int32(0, Math.min(schema.valueGenerators.ckPopulation(), 1000));

                cluster.schemaChange("CREATE KEYSPACE " + schema.keyspace +
                                     " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};");
                cluster.schemaChange(schema.compile());

                Future<?> f = es.submit(new Runnable()
                {
                    final HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);
                    final Iterator<Visit> iterator = history.iterator();
                    final CQLVisitExecutor executor = InJvmDTestVisitExecutor.builder()
                                                                             .nodeSelector(lts -> 1)
                                                                             .retryPolicy(InJvmDTestVisitExecutor.RetryPolicy.RETRY_ON_TIMEOUT)
                                                                             .build(schema, history, cluster);
                    @Override
                    public void run()
                    {
                        while (!stop.get())
                        {
                            // Rate limit to ~10 per second
                            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(500));
                            HistoryBuilderHelper.insertRandomData(schema, pkGen, ckGen, rng, history);
                            history.selectPartition(pkGen.generate(rng));

                            while (iterator.hasNext() && !stop.get())
                            {
                                try
                                {
                                    executor.execute(iterator.next());
                                }
                                catch (Throwable e)
                                {
                                    if (e.getMessage().contains("Can't use shutdown node3"))
                                    {
                                        // ignore and continue
                                        continue;
                                    }

                                    if (e.getMessage().contains("does not exist"))
                                    {
                                        logger.error(e.getMessage());
                                        // ignore and continue
                                        continue;
                                    }
                                    throw e;
                                }
                            }
                        }
                    }
                });
                Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);

                cluster.get(3).shutdown(true).get();
                cluster.get(1).logs().watchFor("/127.0.0.3:.* is now DOWN");
                Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
                cluster.get(3).startup();
                cluster.get(1).logs().watchFor("/127.0.0.3:.* is now UP");
                stop.set(true);
                f.get();

                for (int inst = 1; inst <= 3; inst++)
                {
                    cluster.get(inst).runOnInstance(() -> {
                        for (int i = 1; i <= 3; i++)
                        {
                            boolean stateOk = false;
                            int tries = 0;
                            while (!stateOk)
                            {
                                EndpointState epstate = Gossiper.instance.getEndpointStateForEndpoint(InetAddressAndPort.getByNameUnchecked("127.0.0." + i));
                                stateOk = epstate.getApplicationState(ApplicationState.STATUS_WITH_PORT).value.contains("NORMAL") &&
                                          epstate.getApplicationState(ApplicationState.TOKENS) != null &&
                                          epstate.getHeartBeatState().getGeneration() > 0;
                                if (!stateOk)
                                {
                                    tries++;
                                    if (tries > 20)
                                    {
                                        assertTrue(epstate.getApplicationState(ApplicationState.STATUS_WITH_PORT).value.contains("NORMAL"));
                                        assertTrue(epstate.getApplicationState(ApplicationState.TOKENS) != null);
                                        assertTrue(epstate.getHeartBeatState().getGeneration() > 0);
                                        fail("shouldn't reach this, but epstate: " + epstate);
                                    }
                                    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
                                }
                            }
                        }
                    });
                }
            });
        }
    }

    @Test
    public void gossipPropagatesVersionTest() throws IOException
    {
        try (Cluster cluster = init(builder().withNodes(3)
                                             .withConfig(config -> config.with(NETWORK, GOSSIP))
                                             .withoutVNodes()
                                             .start()))
        {
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key)"));
            int tokensBefore = getGossipTokensVersion(cluster, 2);
            cluster.get(2).nodetoolResult("move", "9999").asserts().success();
            int correctTokensVersion;
            while ((correctTokensVersion = getGossipTokensVersion(cluster, 2)) == tokensBefore)
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS); // wait for LegacyStateListener to actually update gossip
            for (int inst : new int[]{ 1, 3 })
                while (correctTokensVersion != getGossipTokensVersion(cluster, inst))
                {
                    System.out.println(correctTokensVersion + " ::: " + getGossipTokensVersion(cluster, inst));
                    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);  // wait for gossip to propagate
                    correctTokensVersion = getGossipTokensVersion(cluster, 2);
                }
        }
    }

    private static int getGossipTokensVersion(Cluster cluster, int instance)
    {
        return cluster.get(instance).callOnInstance(() -> Gossiper.instance.endpointStateMap.get(InetAddressAndPort.getByNameUnchecked("127.0.0.2"))
                                                                                            .getApplicationState(ApplicationState.TOKENS).version);
    }
}