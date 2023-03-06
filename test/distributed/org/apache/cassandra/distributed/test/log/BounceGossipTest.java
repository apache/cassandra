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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import harry.core.Configuration;
import harry.ddl.SchemaSpec;
import harry.visitors.MutatingVisitor;
import harry.visitors.RandomPartitionValidator;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.harry.ClusterState;
import org.apache.cassandra.distributed.harry.ExistingClusterSUT;
import org.apache.cassandra.distributed.harry.FlaggedRunner;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.concurrent.CountDownLatch;

import static harry.core.Configuration.VisitorPoolConfiguration.pool;
import static harry.ddl.ColumnSpec.asciiType;
import static harry.ddl.ColumnSpec.ck;
import static harry.ddl.ColumnSpec.int64Type;
import static harry.ddl.ColumnSpec.pk;
import static harry.ddl.ColumnSpec.regularColumn;
import static harry.ddl.ColumnSpec.staticColumn;
import static java.util.Arrays.asList;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.fail;
import static org.psjava.util.AssertStatus.assertTrue;

public class BounceGossipTest extends TestBaseImpl
{
    @Test
    public void bounceTest() throws Exception
    {
        try (Cluster cluster = init(builder().withNodes(3)
                                             .withConfig(config -> config.with(NETWORK, GOSSIP)
                                                                         .set(Constants.KEY_DTEST_FULL_STARTUP, true))
                                             .start()))
        {
            ExecutorService es = executorFactory().pooled("harry", 1);

            SchemaSpec schema = new SchemaSpec("harry", "test_table",
                                               asList(pk("pk1", asciiType), pk("pk1", int64Type)),
                                               asList(ck("ck1", asciiType), ck("ck1", int64Type)),
                                               asList(regularColumn("regular1", asciiType), regularColumn("regular1", int64Type)),
                                               asList(staticColumn("static1", asciiType), staticColumn("static1", int64Type)));
            AtomicInteger down = new AtomicInteger(0);
            ClusterState clusterState = (i) -> down.get() == i;
            Configuration config = Configuration.fromFile("conf/example.yaml")
                                                .unbuild()
                                                .setKeyspaceDdl(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': %d};", schema.keyspace, 3))
                                                .setSUT(new ExistingClusterSUT(cluster, clusterState))
                                                .build();

            CountDownLatch stopLatch = CountDownLatch.newCountDownLatch(1);
            Future<?> f = es.submit(() -> {
                try
                {
                    new FlaggedRunner(config.createRun(),
                                      config,
                                      asList(pool("Writer", 1, MutatingVisitor::new),
                                             pool("Reader", 1, RandomPartitionValidator::new)),
                                      stopLatch).run();
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            });
            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);

            down.set(3);
            cluster.get(3).shutdown(true).get();
            cluster.get(1).logs().watchFor("/127.0.0.3:.* is now DOWN");
            Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
            cluster.get(3).startup();
            cluster.get(1).logs().watchFor("/127.0.0.3:.* is now UP");
            down.set(0);
            stopLatch.decrement();
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
                                    fail("shouldn't reach this, but epstate: "+epstate);
                                }
                                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
                            }
                        }
                    }
                });

            }

        }
    }
}
