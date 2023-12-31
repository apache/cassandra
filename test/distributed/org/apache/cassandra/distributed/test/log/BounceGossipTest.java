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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.harry.HarryHelper;
import org.apache.cassandra.harry.runner.FlaggedRunner;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.harry.core.Configuration;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.sut.injvm.InJvmSut;
import org.apache.cassandra.harry.sut.injvm.InJvmSutBase;
import org.apache.cassandra.harry.visitors.MutatingVisitor;
import org.apache.cassandra.harry.visitors.QueryLogger;
import org.apache.cassandra.harry.visitors.RandomPartitionValidator;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.concurrent.CountDownLatch;

import static java.util.Arrays.asList;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.harry.core.Configuration.VisitorPoolConfiguration.pool;
import static org.apache.cassandra.harry.ddl.ColumnSpec.asciiType;
import static org.apache.cassandra.harry.ddl.ColumnSpec.ck;
import static org.apache.cassandra.harry.ddl.ColumnSpec.int64Type;
import static org.apache.cassandra.harry.ddl.ColumnSpec.pk;
import static org.apache.cassandra.harry.ddl.ColumnSpec.regularColumn;
import static org.apache.cassandra.harry.ddl.ColumnSpec.staticColumn;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
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
            Configuration config = HarryHelper.defaultConfiguration()
                                              .setKeyspaceDdl(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': %d};", schema.keyspace, 3))
                                              .setSUT(() -> new InJvmSut(cluster, () -> 1, InJvmSutBase.retryOnTimeout()))
                                              .build();

            CountDownLatch stopLatch = CountDownLatch.newCountDownLatch(1);
            Future<?> f = es.submit(() -> {
                try
                {
                    new FlaggedRunner(config.createRun(),
                                      config,
                                      asList(pool("Writer", 1, MutatingVisitor::new),
                                             pool("Reader", 1, (run) -> new RandomPartitionValidator(run, new Configuration.QuiescentCheckerConfig(), QueryLogger.NO_OP))),
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
            for (int inst : new int[] {1, 3})
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

    private static String createHarryConf()
    {
        return "seed: " + currentTimeMillis() + "\n" +
               "\n" +
               "# Default schema provider generates random schema\n" +
               "schema_provider:\n" +
               "  fixed:\n" +
               "    keyspace: harry\n" +
               "    table: test_table\n" +
               "    partition_keys:\n" +
               "      pk1: bigint\n" +
               "      pk2: ascii\n" +
               "    clustering_keys:\n" +
               "      ck1: ascii\n" +
               "      ck2: bigint\n" +
               "    regular_columns:\n" +
               "      v1: ascii\n" +
               "      v2: bigint\n" +
               "      v3: ascii\n" +
               "      v4: bigint\n" +
               "    static_keys:\n" +
               "      s1: ascii\n" +
               "      s2: bigint\n" +
               "      s3: ascii\n" +
               "      s4: bigint\n" +
               "\n" +
               "clock:\n" +
               "  offset:\n" +
               "    offset: 1000\n" +
               "\n" +
               "drop_schema: false\n" +
               "create_schema: true\n" +
               "truncate_table: true\n" +
               "\n" +
               "partition_descriptor_selector:\n" +
               "  default:\n" +
               "    window_size: 10\n" +
               "    slide_after_repeats: 100\n" +
               "\n" +
               "clustering_descriptor_selector:\n" +
               "  default:\n" +
               "    modifications_per_lts:\n" +
               "      type: \"constant\"\n" +
               "      constant: 2\n" +
               "    rows_per_modification:\n" +
               "      type: \"constant\"\n" +
               "      constant: 2\n" +
               "    operation_kind_weights:\n" +
               "      DELETE_RANGE: 0\n" +
               "      DELETE_SLICE: 0\n" +
               "      DELETE_ROW: 0\n" +
               "      DELETE_COLUMN: 0\n" +
               "      DELETE_PARTITION: 0\n" +
               "      DELETE_COLUMN_WITH_STATICS: 0\n" +
               "      INSERT_WITH_STATICS: 50\n" +
               "      INSERT: 50\n" +
               "      UPDATE_WITH_STATICS: 50\n" +
               "      UPDATE: 50\n" +
               "    column_mask_bitsets: null\n" +
               "    max_partition_size: 1000\n" +
               "\n" +
               "metric_reporter:\n" +
               "  no_op: {}\n" +
               "\n" +
               "data_tracker:\n" +
               "  locking:\n" +
               "    max_seen_lts: -1\n" +
               "    max_complete_lts: -1";
    }

}
