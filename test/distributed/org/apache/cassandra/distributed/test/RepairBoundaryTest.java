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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.utils.concurrent.Condition;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;

import static com.google.common.collect.ImmutableList.of;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.cassandra.dht.Murmur3Partitioner.*;
import static org.apache.cassandra.dht.Murmur3Partitioner.LongToken.keyForToken;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.service.StorageService.instance;
import static org.apache.cassandra.utils.concurrent.Condition.newOneTimeCondition;
import static org.apache.cassandra.utils.progress.ProgressEventType.COMPLETE;

public class RepairBoundaryTest extends TestBaseImpl
{
    private static Cluster cluster;

    private static final String INSERT = withKeyspace("INSERT INTO %s.test (k, c1, c2) VALUES" +
                                                      "(?, 'C1', ?);");

    private static final String DELETE = withKeyspace("DELETE FROM %s.test WHERE k = ?;");
    private static final String ALL = withKeyspace("SELECT c2 FROM %s.test;");

    private final Map<Integer, ByteBuffer> keys = new HashMap<>();

    private Object[][] c2Row(int... keys)
    {
        Object[][] ret = new Object[keys.length][];
        for (int i = 0; i < keys.length; i++)
        {
            ret[i] = new Object[]{ String.valueOf(keys[i]) };
        }
        return ret;
    }

    void delete(IInvokableInstance instance, int... toDelete)
    {
        for (int key : toDelete)
        {
            instance.executeInternal(DELETE, keys.get(key));
        }
    }

    void verify()
    {
        assertRows(c2Row(999, 1000, 2001, 2999, 3000, 3001), cluster.get(1).executeInternal(ALL));
        assertRows(c2Row(999, 1000, 1001, 1999, 2000, 3001), cluster.get(2).executeInternal(ALL));
        assertRows(c2Row(1001, 1999, 2000, 2001, 2999, 3000), cluster.get(3).executeInternal(ALL));
    }

    /**
     * Insert on every token boundary and + or - to first replica.
     */
    void populate()
    {
        try
        {
            cluster.schemaChange(withKeyspace("DROP TABLE IF EXISTS %s.test;"));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.test (k blob, c1 text, c2 text," +
                                              "PRIMARY KEY (k))"));

            for (int i = 1000; i <= 3000; i += 1000)
            {
                keys.put(i, keyForToken(new LongToken(i)));
                keys.put(i - 1, keyForToken(new LongToken(i - 1)));
                keys.put(i + 1, keyForToken(new LongToken(i + 1)));

                cluster.coordinator(1).execute(INSERT, ConsistencyLevel.ALL, keys.get(i), String.valueOf(i));
                cluster.coordinator(1).execute(INSERT, ConsistencyLevel.ALL, keys.get(i - 1), String.valueOf(i - 1));
                cluster.coordinator(1).execute(INSERT, ConsistencyLevel.ALL, keys.get(i + 1), String.valueOf(i + 1));
            }
        }
        catch (Throwable t)
        {
            cluster.close();
            throw t;
        }
    }

    @Test
    public void primaryRangeRepair()
    {
        populate();
        verify();

        delete(cluster.get(1), 999, 1000, 3001);
        delete(cluster.get(2), 1999, 2000, 1001);
        delete(cluster.get(3), 2999, 3000, 2001);

        cluster.forEach(i -> {
            i.nodetoolResult("repair", "-pr", "--full", KEYSPACE).asserts().success();
        });

        assertRows(c2Row(), cluster.get(1).executeInternal(ALL));
        assertRows(c2Row(), cluster.get(2).executeInternal(ALL));
        assertRows(c2Row(), cluster.get(3).executeInternal(ALL));
    }

    @Test
    public void singleTokenRangeRepair()
    {
        populate();
        verify();

        delete(cluster.get(1), 999, 1000);
        delete(cluster.get(3), 1001);

        cluster.get(2).runOnInstance(() -> {
            try
            {
                Map<String, String> options = new HashMap<>();
                options.put("ranges", "999:1000");
                options.put("incremental", "false");
                Condition await = newOneTimeCondition();
                instance.repair(KEYSPACE, options, of((tag, event) -> {
                    if (event.getType() == COMPLETE)
                        await.signalAll();
                })).right.get();
                await.await(1L, MINUTES);
            }
            catch (Exception e)
            {
            }
        });

        assertRows(c2Row(999, 1001, 1999, 2000, 3001), cluster.get(2).executeInternal(ALL));
    }


    @BeforeClass
    public static void init() throws IOException
    {
        cluster = Cluster.build(3)
                         .withConfig(config -> config.set("hinted_handoff_enabled", false)
                                                     .set("num_tokens", 1)
                                                     .set("initial_token", Long.toString(config.num() * 1000))
                                                     .with(NETWORK)
                                                     .with(GOSSIP))
                         .start();
        cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                          "{'class': 'SimpleStrategy', 'replication_factor': 2};"));
    }

    @AfterClass
    public static void closeCluster()
    {
        if (cluster != null)
            cluster.close();
    }
}
