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

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.hints.HintsService;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.net.Verb;
import org.assertj.core.api.Assertions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.LOCAL_QUORUM;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.utils.AssertionUtils.isThrowable;

public class HintDataReappearingTest extends AbstractHintWindowTest
{
    private static final Logger logger = LoggerFactory.getLogger(HintDataReappearingTest.class);

    private static final ScheduledExecutorService scheduler = Executors
            .newScheduledThreadPool(1,
                    new ThreadFactoryBuilder()
                            .setNameFormat("hint reappearance test")
                            .setDaemon(true)
                            .build());

    @Test
    public void testHintCausesDataReappearance() throws Exception
    {
        doHintReappearData(true, false);
    }

    @Test
    public void demonstrateHintCausesDataReappearance() throws Exception
    {
        doHintReappearData(false, false);
    }

    @Test
    public void testHintCausesDataReappearanceWriteTimeout() throws Exception
    {
        doHintReappearData(true, true);
    }

    @Test
    public void demonstrateHintCausesDataReappearanceWriteTimeout() throws Exception
    {
        doHintReappearData(false, true);
    }

    public void doHintReappearData(final boolean preventReappearance, final boolean dropTwoWrites) throws Exception
    {
        try (Cluster cluster = init(Cluster.build(3)
                .withDataDirCount(1)

                .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL)
                        .set("hinted_handoff_enabled", true)
                        .set("max_hints_delivery_threads", "1")
                        .set("use_creation_time_for_hint_ttl", preventReappearance ? "true" : "false")
                        .set("write_request_timeout", "30000ms")
                        .set("hints_flush_period", "1s")
                        .set("max_hints_file_size", "10MiB"))
                .start(), 3))
        {
            final IInvokableInstance node1 = cluster.get(1);
            final IInvokableInstance node2 = cluster.get(2);
            final IInvokableInstance node3 = cluster.get(3);

            waitForExistingRoles(cluster);

            // We create a table with low gc_grace_seconds to ensure we can check the interactions in a timely manner
            // Also make compaction fairly proactive in an attempt avoiding to force compact repeatedly
            final int gc_grace_seconds = 40;
            final String createTableStatement = format("CREATE TABLE %s.cf (k text PRIMARY KEY, c1 text) " +
                    "WITH gc_grace_seconds = " + gc_grace_seconds +
                    " AND compaction = {'class': 'SizeTieredCompactionStrategy', 'min_threshold': 2, 'max_threshold': 32 } ", KEYSPACE);
            cluster.schemaChange(createTableStatement);

            // setup a message filter to drop mutations requests from node1 to node2 so it creates hints for those mutations
            AtomicBoolean dropWritesForNode2 = new AtomicBoolean(true);
            // toggling this on allows the test to timeout the original write, a slight variation, both cause the problem.
            AtomicBoolean dropWritesForNode3 = new AtomicBoolean(dropTwoWrites);
            cluster.filters()
                    .verbs(Verb.MUTATION_REQ.id)
                    .from(1)
                    .messagesMatching((from, to, message) ->
                            (to == 2 && dropWritesForNode2.get()
                                    || (to == 3 && dropWritesForNode3.get())))
                    .drop();


            logger.info("Pausing hint delivery");
            // pause hint delivery to imitate hints being behind/backed up
            pauseHintsDelivery(node1);

            logger.info("Inserting data");

            List<UUID> keys = IntStream.range(0, 1).mapToObj(x -> UUID.randomUUID()).collect(Collectors.toList());

            scheduler.submit(() -> {
                if (dropTwoWrites)
                {
                    Assertions.assertThatThrownBy(() -> insertData(cluster, keys))
                            .is(isThrowable(WriteTimeoutException.class));
                }
                else
                {
                    insertData(cluster, keys);
                }
            });

            Thread.sleep(1000);
            dropWritesForNode2.set(false);
            dropWritesForNode3.set(false);

            node1.flush(KEYSPACE);
            node2.flush(KEYSPACE);
            node3.flush(KEYSPACE);

            logger.info("Deleting data");
            deleteData(cluster, keys);
            long afterDelete = System.currentTimeMillis();

            node1.flush(KEYSPACE);
            node2.flush(KEYSPACE);
            node3.flush(KEYSPACE);


            logger.info("Repairing");
            for (IInvokableInstance node : Arrays.asList(node1, node2, node3))
            {
                node.nodetoolResult(ArrayUtils.addAll(new String[]{ "repair", KEYSPACE }, "--full")).asserts().success();
            }
            logger.info("Done repairing");


            for (SimpleQueryResult result : selectData(cluster, keys))
            {
                Object[][] objectArrays = result.toObjectArrays();
                logger.info("Result after delete: {} {}", result, Arrays.deepToString(objectArrays));
                // We expect the data to appear to be deleted initially
                Assert.assertNull(objectArrays[0][1]);
            }

            // wait to pass gc_grace_seconds with slight buffer of 2 seconds to ensure delete persisted long enough to be gced
            long msSinceDelete = Math.abs(System.currentTimeMillis() - afterDelete);
            long sleepFor = Math.max(0, 1000 * gc_grace_seconds + 2000 - msSinceDelete);
            logger.info("Sleeping {} ms to ensure gc_grace_seconds has ellapsed after tombstone creation", sleepFor);
            Thread.sleep(sleepFor);

            // ensure tombstone purged on all 3 nodes
            node1.forceCompact(KEYSPACE, "cf");
            node3.forceCompact(KEYSPACE, "cf");
            node2.forceCompact(KEYSPACE, "cf");


            IIsolatedExecutor.CallableNoExcept<UUID> node2hostId = node2.callsOnInstance(SystemKeyspace::getLocalHostId);

            transferHints(node1, node2hostId.call());

            // Sleep a bit more to ensure hint is delivered after tombstone is GCed
            Thread.sleep(200);
            logger.info("Total Hints after sleeping: {}", getTotalHintsCount(node1));

            String hintInfo = node1.callsOnInstance(() -> String.valueOf(HintsService.instance.getPendingHintsInfo().size())).call();
            logger.info("Number of pending hints after sleeping: {}", hintInfo);

            // Check the results of reading, we expect the data to remain deleted
            // and hint not to cause data to be visibile again as the hint should have expired
            for (SimpleQueryResult result : selectData(cluster, keys))
            {
                logger.info("Result: {}", result);
                Object[][] objectArrays = result.toObjectArrays();
                if (preventReappearance)
                {
                    logger.info("Preventing reappearance with mutation ttl time, hence expecting null as column value");
                    Assert.assertNull(objectArrays[0][1]);
                }
                else
                {
                    logger.info("Demonstrating reappearance possible, hence observing non null, non empty column");
                    Assert.assertFalse(UUID.fromString((String) objectArrays[0][1]).toString().isEmpty());
                }
            }
        }
    }

    private List<SimpleQueryResult> selectData(Cluster cluster, List<UUID> keys)
    {
        List<SimpleQueryResult> results = new ArrayList<>();
        for (UUID partitionKey : keys)
        {
            SimpleQueryResult result = cluster.coordinator(1)
                    .executeWithResult(withKeyspace("SELECT * FROM %s.cf where k=?;"),
                            ALL, partitionKey.toString());
            results.add(result);
        }
        return results;
    }

    private void insertData(Cluster cluster, List<UUID> inserts)
    {
        for (int i = 0; i < inserts.size(); i++)
        {
            final UUID partitionKey = inserts.get(i);
            cluster.coordinator(1)
                    .execute(withKeyspace("INSERT INTO %s.cf (k, c1) VALUES (?, ?);"),
                            LOCAL_QUORUM, partitionKey.toString(), UUID.randomUUID().toString());
        }
    }


    void deleteData(Cluster cluster, List<UUID> insertedKeys)
    {

        for (UUID partitionKey : insertedKeys)
        {
            cluster.coordinator(1)
                    .execute(withKeyspace("DELETE c1 FROM %s.cf where k=?;"),
                            LOCAL_QUORUM, partitionKey.toString());
        }
    }
}
