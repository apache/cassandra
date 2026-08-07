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

package org.apache.cassandra.distributed.test.accord;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.sai.SAIUtil;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.net.Verb;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class AccordImportSSTableTest extends AccordImportSSTableTestBase
{
    @Test
    public void testHappyPath() throws Throwable
    {
        String file = writeSSTables(new int[] { 1, 2 }, new int[] { 3 });

        try (Cluster cluster = init(builder().withNodes(3)
                                             .withoutVNodes()
                                             .withDataDirCount(1)
                                             .withConfig((config) ->
                                                         config
                                                         .with(Feature.NETWORK, Feature.GOSSIP)).start()))
        {
            createSchema(cluster);

            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);
                Set<String> paths = Set.of(file);
                cfs.importNewSSTables(paths, true, true, true, true, true, true, true);
            });

            Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);

            assertSSTableCount(cluster, 2);
            assertLocalSelect(cluster, rows -> { assertRows(rows, row(1, 1), row(2, 1), row(3, 1)); });
            assertPendingDirs(cluster, (File pendingUuidDir) -> {
                Assertions.assertThat(pendingUuidDir.listUnchecked(File::isFile)).isEmpty();
            });
        }
    }

    @Test
    public void testBuildsIndex() throws Throwable
    {
        String file = writeSSTables(new int[] { 1 });

        try (Cluster cluster = init(builder().withNodes(3)
                                             .withoutVNodes()
                                             .withDataDirCount(1)
                                             .withConfig((config) ->
                                                         config
                                                         .with(Feature.NETWORK, Feature.GOSSIP)).start()))
        {
            createSchema(cluster);

            String indexName = "v_idx";
            cluster.schemaChange(withKeyspace("CREATE INDEX " + indexName + " ON " + KEYSPACE_TABLE + " (v) USING 'sai'"));

            long mark = cluster.get(1).logs().mark();

            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);
                Set<String> paths = Set.of(file);
                cfs.importNewSSTables(paths, true, true, true, true, true, true, true);
            });

            Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);

            List<String> logs = cluster.get(1).logs().watchFor(mark, Duration.ofMinutes(1), "Submitting incremental index build of " + indexName).getResult();
            Assertions.assertThat(logs).isNotEmpty();
            SAIUtil.waitForIndexQueryable(cluster, KEYSPACE, indexName);
            assertLocalSelect(cluster, rows -> assertRows(rows, row(1, 1)));
        }
    }

    @Test
    public void testCleanupWithMultipleDataDirectories() throws Throwable
    {
        String file = writeSSTables(new int[] { 1, 2 }, new int[] { 3 });

        try (Cluster cluster = init(builder().withNodes(3)
                                             .withoutVNodes()
                                             .withDataDirCount(3)
                                             .withConfig((config) ->
                                                         config
                                                         .with(Feature.NETWORK, Feature.GOSSIP)).start()))
        {
            createSchema(cluster);

            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);
                Set<String> paths = Set.of(file);
                cfs.importNewSSTables(paths, true, true, true, true, true, true, true);
            });

            Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);

            assertSSTableCount(cluster, 2);
            assertLocalSelect(cluster, rows -> { assertRows(rows, row(1, 1), row(2, 1), row(3, 1)); });
            assertPendingDirs(cluster, (File pendingUuidDir) -> {
                Assertions.assertThat(pendingUuidDir.listUnchecked(File::isFile)).isEmpty();
            });
        }
    }

    @Test
    public void testBounceAfterStreaming() throws Throwable
    {
        String file = writeSSTables(new int[] { 1, 2, 3 });

        // We disable local delivery so that we can stimulate a network partition by dropping
        // ACCORD_STABLE_THEN_READ_REQ messages
        try (Cluster cluster = init(builder().withNodes(3).withoutVNodes()
                                             .withDataDirCount(1).withConfig((config) ->
                                                                             config
                                                                             .set("accord.permit_local_delivery", false)
                                                                             .with(Feature.NETWORK, Feature.GOSSIP)).start()))
        {
            createSchema(cluster);

            // Prevent SSTables from being moved to the live set
            cluster.filters().outbound().verbs(Verb.ACCORD_STABLE_THEN_READ_REQ.id).drop();

            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);
                Set<String> paths = Set.of(file);
                Assertions.assertThatThrownBy(() -> cfs.importNewSSTables(paths, true, true, true, true, true, true, true));
            });

            assertLocalSelect(cluster, rows -> assertRows(rows, EMPTY_ROWS));

            bounce(cluster);

            Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);

            assertLocalSelect(cluster, rows -> assertRows(rows, EMPTY_ROWS));
            cluster.filters().reset();
        }
    }
}
