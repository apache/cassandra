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

package org.apache.cassandra.distributed.test.tracking;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.sai.SAIUtil;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ownership.DataPlacement;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * For now, tracked import with a replica down is not supported. The intention is to support this scenario by allowing
 * users to provide a {@link ConsistencyLevel} for tracked import operations, where the import will complete if
 * sufficient replicas acknowledge the transfer and activate it.
 */
public class TrackedImportTransferTest extends TrackedTransferTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(TrackedImportTransferTest.class);

    private static Cluster cluster;

    @BeforeClass
    public static void setup() throws IOException
    {
        cluster = cluster();
    }

    @AfterClass
    public static void teardown()
    {
        if (cluster != null)
            cluster.close();
    }

    @Test
    public void importHappyPath() throws Throwable
    {
        String keyspace = "happy_path";
        createSchema(cluster, keyspace);
        doImport(cluster, keyspace);

        // All pending/ dirs should be empty, should have no SSTables left if all the transfers completed
        assertPendingDirs(cluster, keyspace, (File pendingUuidDir) -> {
            assertThat(pendingUuidDir.listUnchecked(File::isFile)).isEmpty();
        });

        // Verify transfer IDs exist before compaction, then compact, then verify they're removed
        assertCompaction(cluster, keyspace, cluster, TRANSFERS_EXIST, TRANSFERS_EMPTY);

        // Run after compaction, to enforce offset persistence + broadcast
        assertSummary(cluster, keyspace, summary -> {
            assertThat(summary).satisfies(s -> {
                assert s.reconciledIds() == 1;
                assert s.unreconciledIds() == 0;
            });
        });

        assertLocalSelect(cluster, keyspace, rows -> assertRows(rows, row(1, 1)));
    }

    @Test
    public void importIndexAlreadyPresent() throws Throwable
    {
        String keyspace = "index_present";
        createSchema(cluster, keyspace);

        String indexName = "v_idx";
        String indexCql = withKeyspace("CREATE INDEX " + indexName + " ON %s." + TABLE + " (v) USING 'sai'", keyspace);
        cluster.schemaChange(indexCql);

        // This will add an SSTable that already has an SAI index, that needs to be distributed alongside the SSTable on transfer
        IInvokableInstance importer = cluster.get(1);
        long mark = importer.logs().mark();
        doImport(cluster, importer, keyspace, indexCql);
        List<String> logs = importer.logs().grep(mark, "Submitting incremental index build of " + indexName).getResult();
        assertThat(logs).isEmpty();

        // Index should exist and be queryable on all replicas after import
        SAIUtil.assertIndexQueryable(cluster, keyspace, indexName);

        // Validate queries using the index
        cluster.forEach(instance -> {
            Object[][] rows = instance.executeInternal(withKeyspace("SELECT * FROM %s." + TABLE + " WHERE v = 1", keyspace));
            assertRows(rows, row(1, 1));
        });
    }

    @Test
    public void importBuildsIndex() throws Throwable
    {
        String keyspace = "import_builds";
        createSchema(cluster, keyspace);

        String indexName = "v_idx";
        cluster.schemaChange(withKeyspace("CREATE INDEX " + indexName + " ON %s." + TABLE + " (v) USING 'sai'", keyspace));

        // This will add an SSTable that's missing an SAI index, and the index will be built during the import on
        // the coordinator
        IInvokableInstance importer = cluster.get(1);
        long mark = importer.logs().mark();
        doImport(cluster, importer, keyspace);
        List<String> logs = importer.logs().watchFor(mark, Duration.ofMinutes(1), "Submitting incremental index build of " + indexName).getResult();
        assertThat(logs).isNotEmpty();

        // Index should exist and be queryable on all replicas after import
        SAIUtil.assertIndexQueryable(cluster, keyspace, indexName);

        // Validate queries using the index
        cluster.forEach(instance -> {
            Object[][] rows = instance.executeInternal(withKeyspace("SELECT * FROM %s." + TABLE + " WHERE v = 1", keyspace));
            assertRows(rows, row(1, 1));
        });
    }

    @Test
    public void importOutOfRange() throws Throwable
    {
        String keyspace = "out_of_range";
        createSchema(cluster, keyspace, 1);

        Set<IInvokableInstance> inRange = new HashSet<>();
        Set<IInvokableInstance> outOfRange = new HashSet<>();
        cluster.forEach(instance -> {
            boolean importReplica = instance.callOnInstance(() -> {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(keyspace, TABLE);
                DataPlacement placement = ClusterMetadata.current().placements.get(cfs.keyspace.getMetadata().params.replication);
                return placement.writes.forToken(IMPORT_TOKEN).get().containsSelf();
            });
            (importReplica ? inRange : outOfRange).add(instance);
        });
        logger.info("inRange: {}, outOfRange: {}", inRange, outOfRange);

        assertThat(inRange).hasSize(1);
        IInvokableInstance onlyInRange = inRange.iterator().next();

        // Reject import out of range
        for (IInvokableInstance instance : outOfRange)
        {
            long mark = instance.logs().mark();
            Consumer<List<String>> onResult = failedDirs -> assertThat(failedDirs).hasSize(1);
            doImport(cluster, instance, onResult, keyspace, null);
            instance.logs().grep(mark, "java.lang.RuntimeException: Key DecoratedKey(-4069959284402364209, 00000001) is not contained in the given ranges");
        }

        doImport(cluster, onlyInRange, keyspace);

        assertSummary(Collections.singleton(onlyInRange), keyspace, summary -> {
            assertThat(summary).satisfies(s -> {
                assert s.reconciledIds() == 1;
                assert s.unreconciledIds() == 0;
            });
        });

        for (IInvokableInstance instance : outOfRange)
        {
            // Out of range shouldn't have any transfers
            assertCompaction(cluster, keyspace, Collections.singleton(instance), TRANSFERS_EMPTY, TRANSFERS_EMPTY);

            // Run after compaction, to enforce offset persistence + broadcast
            assertSummary(Collections.singleton(instance), keyspace, summary -> {
                assertThat(summary).isNull();
            });
        }
    }
}
