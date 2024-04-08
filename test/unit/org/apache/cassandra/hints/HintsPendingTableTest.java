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

package org.apache.cassandra.hints;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.virtual.PendingHintsTable;
import org.apache.cassandra.db.virtual.VirtualKeyspace;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Clock;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.apache.cassandra.Util.dk;
import static org.assertj.core.api.Assertions.assertThat;

public class HintsPendingTableTest extends CQLTester
{
    private static final String KS_NAME = "vts";
    private String table;

    @BeforeClass
    public static void setUpClass()
    {
        CassandraRelevantProperties.SUPERUSER_SETUP_DELAY_MS.setLong(0);
        ServerTestUtils.daemonInitialization();
        if (ROW_CACHE_SIZE_IN_MIB > 0)
            DatabaseDescriptor.setRowCacheSizeInMiB(ROW_CACHE_SIZE_IN_MIB);

        DatabaseDescriptor.setMaxHintsFileSize(1);

        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
        prepareServer();
    }

    @Before
    public void setUpTest()
    {
        PendingHintsTable vtable = new PendingHintsTable(KS_NAME);
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, List.of(vtable)));
        table = createTable("CREATE TABLE %s (pk uuid, val uuid, PRIMARY KEY (pk))");
    }

    @Test
    public void testPendingHintsSizes()
    {
        List<UUID> uuids = createHints();

        UUID firstNodeID = uuids.get(0);
        HintsStore store = HintsService.instance.getCatalog().get(firstNodeID);
        HintsDescriptor descriptor = store.getOrOpenWriter().descriptor();
        store.cleanUp(descriptor);
        store.markCorrupted(descriptor);
        store.closeWriter();

        List<PendingHintsRow> rows = execute(format("SELECT * FROM %s.pending_hints", KS_NAME))
                                     .stream()
                                     .map(PendingHintsRow::new)
                                     .collect(toList());

        assertThat(rows.size()).isEqualTo(10);

        for (PendingHintsRow row : rows)
        {
            assertThat(row.files).isPositive();
            assertThat(row.totalFilesSize).isPositive();

            if (row.hostId.equals(firstNodeID))
            {
                assertThat(row.corruptedFiles).isPositive();
                assertThat(row.totalCorruptedFilesSize).isPositive();
            }
            else
            {
                assertThat(row.corruptedFiles).isZero();
                assertThat(row.totalCorruptedFilesSize).isZero();
            }
        }
    }

    private List<UUID> createHints()
    {
        List<UUID> uuids = new ArrayList<>();
        for (int i = 0; i < 10; i++)
        {
            UUID nodeUUID = UUID.randomUUID();
            uuids.add(nodeUUID);
            for (int j = 0; j < 100_000; j++)
            {
                HintsService.instance.write(nodeUUID, createHint());
            }
        }

        HintsService.instance.flushAndFsyncBlockingly(uuids);

        return uuids;
    }

    private Hint createHint()
    {
        long now = Clock.Global.currentTimeMillis();
        UUID data = UUID.randomUUID();
        String dataAsString = UUID.randomUUID().toString();
        DecoratedKey dkey = dk(dataAsString);

        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(Schema.instance.getTableMetadata(KEYSPACE, table), dkey)
                                                               .timestamp(now);

        builder.row().add("val", data);

        return Hint.create(builder.buildAsMutation(), now);
    }

    private static class PendingHintsRow
    {
        public final UUID hostId;
        public final int files;
        public final long totalFilesSize;
        public final int corruptedFiles;
        public final long totalCorruptedFilesSize;

        public PendingHintsRow(UntypedResultSet.Row rawRow)
        {
            hostId = rawRow.getUUID("host_id");
            files = rawRow.getInt("files");
            totalFilesSize = rawRow.getLong("total_size");
            corruptedFiles = rawRow.getInt("corrupted_files");
            totalCorruptedFilesSize = rawRow.getLong("total_corrupted_files_size");
        }
    }
}
