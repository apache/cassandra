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

package org.apache.cassandra.db.virtual;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

public class SSTableTasksTableTest extends CQLTester
{
    private static final String KS_NAME = "vts";

    @SuppressWarnings("FieldCanBeLocal")
    private SSTableTasksTable table;

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
        CompactionManager.instance.disableAutoCompaction();
    }

    @Before
    public void config()
    {
        table = new SSTableTasksTable(KS_NAME);
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));
        disablePreparedReuseForTest();
    }

    @Test
    public void testSelectAll() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        long bytesCompacted = 123;
        long bytesTotal = 123456;
        TimeUUID compactionId = nextTimeUUID();
        List<SSTableReader> sstables = IntStream.range(0, 10)
                .mapToObj(i -> MockSchema.sstable(i, i * 10L, i * 10L + 9, cfs))
                .collect(Collectors.toList());

        String directory = String.format("/some/datadir/%s/%s-%s", cfs.metadata.keyspace, cfs.metadata.name, cfs.metadata.id.asUUID());

        CompactionInfo.Holder compactionHolder = new CompactionInfo.Holder()
        {
            public CompactionInfo getCompactionInfo()
            {
                return new CompactionInfo(cfs.metadata(), OperationType.COMPACTION, bytesCompacted, bytesTotal, compactionId, sstables, directory);
            }

            public boolean isGlobal()
            {
                return false;
            }
        };

        CompactionManager.instance.active.beginCompaction(compactionHolder);
        UntypedResultSet result = execute("SELECT * FROM vts.sstable_tasks");
        assertRows(result, row(CQLTester.KEYSPACE, currentTable(), compactionId, 1.0 * bytesCompacted / bytesTotal,
                OperationType.COMPACTION.toString().toLowerCase(), bytesCompacted, sstables.size(),
                directory, bytesTotal, CompactionInfo.Unit.BYTES.toString()));

        CompactionManager.instance.active.finishCompaction(compactionHolder);
        result = execute("SELECT * FROM vts.sstable_tasks");
        assertEmpty(result);
    }
}
