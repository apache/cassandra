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
package org.apache.cassandra.db.compaction;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.reads.range.TokenUpdater;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class UserDefinedCleanupMultipleSSTablesTest
{
    private static final String KEYSPACE = "UserDefinedCleanupMultipleSSTablesTest";
    private static final String TABLE = "Standard1";

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, TABLE));
        ServerTestUtils.markCMS();
    }

    @Test
    public void testMultipleSSTablesCleanup() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
        boolean autoCompactionDisabled = cfs.isAutoCompactionDisabled();
        cfs.disableAutoCompaction();
        try
        {
            cfs.truncateBlocking();
            List<DecoratedKey> keys = new ArrayList<>();
            for (int i = 0; i < 6; i++)
                keys.add(cfs.decorateKey(ByteBufferUtil.bytes("key_" + i)));
            Collections.sort(keys);

            // Use the configured partitioner: the first three keys are local, the last three remote.
            new TokenUpdater().withTokens(FBUtilities.getBroadcastAddressAndPort(), keys.get(2).getToken())
                              .withTokens(InetAddressAndPort.getByName("127.0.0.2"), keys.get(5).getToken())
                              .update();
            assertTrue(StorageService.instance.isJoined());

            List<Map<ByteBuffer, ByteBuffer>> originalRows = new ArrayList<>();
            List<SSTableReader> originals = new ArrayList<>();
            for (int i = 0; i < 3; i++)
            {
                Map<ByteBuffer, ByteBuffer> rows = Map.of(keys.get(i).getKey(), ByteBufferUtil.bytes("owned_" + i),
                                                        keys.get(i + 3).getKey(), ByteBufferUtil.bytes("unowned_" + i));
                SSTableReader sstable = writeSSTable(cfs, rows);
                originalRows.add(rows);
                originals.add(sstable);
            }
            assertEquals(3, cfs.getLiveSSTables().size());

            CompactionManager.instance.forceUserDefinedCleanup(originals.get(0).getFilename() + ',' + originals.get(1).getFilename());

            Set<SSTableReader> live = cfs.getLiveSSTables();
            assertEquals(3, live.size());
            for (int i = 0; i < 2; i++)
            {
                SSTableReader original = originals.get(i);
                assertFalse("Requested original must be replaced: " + original.descriptor,
                            live.stream().anyMatch(sstable -> sstable.descriptor.equals(original.descriptor)));
            }
            SSTableReader untouched = originals.get(2);
            assertTrue("Unrequested original must remain live", live.contains(untouched));
            assertEquals(originalRows.get(2), readRows(untouched));

            Set<Map<ByteBuffer, ByteBuffer>> cleanedRows = new HashSet<>();
            for (SSTableReader sstable : live)
            {
                if (!sstable.descriptor.equals(untouched.descriptor))
                    assertTrue("Cleanup must not duplicate an output partition", cleanedRows.add(readRows(sstable)));
            }
            assertEquals(Set.of(Map.of(keys.get(0).getKey(), originalRows.get(0).get(keys.get(0).getKey())),
                                Map.of(keys.get(1).getKey(), originalRows.get(1).get(keys.get(1).getKey()))),
                         cleanedRows);
        }
        finally
        {
            try
            {
                cfs.truncateBlocking();
            }
            finally
            {
                ServerTestUtils.resetCMS();
                if (!autoCompactionDisabled)
                    cfs.enableAutoCompaction();
            }
        }
    }

    private static SSTableReader writeSSTable(ColumnFamilyStore cfs, Map<ByteBuffer, ByteBuffer> rows)
    {
        Set<SSTableReader> before = new HashSet<>(cfs.getLiveSSTables());
        for (Map.Entry<ByteBuffer, ByteBuffer> row : rows.entrySet())
        {
            new RowUpdateBuilder(cfs.metadata(), 1L, row.getKey())
            .clustering("0")
            .add("val", row.getValue())
            .build()
            .applyUnsafe();
        }
        Util.flush(cfs);
        Set<SSTableReader> added = new HashSet<>(cfs.getLiveSSTables());
        added.removeAll(before);
        assertEquals("Each flush must create one mixed-ownership SSTable", 1, added.size());
        return added.iterator().next();
    }

    private static Map<ByteBuffer, ByteBuffer> readRows(SSTableReader sstable)
    {
        Map<ByteBuffer, ByteBuffer> rows = new HashMap<>();
        try (ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator partition = scanner.next())
                {
                    assertTrue(partition.hasNext());
                    Unfiltered unfiltered = partition.next();
                    assertTrue(unfiltered.isRow());
                    Row row = (Row) unfiltered;
                    assertEquals(ByteBufferUtil.bytes("0"), row.clustering().bufferAt(0));
                    ByteBuffer value = row.getCell(sstable.metadata().getColumn(ByteBufferUtil.bytes("val"))).buffer();
                    assertNull("Duplicate partition in SSTable", rows.put(partition.partitionKey().getKey(), value));
                    assertFalse(partition.hasNext());
                }
            }
        }
        return rows;
    }
}
