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
package org.apache.cassandra.db;

import java.io.File;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.SequentialWriter;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class RowIndexEntryTest extends CQLTester
{
    @Test
    public void testSerializedSize() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a int, b text, c int, PRIMARY KEY(a, b))");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);

        final RowIndexEntry simple = new RowIndexEntry(123);

        DataOutputBuffer buffer = new DataOutputBuffer();
        SerializationHeader header = new SerializationHeader(cfs.metadata, cfs.metadata.partitionColumns(), EncodingStats.NO_STATS);
        RowIndexEntry.Serializer serializer = new RowIndexEntry.Serializer(cfs.metadata, BigFormat.latestVersion, header);

        serializer.serialize(simple, buffer);

        assertEquals(buffer.getLength(), serializer.serializedSize(simple));

        // write enough rows to ensure we get a few column index entries
        for (int i = 0; i <= DatabaseDescriptor.getColumnIndexSize() / 4; i++)
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, "" + i, i);

        buffer = new DataOutputBuffer();
        ImmutableBTreePartition partition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs).build());

        File tempFile = File.createTempFile("row_index_entry_test", null);
        tempFile.deleteOnExit();
        SequentialWriter writer = SequentialWriter.open(tempFile);
        ColumnIndex columnIndex = ColumnIndex.writeAndBuildIndex(partition.unfilteredIterator(), writer, header, BigFormat.latestVersion);
        RowIndexEntry<IndexHelper.IndexInfo> withIndex = RowIndexEntry.create(0xdeadbeef, DeletionTime.LIVE, columnIndex);

        // sanity check
        assertTrue(columnIndex.columnsIndex.size() >= 3);

        serializer.serialize(withIndex, buffer);
        assertEquals(buffer.getLength(), serializer.serializedSize(withIndex));
    }
}
