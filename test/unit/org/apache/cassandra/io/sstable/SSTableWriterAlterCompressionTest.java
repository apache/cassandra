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

package org.apache.cassandra.io.sstable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An ALTER TABLE which enables or disables compression must not change the compression of an sstable while it is
 * written: its components, CompressionInfo or CRC, and its data have to agree.
 */
public class SSTableWriterAlterCompressionTest extends CQLTester
{
    private static final int PARTITIONS = 100;

    @Test
    public void disableWhileBuilding()
    {
        alterWhileBuilding("{'class': 'LZ4Compressor'}", "{'enabled': 'false'}");
    }

    @Test
    public void enableWhileBuilding()
    {
        alterWhileBuilding("{'enabled': 'false'}", "{'class': 'LZ4Compressor'}");
    }

    /**
     * Each ALTER flushes the memtable, the next ALTER runs while that flush builds its writer.
     */
    @Test
    public void toggleWhileFlushing()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text) WITH compression = {'class': 'LZ4Compressor'}");
        int rows = 0;
        for (int round = 0; round < 20; round++)
        {
            for (int i = 0; i < 100; i++, rows++)
                execute("INSERT INTO %s (pk, v) VALUES (?, ?)", rows, "value " + rows);
            alterTable(round % 2 == 0 ? "ALTER TABLE %s WITH compression = {'enabled': 'false'}"
                                      : "ALTER TABLE %s WITH compression = {'class': 'LZ4Compressor'}");
        }
        flush();

        assertThat(execute("SELECT * FROM %s").size()).isEqualTo(rows);
    }

    /**
     * Builds a writer as a flush or a compaction does, but alters the compression of the table after the components
     * of the sstable were chosen and before its data writer is opened.
     */
    private void alterWhileBuilding(String compression, String altered)
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text) WITH compression = " + compression);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Descriptor descriptor = cfs.newSSTableDescriptor(cfs.getDirectories().getDirectoryForNewSSTables());
        SSTableWriter.Builder<?, ?> builder = descriptor.getFormat().getWriterFactory().builder(descriptor)
                                                        .setTableMetadataRef(cfs.metadata)
                                                        .setSerializationHeader(new SerializationHeader(true, cfs.metadata(), cfs.metadata().regularAndStaticColumns(), EncodingStats.NO_STATS))
                                                        .setMetadataCollector(new MetadataCollector(cfs.metadata().comparator))
                                                        .addDefaultComponents(Collections.emptyList());
        alterTable("ALTER TABLE %s WITH compression = " + altered);

        LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.WRITE);
        try (SSTableWriter writer = builder.build(txn, cfs))
        {
            List<PartitionUpdate> partitions = new ArrayList<>();
            for (int pk = 0; pk < PARTITIONS; pk++)
                partitions.add(UpdateBuilder.create(cfs.metadata(), pk).withTimestamp(1).newRow().add("v", "value " + pk).build());
            // in token order, as an sstable is written
            partitions.sort(Comparator.comparing(PartitionUpdate::partitionKey));
            for (PartitionUpdate partition : partitions)
                writer.append(partition.unfilteredIterator());

            SSTableReader sstable = writer.finish(true);
            try (ISSTableScanner scanner = sstable.getScanner())
            {
                int rows = 0;
                while (scanner.hasNext())
                {
                    try (UnfilteredRowIterator partition = scanner.next())
                    {
                        while (partition.hasNext())
                        {
                            partition.next();
                            rows++;
                        }
                    }
                }
                assertThat(rows).isEqualTo(PARTITIONS);
            }
            finally
            {
                sstable.selfRef().release();
            }
        }
        finally
        {
            txn.abort();
        }
    }
}
