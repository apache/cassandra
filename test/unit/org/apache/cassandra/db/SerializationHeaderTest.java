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

import com.google.common.io.Files;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.big.BigTableWriter;
import org.apache.cassandra.io.util.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;

public class SerializationHeaderTest
{
    private static String KEYSPACE = "SerializationHeaderTest";

    static
    {
        DatabaseDescriptor.daemonInitialization();
    }
    
    @Test
    public void testWrittenAsDifferentKind() throws Exception
    {
        final String tableName = "testWrittenAsDifferentKind";
        final String schemaCqlWithStatic = String.format("CREATE TABLE %s (k int, c int, v int static, PRIMARY KEY(k, c))", tableName);
        final String schemaCqlWithRegular = String.format("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY(k, c))", tableName);
        ColumnIdentifier v = ColumnIdentifier.getInterned("v", false);
        CFMetaData schemaWithStatic = CFMetaData.compile(schemaCqlWithStatic, KEYSPACE);
        CFMetaData schemaWithRegular = CFMetaData.compile(schemaCqlWithRegular, KEYSPACE);
        ColumnDefinition columnStatic = schemaWithStatic.getColumnDefinition(v);
        ColumnDefinition columnRegular = schemaWithRegular.getColumnDefinition(v);
        schemaWithStatic.recordColumnDrop(columnRegular, 0L);
        schemaWithRegular.recordColumnDrop(columnStatic, 0L);

        final AtomicInteger generation = new AtomicInteger();
        File dir = Files.createTempDir();
        try
        {
            BiFunction<CFMetaData, Function<ByteBuffer, Clustering>, Callable<Descriptor>> writer = (schema, clusteringFunction) -> () -> {
                Descriptor descriptor = new Descriptor(BigFormat.latestVersion, dir, schema.ksName, schema.cfName, generation.incrementAndGet(), SSTableFormat.Type.BIG, Component.DIGEST_CRC32);

                SerializationHeader header = SerializationHeader.makeWithoutStats(schema);
                try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.WRITE);
                     SSTableWriter sstableWriter = BigTableWriter.create(schema, descriptor, 1, 0L, 0, header, Collections.emptyList(),  txn))
                {
                    ColumnDefinition cd = schema.getColumnDefinition(v);
                    for (int i = 0 ; i < 5 ; ++i) {
                        final ByteBuffer value = Int32Type.instance.decompose(i);
                        Cell cell = BufferCell.live(cd, 1L, value);
                        Clustering clustering = clusteringFunction.apply(value);
                        Row row = BTreeRow.singleCellRow(clustering, cell);
                        sstableWriter.append(PartitionUpdate.singleRowUpdate(schema, value, row).unfilteredIterator());
                    }
                    sstableWriter.finish(false);
                    txn.finish();
                }
                return descriptor;
            };

            Descriptor sstableWithRegular = writer.apply(schemaWithRegular, BufferClustering::new).call();
            Descriptor sstableWithStatic = writer.apply(schemaWithStatic, value -> Clustering.STATIC_CLUSTERING).call();
            SSTableReader readerWithStatic = SSTableReader.openNoValidation(sstableWithStatic, schemaWithRegular);
            SSTableReader readerWithRegular = SSTableReader.openNoValidation(sstableWithRegular, schemaWithStatic);

            try (ISSTableScanner partitions = readerWithStatic.getScanner()) {
                for (int i = 0 ; i < 5 ; ++i)
                {
                    UnfilteredRowIterator partition = partitions.next();
                    Assert.assertFalse(partition.hasNext());
                    long value = Int32Type.instance.compose(partition.staticRow().getCell(columnStatic).value());
                    Assert.assertEquals(value, (long)i);
                }
                Assert.assertFalse(partitions.hasNext());
            }
            try (ISSTableScanner partitions = readerWithRegular.getScanner()) {
                for (int i = 0 ; i < 5 ; ++i)
                {
                    UnfilteredRowIterator partition = partitions.next();
                    long value = Int32Type.instance.compose(((Row)partition.next()).getCell(columnRegular).value());
                    Assert.assertEquals(value, (long)i);
                    Assert.assertTrue(partition.staticRow().isEmpty());
                    Assert.assertFalse(partition.hasNext());
                }
                Assert.assertFalse(partitions.hasNext());
            }
        }
        finally
        {
            FileUtils.deleteRecursive(dir);
        }
    }

}
