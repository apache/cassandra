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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.exceptions.UnknownColumnException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;

public class SerializationHeaderTest
{
    private static final String KEYSPACE = "SerializationHeaderTest";

    static
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testDeserializedClusteringTypesAreImmutable() throws Exception
    {
        TableMetadata schema = TableMetadata.builder(KEYSPACE, "testDeserializedClusteringTypesAreImmutable")
                                              .addPartitionKeyColumn("k", Int32Type.instance)
                                              .addClusteringColumn("c", Int32Type.instance)
                                              .build();
        SerializationHeader.Component component = SerializationHeader.makeWithoutStats(schema).toComponent();
        SSTableFormat<?, ?> format = DatabaseDescriptor.getSelectedSSTableFormat();

        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            SerializationHeader.serializer.serialize(format.getLatestVersion(), component, out);
            SerializationHeader.Component deserialized = SerializationHeader.serializer.deserialize(format.getLatestVersion(),
                                                                                                      new DataInputBuffer(out.buffer(), true));

            Assert.assertTrue(deserialized.getClusteringTypes() instanceof ImmutableList);
        }
    }

    /**
     * Common case: schema unchanged since the SSTable was written.
     */
    @Test
    public void testTypeMapNullWhenNoSchemaChanges() throws UnknownColumnException
    {
        TableMetadata schema = TableMetadata.builder(KEYSPACE, "testTypeMapNull")
                .addPartitionKeyColumn("k", Int32Type.instance)
                .addClusteringColumn("c", Int32Type.instance)
                .addRegularColumn("v", Int32Type.instance)
                .build();

        SerializationHeader.Component component = SerializationHeader.makeWithoutStats(schema).toComponent();
        SerializationHeader header = component.toHeader(schema);

        ColumnMetadata v = schema.getColumn(ColumnIdentifier.getInterned("v", false));
        // typeMap == null => getType() returns exactly column.type (same object reference)
        Assert.assertSame(v.type, header.getType(v));
    }

    /**
     * ALTER TABLE scenario: column was written as Int32Type, schema now has LongType.
     * toHeader() must retain typeMap so getType() returns the on-disk Int32Type and
     * not the current schema LongType, which would corrupt deserialization.
     */
    @Test
    public void testTypeMapNonNullWhenColumnTypeChanged() throws UnknownColumnException
    {
        ColumnIdentifier v = ColumnIdentifier.getInterned("v", false);
        TableMetadata schemaAtWrite = TableMetadata.builder(KEYSPACE, "testTypeMapNonNull")
                .addPartitionKeyColumn("k", Int32Type.instance)
                .addClusteringColumn("c", Int32Type.instance)
                .addRegularColumn("v", Int32Type.instance)
                .build();
        TableMetadata schemaAfterAlter = TableMetadata.builder(KEYSPACE, "testTypeMapNonNull")
                .addPartitionKeyColumn("k", Int32Type.instance)
                .addClusteringColumn("c", Int32Type.instance)
                .addRegularColumn("v", LongType.instance) // type is changed
                .build();

        SerializationHeader.Component component = SerializationHeader.makeWithoutStats(schemaAtWrite).toComponent();
        SerializationHeader header = component.toHeader(schemaAfterAlter);

        ColumnMetadata vNew = schemaAfterAlter.getColumn(v);
        // getType() must return the on-disk Int32Type, not the current LongType
        Assert.assertEquals(Int32Type.instance, header.getType(vNew));
    }

    /**
     * Column was dropped at the same type it was written with — no actual type mismatch.
     */
    @Test
    public void testTypeMapNullForDroppedColumnWithSameType() throws UnknownColumnException
    {
        ColumnIdentifier v = ColumnIdentifier.getInterned("v", false);
        TableMetadata schemaAtWrite = TableMetadata.builder(KEYSPACE, "testTypeMapDroppedSame")
                .addPartitionKeyColumn("k", Int32Type.instance)
                .addClusteringColumn("c", Int32Type.instance)
                .addRegularColumn("v", Int32Type.instance)
                .build();
        ColumnMetadata vColumn = schemaAtWrite.getColumn(v);
        TableMetadata schemaAfterDrop = schemaAtWrite.unbuild().recordColumnDrop(vColumn, 0L).build();

        SerializationHeader.Component component = SerializationHeader.makeWithoutStats(schemaAtWrite).toComponent();
        SerializationHeader header = component.toHeader(schemaAfterDrop);

        ColumnMetadata vDropped = schemaAfterDrop.getDroppedColumn(vColumn.name.bytes);
        // disk type == dropped type => typeMap null => same reference
        Assert.assertSame(vDropped.type, header.getType(vDropped));
        Assert.assertEquals(Int32Type.instance, header.getType(vDropped));
    }

    /**
     * Column was altered from Int32Type to LongType and then dropped.
     * An SSTable written before the ALTER stores Int32Type on disk, but the drop record
     * has LongType. The type mismatch must be detected and typeMap retained so
     * the Int32Type bytes can still be skipped correctly.
     */
    @Test
    public void testTypeMapNonNullForDroppedColumnWithChangedType() throws UnknownColumnException
    {
        ColumnIdentifier v = ColumnIdentifier.getInterned("v", false);
        TableMetadata schemaAtWrite = TableMetadata.builder(KEYSPACE, "testTypeMapDroppedChanged")
                .addPartitionKeyColumn("k", Int32Type.instance)
                .addClusteringColumn("c", Int32Type.instance)
                .addRegularColumn("v", Int32Type.instance)
                .build();
        TableMetadata schemaWithAlteredType = TableMetadata.builder(KEYSPACE, "testTypeMapDroppedChanged")
                .addPartitionKeyColumn("k", Int32Type.instance)
                .addClusteringColumn("c", Int32Type.instance)
                .addRegularColumn("v", LongType.instance) // type is changed
                .build();
        ColumnMetadata vLong = schemaWithAlteredType.getColumn(v);
        TableMetadata schemaAfterDrop = schemaWithAlteredType.unbuild()
                                                             .recordColumnDrop(vLong, 0L) // drop the value column
                                                             .build();

        SerializationHeader.Component component = SerializationHeader.makeWithoutStats(schemaAtWrite).toComponent();
        SerializationHeader header = component.toHeader(schemaAfterDrop);

        ColumnMetadata vDropped = schemaAfterDrop.getDroppedColumn(vLong.name.bytes);
        // disk type (Int32) != drop-record type (Long) => typeMap retained, returns Int32
        Assert.assertEquals(Int32Type.instance, header.getType(vDropped));
        Assert.assertNotSame(vDropped.type, header.getType(vDropped));
    }

    /**
     * Static-column variant of {@link #testTypeMapNonNullWhenColumnTypeChanged}.
     * The mismatch detection in toHeader() iterates staticColumns and regularColumns through
     * the same loop; this covers the static branch.
     */
    @Test
    public void testTypeMapNonNullWhenStaticColumnTypeChanged() throws UnknownColumnException
    {
        ColumnIdentifier s = ColumnIdentifier.getInterned("s", false);
        TableMetadata schemaAtWrite = TableMetadata.builder(KEYSPACE, "testTypeMapNonNullStatic")
                .addPartitionKeyColumn("k", Int32Type.instance)
                .addClusteringColumn("c", Int32Type.instance)
                .addStaticColumn("s", Int32Type.instance)
                .build();
        TableMetadata schemaAfterAlter = TableMetadata.builder(KEYSPACE, "testTypeMapNonNullStatic")
                .addPartitionKeyColumn("k", Int32Type.instance)
                .addClusteringColumn("c", Int32Type.instance)
                .addStaticColumn("s", LongType.instance) // static column type is changed
                .build();

        SerializationHeader.Component component = SerializationHeader.makeWithoutStats(schemaAtWrite).toComponent();
        SerializationHeader header = component.toHeader(schemaAfterAlter);

        ColumnMetadata sNew = schemaAfterAlter.getColumn(s);
        // getType() must return the on-disk Int32Type, not the current LongType
        Assert.assertEquals(Int32Type.instance, header.getType(sNew));
    }

    /**
     * Multi-cell vs frozen collections serialize differently and CollectionType.equals
     * checks isMultiCell. A schema flipping a list from multi-cell to frozen (or vice versa)
     * must be detected as a type mismatch so typeMap is retained.
     */
    @Test
    public void testTypeMapNonNullWhenCollectionMultiCellChanged() throws UnknownColumnException
    {
        ColumnIdentifier v = ColumnIdentifier.getInterned("v", false);
        ListType<Integer> multiCellList = ListType.getInstance(Int32Type.instance, true);
        ListType<Integer> frozenList = ListType.getInstance(Int32Type.instance, false);

        TableMetadata schemaAtWrite = TableMetadata.builder(KEYSPACE, "testTypeMapNonNullCollection")
                .addPartitionKeyColumn("k", Int32Type.instance)
                .addClusteringColumn("c", Int32Type.instance)
                .addRegularColumn("v", multiCellList)
                .build();
        TableMetadata schemaAfterFreeze = TableMetadata.builder(KEYSPACE, "testTypeMapNonNullCollection")
                .addPartitionKeyColumn("k", Int32Type.instance)
                .addClusteringColumn("c", Int32Type.instance)
                .addRegularColumn("v", frozenList) // multi-cell -> frozen
                .build();

        SerializationHeader.Component component = SerializationHeader.makeWithoutStats(schemaAtWrite).toComponent();
        SerializationHeader header = component.toHeader(schemaAfterFreeze);

        ColumnMetadata vNew = schemaAfterFreeze.getColumn(v);
        // getType() must return the on-disk multi-cell list, not the frozen schema type
        Assert.assertEquals(multiCellList, header.getType(vNew));
    }

    @Test
    public void testWrittenAsDifferentKind() throws Exception
    {
        SSTableFormat<?, ?> format = DatabaseDescriptor.getSelectedSSTableFormat();
        final String tableName = "testWrittenAsDifferentKind";
        ColumnIdentifier v = ColumnIdentifier.getInterned("v", false);
        TableMetadata schemaWithStatic = TableMetadata.builder(KEYSPACE, tableName)
                .addPartitionKeyColumn("k", Int32Type.instance)
                .addClusteringColumn("c", Int32Type.instance)
                .addStaticColumn("v", Int32Type.instance)
                .build();
        TableMetadata schemaWithRegular = TableMetadata.builder(KEYSPACE, tableName)
                .addPartitionKeyColumn("k", Int32Type.instance)
                .addClusteringColumn("c", Int32Type.instance)
                .addRegularColumn("v", Int32Type.instance)
                .build();
        ColumnMetadata columnStatic = schemaWithStatic.getColumn(v);
        ColumnMetadata columnRegular = schemaWithRegular.getColumn(v);
        schemaWithStatic = schemaWithStatic.unbuild().recordColumnDrop(columnRegular, 0L).build();
        schemaWithRegular = schemaWithRegular.unbuild().recordColumnDrop(columnStatic, 0L).build();

        SSTableReader readerWithStatic = null;
        SSTableReader readerWithRegular = null;
        Supplier<SequenceBasedSSTableId> id = Util.newSeqGen();
        File dir = new File(Files.createTempDir());
        try
        {
            BiFunction<TableMetadata, Function<ByteBuffer, Clustering<?>>, Callable<Descriptor>> writer = (schema, clusteringFunction) -> () -> {
                Descriptor descriptor = new Descriptor(format.getLatestVersion(), dir, schema.keyspace, schema.name, id.get());

                SerializationHeader header = SerializationHeader.makeWithoutStats(schema);
                try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.WRITE);
                     SSTableWriter sstableWriter = descriptor.getFormat().getWriterFactory()
                                                             .builder(descriptor)
                                                             .setTableMetadataRef(TableMetadataRef.forOfflineTools(schema))
                                                             .setKeyCount(1)
                                                             .setSerializationHeader(header)
                                                             .setMetadataCollector(new MetadataCollector(schema.comparator))
                                                             .addDefaultComponents(Collections.emptySet())
                                                             .build(txn, null))
                {
                    ColumnMetadata cd = schema.getColumn(v);
                    for (int i = 0 ; i < 5 ; ++i) {
                        final ByteBuffer value = Int32Type.instance.decompose(i);
                        Cell<?> cell = BufferCell.live(cd, 1L, value);
                        Clustering<?> clustering = clusteringFunction.apply(value);
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
            readerWithStatic = SSTableReader.openNoValidation(null, sstableWithStatic, TableMetadataRef.forOfflineTools(schemaWithRegular));
            readerWithRegular = SSTableReader.openNoValidation(null, sstableWithRegular, TableMetadataRef.forOfflineTools(schemaWithStatic));

            try (ISSTableScanner partitions = readerWithStatic.getScanner()) {
                for (int i = 0 ; i < 5 ; ++i)
                {
                    UnfilteredRowIterator partition = partitions.next();
                    Assert.assertFalse(partition.hasNext());
                    long value = Int32Type.instance.compose(partition.staticRow().getCell(columnStatic).buffer());
                    Assert.assertEquals(value, (long)i);
                }
                Assert.assertFalse(partitions.hasNext());
            }
            try (ISSTableScanner partitions = readerWithRegular.getScanner()) {
                for (int i = 0 ; i < 5 ; ++i)
                {
                    UnfilteredRowIterator partition = partitions.next();
                    long value = Int32Type.instance.compose(((Row)partition.next()).getCell(columnRegular).buffer());
                    Assert.assertEquals(value, i);
                    Assert.assertTrue(partition.staticRow().isEmpty());
                    Assert.assertFalse(partition.hasNext());
                }
                Assert.assertFalse(partitions.hasNext());
            }
        }
        finally
        {
            if (readerWithStatic != null)
                readerWithStatic.selfRef().close();
            if (readerWithRegular != null)
                readerWithRegular.selfRef().close();
            FileUtils.deleteRecursive(dir);
        }
    }

}
