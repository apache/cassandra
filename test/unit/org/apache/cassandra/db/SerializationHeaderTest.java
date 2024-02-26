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
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.exceptions.UnknownColumnException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.big.BigTableWriter;
import org.apache.cassandra.io.sstable.format.trieindex.TrieIndexFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

public class SerializationHeaderTest
{
    private static String KEYSPACE = "SerializationHeaderTest";

    private final static Version versionWithExplicitFrozenTuples;
    private final static Version versionWithImplicitFrozenTuples;

    static
    {
        DatabaseDescriptor.daemonInitialization();

        versionWithImplicitFrozenTuples = TrieIndexFormat.instance.getVersion("cb");
        assertThat(versionWithImplicitFrozenTuples.hasImplicitlyFrozenTuples()).isTrue();

        versionWithExplicitFrozenTuples = TrieIndexFormat.instance.getVersion("cc");
        assertThat(versionWithExplicitFrozenTuples.hasImplicitlyFrozenTuples()).isFalse();
    }
    
    @Test
    public void testWrittenAsDifferentKind() throws Exception
    {
        final String tableName = "testWrittenAsDifferentKind";
//        final String schemaCqlWithStatic = String.format("CREATE TABLE %s (k int, c int, v int static, PRIMARY KEY(k, c))", tableName);
//        final String schemaCqlWithRegular = String.format("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY(k, c))", tableName);
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

        Supplier<SequenceBasedSSTableId> id = Util.newSeqGen();
        File dir = new File(Files.createTempDir());
        try
        {
            BiFunction<TableMetadata, Function<ByteBuffer, Clustering<?>>, Callable<Descriptor>> writer = (schema, clusteringFunction) -> () -> {
                Descriptor descriptor = new Descriptor(BigFormat.latestVersion, dir, schema.keyspace, schema.name, id.get(), SSTableFormat.Type.BIG);

                SerializationHeader header = SerializationHeader.makeWithoutStats(schema);
                try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.WRITE, TableMetadataRef.forOfflineTools(schema));
                     SSTableWriter sstableWriter = BigTableWriter.create(TableMetadataRef.forOfflineTools(schema), descriptor, 1, 0L, null, false, 0, header, Collections.emptyList(),  txn))
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
            SSTableReader readerWithStatic = sstableWithStatic.getFormat().getReaderFactory().openNoValidation(sstableWithStatic, TableMetadataRef.forOfflineTools(schemaWithRegular));
            SSTableReader readerWithRegular = sstableWithStatic.getFormat().getReaderFactory().openNoValidation(sstableWithRegular, TableMetadataRef.forOfflineTools(schemaWithStatic));

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

    @Test
    public void testDecodeFrozenTuplesEncodedAsNonFrozen() throws UnknownColumnException
    {
        TupleType multicellTupleType = new TupleType(Arrays.asList(Int32Type.instance, Int32Type.instance), true);
        AbstractType<?> frozenTupleType = multicellTupleType.freeze();

        TableMetadata metadata = TableMetadata.builder("ks", "tab")
                                              .addPartitionKeyColumn("k", frozenTupleType)
                                              .addStaticColumn("s", frozenTupleType)
                                              .addClusteringColumn("c", frozenTupleType)
                                              .addRegularColumn("v", frozenTupleType)
                                              .build();

        SerializationHeader.Component component = SerializationHeader.Component.buildComponentForTools(multicellTupleType,
                                                                                                       Arrays.asList(multicellTupleType),
                                                                                                       new LinkedHashMap<>(Map.of(ByteBufferUtil.bytes("s"), multicellTupleType)),
                                                                                                       new LinkedHashMap<>(Map.of(ByteBufferUtil.bytes("v"), multicellTupleType)),
                                                                                                       EncodingStats.NO_STATS);

        SerializationHeader header = component.toHeader("asdfa", metadata, versionWithImplicitFrozenTuples, false);
        assertThat(header.keyType().isMultiCell()).isFalse();
        assertThat(header.clusteringTypes().get(0).isMultiCell()).isFalse();
        assertThat(header.columns().statics.iterator().next().type.isMultiCell()).isFalse();
        assertThat(header.columns().regulars.iterator().next().type.isMultiCell()).isFalse();

        assertThatIllegalArgumentException().isThrownBy(() -> component.toHeader("asdfa", metadata, versionWithExplicitFrozenTuples, false));
    }

    @Test
    public void testDecodeDroppedUTDsEncodedAsNonFrozenTuples() throws UnknownColumnException
    {
        TupleType multicellTupleType = new TupleType(Arrays.asList(Int32Type.instance, Int32Type.instance), true);

        TableMetadata metadata = TableMetadata.builder("ks", "tab")
                                              .addPartitionKeyColumn("k", Int32Type.instance)
                                              .addStaticColumn("s", Int32Type.instance)
                                              .addClusteringColumn("c", Int32Type.instance)
                                              .addRegularColumn("v", Int32Type.instance)
                                              .recordColumnDrop(ColumnMetadata.regularColumn("ks", "tab", "dv", multicellTupleType), 0L)
                                              .recordColumnDrop(ColumnMetadata.staticColumn("ks", "tab", "ds", multicellTupleType), 0L)
                                              .build();

        SerializationHeader.Component component = SerializationHeader.Component.buildComponentForTools(multicellTupleType,
                                                                                                       Arrays.asList(Int32Type.instance),
                                                                                                       new LinkedHashMap<>(Map.of(ByteBufferUtil.bytes("s"), Int32Type.instance,
                                                                                                                                  ByteBufferUtil.bytes("ds"), multicellTupleType)),
                                                                                                       new LinkedHashMap<>(Map.of(ByteBufferUtil.bytes("v"), Int32Type.instance,
                                                                                                                                  ByteBufferUtil.bytes("dv"), multicellTupleType)),
                                                                                                       EncodingStats.NO_STATS);

        SerializationHeader header = component.toHeader("tab", metadata, versionWithImplicitFrozenTuples, false);
        assertThat(Iterables.find(header.columns().regulars, md -> md.name.toString().equals("dv")).type.isMultiCell()).isTrue();
        assertThat(Iterables.find(header.columns().statics, md -> md.name.toString().equals("ds")).type.isMultiCell()).isTrue();

        assertThatIllegalArgumentException().isThrownBy(() -> component.toHeader("tab", metadata, versionWithExplicitFrozenTuples, false));
    }

}
