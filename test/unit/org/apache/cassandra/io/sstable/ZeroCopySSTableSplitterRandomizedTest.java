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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.quicktheories.core.Gen;
import org.quicktheories.impl.JavaRandom;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.Child;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.Result;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.apache.cassandra.utils.AbstractTypeGenerators.TypeGenBuilder;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraGenerators;
import org.apache.cassandra.utils.CassandraGenerators.TableMetadataBuilder;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_RANDOM_SEED;
import static org.apache.cassandra.utils.AbstractTypeGenerators.TypeKind.PRIMITIVE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Random-schema coverage for the zero-copy path used by {@code sstablesplit}. */
public class ZeroCopySSTableSplitterRandomizedTest extends CQLTester
{
    private static final AtomicInteger idGen = new AtomicInteger();
    private static final int PARTITIONS = 96;
    private static final int[] CHILD_COUNTS = { 2, 3, 5, 10, 31 };

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void unsortedWriterWalkMatchesAcrossSplitCounts() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        long seed = TEST_RANDOM_SEED.getLong(new Random().nextLong());
        JavaRandom random = new JavaRandom(seed);
        String keyspace = "zero_copy_random_" + idGen.incrementAndGet();
        String table = "table_" + idGen.incrementAndGet();
        TableMetadata metadata = randomSchema(keyspace, table, random);
        String createTable = metadata.toCqlString(false, false, false);
        logger.info("zero-copy randomized split seed={} schema:\n{}", seed, createTable);

        File directory = new File(temporaryFolder.newFolder());
        List<SSTableReader> produced = new ArrayList<>();
        Gen<ByteBuffer[]> data = CassandraGenerators.data(metadata, null);
        List<ByteBuffer[]> rows = new ArrayList<>(PARTITIONS);
        for (int i = 0; i < PARTITIONS; i++)
        {
            ByteBuffer[] row = data.generate(random);
            row[0] = ByteBufferUtil.bytes(i);
            rows.add(row);
        }
        Collections.shuffle(rows, new Random(seed));

        // CQLSSTableWriter uses SSTableSimpleUnsortedWriter unless sorted() is explicitly requested.
        try (CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                        .inDirectory(directory)
                                                        .forTable(createTable)
                                                        .using(insertStatement(metadata))
                                                        .withPartitioner(Murmur3Partitioner.instance)
                                                        .withFormat(BigFormat.getInstance())
                                                        .withSSTableProducedListener(produced::addAll)
                                                        .openSSTableOnProduced()
                                                        .build())
        {
            for (ByteBuffer[] row : rows)
                writer.rawAddRow(Arrays.asList(row));
        }

        assertEquals("the unsorted writer fixture must produce one parent", 1, produced.size());
        SSTableReader writerReader = produced.get(0);
        SSTableReader parent;
        try
        {
            // sstablesplit opens its input offline. Reopen here as well so the split uses the directory-derived ID
            // allocator rather than the temporary live CFS that CQLSSTableWriter registers while building a schema.
            parent = SSTableReader.openNoValidation(null,
                                                    writerReader.descriptor,
                                                    TableMetadataRef.forOfflineTools(metadata));
        }
        finally
        {
            writerReader.selfRef().release();
        }
        try
        {
            assertTrue("the generated parent is unsupported by the zero-copy splitter",
                       ZeroCopySSTableSplitter.isSupported(parent));
            assertEquals(PARTITIONS, countPartitions(parent));

            for (int childCount : CHILD_COUNTS)
            {
                Result split = ZeroCopySSTableSplitter.splitForTesting(parent, childCount);
                try
                {
                    assertEquals(childCount, split.children.size());
                    assertWalkEquals(parent, split, seed, createTable, childCount);
                }
                finally
                {
                    release(split);
                }
            }
        }
        finally
        {
            parent.selfRef().release();
        }
    }

    private static TableMetadata randomSchema(String keyspace, String table, JavaRandom random)
    {
        TypeGenBuilder partitionType = AbstractTypeGenerators.builder()
                                                             .withTypeKinds(PRIMITIVE)
                                                             .withPrimitives(BytesType.instance)
                                                             .withMaxDepth(0);
        TypeGenBuilder valueTypes = AbstractTypeGenerators.builder()
                                                          .withTypeKinds(PRIMITIVE)
                                                          .withoutEmpty()
                                                          .withoutPrimitive(BytesType.instance)
                                                          .withMaxDepth(0);
        TableMetadata metadata = new TableMetadataBuilder()
                                 .withKeyspaceName(keyspace)
                                 .withTableName(table)
                                 .withSimpleColumnNames()
                                 .withPartitioner(Murmur3Partitioner.instance)
                                 .withPartitionColumnsCount(1)
                                 .withPartitionColumnTypeGen(partitionType)
                                 .withClusteringColumnsBetween(1, 3)
                                 .withRegularColumnsBetween(1, 6)
                                 .withStaticColumnsBetween(0, 3)
                                 .withDefaultTypeGen(valueTypes)
                                 .build(random);
        return metadata.unbuild()
                       .params(metadata.params.unbuild()
                                              .compression(CompressionParams.lz4(4 * 1024))
                                              .build())
                       .build();
    }

    private static String insertStatement(TableMetadata metadata)
    {
        StringBuilder statement = new StringBuilder("INSERT INTO ").append(metadata).append(" (");
        Iterator<ColumnMetadata> columns = metadata.allColumnsInSelectOrder();
        int count = 0;
        while (columns.hasNext())
        {
            if (count++ > 0)
                statement.append(", ");
            statement.append(columns.next().name.toCQLString());
        }

        statement.append(") VALUES (");
        for (int i = 0; i < count; i++)
        {
            if (i > 0)
                statement.append(", ");
            statement.append('?');
        }
        return statement.append(')').toString();
    }

    private static int countPartitions(SSTableReader reader)
    {
        int partitions = 0;
        try (ISSTableScanner scanner = reader.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator ignored = scanner.next())
                {
                    partitions++;
                }
            }
        }
        return partitions;
    }

    private static void assertWalkEquals(SSTableReader parent, Result split, long seed,
                                         String schema, int childCount)
    {
        try (ISSTableScanner expected = parent.getScanner())
        {
            for (Child child : split.children)
            {
                try (ISSTableScanner actual = child.reader.getScanner())
                {
                    while (actual.hasNext())
                    {
                        assertTrue(failureContext("split has an extra partition", seed, schema, childCount),
                                   expected.hasNext());
                        try (UnfilteredRowIterator expectedPartition = expected.next();
                             UnfilteredRowIterator actualPartition = actual.next())
                        {
                            assertTrue(failureContext("partition differs at " + expectedPartition.partitionKey(),
                                                      seed, schema, childCount),
                                       Util.sameContent(expectedPartition, actualPartition));
                        }
                    }
                }
            }
            assertFalse(failureContext("split omitted parent partitions", seed, schema, childCount),
                        expected.hasNext());
        }
    }

    private static String failureContext(String message, long seed, String schema, int childCount)
    {
        return message + " for seed " + seed + ", childCount " + childCount + ", schema:\n" + schema;
    }

    private static void release(Result split)
    {
        for (Child child : split.children)
            child.reader.selfRef().release();
    }
}
