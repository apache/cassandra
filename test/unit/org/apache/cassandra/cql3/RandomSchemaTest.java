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

package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.apache.cassandra.utils.AbstractTypeGenerators.TypeGenBuilder;
import org.apache.cassandra.utils.CassandraGenerators;
import org.apache.cassandra.utils.CassandraGenerators.TableMetadataBuilder;
import org.apache.cassandra.utils.FailingConsumer;
import org.apache.cassandra.utils.Generators;
import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.generators.SourceDSL;
import org.quicktheories.impl.JavaRandom;

import static org.apache.cassandra.utils.Generators.IDENTIFIER_GEN;

public class RandomSchemaTest extends CQLTester.InMemory
{
    private static final Logger logger = LoggerFactory.getLogger(RandomSchemaTest.class);

    static
    {
        // make sure blob is always the same
        CassandraRelevantProperties.TEST_BLOB_SHARED_SEED.setInt(42);

        requireNetwork();
    }

    @Test
    public void test()
    {
        // in accord branch there is a much cleaner api for this pattern...
        Gen<AbstractTypeGenerators.ValueDomain> domainGen = SourceDSL.integers().between(1, 100).map(i -> i < 2 ? AbstractTypeGenerators.ValueDomain.NULL : i < 4 ? AbstractTypeGenerators.ValueDomain.EMPTY_BYTES : AbstractTypeGenerators.ValueDomain.NORMAL);
        // make sure ordering is determanstic, else repeatability breaks
        NavigableMap<String, SSTableFormat<?, ?>> formats = new TreeMap<>(DatabaseDescriptor.getSSTableFormats());
        Gen<SSTableFormat<?, ?>> ssTableFormatGen = SourceDSL.arbitrary().pick(new ArrayList<>(formats.values()));
        qt().checkAssert(random -> {
            resetSchema();

            // TODO : when table level override of sstable format is allowed, migrate to that
            SSTableFormat<?, ?> sstableFormat = ssTableFormatGen.generate(random);
            DatabaseDescriptor.setSelectedSSTableFormat(sstableFormat);

            Gen<String> udtName = Generators.unique(IDENTIFIER_GEN);

            TypeGenBuilder withoutUnsafeEquality = AbstractTypeGenerators.withoutUnsafeEquality()
                                                                         .withUserTypeKeyspace(KEYSPACE)
                                                                         .withUDTNames(udtName);
            TableMetadata metadata = new TableMetadataBuilder()
                                     .withKeyspaceName(KEYSPACE)
                                     .withTableKinds(TableMetadata.Kind.REGULAR)
                                     .withKnownMemtables()
                                     .withDefaultTypeGen(AbstractTypeGenerators.builder()
                                                                               .withoutEmpty()
                                                                               .withUserTypeKeyspace(KEYSPACE)
                                                                               .withMaxDepth(2)
                                                                               .withDefaultSetKey(withoutUnsafeEquality)
                                                                               .withoutTypeKinds(AbstractTypeGenerators.TypeKind.COUNTER)
                                                                               .withUDTNames(udtName)
                                                                               .build())
                                     .withPartitionColumnsCount(1)
                                     .withPrimaryColumnTypeGen(new TypeGenBuilder(withoutUnsafeEquality)
                                                               // map of vector of map crossed the size cut-off for one of the tests, so changed max depth from 2 to 1, so we can't have the second map
                                                               .withMaxDepth(1)
                                                               .build())
                                     .withClusteringColumnsBetween(1, 2)
                                     .withRegularColumnsBetween(1, 5)
                                     .withStaticColumnsBetween(0, 2)
                                     .build(random);
            maybeCreateUDTs(metadata);
            String createTable = metadata.toCqlString(false, false);
            // just to make the CREATE TABLE stmt easier to read for CUSTOM types
            createTable = createTable.replaceAll("org.apache.cassandra.db.marshal.", "");
            createTable(KEYSPACE, createTable);

            Gen<ByteBuffer[]> dataGen = CassandraGenerators.data(metadata, domainGen);
            String insertStmt = insertStmt(metadata);
            int partitionColumnCount = metadata.partitionKeyColumns().size();
            int primaryColumnCount = primaryColumnCount(metadata);
            String selectStmt = selectStmt(metadata);
            String tokenStmt = tokenStmt(metadata);

            for (int i = 0; i < 100; i++)
            {
                ByteBuffer[] expected = dataGen.generate(random);
                try
                {
                    ByteBuffer[] partitionKeys = Arrays.copyOf(expected, partitionColumnCount);
                    ByteBuffer[] rowKey = Arrays.copyOf(expected, primaryColumnCount);
                    execute(insertStmt, (Object[]) expected);
                    // check memtable
                    assertRows(execute(selectStmt, (Object[]) rowKey), expected);
                    assertRows(execute(tokenStmt, (Object[]) partitionKeys), partitionKeys);
                    assertRowsNet(executeNet(selectStmt, (Object[]) rowKey), expected);

                    // check sstable
                    flush(KEYSPACE, metadata.name);
                    compact(KEYSPACE, metadata.name);
                    assertRows(execute(selectStmt, (Object[]) rowKey), expected);
                    assertRows(execute(tokenStmt, (Object[]) partitionKeys), partitionKeys);
                    assertRowsNet(executeNet(selectStmt, (Object[]) rowKey), expected);

                    execute("TRUNCATE " + metadata);
                }
                catch (Throwable t)
                {
                    Iterator<ColumnMetadata> it = metadata.allColumnsInSelectOrder();
                    List<String> literals = new ArrayList<>(expected.length);
                    for (int idx = 0; idx < expected.length; idx++)
                    {
                        assert it.hasNext();
                        ColumnMetadata meta = it.next();
                        ByteBuffer expct = expected[idx];
                        literals.add(expct == null ? "null" : !expct.hasRemaining() ? "empty" : meta.type.asCQL3Type().toCQLLiteral(expct));
                    }
                    throw new AssertionError(String.format("Failure at attempt %d with schema\n%s\nAnd SSTable Format %s\nfor values %s", i, createTable, DatabaseDescriptor.getSelectedSSTableFormat(), literals), t);
                }
            }
        });
    }

    private void maybeCreateUDTs(TableMetadata metadata)
    {
        Set<UserType> udts = CassandraGenerators.extractUDTs(metadata);
        if (!udts.isEmpty())
        {
            Deque<UserType> pending = new ArrayDeque<>(udts);
            Set<ByteBuffer> created = new HashSet<>();
            while (!pending.isEmpty())
            {
                UserType next = pending.poll();
                Set<UserType> subTypes = AbstractTypeGenerators.extractUDTs(next);
                subTypes.remove(next); // it includes self
                if (subTypes.isEmpty() || subTypes.stream().allMatch(t -> created.contains(t.name)))
                {
                    String cql = next.toCqlString(false, false);
                    logger.warn("Creating UDT {}", cql);
                    schemaChange(cql);
                    created.add(next.name);
                }
                else
                {
                    logger.warn("Unable to create UDT {}; following sub-types still not created: {}",
                                next.getCqlTypeName(),
                                subTypes.stream().filter(t -> !created.contains(t.name)).collect(Collectors.toSet()));
                    pending.add(next);
                }
            }
        }
    }

    private static int primaryColumnCount(TableMetadata metadata)
    {
        return metadata.partitionKeyColumns().size() + metadata.clusteringColumns().size();
    }

    private static String tokenStmt(TableMetadata metadata)
    {
        StringBuilder sb = new StringBuilder();
        List<String> columns = metadata.partitionKeyColumns().stream().map(column -> column.name.toCQLString()).collect(Collectors.toList());
        List<String> binds = columns.stream().map(ignore -> "?").collect(Collectors.toList());
        sb.append("SELECT ").append(String.join(", ", columns));
        sb.append(" FROM ").append(metadata);
        sb.append(" WHERE token(").append(String.join(", ", columns)).append(") = token(").append(String.join(", ", binds)).append(')');
        return sb.toString();
    }

    private String selectStmt(TableMetadata metadata)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT * FROM ").append(metadata).append(" WHERE ");
        for (ColumnMetadata column : ImmutableList.<ColumnMetadata>builder()
                                                  .addAll(metadata.partitionKeyColumns())
                                                  .addAll(metadata.clusteringColumns())
                                                  .build())
        {
            sb.append(column.name.toCQLString()).append(" = ? AND ");
        }
        sb.setLength(sb.length() - " AND ".length());
        return sb.toString();
    }

    private String insertStmt(TableMetadata metadata)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(metadata.toString()).append(" (");
        Iterator<ColumnMetadata> cols = metadata.allColumnsInSelectOrder();
        while (cols.hasNext())
            sb.append(cols.next().name.toCQLString()).append(", ");
        sb.setLength(sb.length() - 2); // remove last ", "
        sb.append(") VALUES (");
        for (int i = 0; i < metadata.columns().size(); i++)
        {
            if (i > 0)
                sb.append(", ");
            sb.append('?');
        }
        sb.append(')');
        return sb.toString();
    }

    private static Builder qt()
    {
        return new Builder();
    }

    public static class Builder
    {
        private long seed = System.currentTimeMillis();
        private int examples = 10;

        /**
         * Used to reproduce the test with a fixed seed; main use case is to rerun the same test that failed in CI.
         *
         * This function is dead code, it exists to help authors debug when a failing test is found.
         */
        @SuppressWarnings("unused")
        public Builder withFixedSeed(long seed)
        {
            this.seed = seed;
            return this;
        }

        /**
         * Used to override how many examples to run with.  This is dead code, but exists to allow authors to run the test
         * many times to make sure its stable before committing.
         */
        @SuppressWarnings("unused")
        public Builder withExamples(int examples)
        {
            this.examples = examples;
            return this;
        }

        // copied from java.util.Random
        private static final long multiplier = 0x5DEECE66DL;
        private static final long addend = 0xBL;
        private static final long mask = (1L << 48) - 1;

        public void checkAssert(FailingConsumer<RandomnessSource> test)
        {
            JavaRandom random = new JavaRandom(seed);
            for (int i = 0; i < examples; i++)
            {
                if (i > 0)
                    seed = (seed * multiplier + addend) & mask;
                random.setSeed(seed);
                try
                {
                    test.doAccept(random);
                }
                catch (Throwable e)
                {
                    throw new PropertyError(seed, e);
                }
            }
        }
    }

    public static class PropertyError extends AssertionError
    {
        public PropertyError(long seed, Throwable cause)
        {
            super(message(seed), cause);
        }

        private static String message(long seed)
        {
            return "Failure for seed " + seed;
        }
    }
}
