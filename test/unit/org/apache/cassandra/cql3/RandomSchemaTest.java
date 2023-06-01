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
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.apache.cassandra.utils.AbstractTypeGenerators.TypeGenBuilder;
import org.apache.cassandra.utils.CassandraGenerators;
import org.apache.cassandra.utils.CassandraGenerators.TableMetadataBuilder;
import org.apache.cassandra.utils.FailingConsumer;
import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.generators.SourceDSL;
import org.quicktheories.impl.JavaRandom;

public class RandomSchemaTest extends CQLTester.InMemory
{
    private static final Logger logger = LoggerFactory.getLogger(RandomSchemaTest.class);

    static
    {
        // make sure blob is always the same
        CassandraRelevantProperties.TEST_BLOB_SHARED_SEED.setInt(42);
    }

    {
        requireNetwork();
    }

    @Test
    public void test()
    {
        Gen<Boolean> nulls = SourceDSL.integers().between(1, 100).map(i -> i < 5);
        qt().checkAssert(random -> {
            TypeGenBuilder withoutUnsafeEquality = AbstractTypeGenerators.builder()
                                                                         .withoutEmpty()
                                                                         .withUserTypeKeyspace(KEYSPACE)
                                                                         .withoutPrimitive(DurationType.instance)
                                                                         // decimal "normalizes" the data to compare, so primary columns "may" mutate the data, causing missmatches
                                                                         // see CASSANDRA-18530
                                                                         .withoutPrimitive(DecimalType.instance);
            TableMetadata metadata = new TableMetadataBuilder()
                                     .withKeyspaceName(KEYSPACE)
                                     .withTableKinds(TableMetadata.Kind.REGULAR)
                                     .withDefaultTypeGen(AbstractTypeGenerators.builder()
                                                                               .withoutEmpty()
                                                                               .withUserTypeKeyspace(KEYSPACE)
                                                                               .withMaxDepth(2)
                                                                               .withDefaultSetKey(withoutUnsafeEquality)
                                                                               .build())
                                     .withPartitionColumnsCount(1)
                                     .withPrimaryColumnTypeGen(new TypeGenBuilder(withoutUnsafeEquality)
                                                               .withMaxDepth(2)
                                                               .build())
                                     .withClusteringColumnsBetween(1, 2)
                                     .build(random);
            maybeCreateUDTs(metadata);
            createTable(KEYSPACE, metadata.toCqlString(false, false));

            Gen<ByteBuffer[]> dataGen = CassandraGenerators.data(metadata, nulls);
            String insertStmt = insertStmt(metadata);
            int primaryColumnCount = primaryColumnCount(metadata);
            String selectStmt = selectStmt(metadata);

            for (int i = 0; i < 1000; i++)
            {
                ByteBuffer[] expected = dataGen.generate(random);
                try
                {
                    ByteBuffer[] rowKey = Arrays.copyOf(expected, primaryColumnCount);
                    execute(insertStmt, expected);
                    // check memtable
                    assertRows(execute(selectStmt, rowKey), expected);
                    assertRowsNet(executeNet(selectStmt, rowKey), expected);

                    // check sstable
                    flush(KEYSPACE, metadata.name);
                    compact(KEYSPACE, metadata.name);
                    assertRows(execute(selectStmt, rowKey), expected);
                    assertRowsNet(executeNet(selectStmt, rowKey), expected);

                    execute("TRUNCATE " + metadata);
                }
                catch (Throwable t)
                {
                    Iterator<ColumnMetadata> it = metadata.allColumnsInSelectOrder();
                    List<String> cql = new ArrayList<>(expected.length);
                    for (int idx = 0; idx < expected.length; idx++)
                    {
                        assert it.hasNext();
                        ColumnMetadata meta = it.next();
                        cql.add(meta.type.asCQL3Type().toCQLLiteral(expected[idx]));
                    }
                    AssertionError error = new AssertionError(String.format("Failure for values %s with schema\n%s", cql, metadata.toCqlString(false, false)), t);
                    throw error;
                }
            }
        });
    }

    private void maybeCreateUDTs(TableMetadata metadata)
    {
        Set<UserType> udts = CassandraGenerators.extractUDTs(metadata);
        if (!udts.isEmpty())
        {
            Deque<UserType> pending = new ArrayDeque<>();
            pending.addAll(udts);
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
                    logger.warn("Unable to create UDT {}; following sub-types still not created: {}", next.getCqlTypeName(), subTypes.stream().filter(t -> !created.contains(t.name)).collect(Collectors.toSet()));
                    pending.add(next);
                }
            }
        }
    }

    private static int primaryColumnCount(TableMetadata metadata)
    {
        return metadata.partitionKeyColumns().size() + metadata.clusteringColumns().size();
    }

    private String selectStmt(TableMetadata metadata)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT * FROM ").append(metadata).append(" WHERE ");
        for (ColumnMetadata column : ImmutableList.<ColumnMetadata>builder().addAll(metadata.partitionKeyColumns()).addAll(metadata.clusteringColumns()).build())
            sb.append(column.name.toCQLString()).append(" = ? AND ");
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
        sb.append(")");
        return sb.toString();
    }

    private static Builder qt()
    {
        return new Builder();
    }

    public static class Builder
    {
        private long seed = System.currentTimeMillis();

        public Builder withFixedSeed(long seed)
        {
            this.seed = seed;
            return this;
        }

        public void checkAssert(FailingConsumer<RandomnessSource> test)
        {
            JavaRandom random = new JavaRandom(seed);
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
