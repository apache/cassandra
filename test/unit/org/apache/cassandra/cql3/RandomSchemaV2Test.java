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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.junit.Test;

import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ast.CreateIndexDDL;
import org.apache.cassandra.cql3.ast.CreateIndexDDL.Indexer;
import org.apache.cassandra.cql3.ast.Mutation;
import org.apache.cassandra.cql3.ast.ReferenceExpression;
import org.apache.cassandra.cql3.ast.Select;
import org.apache.cassandra.cql3.ast.Symbol;
import org.apache.cassandra.cql3.ast.TableReference;
import org.apache.cassandra.cql3.ast.Txn;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.apache.cassandra.utils.CassandraGenerators;
import org.awaitility.Awaitility;

import static accord.utils.Property.qt;
import static java.lang.String.format;

public class RandomSchemaV2Test extends CQLTester
{
    @BeforeClass
    public static void setUpClass()
    {
        prePrepareServer();

        // When values are large SAI will drop them... soooo... disable that... this test does not care about perf but correctness
        DatabaseDescriptor.getRawConfig().sai_frozen_term_size_warn_threshold = null;
        DatabaseDescriptor.getRawConfig().sai_frozen_term_size_fail_threshold = null;

        // Once per-JVM is enough
        prepareServer();
    }

    enum Mode { AccordEnabled, Default}

    @Test
    public void test()
    {
        requireNetwork();
        Gen<ProtocolVersion> clientVersionGen = rs -> {
            if (rs.decide(.8)) return null;
            return rs.pickOrderedSet(ProtocolVersion.SUPPORTED);
        };
        Gen<Mode> modeGen = Gens.enums().all(Mode.class);
        qt().withExamples(100).forAll(Gens.random(), modeGen, clientVersionGen).check((rs, mode, protocolVersion) -> {
            clearState();

            KeyspaceMetadata ks = createKeyspace(rs);
            TableMetadata metadata = createTable(rs, ks.name);
            if (mode == Mode.AccordEnabled)
            {
                // enable accord
                schemaChange(String.format("ALTER TABLE %s WITH " + TransactionalMode.full.asCqlParam(), metadata));
                metadata = Objects.requireNonNull(Schema.instance.getTableMetadata(ks.name, metadata.name));
            }
            Map<ColumnMetadata, CreateIndexDDL> indexedColumns = createIndex(rs, metadata);
            Mutation mutation = nonTransactionMutation(rs, metadata);
            if (mode == Mode.AccordEnabled)
                mutation = mutation.withoutTimestamp().withoutTTL(); // Accord doesn't allow custom timestamps or TTL
            Select select = select(mutation);
            Object[][] expectedRows = rows(mutation);
            try
            {
                if (protocolVersion != null)
                {
                    //TODO (usability): as of this moment reads don't go through Accord when transaction_mode='full', so need to wrap the select in a txn
                    executeNet(protocolVersion, mode == Mode.AccordEnabled ? Txn.wrap(mutation) : mutation);
                    assertRowsNet(executeNet(protocolVersion, mode == Mode.AccordEnabled ? Txn.wrap(select) : select), expectedRows);
                }
                else
                {
                    execute(mode == Mode.AccordEnabled ? Txn.wrap(mutation) : mutation);
                    assertRows(execute(mode == Mode.AccordEnabled ? Txn.wrap(select) : select), expectedRows);
                }
            }
            catch (Throwable t)
            {
                StringBuilder sb = new StringBuilder();
                sb.append("Error writing/reading:");
                sb.append("\nKeyspace:\n ").append(Keyspace.open(metadata.keyspace).getMetadata().toCqlString(false, false, false));
                CassandraGenerators.visitUDTs(metadata, udt -> sb.append("\nUDT:\n").append(udt.toCqlString(false, false, false)));
                sb.append("\nTable:\n").append(metadata.toCqlString(false, false, false));
                sb.append("\nMutation:\n").append(mutation);
                sb.append("\nSelect:\n").append(select);
                throw new AssertionError(sb.toString(), t);
            }

            checkIndexes(metadata, indexedColumns, mutation, mode, protocolVersion);
        });
    }

    private void checkIndexes(TableMetadata metadata, Map<ColumnMetadata, CreateIndexDDL> indexedColumns, Mutation mutation, Mode mode, ProtocolVersion protocolVersion)
    {
        if (indexedColumns.isEmpty()) return;

        // 2i time!
        // check each column 1 by 1
        for (ColumnMetadata col : indexedColumns.keySet())
        {
            Select select = select(mutation, col, mode);
            try
            {
                if (protocolVersion != null)
                {
                    assertRowsNet(executeNet(protocolVersion, mode == Mode.AccordEnabled ? Txn.wrap(select) : select),
                                  rows(mutation));
                }
                else
                {
                    assertRows(execute(mode == Mode.AccordEnabled ? Txn.wrap(select) : select),
                               rows(mutation));
                }
            }
            catch (Throwable t)
            {
                throw new AssertionError(format("Error reading %s index for col %s: %s:\nKeyspace:\n %s\nTable:\n%s",
                                                indexedColumns.get(col).indexer.name().toCQL(),
                                                col, col.type.unwrap().asCQL3Type(),
                                                Keyspace.open(metadata.keyspace).getMetadata().toCqlString(false, false, false),
                                                metadata.toCqlString(false, false, false)),
                                         t);
            }
        }
        filterKnownIssues(indexedColumns);

        // check what happens when we query all columns within a single indexer
        var groupByIndexer = indexedColumns.entrySet().stream().collect(Collectors.groupingBy(e -> e.getValue().indexer.name(), Collectors.toMap(e -> e.getKey(), e -> e.getValue())));
        for (var e : groupByIndexer.entrySet())
        {
            if (e.getValue().size() == 1) continue; // tested already doing single column queries
            checkMultipleIndexes(metadata, e.getValue(), mutation, mode, protocolVersion);
        }

        // check what happens when we query with all columns
        checkMultipleIndexes(metadata, indexedColumns, mutation, mode, protocolVersion);
    }

    private void filterKnownIssues(Map<ColumnMetadata, CreateIndexDDL> indexedColumns)
    {
        // Remove once CASSANDRA-19891 is fixed
        List<ColumnMetadata> toRemove = new ArrayList<>();
        for (Map.Entry<ColumnMetadata, CreateIndexDDL> e : indexedColumns.entrySet())
        {
            AbstractType<?> type = e.getKey().type.unwrap();
            if (type instanceof CompositeType && AbstractTypeGenerators.contains(type, t -> t instanceof MapType))
                toRemove.add(e.getKey());
        }
        toRemove.forEach(indexedColumns::remove);
    }

    private void checkMultipleIndexes(TableMetadata metadata, Map<ColumnMetadata, CreateIndexDDL> indexedColumns, Mutation mutation, Mode mode, ProtocolVersion protocolVersion)
    {
        Select select = select(mutation, indexedColumns.keySet(), mode);
        if (requiresAllowFiltering(indexedColumns))
            select = select.withAllowFiltering();
        try
        {
            if (protocolVersion != null)
            {
                assertRowsNet(executeNet(protocolVersion, mode == Mode.AccordEnabled ? Txn.wrap(select) : select),
                              rows(mutation));
            }
            else
            {
                assertRows(execute(mode == Mode.AccordEnabled ? Txn.wrap(select) : select),
                           rows(mutation));
            }
        }
        catch (Throwable t)
        {
            throw new AssertionError(format("Error reading all indexes:\nKeyspace:\n %s\nTable:\n%s\nIndexes: [%s]",
                                            Keyspace.open(metadata.keyspace).getMetadata().toCqlString(false, false, false),
                                            metadata.toCqlString(false, false, false),
                                            indexedColumns.values().stream().map(ddl -> ddl.toCQL().replace("org.apache.cassandra.db.marshal.", "")).collect(Collectors.joining(";\n\t"))),
                                     t);
        }
    }

    private boolean requiresAllowFiltering(Map<ColumnMetadata, CreateIndexDDL> indexedColumns)
    {
        return indexedColumns.values().stream().anyMatch(c -> c.indexer.kind() != Indexer.Kind.sai);
    }

    private static Select select(Mutation mutation, Set<ColumnMetadata> columns, Mode mode)
    {
        Select.Builder builder = new Select.Builder().withTable(mutation.table);
        for (ColumnMetadata col : columns)
        {
            Symbol symbol = Symbol.from(col);
            builder.withColumnEquals(symbol, mutation.values.get(symbol));
        }
        if (mode == Mode.AccordEnabled )
        {
            for (ColumnMetadata col : mutation.table.partitionKeyColumns())
            {
                if (columns.contains(col)) continue; // already included
                Symbol symbol = Symbol.from(col);
                builder.withColumnEquals(symbol, mutation.values.get(symbol));
            }
            builder.withLimit(1);
        }
        return builder.build();
    }

    private static Select select(Mutation mutation, ColumnMetadata col, Mode mode)
    {
        return select(mutation, Collections.singleton(col), mode);
    }

    private Map<ColumnMetadata, CreateIndexDDL> createIndex(RandomSource rs, TableMetadata metadata)
    {
        Map<ColumnMetadata, CreateIndexDDL> indexed = new LinkedHashMap<>();
        for (ColumnMetadata col : metadata.columns())
        {
            if (col.type.isReversed()) continue; //TODO (correctness): see https://issues.apache.org/jira/browse/CASSANDRA-19889
            if (col.name.toString().length() >= 48) continue; // TODO (correctness): https://issues.apache.org/jira/browse/CASSANDRA-19897

            AbstractType<?> type = col.type.unwrap();
            if (type.isCollection() && !type.isFrozenCollection()) continue; //TODO (coverage): include non-frozen collections;  the index part works fine, its the select that fails... basic equality isn't allowed for map type... so how do you query?
            List<Indexer> allowed = allowed(metadata, col);
            if (allowed.isEmpty()) continue;
            Indexer indexer = rs.pick(allowed);
            ReferenceExpression colExpression = Symbol.from(col);
            if (type.isFrozenCollection())
                colExpression = new CreateIndexDDL.CollectionReference(CreateIndexDDL.CollectionReference.Kind.FULL, colExpression);

            String name = createIndexName();
            CreateIndexDDL ddl = new CreateIndexDDL(rs.pick(CreateIndexDDL.Version.values()),
                                                    indexer,
                                                    Optional.of(new Symbol(name, UTF8Type.instance)),
                                                    TableReference.from(metadata),
                                                    Collections.singletonList(colExpression),
                                                    Collections.emptyMap());
            String stmt = ddl.toCQL();
            logger.info(stmt);
            schemaChange(stmt);

            SecondaryIndexManager indexManager = Keyspace.open(metadata.keyspace).getColumnFamilyStore(metadata.name).indexManager;
            Index index = indexManager.getIndexByName(name);
            Awaitility.await(stmt)
                      .atMost(1, TimeUnit.MINUTES)
                      .until(() -> indexManager.isIndexQueryable(index));

            indexed.put(col, ddl);
        }
        return indexed;
    }

    private List<Indexer> allowed(TableMetadata metadata, ColumnMetadata col)
    {
        return CreateIndexDDL.supportedIndexers().stream()
                             .filter(i -> i.supported(metadata, col))
                             .collect(Collectors.toList());
    }
}
