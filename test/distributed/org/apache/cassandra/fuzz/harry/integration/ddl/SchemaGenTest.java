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

package org.apache.cassandra.fuzz.harry.integration.ddl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.fuzz.harry.integration.QuickTheoriesAdapter;
import org.apache.cassandra.harry.ddl.ColumnSpec;
import org.apache.cassandra.harry.ddl.SchemaGenerators;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.operations.CompiledStatement;

import org.apache.cassandra.harry.operations.Query;
import org.apache.cassandra.harry.util.TestRunner;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Pair;
import org.quicktheories.QuickTheory;
import org.quicktheories.core.Gen;

import static org.quicktheories.generators.SourceDSL.integers;

public class SchemaGenTest extends CQLTester
{
    private static final int CYCLES = 10;

    // TODO: compact storage tests
    @Test
    public void testSelectForwardAndReverseIteration() throws Throwable
    {
        Generator<SchemaSpec> gen = new SchemaGenerators.Builder(KEYSPACE).partitionKeyColumnCount(1, 4)
                                                                          .clusteringColumnCount(1, 10)
                                                                          .regularColumnCount(0, 10)
                                                                          .staticColumnCount(0, 10)
                                                                          .generator();

        TestRunner.test(gen,
                        schemaDefinition -> {
                            String tableDef = schemaDefinition.compile().cql();
                            createTable(tableDef);

                            try
                            {
                                CompiledStatement statement = Query.selectAllColumns(schemaDefinition, 1, false).toSelectStatement();
                                execute(statement.cql(), statement.bindings());
                                statement = Query.selectAllColumns(schemaDefinition, 1, true).toSelectStatement();
                                execute(statement.cql(), statement.bindings());
                            }
                            catch (Throwable t)
                            {
                                throw new AssertionError("Exception caught", t);
                            }
                        });
    }

    @Test
    public void createTableRoundTrip() throws Throwable
    {
        Generator<SchemaSpec> gen = new SchemaGenerators.Builder(KEYSPACE).partitionKeyColumnCount(1, 10)
                                                                          .clusteringColumnCount(1, 10)
                                                                          .regularColumnCount(0, 10)
                                                                          .staticColumnCount(0, 10)
                                                                          .generator();

        TestRunner.test(gen,
                        schemaDefinition -> {
                            String tableDef = schemaDefinition.compile().cql();
                            createTable(KEYSPACE, tableDef);
                            TableMetadata tableMetadata = Keyspace.open(KEYSPACE).getColumnFamilyStore(schemaDefinition.table).metadata.get();
                            compareColumns(schemaDefinition.partitionKeys, tableMetadata.partitionKeyColumns());
                            compareColumns(schemaDefinition.clusteringKeys, tableMetadata.clusteringColumns());
                            compareColumns(schemaDefinition.regularColumns, tableMetadata.regularColumns());
                            compareColumns(schemaDefinition.staticColumns, tableMetadata.staticColumns());
                        });
    }

    @Test
    public void testReverseComparator()
    {
        SchemaSpec spec = new SchemaSpec(KEYSPACE, "tbl1",
                                         Arrays.asList(ColumnSpec.pk("pk1", ColumnSpec.asciiType),
                                                       ColumnSpec.pk("pk2", ColumnSpec.int64Type)),
                                         Arrays.asList(ColumnSpec.ck("ck1", ColumnSpec.asciiType, true),
                                                       ColumnSpec.ck("ck2", ColumnSpec.int64Type, false)),
                                         Arrays.asList(ColumnSpec.regularColumn("v1", ColumnSpec.asciiType),
                                                       ColumnSpec.regularColumn("v2", ColumnSpec.asciiType),
                                                       ColumnSpec.regularColumn("v3", ColumnSpec.int64Type),
                                                       ColumnSpec.regularColumn("v4", ColumnSpec.int64Type)),
                                         Arrays.asList(ColumnSpec.staticColumn("static1", ColumnSpec.asciiType),
                                                       ColumnSpec.staticColumn("static2", ColumnSpec.int64Type)));


        String tableDef = spec.compile().cql();
        createTable(KEYSPACE, tableDef);
        TableMetadata tableMetadata = Keyspace.open(KEYSPACE).getColumnFamilyStore(spec.table).metadata.get();
        compareColumns(spec.partitionKeys, tableMetadata.partitionKeyColumns());
        compareColumns(spec.clusteringKeys, tableMetadata.clusteringColumns());
        compareColumns(spec.regularColumns, tableMetadata.regularColumns());
        compareColumns(spec.staticColumns, tableMetadata.staticColumns());
    }


    @Test
    public void testSchemaGeneration()
    {
        Gen<Pair<Integer, Integer>> ckCounts = integers().between(0, 4).zip(integers().between(0, 6), Pair::create);
        Gen<Pair<Integer, Integer>> regCounts = integers().between(0, 4).zip(integers().between(0, 6), Pair::create);
//        Gen<Pair<Integer, Integer>> staticCounts = integers().between(0, 4).zip(integers().between(0, 6), Pair::create);
        Gen<Pair<Integer, Integer>> pkCounts = integers().between(1, 4).zip(integers().between(0, 6), Pair::create);

        Gen<SchemaGenerationInputs> inputs = pkCounts.zip(ckCounts, regCounts,
                                                          (pks, cks, regs) ->
                                                          new SchemaGenerationInputs(pks.left, pks.left + pks.right,
                                                                                     cks.left, cks.left + cks.right,
                                                                                     regs.left, regs.left + regs.right));

        Gen<Pair<SchemaGenerationInputs, SchemaSpec>> schemaAndInputs = inputs.flatMap(input -> {
            Generator<SchemaSpec> gen = new SchemaGenerators.Builder("test")
                    .partitionKeyColumnCount(input.minPk, input.maxPk)
                    .clusteringColumnCount(input.minCks, input.maxCks)
                    .regularColumnCount(input.minRegs, input.maxRegs)
                    .generator();

            return QuickTheoriesAdapter.convert(gen).map(schema -> Pair.create(input, schema));
        });

        qt().forAll(schemaAndInputs)
            .check(schemaAndInput -> {
                SchemaGenerationInputs input = schemaAndInput.left;
                SchemaSpec schema = schemaAndInput.right;

                return schema.partitionKeys.size() <= input.maxPk && schema.partitionKeys.size() >= input.minPk &&
                       schema.clusteringKeys.size() <= input.maxCks && schema.clusteringKeys.size() >= input.minCks &&
                       schema.regularColumns.size() <= input.maxRegs && schema.regularColumns.size() >= input.minRegs;
            });
    }

    private static class SchemaGenerationInputs {
        private final int minPk;
        private final int maxPk;
        private final int minCks;
        private final int maxCks;
        private final int minRegs;
        private final int maxRegs;

        public SchemaGenerationInputs(int minPk, int maxPk, int minCks, int maxCks, int minRegs, int maxRegs)
        {
            this.minPk = minPk;
            this.maxPk = maxPk;
            this.minCks = minCks;
            this.maxCks = maxCks;
            this.minRegs = minRegs;
            this.maxRegs = maxRegs;
        }
    }

    private static boolean compareColumns(Collection<ColumnSpec<?>> expectedColl, Collection<ColumnMetadata> actualColl)
    {
        List<ColumnSpec<?>> expectedSorted = new ArrayList<>(expectedColl);
        expectedSorted.sort(Comparator.comparing(Object::toString));
        List<ColumnMetadata> actualSorted = new ArrayList<>(actualColl);
        actualSorted.sort(Comparator.comparing(Object::toString));

        Iterator<ColumnSpec<?>> expectedIter = expectedSorted.iterator();
        Iterator<ColumnMetadata> actualIter = actualSorted.iterator();

        while (expectedIter.hasNext() && actualIter.hasNext())
        {
            ColumnSpec expected = expectedIter.next();
            ColumnMetadata actual = actualIter.next();

            Assert.assertEquals(expected.kind.toString(), actual.kind.toString());
            Assert.assertEquals(expected.name, actual.name.toString());
            Assert.assertEquals(expected.type.toString(), actual.type.asCQL3Type().toString());
        }

        Assert.assertEquals(String.format("Collections %s and %s have different sizes", expectedColl, actualColl),
                            expectedIter.hasNext(), actualIter.hasNext());
        return true;
    }

    public static QuickTheory qt()
    {
        return QuickTheory.qt()
                          .withExamples(CYCLES)
                          .withShrinkCycles(0);
    }
}