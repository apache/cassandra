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

package org.apache.cassandra.index.sai.cql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Ordering;
import com.google.common.collect.Streams;
import com.google.common.collect.TreeMultimap;
import org.junit.Test;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Relation;
import org.apache.cassandra.cql3.SingleColumnRelation;
import org.apache.cassandra.cql3.WhereClause;
import org.apache.cassandra.index.sai.SAITester;
import org.assertj.core.util.Lists;

/**
 * This test produces a random schema, loads it with random data and then runs a series of
 * random queries against it.
 *
 * The purpose of the test is to test that the <code>RowFilter</code> and <code>Operation</code>
 * classes correctly support complex queries.
 *
 * At present the test only supports ascii and int datatypes and only supports EQ expressions.
 * It is intended that this can be extended in the future to support more functionality.
 */
public class RandomisedComplexQueryTest extends SAITester
{
    private static final List<TypeInfo> types = Lists.list(TypeInfo.create(CQL3Type.Native.ASCII, () -> CQLTester.getRandom().nextAsciiString(4, 30), true),
                                                           TypeInfo.create(CQL3Type.Native.INT, () -> CQLTester.getRandom().nextIntBetween(0, 1000), false));

    @Test
    public void test() throws Throwable
    {
        for (int test = 0; test < getRandom().nextIntBetween(10, 50); test++)
            runRandomTest();
    }

    private void runRandomTest() throws Throwable
    {
        RandomSchema schema = new RandomSchema();

        createTable(schema.toTableDefinition());

        schema.generateIndexStrings().stream().forEach(index -> createIndex(index));

        waitForIndexQueryable();

        List<RandomRow> data = schema.generateDataset();

        String insert = schema.toInsert();

        for (RandomRow row : data)
            execute(insert, row.toArray());

        for (int query = 0; query < getRandom().nextIntBetween(100, 1000); query++)
            schema.generateQuery().test(this, data);
    }

    public static class RandomSchema
    {
        private final Map<String, RandomColumn> columnMap = new HashMap<>();
        private final TreeMultimap<String, Object> values = TreeMultimap.create(Ordering.natural(), Ordering.arbitrary());

        private final List<RandomColumn> partitionKeys;
        private final List<RandomColumn> clusteringKeys;
        private final List<RandomColumn> normalColumns;

        RandomSchema()
        {
            int bindPosition = 0;
            partitionKeys = generateColumns("pk", CQLTester.getRandom().nextIntBetween(1, 3), bindPosition);
            bindPosition += partitionKeys.size();
            clusteringKeys = generateColumns("ck", CQLTester.getRandom().nextIntBetween(0, 4), bindPosition);
            bindPosition += clusteringKeys.size();
            normalColumns = generateColumns("nc", CQLTester.getRandom().nextIntBetween(1, 10), bindPosition);
        }

        public String toTableDefinition()
        {
            StringBuilder builder = new StringBuilder();

            builder.append("CREATE TABLE %s (");
            builder.append(Streams.concat(partitionKeys.stream(), clusteringKeys.stream(), normalColumns.stream())
                                  .map(RandomColumn::toColumnDefinition)
                                  .collect(Collectors.joining(", ")));
            builder.append(", PRIMARY KEY (");
            String partitionKeyString = partitionKeys.stream().map(RandomColumn::name).collect(Collectors.joining(", ", "(", ")"));
            if (clusteringKeys.isEmpty())
                builder.append(partitionKeyString);
            else
                builder.append(Stream.of(partitionKeyString,
                                         clusteringKeys.stream().map(RandomColumn::name).collect(Collectors.joining(", ")))
                                     .collect(Collectors.joining(", ")));
            builder.append("))");
            return builder.toString();
        }

        public String toInsert()
        {
            StringBuilder builder = new StringBuilder();

            builder.append("INSERT INTO %s (");
            builder.append(Streams.concat(partitionKeys.stream(), clusteringKeys.stream(), normalColumns.stream())
                                  .map(RandomColumn::name)
                                  .collect(Collectors.joining(", ")));
            builder.append(") VALUES (");
            builder.append(Streams.concat(partitionKeys.stream(), clusteringKeys.stream(), normalColumns.stream())
                                  .map(RandomColumn::bindMarker)
                                  .collect(Collectors.joining(", ")));
            builder.append(")");

            return builder.toString();
        }

        public List<String> generateIndexStrings()
        {
            List<String> indexes = new ArrayList<>();
            clusteringKeys.stream().map(RandomColumn::toIndexDefinition).forEach(indexes::add);
            normalColumns.stream().map(RandomColumn::toIndexDefinition).forEach(indexes::add);
            return indexes;
        }

        public List<RandomRow> generateDataset()
        {
            List<RandomRow> data = new ArrayList<>();

            for (int row = 0; row < CQLTester.getRandom().nextIntBetween(100, 1000); row++)
            {
                RandomRow newRow = generateRow();
                // Remove any duplicate rows - makes it easier to build result set
                // It may be possible to handle duplicates in the filtering but for the
                // time being we just get rid of them
                List<RandomRow> duplicates = data.stream()
                                                 .filter(r -> {
                                                    for (int pk = 0; pk < partitionKeys.size(); pk++)
                                                        if (!r.values.get(pk).equals(newRow.values.get(pk)))
                                                            return true;
                                                    return false;
                                                 })
                                                 .collect(Collectors.toList());
                duplicates.stream().forEach(data::remove);
                data.add(newRow);
            }

            return data;
        }

        public RandomQuery generateQuery() throws Throwable
        {
            StringBuilder builder = new StringBuilder();

            boolean applyPrecedence = CQLTester.getRandom().nextBoolean();

            List<RandomColumn> allColumns = Lists.newArrayList(clusteringKeys);
            allColumns.addAll(normalColumns);
            int numberOfElements = CQLTester.getRandom().nextIntBetween(1, allColumns.size());
            Set<RandomColumn> columns = new HashSet<>();
            while (columns.size() < numberOfElements)
                columns.add(allColumns.get(CQLTester.getRandom().nextIntBetween(0, allColumns.size() - 1)));

            RandomColumn[] columnArray = columns.toArray(new RandomColumn[] {});
            int precedenceLevel = 0;
            for (int element = 0; element < numberOfElements - 1; element++)
            {
                if (applyPrecedence && CQLTester.getRandom().nextIntBetween(0, 2) == 0)
                {
                    builder.append("(");
                    precedenceLevel++;
                }
                builder.append(columnArray[element].name);
                builder.append(" = ");
                builder.append(columnArray[element].randomQueryValue());

                if (applyPrecedence && CQLTester.getRandom().nextIntBetween(0, 2) == 2 && precedenceLevel > 0)
                {
                    builder.append(")");
                    precedenceLevel--;
                }
                builder.append(CQLTester.getRandom().nextBoolean() ? " AND " : " OR ");
            }
            builder.append(columnArray[columnArray.length - 1].name);
            builder.append(" = ");
            builder.append(columnArray[columnArray.length - 1].randomQueryValue());
            if (applyPrecedence)
                while (precedenceLevel-- > 0)
                    builder.append(")");

            return new RandomQuery(this, builder.toString());
        }

        private RandomRow generateRow()
        {
            RandomRow row = new RandomRow();
            Streams.concat(partitionKeys.stream(), clusteringKeys.stream(), normalColumns.stream())
                   .map(RandomColumn::nextValue).forEach(row::add);
            return row;
        }

        private List<RandomColumn> generateColumns(String prefix, int count, int bindPosition)
        {
            List<RandomColumn> columns = new ArrayList<>(count);
            for (int index = 0; index < count; index++)
            {
                RandomColumn column = new RandomColumn(this, prefix + index, getRandomType(), bindPosition++);
                columns.add(column);
                columnMap.put(column.name, column);
            }
            return columns;
        }

        private static TypeInfo getRandomType()
        {
            return types.get(CQLTester.getRandom().nextIntBetween(0, types.size() - 1));
        }
    }

    public static class RandomQuery
    {
        private final RandomSchema schema;
        private final String query;
        private final Filter filter;

        RandomQuery(RandomSchema schema, String query) throws Throwable
        {
            this.schema = schema;
            this.query = query;
            filter = buildFilter(WhereClause.parse(query).root());
        }

        void test(SAITester tester, List<RandomRow> data) throws Throwable
        {
            CQLTester.assertRowsIgnoringOrder(tester.execute("SELECT * FROM %s WHERE " + query), expectedRows(data));
        }

        Object[][] expectedRows(List<RandomRow> data)
        {
            List<Object[]> expected = new ArrayList<>();

            for (RandomRow row : data)
            {
                if (filter.isSatisfiedBy(row))
                    expected.add(row.toArray());
            }

            return expected.toArray(new Object[][]{});
        }

        Filter buildFilter(WhereClause.ExpressionElement element)
        {
            Filter filter = new Filter();
            filter.isDisjunction = element.isDisjunction();
            for (Relation relation : element.relations())
            {
                filter.expressions.add(new Expression(schema, relation));
            }
            for (WhereClause.ExpressionElement child : element.operations())
                filter.children.add(buildFilter(child));
            return filter;
        }

        static class Filter
        {
            boolean isDisjunction;

            List<Expression> expressions = new ArrayList<>();

            List<Filter> children = new ArrayList<>();

            boolean isSatisfiedBy(RandomRow row)
            {
                if (isDisjunction)
                {
                    for (Expression e : expressions)
                        if (e.isSatisfiedBy(row))
                            return true;
                    for (Filter child : children)
                        if (child.isSatisfiedBy(row))
                            return true;
                    return false;
                }
                else
                {
                    for (Expression e : expressions)
                        if (!e.isSatisfiedBy(row))
                            return false;
                    for (Filter child : children)
                        if (!child.isSatisfiedBy(row))
                            return false;
                    return true;
                }
            }
        }

        static class Expression
        {
            ColumnIdentifier column;
            String value;
            int bindPosition;

            Expression(RandomSchema schema, Relation relation)
            {
                assert relation instanceof SingleColumnRelation;
                SingleColumnRelation singleColumnRelation = (SingleColumnRelation)relation;
                column = singleColumnRelation.getEntity();
                value = ((Constants.Literal)singleColumnRelation.getValue()).getRawText();
                bindPosition = schema.columnMap.get(column.toString()).bindPosition;
            }

            boolean isSatisfiedBy(RandomRow row)
            {
                Object rowValue = row.values.get(bindPosition);
                return rowValue.toString().equals(value);
            }
        }
    }

    public static class RandomColumn
    {
        private final RandomSchema schema;
        private final String name;
        private final TypeInfo type;
        private final int bindPosition;

        RandomColumn(RandomSchema schema, String name, TypeInfo type, int bindPosition)
        {
            this.schema = schema;
            this.name = name;
            this.type = type;
            this.bindPosition = bindPosition;
        }

        public String name()
        {
            return name;
        }

        public String toColumnDefinition()
        {
            return name + " " + type.type.toString();
        }

        public String bindMarker()
        {
            return "?";
        }

        public String randomQueryValue()
        {
            Object[] columnValues = schema.values.get(name).toArray();
            Object randomValue = columnValues[CQLTester.getRandom().nextIntBetween(0, columnValues.length - 1)];
            return type.toCqlString(randomValue);
        }

        public String toIndexDefinition()
        {
            StringBuilder builder = new StringBuilder();
            builder.append("CREATE CUSTOM INDEX ON %s(");
            builder.append(name);
            builder.append(") USING 'StorageAttachedIndex'");
            return builder.toString();
        }

        public Object nextValue()
        {
            Object value = type.nextValue();
            schema.values.put(name, value);
            return value;
        }

        @Override
        public String toString()
        {
            return name;
        }
    }

    public static class RandomRow
    {
        List<Object> values = new ArrayList<>();

        public void add(Object value)
        {
            values.add(value);
        }

        public Object[] toArray()
        {
            return values.toArray();
        }
    }

    public static class TypeInfo
    {
        private final CQL3Type.Native type;
        private final Supplier<?> supplier;
        private final boolean quoted;

        TypeInfo(CQL3Type.Native type, Supplier<?> supplier, boolean quoted)
        {
            this.type = type;
            this.supplier = supplier;
            this.quoted = quoted;
        }

        static TypeInfo create(CQL3Type.Native type, Supplier<?> supplier, boolean quoted)
        {
            return new TypeInfo(type, supplier, quoted);
        }

        public Object nextValue()
        {
            return supplier.get();
        }

        public String toCqlString(Object value)
        {
            return quoted ? "'" + value + "'" : value.toString();
        }
    }
}
