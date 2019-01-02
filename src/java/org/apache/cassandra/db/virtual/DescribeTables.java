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

package org.apache.cassandra.db.virtual;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;

import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.UDAggregate;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SchemaCQLHelper;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.Functions;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.schema.ViewMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

public class DescribeTables
{
    private static final String KEYSPACE = "keyspace_name";
    private static final String CQL = "cql";

    private static final CompositeType utfComposite = CompositeType.getInstance(UTF8Type.instance, UTF8Type.instance);

    public static Collection<VirtualTable> getAll(String name)
    {
        return ImmutableList.of(new DescribeKeyspaceTable(name),
                                new DescribeIndexesTable(name),
                                new DescribeTypesTable(name),
                                new DescribeAggregatesTable(name),
                                new DescribeFunctionsTable(name),
                                new DescribeViewsTable(name),
                                new DescribeTablesTable(name));
    }

    static final class DescribeKeyspaceTable extends AbstractVirtualTable
    {
        DescribeKeyspaceTable(String keyspace)
        {
            super(TableMetadata.builder(keyspace, "describe_keyspace")
                               .comment("cql for keyspace metadata")
                               .kind(TableMetadata.Kind.VIRTUAL)
                               .partitioner(new LocalPartitioner(UTF8Type.instance))
                               .addPartitionKeyColumn(KEYSPACE, UTF8Type.instance)
                               .addRegularColumn(CQL, UTF8Type.instance)
                               .build());
        }

        @Override
        public DataSet data(DecoratedKey partitionKey)
        {
            String keyspace = UTF8Type.instance.compose(partitionKey.getKey());

            SimpleDataSet result = new SimpleDataSet(metadata());
            result.row(keyspace)
                  .column(CQL, SchemaCQLHelper.getKeyspaceAsCQL(keyspace));
            return result;
        }

        public DataSet data()
        {
            SimpleDataSet result = new SimpleDataSet(metadata());
            for (String keyspace : Schema.instance.getKeyspaces())
            {
                result.row(keyspace)
                      .column(CQL, SchemaCQLHelper.getKeyspaceAsCQL(keyspace));
            }
            return result;
        }
    }

    static abstract class AbstractDescribeTable extends AbstractVirtualTable
    {
        AbstractDescribeTable(String keyspace, String name)
        {
            super(TableMetadata.builder(keyspace, "describe_" + name)
                               .kind(TableMetadata.Kind.VIRTUAL)
                               .partitioner(new LocalPartitioner(utfComposite))
                               .addPartitionKeyColumn(KEYSPACE, UTF8Type.instance)
                               .addPartitionKeyColumn(name + "_name", UTF8Type.instance)
                               .addRegularColumn(CQL, UTF8Type.instance)
                               .build());
        }

        public abstract String cqlFor(String keyspace, String value);

        public abstract Iterable<String> valuesForKeyspace(String keyspace);

        @Override
        public DataSet data(DecoratedKey partitionKey)
        {
            ByteBuffer[] parts = utfComposite.split(partitionKey.getKey());
            String keyspace = UTF8Type.instance.compose(parts[0]);
            String value = UTF8Type.instance.compose(parts[1]);

            SimpleDataSet result = new SimpleDataSet(metadata());
            String cql = cqlFor(keyspace, value);
            if (cql == null)
                return result;

            result.row(keyspace, value)
                  .column(CQL, cql);
            return result;
        }

        public DataSet data()
        {
            SimpleDataSet result = new SimpleDataSet(metadata());
            for (String keyspace : Schema.instance.getKeyspaces())
            {
                for (String value : valuesForKeyspace(keyspace))
                {
                    String cql = cqlFor(keyspace, value);
                    if (cql == null)
                        continue;
                    result.row(keyspace, value)
                          .column(CQL, cql);
                }
            }
            return result;
        }
    }

    static final class DescribeIndexesTable extends AbstractDescribeTable
    {
        DescribeIndexesTable(String keyspace)
        {
            super(keyspace, "index");
        }

        public String cqlFor(String keyspace, String index)
        {
            Optional<TableMetadata> baseTable = Keyspace.open(keyspace).getMetadata().findIndexedTable(index);
            if (!baseTable.isPresent()) return null;
            return SchemaCQLHelper.toCQL(baseTable.get(), baseTable.get().indexes.get(index).get());
        }

        public Iterable<String> valuesForKeyspace(String keyspace)
        {
            Iterable<TableMetadata> tables = Keyspace.open(keyspace).getMetadata().tablesAndViews();
            List<String> result = Lists.newArrayList();
            for (TableMetadata metadata : tables)
            {
                for (IndexMetadata indexMetadata : metadata.indexes)
                    result.add(indexMetadata.name);
            }
            return result;
        }
    }

    static final class DescribeTypesTable extends AbstractDescribeTable
    {
        DescribeTypesTable(String keyspace)
        {
            super(keyspace, "type");
        }

        public String cqlFor(String keyspace, String type)
        {
            Optional<UserType> udt = Keyspace.open(keyspace).getMetadata().types.get(ByteBufferUtil.bytes(type));
            if (!udt.isPresent()) return null;
            return SchemaCQLHelper.toCQL(udt.get());
        }

        public Iterable<String> valuesForKeyspace(String keyspace)
        {
            Types types = Keyspace.open(keyspace).getMetadata().types;
            List<String> result = Lists.newArrayList();
            for (UserType type : types.dependencyOrder())
            {
                result.add(type.asCQL3Type().toString());
            }
            return result;
        }
    }

    static final class DescribeAggregatesTable extends AbstractDescribeTable
    {
        DescribeAggregatesTable(String keyspace)
        {
            super(keyspace, "aggregate");
        }

        public String cqlFor(String keyspace, String value)
        {
            FunctionName name = new FunctionName(keyspace, value);
            Collection<Function> funcs = Keyspace.open(keyspace).getMetadata().functions.get(name);
            if (funcs.size() != 1 || !funcs.iterator().next().isAggregate())
                return null;
            return SchemaCQLHelper.toCQL((UDAggregate) funcs.iterator().next());
        }

        public Iterable<String> valuesForKeyspace(String keyspace)
        {
            Functions functions = Keyspace.open(keyspace).getMetadata().functions;
            return functions.udas()
                            .map(uda -> uda.name().name)
                            .collect(Collectors.toList());
        }
    }

    static final class DescribeFunctionsTable extends AbstractDescribeTable
    {
        DescribeFunctionsTable(String keyspace)
        {
            super(keyspace, "function");
        }

        public String cqlFor(String keyspace, String value)
        {
            FunctionName name = new FunctionName(keyspace, value);
            Collection<Function> funcs = Keyspace.open(keyspace).getMetadata().functions.get(name);
            if (funcs.size() != 1 || funcs.iterator().next().isAggregate())
                return null;
            return SchemaCQLHelper.toCQL((UDFunction) funcs.iterator().next());
        }

        public Iterable<String> valuesForKeyspace(String keyspace)
        {
            Functions functions = Keyspace.open(keyspace).getMetadata().functions;
            return functions.udfs()
                            .map(udf -> udf.name().name)
                            .collect(Collectors.toList());
        }
    }

    static final class DescribeViewsTable extends AbstractDescribeTable
    {
        DescribeViewsTable(String keyspace)
        {
            super(keyspace, "view");
        }

        public String cqlFor(String keyspace, String view)
        {
            ViewMetadata metadata = Schema.instance.getView(keyspace, view);
            if (metadata == null) return null;
            return SchemaCQLHelper.toCQL(metadata);
        }

        public Iterable<String> valuesForKeyspace(String keyspace)
        {
            return Streams.stream(Keyspace.open(keyspace).getMetadata().views)
                          .map(v -> v.name())
                          .collect(Collectors.toList());
        }
    }

    static final class DescribeTablesTable extends AbstractDescribeTable
    {
        DescribeTablesTable(String keyspace)
        {
            super(keyspace, "table");
        }

        public String cqlFor(String keyspace, String table)
        {
            TableMetadata metadata = Schema.instance.getTableMetadata(keyspace, table);
            if (metadata == null) return null;
            StringBuilder sb = new StringBuilder();
            sb.append(SchemaCQLHelper.getTableMetadataAsCQL(metadata, false, false));
            for (String s : SchemaCQLHelper.getIndexesAsCQL(metadata))
                sb.append('\n').append(s);
            return sb.toString();
        }

        public Iterable<String> valuesForKeyspace(String keyspace)
        {
            return Streams.stream(Keyspace.open(keyspace).getMetadata().tables)
                          .map(t -> t.name)
                          .collect(Collectors.toList());
        }
    }
}
