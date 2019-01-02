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
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.functions.UDAggregate;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.*;

/**
 * Helper methods to represent TableMetadata and related objects in CQL format
 */
public class SchemaCQLHelper
{
    private static final Pattern SINGLE_QUOTE = Pattern.compile("'");

    public static List<String> dumpReCreateStatements(TableMetadata metadata)
    {
        List<String> l = new ArrayList<>();
        // Types come first, as table can't be created without them
        l.addAll(SchemaCQLHelper.getUserTypesAsCQL(metadata));
        // Record re-create schema statements
        l.add(SchemaCQLHelper.getTableMetadataAsCQL(metadata, true, true));
        // Dropped columns (and re-additions)
        l.addAll(SchemaCQLHelper.getDroppedColumnsAsCQL(metadata));
        // Indexes applied as last, since otherwise they may interfere with column drops / re-additions
        l.addAll(SchemaCQLHelper.getIndexesAsCQL(metadata));
        return l;
    }

    public static String getKeyspaceAsCQL(String keyspace)
    {
        StringBuilder sb = new StringBuilder();

        KeyspaceMetadata metadata = Schema.instance.getKeyspaceMetadata(keyspace);
        sb.append(toCQL(metadata));
        sb.append("\n\n");

        // UDTs first in dependancy order
        for (UserType udt : metadata.types.dependencyOrder())
        {
            sb.append(toCQL(udt));
            sb.append("\n\n");
        }

        for (TableMetadata table : metadata.tables)
        {
            if (table.isView()) continue;
            sb.append(getTableMetadataAsCQL(table, false, false));
            for (String s : SchemaCQLHelper.getIndexesAsCQL(table))
                sb.append('\n').append(s);
            sb.append("\n\n");
        }

        for (ViewMetadata view : metadata.views)
        {
            sb.append(toCQL(view));
            sb.append("\n\n");
        }

        metadata.functions.udfs().forEach(fn ->
                                          {
                                              sb.append(toCQL(fn));
                                              sb.append("\n\n");
                                          });

        metadata.functions.udas().forEach(fn ->
                                          {
                                              sb.append(toCQL(fn));
                                              sb.append("\n\n");
                                          });

        return sb.toString();
    }

    public static String toCQL(UDAggregate uda)
    {
        StringBuilder sb = new StringBuilder("CREATE AGGREGATE ");
        sb.append(maybeQuote(uda.name().keyspace)).append('.').append(maybeQuote(uda.name().name));
        sb.append(" (");
        Consumer<StringBuilder> commas = commaAppender(" ");
        for (String arg : uda.argumentsList())
        {
            commas.accept(sb);
            sb.append(arg);
        }
        sb.append(')');
        sb.append("\n\tSFUNC ").append(maybeQuote(uda.stateFunction().name().name));
        sb.append("\n\tSTYPE ").append(unfrozen(uda.stateType()));
        if (uda.finalFunction() != null)
            sb.append("\n\tFINALFUNC ").append(maybeQuote(uda.finalFunction().name().name));
        if (uda.initialCondition() != null)
            sb.append("\n\tINITCOND ").append(uda.stateType().asCQL3Type().toCQLLiteral(uda.initialCondition(), ProtocolVersion.CURRENT));
        return sb.append(';').toString();
    }

    public static String toCQL(UDFunction udf)
    {
        StringBuilder sb = new StringBuilder("CREATE FUNCTION ");
        sb.append(maybeQuote(udf.name().keyspace)).append('.').append(maybeQuote(udf.name().name));
        sb.append(" (");
        Consumer<StringBuilder> commas = commaAppender(" ");
        for (int i = 0; i < Math.min(udf.argNames().size(), udf.argTypes().size()); i++)
        {
            commas.accept(sb);
            String argName = udf.argNames().get(i).toCQLString();
            String argType = unfrozen(udf.argTypes().get(i));
            sb.append(String.format("%s %s", argName, argType));
        }
        sb.append(")\n\t");

        sb.append(udf.isCalledOnNullInput() ? "CALLED ON NULL INPUT" : "RETURNS NULL ON NULL INPUT" );
        sb.append("\n\tRETURNS ").append(unfrozen(udf.returnType()));
        sb.append("\n\tLANGUAGE ").append(udf.language());
        sb.append("\n\tAS '").append(SINGLE_QUOTE.matcher(udf.body()).replaceAll("''")).append("';");

        return sb.toString();
    }

    private static String toCQL(KeyspaceMetadata metadata)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE KEYSPACE ").append(maybeQuote(metadata.name));
        sb.append(" WITH replication = ");
        sb.append(toCQL(metadata.params.replication.asMap()));
        sb.append(" AND durable_writes = ").append(metadata.params.durableWrites);
        sb.append(';');
        return sb.toString();
    }

    private static List<ColumnMetadata> getClusteringColumns(TableMetadata metadata)
    {
        List<ColumnMetadata> cds = new ArrayList<>(metadata.clusteringColumns().size());

        if (!metadata.isStaticCompactTable())
            cds.addAll(metadata.clusteringColumns());

        return cds;
    }

    private static List<ColumnMetadata> getPartitionColumns(TableMetadata metadata)
    {
        List<ColumnMetadata> cds = new ArrayList<>(metadata.regularAndStaticColumns().size());

        cds.addAll(metadata.staticColumns());

        if (metadata.isDense())
        {
            // remove an empty type
            for (ColumnMetadata cd : metadata.regularColumns())
                if (!cd.type.equals(EmptyType.instance))
                    cds.add(cd);
        }
        // "regular" columns are not exposed for static compact tables
        else if (!metadata.isStaticCompactTable())
        {
            cds.addAll(metadata.regularColumns());
        }

        return cds;
    }

    public static String toCQL(ViewMetadata view)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE MATERIALIZED VIEW ")
          .append(view.metadata.toString())
          .append(" AS\n\t");
        sb.append("SELECT ");

        if (view.includeAllColumns) {
            sb.append('*');
        } else
        {
            Consumer<StringBuilder> selectAppender = commaAppender(" ");
            for (ColumnMetadata column : view.metadata.columns())
            {
                selectAppender.accept(sb);
                sb.append(column.name.toCQLString());
            }
        }
        sb.append("\n\tFROM ").append(view.baseTableMetadata().toString());
        sb.append("\n\tWHERE ").append(view.whereClause.toString());
        sb.append("\n\t").append(getPrimaryKeyCql(view.metadata));
        sb.append("\n\tWITH ");

        List<ColumnMetadata> clusteringColumns = getClusteringColumns(view.metadata);
        sb.append(getClusteringOrderCql(clusteringColumns));
        sb.append(toCQL(view.metadata.params));
        sb.append(';');
        return sb.toString();
    }

    private static String getClusteringOrderCql(List<ColumnMetadata> clusteringColumns)
    {
        StringBuilder sb = new StringBuilder();
        if (clusteringColumns.size() > 0)
        {
            sb.append("CLUSTERING ORDER BY (");

            Consumer<StringBuilder> cOrderCommaAppender = commaAppender(" ");
            for (ColumnMetadata cd : clusteringColumns)
            {
                cOrderCommaAppender.accept(sb);
                sb.append(cd.name.toCQLString()).append(' ').append(cd.clusteringOrder().toString());
            }
            sb.append(")\n\tAND ");
        }
        return sb.toString();
    }

    private static String getPrimaryKeyCql(TableMetadata metadata)
    {
        List<ColumnMetadata> partitionKeyColumns = metadata.partitionKeyColumns();

        StringBuilder sb = new StringBuilder();
        sb.append("PRIMARY KEY (");
        if (partitionKeyColumns.size() > 1)
        {
            sb.append('(');
            Consumer<StringBuilder> pkCommaAppender = commaAppender(" ");
            for (ColumnMetadata cfd : partitionKeyColumns)
            {
                pkCommaAppender.accept(sb);
                sb.append(cfd.name.toCQLString());
            }
            sb.append(')');
        }
        else
        {
            sb.append(partitionKeyColumns.get(0).name.toCQLString());
        }

        for (ColumnMetadata cfd : metadata.clusteringColumns())
            sb.append(", ").append(cfd.name.toCQLString());

        return sb.append(')').toString();
    }

    /**
     * Build a CQL String representation of Table Metadata
     */
    @VisibleForTesting
    public static String getTableMetadataAsCQL(TableMetadata metadata, boolean includeDroppedColumns, boolean includeId)
    {
        StringBuilder sb = new StringBuilder();
        if (!isCqlCompatible(metadata))
        {
            sb.append(String.format("/*\nWarning: Table %s omitted because it has constructs not compatible with CQL.\n",
                                    metadata.toString()));
            sb.append("\nApproximate structure, for reference:");
            sb.append("\n(this should not be used to reproduce this schema)\n\n");
        }

        sb.append("CREATE TABLE ");
        sb.append(metadata.toString()).append(" (");

        List<ColumnMetadata> partitionKeyColumns = metadata.partitionKeyColumns();
        List<ColumnMetadata> clusteringColumns = getClusteringColumns(metadata);
        List<ColumnMetadata> partitionColumns = getPartitionColumns(metadata);

        Consumer<StringBuilder> cdCommaAppender = commaAppender("\n\t");
        sb.append("\n\t");
        for (ColumnMetadata cfd: partitionKeyColumns)
        {
            cdCommaAppender.accept(sb);
            sb.append(toCQL(cfd));
            if (partitionKeyColumns.size() == 1 && clusteringColumns.size() == 0)
                sb.append(" PRIMARY KEY");
        }

        for (ColumnMetadata cfd: clusteringColumns)
        {
            cdCommaAppender.accept(sb);
            sb.append(toCQL(cfd));
        }

        for (ColumnMetadata cfd: partitionColumns)
        {
            cdCommaAppender.accept(sb);
            sb.append(toCQL(cfd, metadata.isStaticCompactTable()));
        }

        if (includeDroppedColumns)
        {
            for (Map.Entry<ByteBuffer, DroppedColumn> entry: metadata.droppedColumns.entrySet())
            {
                if (metadata.getColumn(entry.getKey()) != null)
                    continue;

                DroppedColumn droppedColumn = entry.getValue();
                cdCommaAppender.accept(sb);
                sb.append(droppedColumn.column.name.toCQLString());
                sb.append(' ');
                sb.append(droppedColumn.column.type.asCQL3Type().toString());
            }
        }

        if (clusteringColumns.size() > 0 || partitionKeyColumns.size() > 1)
        {
            sb.append(",\n\t").append(getPrimaryKeyCql(metadata));
        }
        sb.append("\n) WITH ");

        if(includeId)
            sb.append("ID = ").append(metadata.id).append("\n\tAND ");

        if (metadata.isCompactTable())
            sb.append("COMPACT STORAGE\n\tAND ");

        sb.append(getClusteringOrderCql(clusteringColumns));

        sb.append(toCQL(metadata.params));
        sb.append(';');

        if (!isCqlCompatible(metadata))
        {
            sb.append("\n*/");
        }
        return sb.toString();
    }

    /**
     * Build a CQL String representation of User Types used in the given Table.
     *
     * Type order is ensured as types are built incrementally: from the innermost (most nested)
     * to the outermost.
     */
    @VisibleForTesting
    static List<String> getUserTypesAsCQL(TableMetadata metadata)
    {
        List<AbstractType> types = new ArrayList<>();
        Set<AbstractType> typeSet = new HashSet<>();
        for (ColumnMetadata cd: Iterables.concat(metadata.partitionKeyColumns(), metadata.clusteringColumns(), metadata.regularAndStaticColumns()))
        {
            AbstractType type = cd.type;
            if (type.isUDT())
                resolveUserType((UserType) type, typeSet, types);
        }

        List<String> typeStrings = new ArrayList<>(types.size());
        for (AbstractType type: types)
            typeStrings.add(toCQL((UserType) type));
        return typeStrings;
    }

    /**
     * Build a CQL String representation of Dropped Columns in the given Table.
     *
     * If the column was dropped once, but is now re-created `ADD` will be appended accordingly.
     */
    @VisibleForTesting
    static List<String> getDroppedColumnsAsCQL(TableMetadata metadata)
    {
        List<String> droppedColumns = new ArrayList<>();

        for (Map.Entry<ByteBuffer, DroppedColumn> entry: metadata.droppedColumns.entrySet())
        {
            DroppedColumn column = entry.getValue();
            droppedColumns.add(toCQLDrop(metadata, column));
            if (metadata.getColumn(entry.getKey()) != null)
                droppedColumns.add(toCQLAdd(metadata, metadata.getColumn(entry.getKey())));
        }

        return droppedColumns;
    }

    /**
     * Build a CQL String representation of Indexes on columns in the given Table
     */
    @VisibleForTesting
    public static List<String> getIndexesAsCQL(TableMetadata metadata)
    {
        List<String> indexes = new ArrayList<>(metadata.indexes.size());
        for (IndexMetadata indexMetadata: metadata.indexes)
            indexes.add(toCQL(metadata, indexMetadata));
        return indexes;
    }

    public static String toCQL(TableMetadata baseTable, IndexMetadata indexMetadata)
    {
        if (indexMetadata.isCustom())
        {
            Map<String, String> options = new HashMap<>();
            indexMetadata.options.forEach((k, v) -> {
                if (!k.equals(IndexTarget.TARGET_OPTION_NAME) && !k.equals(IndexTarget.CUSTOM_INDEX_OPTION_NAME))
                    options.put(k, v);
            });

            return String.format("CREATE CUSTOM INDEX %s ON %s (%s) USING '%s'%s;",
                                 indexMetadata.toCQLString(),
                                 baseTable.toString(),
                                 indexMetadata.options.get(IndexTarget.TARGET_OPTION_NAME),
                                 indexMetadata.options.get(IndexTarget.CUSTOM_INDEX_OPTION_NAME),
                                 options.isEmpty() ? "" : " WITH OPTIONS " + toCQL(options));
        }
        else
        {
            return String.format("CREATE INDEX %s ON %s (%s);",
                                 indexMetadata.toCQLString(),
                                 baseTable.toString(),
                                 indexMetadata.options.get(IndexTarget.TARGET_OPTION_NAME));
        }
    }

    public static String toCQL(UserType userType)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TYPE ").append(userType.toCQLString()).append(" (\n\t");

        Consumer<StringBuilder> commaAppender = commaAppender("\n\t");
        for (int i = 0; i < userType.size(); i++)
        {
            commaAppender.accept(sb);
            sb.append(maybeQuote(userType.fieldNameAsString(i)))
              .append(' ')
              .append(userType.fieldType(i).asCQL3Type());
        }
        sb.append("\n);");
        return sb.toString();
    }

    private static void appendOption(TableParams tableParams, TableParams.Option option, StringBuilder sb)
    {
        switch (option)
        {
            case BLOOM_FILTER_FP_CHANCE:
                sb.append(tableParams.bloomFilterFpChance);
                return;
            case CACHING:
                sb.append(toCQL(tableParams.caching.asMap()));
                return;
            case CDC:
                sb.append(tableParams.cdc);
                return;
            case COMMENT:
                sb.append(singleQuote(tableParams.comment));
                return;
            case COMPACTION:
                sb.append(toCQL(tableParams.compaction.asMap()));
                return;
            case COMPRESSION:
                sb.append(toCQL(tableParams.compression.asMap()));
                return;
            case CRC_CHECK_CHANCE:
                sb.append(tableParams.crcCheckChance);
                return;
            case DEFAULT_TIME_TO_LIVE:
                sb.append(tableParams.defaultTimeToLive);
                return;
            case EXTENSIONS:
                sb.append('{');
                boolean first = true;
                for (Map.Entry<String, ByteBuffer> entry : tableParams.extensions.entrySet())
                {
                    if (first)
                        first = false;
                    else
                        sb.append(", ");
                    sb.append(singleQuote(entry.getKey()));
                    sb.append(": ");
                    sb.append("0x").append(ByteBufferUtil.bytesToHex(entry.getValue()));
                }
                sb.append('}');
                return;
            case GC_GRACE_SECONDS:
                sb.append(tableParams.gcGraceSeconds);
                return;
            case MAX_INDEX_INTERVAL:
                sb.append(tableParams.maxIndexInterval);
                return;
            case MEMTABLE_FLUSH_PERIOD_IN_MS:
                sb.append(tableParams.memtableFlushPeriodInMs);
                return;
            case MIN_INDEX_INTERVAL:
                sb.append(tableParams.minIndexInterval);
                return;
            case SPECULATIVE_RETRY:
                sb.append('\'').append(tableParams.speculativeRetry).append('\'');
                return;
            case ADDITIONAL_WRITE_POLICY:
                sb.append('\'').append(tableParams.additionalWritePolicy).append('\'');
                return;
            case READ_REPAIR:
                sb.append(singleQuote(tableParams.readRepair.toString()));
                return;
            default:
                throw new RuntimeException("Unknown option: " + option);
        }
    }

    private static String toCQL(TableParams tableParams)
    {
        StringBuilder builder = new StringBuilder();
        boolean first = true;
        for (TableParams.Option opt : TableParams.Option.values())
        {
            if (opt.equals(TableParams.Option.EXTENSIONS))
                continue;
            if (!first)
                builder.append("\n\tAND ");
            first = false;
            builder.append(opt.toString()).append(" = ");
            appendOption(tableParams, opt, builder);
        }
        return builder.toString();
    }

    private static String toCQL(Map<?, ?> map)
    {
        StringBuilder builder = new StringBuilder("{");

        boolean isFirst = true;
        for (Map.Entry entry: map.entrySet())
        {
            if (isFirst)
                isFirst = false;
            else
                builder.append(", ");
            builder.append(singleQuote(entry.getKey().toString()));
            builder.append(": ");
            builder.append(singleQuote(entry.getValue().toString()));
        }

        builder.append('}');
        return builder.toString();
    }

    private static String toCQL(ColumnMetadata cd)
    {
        return toCQL(cd, false);
    }

    private static String toCQL(ColumnMetadata cd, boolean isStaticCompactTable)
    {
        return String.format("%s %s%s",
                             cd.name.toCQLString(),
                             cd.type.asCQL3Type().toString(),
                             cd.isStatic() && !isStaticCompactTable ? " static" : "");
    }

    private static String toCQLAdd(TableMetadata table, ColumnMetadata cd)
    {
        return String.format("ALTER TABLE %s ADD %s %s%s;",
                             table.toString(),
                             cd.name.toCQLString(),
                             cd.type.asCQL3Type().toString(),
                             cd.isStatic() ? " static" : "");
    }

    private static String toCQLDrop(TableMetadata table, DroppedColumn droppedColumn)
    {
        return String.format("ALTER TABLE %s DROP %s USING TIMESTAMP %s;",
                             table.toString(),
                             droppedColumn.column.name.toCQLString(),
                             droppedColumn.droppedTime);
    }

    /**
     * This works around things like tuples being hard coded frozen&lt;&gt; yet the CREATE statements refusing to accept
     * frozen types. CASSANDRA-10826. If frozen restriction dropped on `create aggregate/function` types remove this.
     */
    private static String unfrozen(AbstractType<?> type)
    {
        return CQLTypeParser.parseRaw(type.asCQL3Type().toString()).toString();
    }

    private static void resolveUserType(UserType type, Set<AbstractType> typeSet, List<AbstractType> types)
    {
        for (AbstractType subType: type.fieldTypes())
            if (!typeSet.contains(subType) && subType.isUDT())
                resolveUserType((UserType) subType, typeSet, types);

        if (!typeSet.contains(type))
        {
            typeSet.add(type);
            types.add(type);
        }
    }

    private static String singleQuote(String s)
    {
        return String.format("'%s'", SINGLE_QUOTE.matcher(s).replaceAll("''"));
    }

    private static Consumer<StringBuilder> commaAppender(String afterComma)
    {
        AtomicBoolean isFirst = new AtomicBoolean(true);
        return stringBuilder ->
        {
            if (!isFirst.getAndSet(false))
                stringBuilder.append(',').append(afterComma);
        };
    }

    /* just an alias */
    private static String maybeQuote(String text)
    {
        return ColumnIdentifier.maybeQuote(text);
    }

    /**
     * Whether or not the given metadata is compatible / representable with CQL Language
     */
    private static boolean isCqlCompatible(TableMetadata metaData)
    {
        if (metaData.isSuper() || metaData.isVirtual())
            return false;

        return !(metaData.isCompactTable()
                 && metaData.regularColumns().size() > 1
                 && metaData.clusteringColumns().size() >= 1);
    }

}