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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;

import org.apache.cassandra.config.*;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.statements.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.utils.*;

/**
 * Helper methods to represent CFMetadata and related objects in CQL format
 */
public class ColumnFamilyStoreCQLHelper
{
    public static List<String> dumpReCreateStatements(CFMetaData metadata)
    {
        List<String> l = new ArrayList<>();
        // Types come first, as table can't be created without them
        l.addAll(ColumnFamilyStoreCQLHelper.getUserTypesAsCQL(metadata));
        // Record re-create schema statements
        l.add(ColumnFamilyStoreCQLHelper.getCFMetadataAsCQL(metadata, true));
        // Dropped columns (and re-additions)
        l.addAll(ColumnFamilyStoreCQLHelper.getDroppedColumnsAsCQL(metadata));
        // Indexes applied as last, since otherwise they may interfere with column drops / re-additions
        l.addAll(ColumnFamilyStoreCQLHelper.getIndexesAsCQL(metadata));
        return l;
    }

    private static List<ColumnDefinition> getClusteringColumns(CFMetaData metadata)
    {
        List<ColumnDefinition> cds = new ArrayList<>(metadata.clusteringColumns().size());

        if (!metadata.isStaticCompactTable())
            for (ColumnDefinition cd : metadata.clusteringColumns())
                cds.add(cd);

        return cds;
    }

    private static List<ColumnDefinition> getPartitionColumns(CFMetaData metadata)
    {
        List<ColumnDefinition> cds = new ArrayList<>(metadata.partitionColumns().size());

        for (ColumnDefinition cd : metadata.partitionColumns().statics)
            cds.add(cd);

        if (metadata.isDense())
        {
            // remove an empty type
            for (ColumnDefinition cd : metadata.partitionColumns().withoutStatics())
                if (!cd.type.equals(EmptyType.instance))
                    cds.add(cd);
        }
        // "regular" columns are not exposed for static compact tables
        else if (!metadata.isStaticCompactTable())
        {
            for (ColumnDefinition cd : metadata.partitionColumns().withoutStatics())
                cds.add(cd);
        }

        return cds;
    }

    /**
     * Build a CQL String representation of Column Family Metadata
     */
    @VisibleForTesting
    public static String getCFMetadataAsCQL(CFMetaData metadata, boolean includeDroppedColumns)
    {
        StringBuilder sb = new StringBuilder();
        if (!isCqlCompatible(metadata))
        {
            sb.append(String.format("/*\nWarning: Table %s.%s omitted because it has constructs not compatible with CQL (was created via legacy API).\n",
                                    metadata.ksName,
                                    metadata.cfName));
            sb.append("\nApproximate structure, for reference:");
            sb.append("\n(this should not be used to reproduce this schema)\n\n");
        }

        sb.append("CREATE TABLE IF NOT EXISTS ");
        sb.append(quoteIdentifier(metadata.ksName)).append('.').append(quoteIdentifier(metadata.cfName)).append(" (");

        List<ColumnDefinition> partitionKeyColumns = metadata.partitionKeyColumns();
        List<ColumnDefinition> clusteringColumns = getClusteringColumns(metadata);
        List<ColumnDefinition> partitionColumns = getPartitionColumns(metadata);

        Consumer<StringBuilder> cdCommaAppender = commaAppender("\n\t");
        sb.append("\n\t");
        for (ColumnDefinition cfd: partitionKeyColumns)
        {
            cdCommaAppender.accept(sb);
            sb.append(toCQL(cfd));
            if (partitionKeyColumns.size() == 1 && clusteringColumns.size() == 0)
                sb.append(" PRIMARY KEY");
        }

        for (ColumnDefinition cfd: clusteringColumns)
        {
            cdCommaAppender.accept(sb);
            sb.append(toCQL(cfd));
        }

        for (ColumnDefinition cfd: partitionColumns)
        {
            cdCommaAppender.accept(sb);
            sb.append(toCQL(cfd, metadata.isStaticCompactTable()));
        }

        if (includeDroppedColumns)
        {
            for (Map.Entry<ByteBuffer, CFMetaData.DroppedColumn> entry: metadata.getDroppedColumns().entrySet())
            {
                if (metadata.getColumnDefinition(entry.getKey()) != null)
                    continue;

                CFMetaData.DroppedColumn droppedColumn = entry.getValue();
                cdCommaAppender.accept(sb);
                sb.append(quoteIdentifier(droppedColumn.name));
                sb.append(' ');
                sb.append(droppedColumn.type.asCQL3Type().toString());
            }
        }

        if (clusteringColumns.size() > 0 || partitionKeyColumns.size() > 1)
        {
            sb.append(",\n\tPRIMARY KEY (");
            if (partitionKeyColumns.size() > 1)
            {
                sb.append("(");
                Consumer<StringBuilder> pkCommaAppender = commaAppender(" ");
                for (ColumnDefinition cfd : partitionKeyColumns)
                {
                    pkCommaAppender.accept(sb);
                    sb.append(quoteIdentifier(cfd.name.toString()));
                }
                sb.append(")");
            }
            else
            {
                sb.append(quoteIdentifier(partitionKeyColumns.get(0).name.toString()));
            }

            for (ColumnDefinition cfd : metadata.clusteringColumns())
                sb.append(", ").append(quoteIdentifier(cfd.name.toString()));

            sb.append(')');
        }
        sb.append(")\n\t");
        sb.append("WITH ");

        sb.append("ID = ").append(metadata.cfId).append("\n\tAND ");

        if (metadata.isCompactTable())
            sb.append("COMPACT STORAGE\n\tAND ");

        if (clusteringColumns.size() > 0)
        {
            sb.append("CLUSTERING ORDER BY (");

            Consumer<StringBuilder> cOrderCommaAppender = commaAppender(" ");
            for (ColumnDefinition cd : clusteringColumns)
            {
                cOrderCommaAppender.accept(sb);
                sb.append(quoteIdentifier(cd.name.toString())).append(' ').append(cd.clusteringOrder().toString());
            }
            sb.append(")\n\tAND ");
        }

        sb.append(toCQL(metadata.params));
        sb.append(";");

        if (!isCqlCompatible(metadata))
        {
            sb.append("\n*/");
        }
        return sb.toString();
    }

    /**
     * Build a CQL String representation of User Types used in the given Column Family.
     *
     * Type order is ensured as types are built incrementally: from the innermost (most nested)
     * to the outermost.
     */
    @VisibleForTesting
    public static List<String> getUserTypesAsCQL(CFMetaData metadata)
    {
        List<AbstractType> types = new ArrayList<>();
        Set<AbstractType> typeSet = new HashSet<>();
        for (ColumnDefinition cd: Iterables.concat(metadata.partitionKeyColumns(), metadata.clusteringColumns(), metadata.partitionColumns()))
        {
            AbstractType type = cd.type;
            if (type.isUDT())
                resolveUserType((UserType) type, typeSet, types);
        }

        List<String> typeStrings = new ArrayList<>();
        for (AbstractType type: types)
            typeStrings.add(toCQL((UserType) type));
        return typeStrings;
    }

    /**
     * Build a CQL String representation of Dropped Columns in the given Column Family.
     *
     * If the column was dropped once, but is now re-created `ADD` will be appended accordingly.
     */
    @VisibleForTesting
    public static List<String> getDroppedColumnsAsCQL(CFMetaData metadata)
    {
        List<String> droppedColumns = new ArrayList<>();

        for (Map.Entry<ByteBuffer, CFMetaData.DroppedColumn> entry: metadata.getDroppedColumns().entrySet())
        {
            CFMetaData.DroppedColumn column = entry.getValue();
            droppedColumns.add(toCQLDrop(metadata.ksName, metadata.cfName, column));
            if (metadata.getColumnDefinition(entry.getKey()) != null)
                droppedColumns.add(toCQLAdd(metadata.ksName, metadata.cfName, metadata.getColumnDefinition(entry.getKey())));
        }

        return droppedColumns;
    }

    /**
     * Build a CQL String representation of Indexes on columns in the given Column Family
     */
    @VisibleForTesting
    public static List<String> getIndexesAsCQL(CFMetaData metadata)
    {
        List<String> indexes = new ArrayList<>();
        for (IndexMetadata indexMetadata: metadata.getIndexes())
            indexes.add(toCQL(metadata.ksName, metadata.cfName, indexMetadata));
        return indexes;
    }

    private static String toCQL(String keyspace, String cf, IndexMetadata indexMetadata)
    {
        if (indexMetadata.isCustom())
        {
            Map<String, String> options = new HashMap<>();
            indexMetadata.options.forEach((k, v) -> {
                if (!k.equals(IndexTarget.TARGET_OPTION_NAME) && !k.equals(IndexTarget.CUSTOM_INDEX_OPTION_NAME))
                    options.put(k, v);
            });

            return String.format("CREATE CUSTOM INDEX %s ON %s.%s (%s) USING '%s'%s;",
                                 quoteIdentifier(indexMetadata.name),
                                 quoteIdentifier(keyspace),
                                 quoteIdentifier(cf),
                                 indexMetadata.options.get(IndexTarget.TARGET_OPTION_NAME),
                                 indexMetadata.options.get(IndexTarget.CUSTOM_INDEX_OPTION_NAME),
                                 options.isEmpty() ? "" : " WITH OPTIONS " + toCQL(options));
        }
        else
        {
            return String.format("CREATE INDEX %s ON %s.%s (%s);",
                                 quoteIdentifier(indexMetadata.name),
                                 quoteIdentifier(keyspace),
                                 quoteIdentifier(cf),
                                 indexMetadata.options.get(IndexTarget.TARGET_OPTION_NAME));
        }
    }
    private static String toCQL(UserType userType)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("CREATE TYPE %s.%s(",
                                quoteIdentifier(userType.keyspace),
                                quoteIdentifier(userType.getNameAsString())));

        Consumer<StringBuilder> commaAppender = commaAppender(" ");
        for (int i = 0; i < userType.size(); i++)
        {
            commaAppender.accept(sb);
            sb.append(String.format("%s %s",
                                    userType.fieldNameAsString(i),
                                    userType.fieldType(i).asCQL3Type()));
        }
        sb.append(");");
        return sb.toString();
    }

    private static String toCQL(TableParams tableParams)
    {
        StringBuilder builder = new StringBuilder();

        builder.append("bloom_filter_fp_chance = ").append(tableParams.bloomFilterFpChance);
        builder.append("\n\tAND dclocal_read_repair_chance = ").append(tableParams.dcLocalReadRepairChance);
        builder.append("\n\tAND crc_check_chance = ").append(tableParams.crcCheckChance);
        builder.append("\n\tAND default_time_to_live = ").append(tableParams.defaultTimeToLive);
        builder.append("\n\tAND gc_grace_seconds = ").append(tableParams.gcGraceSeconds);
        builder.append("\n\tAND min_index_interval = ").append(tableParams.minIndexInterval);
        builder.append("\n\tAND max_index_interval = ").append(tableParams.maxIndexInterval);
        builder.append("\n\tAND memtable_flush_period_in_ms = ").append(tableParams.memtableFlushPeriodInMs);
        builder.append("\n\tAND read_repair_chance = ").append(tableParams.readRepairChance);
        builder.append("\n\tAND speculative_retry = '").append(tableParams.speculativeRetry).append("'");
        builder.append("\n\tAND comment = ").append(singleQuote(tableParams.comment));
        builder.append("\n\tAND caching = ").append(toCQL(tableParams.caching.asMap()));
        builder.append("\n\tAND compaction = ").append(toCQL(tableParams.compaction.asMap()));
        builder.append("\n\tAND compression = ").append(toCQL(tableParams.compression.asMap()));
        builder.append("\n\tAND cdc = ").append(tableParams.cdc);

        builder.append("\n\tAND extensions = { ");
        for (Map.Entry<String, ByteBuffer> entry : tableParams.extensions.entrySet())
        {
            builder.append(singleQuote(entry.getKey()));
            builder.append(": ");
            builder.append("0x").append(ByteBufferUtil.bytesToHex(entry.getValue()));
        }
        builder.append(" }");
        return builder.toString();
    }

    private static String toCQL(Map<?, ?> map)
    {
        StringBuilder builder = new StringBuilder("{ ");

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

        builder.append(" }");
        return builder.toString();
    }

    private static String toCQL(ColumnDefinition cd)
    {
        return toCQL(cd, false);
    }

    private static String toCQL(ColumnDefinition cd, boolean isStaticCompactTable)
    {
        return String.format("%s %s%s",
                             quoteIdentifier(cd.name.toString()),
                             cd.type.asCQL3Type().toString(),
                             cd.isStatic() && !isStaticCompactTable ? " static" : "");
    }

    private static String toCQLAdd(String keyspace, String cf, ColumnDefinition cd)
    {
        return String.format("ALTER TABLE %s.%s ADD %s %s%s;",
                             quoteIdentifier(keyspace),
                             quoteIdentifier(cf),
                             quoteIdentifier(cd.name.toString()),
                             cd.type.asCQL3Type().toString(),
                             cd.isStatic() ? " static" : "");
    }

    private static String toCQLDrop(String keyspace, String cf, CFMetaData.DroppedColumn droppedColumn)
    {
        return String.format("ALTER TABLE %s.%s DROP %s USING TIMESTAMP %s;",
                             quoteIdentifier(keyspace),
                             quoteIdentifier(cf),
                             quoteIdentifier(droppedColumn.name),
                             droppedColumn.droppedTime);
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
        return String.format("'%s'", s.replaceAll("'", "''"));
    }

    private static Consumer<StringBuilder> commaAppender(String afterComma)
    {
        AtomicBoolean isFirst = new AtomicBoolean(true);
        return new Consumer<StringBuilder>()
        {
            public void accept(StringBuilder stringBuilder)
            {
                if (!isFirst.getAndSet(false))
                    stringBuilder.append(',').append(afterComma);
            }
        };
    }

    private static String quoteIdentifier(String id)
    {
        return ColumnIdentifier.maybeQuote(id);
    }

    /**
     * Whether or not the given metadata is compatible / representable with CQL Language
     */
    public static boolean isCqlCompatible(CFMetaData metaData)
    {
        if (metaData.isSuper())
            return false;

        if (metaData.isCompactTable()
            && metaData.partitionColumns().withoutStatics().size() > 1
            && metaData.clusteringColumns().size() >= 1)
            return false;

        return true;
    }
}
