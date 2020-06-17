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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.functions.UDAggregate;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.EmptyType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.DroppedColumn;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.schema.ViewMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Helper methods to represent TableMetadata and related objects in CQL format
 */
public class SchemaCQLHelper
{
    private static final Logger logger = LoggerFactory.getLogger(SchemaCQLHelper.class);

    /**
     * Generates the DDL statement for a {@code schema.cql} snapshot file.
     */
    public static Stream<String> reCreateStatementsForSchemaCql(TableMetadata metadata, Types types)
    {
        // Types come first, as table can't be created without them
        Stream<String> udts = SchemaCQLHelper.getUserTypesAsCQL(metadata, types);

        return Stream.concat(udts,
                             reCreateStatements(metadata,
                                                types,
                                                true,
                                                true,
                                                true,
                                                true));
    }

    public static Stream<String> reCreateStatements(TableMetadata metadata,
                                                    Types types,
                                                    boolean includeDroppedColumns,
                                                    boolean internals,
                                                    boolean ifNotExists,
                                                    boolean includeIndexes)
    {
        // We also need to add any user types that needs to be temporarily recreated for dropped columns
        UserTypesForDroppedColumns droppedColumnsTypes = includeDroppedColumns ? new UserTypesForDroppedColumns(metadata, types) : null;
        Stream<String> droppedUdts = includeDroppedColumns ? droppedColumnsTypes.createTypeStatements() : Stream.empty();

        // Record re-create schema statements
        Stream<String> tableMeta = Stream.of(metadata)
                                         .map((tm) -> SchemaCQLHelper.getTableMetadataAsCQL(tm,
                                                                                            droppedColumnsTypes,
                                                                                            includeDroppedColumns,
                                                                                            internals,
                                                                                            ifNotExists));
        Stream<String> r = Stream.concat(droppedUdts, tableMeta);

        if (includeDroppedColumns)
        {
            // Dropped columns (and re-additions)
            r = Stream.concat(r, SchemaCQLHelper.getDroppedColumnsAsCQL(metadata));
        }

        if (includeIndexes)
        {
            // Indexes applied as last, since otherwise they may interfere with column drops / re-additions
            r = Stream.concat(r, SchemaCQLHelper.getIndexesAsCQL(metadata));
        }

        if (includeDroppedColumns)
        {
            // Lastly, cleanup any user types created just for the sake of re-creating dropped columns
            r = Stream.concat(r, droppedColumnsTypes.dropTypeStatements());
        }

        return r;
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

    public static String getKeyspaceMetadataAsCQL(KeyspaceMetadata object)
    {
        ReplicationParams repl = object.params.replication;

        if (object.isVirtual())
            return String.format("/*\n" +
                                 "Warning: Keyspace %s is a virtual keyspace and cannot be recreated with CQL.\n" +
                                 "Structure, for reference:*/\n" +
                                 "// VIRTUAL KEYSPACE %s;",
                                 object.name,
                                 object.name);

        return String.format("CREATE KEYSPACE %s\n" +
                             "    WITH replication = {'class': '%s'%s}\n" +
                             "     AND durable_writes = %s;",
                             ColumnIdentifier.maybeQuote(object.name),
                             repl.klass.getName(),
                             repl.options.isEmpty() ? ""
                                                    : repl.options.entrySet()
                                                                  .stream()
                                                                  .map(e -> String.format("%s: %s",
                                                                                          singleQuote(e.getKey()),
                                                                                          singleQuote(e.getValue())))
                                                                  .collect(Collectors.joining(", ", ", ", "")),
                             object.params.durableWrites);
    }

    public static String getFunctionAsCQL(UDFunction function)
    {
        StringBuilder argNamesAndTypes = new StringBuilder();
        for (int i = 0; i < function.argNames().size(); i++)
        {
            if (argNamesAndTypes.length() > 0)
                argNamesAndTypes.append(", ");
            argNamesAndTypes.append(function.argNames().get(i).toCQLString())
                            .append(' ')
                            .append(function.argTypes().get(i).asCQL3Type());
        }
        return String.format("CREATE FUNCTION %s(%s)\n" +
                             "    %s ON NULL INPUT\n" +
                             "    RETURNS %s\n" +
                             "    LANGUAGE %s\n" +
                             "    AS $$%s$$;",
                             function.name().toCQLString(),
                             argNamesAndTypes,
                             function.isCalledOnNullInput() ? "CALLED" : "RETURNS NULL",
                             function.returnType().asCQL3Type(),
                             function.language(),
                             function.body()
                            );
    }

    public static String getAggregateAsCQL(UDAggregate aggregate)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("CREATE AGGREGATE %s(%s)\n" +
                                "    SFUNC %s\n" +
                                "    STYPE %s",
                                aggregate.name().toCQLString(),
                                aggregate.argTypes().stream().map(AbstractType::asCQL3Type).map(CQL3Type::toString).collect(Collectors.joining(", ")),
                                ColumnIdentifier.maybeQuote(aggregate.stateFunction().name().name),
                                aggregate.stateType().asCQL3Type().toString()));
        if (aggregate.finalFunction() != null)
            sb.append(String.format("\n    FINALFUNC %s",
                                    ColumnIdentifier.maybeQuote(aggregate.finalFunction().name().name)));
        if (aggregate.initialCondition() != null)
            sb.append(String.format("\n    INITCOND %s",
                                    aggregate.stateType().asCQL3Type().toCQLLiteral(aggregate.initialCondition(), ProtocolVersion.CURRENT)));

        sb.append(";");

        return sb.toString();
    }

    /**
     * Build a CQL String representation of Column Family Metadata.
     *
     * *Note*: this is _only_ visible for testing; you generally shouldn't re-create a single table in isolation as
     * that will not contain everything needed for user types.
     */
    @VisibleForTesting
    public static String getTableMetadataAsCQL(TableMetadata metadata,
                                               Types types,
                                               boolean includeDroppedColumns,
                                               boolean internals,
                                               boolean ifNotExists)
    {
        UserTypesForDroppedColumns droppedColumnsTypes = new UserTypesForDroppedColumns(metadata, types);
        return getTableMetadataAsCQL(metadata, droppedColumnsTypes, includeDroppedColumns, internals, ifNotExists);
    }

    private static String getTableMetadataAsCQL(TableMetadata metadata,
                                                UserTypesForDroppedColumns droppedColumnsTypes,
                                                boolean includeDroppedColumns,
                                                boolean internals,
                                                boolean ifNotExists)
    {
        StringBuilder sb = new StringBuilder(2048);
        String createKeyword = "CREATE";
        if (notCqlCompatible(metadata))
        {
            sb.append(String.format("/*\nWarning: Table %s omitted because it has constructs not compatible with CQL (was created via legacy API).\n",
                                    metadata.toString()));
            sb.append("\nApproximate structure, for reference:");
            sb.append("\n(this should not be used to reproduce this schema)\n\n");
        }
        else if (metadata.isVirtual())
        {
            sb.append(String.format("/*\n" +
                                    "Warning: Table %s is a virtual table and cannot be recreated with CQL.\n" +
                                    "Structure, for reference:\n",
                                    metadata.toString()));
            createKeyword = "VIRTUAL";
        }

        List<ColumnMetadata> clusteringColumns = getClusteringColumns(metadata);

        if (metadata.isView())
        {
            getViewMetadataAsCQL(sb, metadata, ifNotExists, createKeyword);
        }
        else
        {
            getBaseTableMetadataAsCQL(sb, metadata, droppedColumnsTypes, ifNotExists, includeDroppedColumns, createKeyword);
        }

        sb.append("WITH ");

        if (internals)
            sb.append("ID = ")
              .append(metadata.id)
              .append("\n    AND ");

        if (metadata.isCompactTable())
            sb.append("COMPACT STORAGE\n    AND ");

        if (clusteringColumns.size() > 0)
        {
            sb.append("CLUSTERING ORDER BY (");

            sb.append(clusteringColumns.stream()
                                       .map(cd -> cd.name.toCQLString() + ' ' + cd.clusteringOrder().toString())
                                       .collect(Collectors.joining(", ")));
            sb.append(")\n    AND ");
        }

        toCQL(sb, metadata.params, metadata.isVirtual());
        sb.append(";");

        if (notCqlCompatible(metadata) || metadata.isVirtual())
        {
            sb.append("\n*/");
        }
        return sb.toString();
    }

    private static void getViewMetadataAsCQL(StringBuilder sb,
                                             TableMetadata metadata,
                                             boolean ifNotExists,
                                             String createKeyword)
    {
        assert metadata.isView();
        KeyspaceMetadata keyspaceMetadata = Schema.instance.getKeyspaceMetadata(metadata.keyspace);
        assert keyspaceMetadata != null;
        ViewMetadata viewMetadata = keyspaceMetadata.views.get(metadata.name).orElse(null);
        assert viewMetadata != null;

        List<ColumnMetadata> partitionKeyColumns = metadata.partitionKeyColumns();
        List<ColumnMetadata> clusteringColumns = getClusteringColumns(metadata);

        sb.append(String.format("%s MATERIALIZED VIEW ", createKeyword));
        if (ifNotExists)
            sb.append("IF NOT EXISTS ");
        sb.append(metadata.toString()).append(" AS\n    SELECT ");

        if (viewMetadata.includeAllColumns)
        {
            sb.append("*");
        }
        else
        {
            sb.append(metadata.columns()
                              .stream()
                              .sorted(Comparator.comparing((c -> c.name.toCQLString())))
                              .map(c -> c.name.toCQLString())
                              .collect(Collectors.joining(", ")));
        }

        sb.append("\n    FROM ")
          .append(ColumnIdentifier.maybeQuote(viewMetadata.metadata.keyspace))
          .append(".")
          .append(ColumnIdentifier.maybeQuote(viewMetadata.baseTableName))
          .append("\n    WHERE ")
          .append(viewMetadata.whereClause)
          .append("\n    ");

        if (clusteringColumns.size() > 0 || partitionKeyColumns.size() > 1)
        {
            sb.append("PRIMARY KEY (");
            if (partitionKeyColumns.size() > 1)
            {
                sb.append("(");
                sb.append(partitionKeyColumns.stream()
                                             .map(cdf -> cdf.name.toCQLString())
                                             .collect(Collectors.joining(", ")));
                sb.append(")");
            }
            else
            {
                sb.append(partitionKeyColumns.get(0).name.toCQLString());
            }

            for (ColumnMetadata cfd : metadata.clusteringColumns())
                sb.append(", ").append(cfd.name.toCQLString());

            sb.append(')');
        }
        sb.append("\n ");
    }

    private static void getBaseTableMetadataAsCQL(StringBuilder sb,
                                                  TableMetadata metadata,
                                                  UserTypesForDroppedColumns droppedColumnsTypes,
                                                  boolean ifNotExists,
                                                  boolean includeDroppedColumns,
                                                  String createKeyword)
    {
        sb.append(String.format("%s TABLE ", createKeyword));
        if (ifNotExists)
            sb.append("IF NOT EXISTS ");
        sb.append(metadata.toString()).append(" (");

        // We sort regular and static columns: this is better than a largely random order, and make it more predictable
        // for testing.
        List<ColumnMetadata> partitionKeyColumns = new ArrayList<>(metadata.partitionKeyColumns());
        partitionKeyColumns.sort(ColumnMetadata::compareTo);

        List<ColumnMetadata> clusteringColumns = getClusteringColumns(metadata);
        List<ColumnMetadata> partitionColumns = getPartitionColumns(metadata);

        AtomicBoolean first = new AtomicBoolean(true);
        sb.append("\n    ");
        for (ColumnMetadata cfd : partitionKeyColumns)
        {
            if (!first.getAndSet(false))
                sb.append(",\n    ");
            toCQL(sb, cfd);
            if (partitionKeyColumns.size() == 1 && clusteringColumns.size() == 0)
                sb.append(" PRIMARY KEY");
        }

        for (ColumnMetadata cfd : clusteringColumns)
        {
            if (!first.getAndSet(false))
                sb.append(",\n    ");
            toCQL(sb, cfd);
        }

        for (ColumnMetadata cfd : partitionColumns)
        {
            // If the column has been re-added after a drop, we don't include it right away. Instead, we'll add the
            // dropped one first below, then we'll issue the DROP and then the actual ADD for this column, thus
            // simulating the proper sequence of events.
            if (includeDroppedColumns && metadata.droppedColumns.containsKey(cfd.name.bytes))
                continue;

            if (!first.getAndSet(false))
                sb.append(",\n    ");
            toCQL(sb, cfd, metadata.isStaticCompactTable());
        }

        if (includeDroppedColumns)
        {
            for (Map.Entry<ByteBuffer, DroppedColumn> entry : metadata.droppedColumns.entrySet())
            {
                DroppedColumn droppedColumn = entry.getValue();
                if (!first.getAndSet(false))
                    sb.append(",\n    ");
                toCQL(sb, droppedColumn, droppedColumnsTypes);
            }
        }

        if (clusteringColumns.size() > 0 || partitionKeyColumns.size() > 1)
        {
            sb.append(",\n    PRIMARY KEY (");
            if (partitionKeyColumns.size() > 1)
            {
                sb.append("(");
                sb.append(partitionKeyColumns.stream()
                                             .map(cfd -> cfd.name.toCQLString())
                                             .collect(Collectors.joining(", ")));
                sb.append(")");
            }
            else
            {
                sb.append(partitionKeyColumns.get(0).name.toCQLString());
            }

            for (ColumnMetadata cfd : metadata.clusteringColumns())
                sb.append(", ").append(cfd.name.toCQLString());

            sb.append(')');
        }
        sb.append("\n) ");
    }

    private static void toCQL(StringBuilder sb, DroppedColumn droppedColumn, UserTypesForDroppedColumns droppedColumnsTypes)
    {
        ColumnMetadata cd = droppedColumn.column;
        String userType = droppedColumnsTypes.forColumn(cd);
        sb.append(cd.name.toCQLString())
          .append(' ')
          .append(userType == null ? cd.type.asCQL3Type().toString() : userType);
        if (cd.isStatic())
            sb.append(" static");
    }

    /**
     * Build a CQL String representation of User Types used in the given table.
     *
     * Type order is ensured as types are built incrementally: from the innermost (most nested)
     * to the outermost.
     *
     * @param metadata the table for which to extract the user types CQL statements.
     * @param types the user types defined in the keyspace of the dumped table (which will thus contain any user type
     * used by {@code metadata}).
     * @return a list of {@code CREATE TYPE} statements corresponding to all the types used in {@code metadata}.
     */
    @VisibleForTesting
    public static Stream<String> getUserTypesAsCQL(TableMetadata metadata, Types types)
    {
        /*
         * Implementation note: at first approximation, it may seem like we don't need the Types argument and instead
         * directly extract the user types from the provided TableMetadata. Indeed, full user types definitions are
         * contained in UserType instances.
         *
         * However, the UserType instance found within the TableMetadata may have been frozen in such a way that makes
         * it challenging.
         *
         * Consider the user has created:
         *   CREATE TYPE inner (a set<int>);
         *   CREATE TYPE outer (b inner);
         *   CREATE TABLE t (k int PRIMARY KEY, c1 frozen<outer>, c2 set<frozen<inner>>)
         * The corresponding TableMetadata would have, as types (where 'mc=true' means that the type has his isMultiCell
         * set to true):
         *   c1: UserType(mc=false, "outer", b->UserType(mc=false, "inner", a->SetType(mc=fase, Int32Type)))
         *   c2: SetType(mc=true, UserType(mc=false, "inner", a->SetType(mc=fase, Int32Type)))
         * From which, it's impossible to decide if we should dump the types above, or instead:
         *   CREATE TYPE inner (a frozen<set<int>>);
         *   CREATE TYPE outer (b frozen<inner>);
         * or anything in-between.
         *
         * And while, as of the current limitation around multi-cell types (that are only support non-frozen at
         * top-level), any of the generated definition would kind of "work", 1) this could confuse users and 2) this
         * would break if we do lift the limitation, which wouldn't be future proof.
         */
        LinkedHashSet<ByteBuffer> toDump = new LinkedHashSet<>();
        metadata.columns().forEach(c -> findUserType(c.type, toDump));

        return toDump.stream()
                     .map(name -> toCQL(metadata, types, name));
    }

    /**
     * Build a CQL String representation of Dropped Columns in the given Column Family.
     *
     * If the column was dropped once, but is now re-created `ADD` will be appended accordingly.
     */
    @VisibleForTesting
    public static Stream<String> getDroppedColumnsAsCQL(TableMetadata metadata)
    {
        return metadata.droppedColumns.entrySet()
                                      .stream()
                                      .flatMap(entry -> {
                                          if (metadata.getColumn(entry.getKey()) == null)
                                              return Stream.of(toCQLDrop(metadata, entry.getValue()));
                                          else
                                              return Stream.of(toCQLDrop(metadata, entry.getValue()),
                                                               toCQLAdd(metadata, metadata.getColumn(entry.getKey())));
                                      });
    }

    /**
     * Build a CQL String representation of Indexes on columns in the given Column Family
     */
    @VisibleForTesting
    public static Stream<String> getIndexesAsCQL(TableMetadata metadata)
    {
        return metadata.indexes
                .stream()
                .map(indexMetadata -> toCQL(metadata, indexMetadata));
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
                                 options.isEmpty() ? "" : " WITH OPTIONS = " + toCQL(options));
        }
        else
        {
            return String.format("CREATE INDEX %s ON %s (%s);",
                                 indexMetadata.toCQLString(),
                                 baseTable.toString(),
                                 indexMetadata.options.get(IndexTarget.TARGET_OPTION_NAME));
        }
    }

    private static String toCQL(TableMetadata metadata, Types types, ByteBuffer name)
    {
        Optional<UserType> type = types.get(name);
        if (type.isPresent())
            return toCQL(type.get());

        String typeName = UTF8Type.instance.getString(name);
        // This really shouldn't happen, but if it does (a bug), we can at least dump what we know about and
        // log an error to tell users they will have to fill the gaps. We also include the error as a CQL
        // comment in the output of this method (in place of the missing statement) in case the user see it
        // more there.
        logger.error("Cannot find user type {} definition when recreating CQL schema for table {}. This is a "
                     + "bug and should be reported. The type has been ignored and the "
                     + "recreated schema statements will be incomplete and may need manual correction",
                     typeName, metadata);
        return String.format("// ERROR: user type %s is part of table %s definition but its "
                             + "definition was missing", typeName, metadata);
    }

    public static String toCQL(UserType userType)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TYPE ").append(userType.toCQLString()).append(" (\n    ");

        for (int i = 0; i < userType.size(); i++)
        {
            if (i > 0)
                sb.append(",\n    ");
            sb.append(String.format("%s %s",
                                    userType.fieldNameAsString(i),
                                    userType.fieldType(i).asCQL3Type()));
        }
        sb.append("\n);");
        return sb.toString();
    }

    private static void toCQL(StringBuilder sb, TableParams tableParams, boolean virtual)
    {
        if (!virtual)
        {
            sb.append("bloom_filter_fp_chance = ").append(tableParams.bloomFilterFpChance);
            sb.append("\n    AND caching = ").append(toCQL(tableParams.caching.asMap()));
            sb.append("\n    AND cdc = ").append(tableParams.cdc);
            sb.append("\n    AND comment = ").append(singleQuote(tableParams.comment));
            sb.append("\n    AND compaction = ").append(toCQL(tableParams.compaction.asMap()));
            sb.append("\n    AND compression = ").append(toCQL(tableParams.compression.asMap()));
            sb.append("\n    AND crc_check_chance = ").append(tableParams.crcCheckChance);
            sb.append("\n    AND default_time_to_live = ").append(tableParams.defaultTimeToLive);
            sb.append("\n    AND extensions = ").append(toCQL(tableParams.extensions, v -> "0x" + ByteBufferUtil.bytesToHex(v)));
            sb.append("\n    AND gc_grace_seconds = ").append(tableParams.gcGraceSeconds);
            sb.append("\n    AND max_index_interval = ").append(tableParams.maxIndexInterval);
            sb.append("\n    AND memtable_flush_period_in_ms = ").append(tableParams.memtableFlushPeriodInMs);
            sb.append("\n    AND min_index_interval = ").append(tableParams.minIndexInterval);
            sb.append("\n    AND read_repair = '").append(tableParams.readRepair.toString()).append("'");
            sb.append("\n    AND speculative_retry = '").append(tableParams.speculativeRetry).append("'");
            sb.append("\n    AND additional_write_policy = '").append(tableParams.additionalWritePolicy).append("'");
        }
        else
        {
            sb.append("comment = ").append(singleQuote(tableParams.comment));
        }
    }

    public static String toCQL(Map<?, ?> map)
    {
        return toCQL(map, v -> SchemaCQLHelper.singleQuote(v.toString()));
    }

    private static <V> String toCQL(Map<?, V> map, Function<V, String> valueMapper)
    {
        if (map.isEmpty())
            return "{}";

        StringBuilder builder = new StringBuilder("{");

        boolean isFirst = true;
        Object[] keys = map.keySet().toArray();
        Arrays.sort(keys, Comparator.comparing(Object::toString));
        for (Object key: keys)
        {
            V value = map.get(key);
            if (isFirst)
                isFirst = false;
            else
                builder.append(", ");
            builder.append(singleQuote(key.toString()));
            builder.append(": ");
            builder.append(valueMapper.apply(value));
        }

        builder.append("}");
        return builder.toString();
    }

    private static void toCQL(StringBuilder sb, ColumnMetadata cd)
    {
        toCQL(sb, cd, false);
    }

    private static void toCQL(StringBuilder sb, ColumnMetadata cd, boolean isStaticCompactTable)
    {
        sb.append(String.format("%s %s%s",
                                cd.name.toCQLString(),
                                cd.type.asCQL3Type().toString(),
                                cd.isStatic() && !isStaticCompactTable ? " static" : ""));
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
     * Find all user types used by the provided type and add them to the provided list if not already there.
     *
     * @param type the type to check for user types.
     * @param types the set of UDT names to which to add new user types found in {@code type}. Note that the
     * insertion ordering is important and ensures that if a user type A uses another user type B, then B will appear
     * before A in iteration order.
     */
    public static void findUserType(AbstractType<?> type, LinkedHashSet<ByteBuffer> types)
    {
        // Reach into subtypes first, so that if the type is a UDT, it's dependencies are recreated first.
        for (AbstractType subType: type.subTypes())
            findUserType(subType, types);

        if (type.isUDT())
            types.add(((UserType)type).name);
    }

    public static String singleQuote(String s)
    {
        return String.format("'%s'", s.replaceAll("'", "''"));
    }

    /**
     * Whether or not the given metadata is compatible / representable with CQL Language
     */
    private static boolean notCqlCompatible(TableMetadata metaData)
    {
        if (metaData.isSuper())
            return true;

        return metaData.isCompactTable()
               && metaData.regularColumns().size() > 1
               && metaData.clusteringColumns().size() >= 1;
    }

    /**
     * Helper class to handle non-frozen user types in dropped columns.
     *
     * <p>When dropping columns, we "expand" user types into tuples in order to avoid having to record dropped user
     * types definition that are not used anymore but were used by a now dropped column. Because we support non-frozen
     * user types, this mean we have to distinguish non-frozen and frozen tuples at least for dropped columns. Which
     * we do (see {@link org.apache.cassandra.schema.CQLTypeParser#parseDroppedType(String, String)} for instance), but this is specific to
     * dropped columns as tuple are otherwise frozen by default in CQL. This poses a problem when generating the
     * schema in this class, as we handle dropped columns by adding them to the CREATE statement of their table and
     * _then_ generate a ALTER DROP for them. Problem being, if we use a tuple type for those dropped columns that
     * correspond to non-frozen user types, said tuple will end-up frozen due to the rule of CQL, and that is wrong.
     * So instead, when we detect a non-frozen tuple in a dropped column, we re-create a user type with the same
     * types but faked type name and field names: that way, non-frozenness is respected when re-creating the dropped
     * column. This class helps implementing this process.
     */
    private static class UserTypesForDroppedColumns
    {
        private final TableMetadata table;
        private final Types types;
        private final Map<ByteBuffer, String> createdTypes = new HashMap<>();
        private final List<UserType> generatedTypes = new ArrayList<>();

        private UserTypesForDroppedColumns(TableMetadata table, Types types)
        {
            this.table = table;
            this.types = types;

            table.droppedColumns.values()
                                .stream()
                                .filter(dropped -> {
                                    // We leave the frozen tuple as is, first because they are not a problem, and second because we cannot
                                    // know anyway if this was a user type or a genuine tuple pre-DROP, so converting to a user type could
                                    // actually be more confusing than not.
                                    AbstractType<?> type = dropped.column.type;
                                    return type.isTuple() && type.isMultiCell();
                                })
                                .forEach(dropped -> {
                                    String typeName = generateTypeName(dropped.column.name);
                                    UserType userType = recreateUserType(typeName, (TupleType) dropped.column.type);
                                    createdTypes.put(dropped.column.name.bytes, typeName);
                                    generatedTypes.add(userType);
                                });
        }

        private Stream<String> createTypeStatements()
        {
            return generatedTypes.stream().map(SchemaCQLHelper::toCQL);
        }

        private String forColumn(ColumnMetadata column)
        {
            return createdTypes.get(column.name.bytes);
        }

        private String generateTypeName(ColumnIdentifier column)
        {
            String base = column.toString() + "_pre_drop_type";
            if (!types.get(ByteBufferUtil.bytes(base)).isPresent())
                return base;

            for (int i = 1; i < Integer.MAX_VALUE; i++)
            {
                String candidate = base + i;
                if (!types.get(ByteBufferUtil.bytes(base)).isPresent())
                    return candidate;
            }
            throw new AssertionError(String.format("was not able to find a type name for dropped %s in %s",
                                                   column, table));
        }

        private UserType recreateUserType(String typeName, TupleType type)
        {
            // This is reasonably easy because so far we've only allowed non-frozen user types at top-level, so we only
            // have to recreate the top level type, and can ignore anything nested (leave it be tuple). If we even
            // change this, allowing nested non-frozen UDT, this would have to be updated to take this into account.
            List<FieldIdentifier> fieldNames = new ArrayList<>(type.size());
            for (int i = 0; i < type.size(); i++)
                fieldNames.add(FieldIdentifier.forInternalString("f" + (i+1)));
            return new UserType(table.keyspace,
                                ByteBufferUtil.bytes(typeName),
                                fieldNames,
                                type.subTypes(),
                                type.isMultiCell());
        }

        private Stream<String> dropTypeStatements()
        {
            return createdTypes.values()
                               .stream()
                               .map(t -> String.format("DROP TYPE %s;", t));
        }
    }

}
