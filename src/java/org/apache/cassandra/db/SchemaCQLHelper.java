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
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.schema.*;

/**
 * Helper methods to represent TableMetadata and related objects in CQL format
 */
public class SchemaCQLHelper
{
    private static final Pattern EMPTY_TYPE_REGEX = Pattern.compile("empty", Pattern.LITERAL);
    private static final String EMPTY_TYPE_QUOTED = Matcher.quoteReplacement("'org.apache.cassandra.db.marshal.EmptyType'");

    /**
     * Generates the DDL statement for a {@code schema.cql} snapshot file.
     */
    public static Stream<String> reCreateStatementsForSchemaCql(TableMetadata metadata, KeyspaceMetadata keyspaceMetadata)
    {
        // Types come first, as table can't be created without them
        Stream<String> udts = SchemaCQLHelper.getUserTypesAsCQL(metadata, keyspaceMetadata.types, true);

        Stream<String> tableMatadata = Stream.of(SchemaCQLHelper.getTableMetadataAsCQL(metadata, keyspaceMetadata));

        Stream<String> indexes = SchemaCQLHelper.getIndexesAsCQL(metadata, true);
        return Stream.of(udts, tableMatadata, indexes).flatMap(Function.identity());
    }

    /**
     * Build a CQL String representation of Column Family Metadata.
     *
     * *Note*: this is _only_ visible for testing; you generally shouldn't re-create a single table in isolation as
     * that will not contain everything needed for user types.
     */
    @VisibleForTesting
    public static String getTableMetadataAsCQL(TableMetadata metadata, KeyspaceMetadata keyspaceMetadata)
    {
        if (metadata.isView())
        {
            ViewMetadata viewMetadata = keyspaceMetadata.views.get(metadata.name).orElse(null);
            assert viewMetadata != null;
            /*
             * first argument(withInternals) indicates to include table metadata id and clustering columns order,
             * second argument(ifNotExists) instructs to include IF NOT EXISTS statement within creation statements.
             */
            return viewMetadata.toCqlString(true, true);
        }

        /*
         * With addition to withInternals and ifNotExists arguments, includeDroppedColumns will include dropped
         * columns as ALTER TABLE statements appended into the snapshot.
         */
        return metadata.toCqlString(true, true, true);
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
     * @param ifNotExists set to true if IF NOT EXISTS should be appended after CREATE TYPE string.
     * @return a list of {@code CREATE TYPE} statements corresponding to all the types used in {@code metadata}.
     */
    @VisibleForTesting
    public static Stream<String> getUserTypesAsCQL(TableMetadata metadata, Types types, boolean ifNotExists)
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
        return metadata.getReferencedUserTypes()
                       .stream()
                       .map(name -> getType(metadata, types, name).toCqlString(false, ifNotExists));
    }

    /**
     * Build a CQL String representation of Indexes on columns in the given Column Family
     *
     * @param metadata the table for which to extract the index CQL statements.
     * @param ifNotExists set to true if IF NOT EXISTS should be appended after CREATE INDEX string.
     * @return a list of {@code CREATE INDEX} statements corresponding to table {@code metadata}.
     */
    @VisibleForTesting
    public static Stream<String> getIndexesAsCQL(TableMetadata metadata, boolean ifNotExists)
    {
        return metadata.indexes
                .stream()
                .map(indexMetadata -> indexMetadata.toCqlString(metadata, ifNotExists));
    }

    private static UserType getType(TableMetadata metadata, Types types, ByteBuffer name)
    {
        return types.get(name)
                    .orElseThrow(() -> new IllegalStateException(String.format("user type %s is part of table %s definition but its definition was missing",
                                                                              UTF8Type.instance.getString(name),
                                                                              metadata)));
    }

    /**
     * Converts the type to a CQL type.  This method special cases empty and UDTs so the string can be used in a create
     * statement.
     *
     * Special cases
     * <ul>
     *     <li>empty - replaces with 'org.apache.cassandra.db.marshal.EmptyType'.  empty is the tostring of the type in
     *     CQL but not allowed to create as empty, but fully qualified name is allowed</li>
     *     <li>UserType - replaces with TupleType</li>
     * </ul>
     */
    public static String toCqlType(AbstractType<?> type)
    {
        return EMPTY_TYPE_REGEX.matcher(type.expandUserTypes().asCQL3Type().toString()).replaceAll(EMPTY_TYPE_QUOTED);
    }
}
