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

import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Small utility methods pertaining to the encoding of COMPACT STORAGE tables.
 *
 * COMPACT STORAGE tables exists mainly for the sake of encoding internally thrift tables (as well as
 * exposing those tables through CQL). Note that due to these constraints, the internal representation
 * of compact tables does *not* correspond exactly to their CQL definition.
 *
 * The internal layout of such tables is such that it can encode any thrift table. That layout is as follow:
 *   CREATE TABLE compact (
 *      key [key_validation_class],
 *      [column_metadata_1] [type1] static,
 *      ...,
 *      [column_metadata_n] [type1] static,
 *      column [comparator],
 *      value [default_validation_class]
 *      PRIMARY KEY (key, column)
 *   )
 * More specifically, the table:
 *  - always has a clustering column and a regular value, which are used to store the "dynamic" thrift columns name and value.
 *    Those are always present because we have no way to know in advance if "dynamic" columns will be inserted or not. Note
 *    that when declared from CQL, compact tables may not have any clustering: in that case, we still have a clustering
 *    defined internally, it is just ignored as far as interacting from CQL is concerned.
 *  - have a static column for every "static" column defined in the thrift "column_metadata". Note that when declaring a compact
 *    table from CQL without any clustering (but some non-PK columns), the columns ends up static internally even though they are
 *    not in the declaration
 *
 * On variation is that if the table comparator is a CompositeType, then the underlying table will have one clustering column by
 * element of the CompositeType, but the rest of the layout is as above.
 *
 * As far as thrift is concerned, one exception to this is super column families, which have a different layout. Namely, a super
 * column families is encoded with:
 * {@code
 *   CREATE TABLE super (
 *      key [key_validation_class],
 *      super_column_name [comparator],
 *      [column_metadata_1] [type1],
 *      ...,
 *      [column_metadata_n] [type1],
 *      "" map<[sub_comparator], [default_validation_class]>
 *      PRIMARY KEY (key, super_column_name)
 *   )
 * }
 * In other words, every super column is encoded by a row. That row has one column for each defined "column_metadata", but it also
 * has a special map column (whose name is the empty string as this is guaranteed to never conflict with a user-defined
 * "column_metadata") which stores the super column "dynamic" sub-columns.
 */
public abstract class CompactTables
{
    // We use an empty value for the 1) this can't conflict with a user-defined column and 2) this actually
    // validate with any comparator.
    public static final ByteBuffer SUPER_COLUMN_MAP_COLUMN = ByteBufferUtil.EMPTY_BYTE_BUFFER;

    private CompactTables() {}

    public static ColumnMetadata getCompactValueColumn(RegularAndStaticColumns columns, boolean isSuper)
    {
        if (isSuper)
        {
            for (ColumnMetadata column : columns.regulars)
                if (column.name.bytes.equals(SUPER_COLUMN_MAP_COLUMN))
                    return column;
            throw new AssertionError("Invalid super column table definition, no 'dynamic' map column");
        }
        assert columns.regulars.simpleColumnCount() == 1 && columns.regulars.complexColumnCount() == 0;
        return columns.regulars.getSimple(0);
    }

    public static boolean hasEmptyCompactValue(TableMetadata metadata)
    {
        return metadata.compactValueColumn.type instanceof EmptyType;
    }

    public static boolean isSuperColumnMapColumn(ColumnMetadata column)
    {
        return column.kind == ColumnMetadata.Kind.REGULAR && column.name.bytes.equals(SUPER_COLUMN_MAP_COLUMN);
    }

    public static DefaultNames defaultNameGenerator(Set<String> usedNames)
    {
        return new DefaultNames(new HashSet<>(usedNames));
    }

    public static class DefaultNames
    {
        private static final String DEFAULT_CLUSTERING_NAME = "column";
        private static final String DEFAULT_COMPACT_VALUE_NAME = "value";

        private final Set<String> usedNames;
        private int clusteringIndex = 1;
        private int compactIndex = 0;

        private DefaultNames(Set<String> usedNames)
        {
            this.usedNames = usedNames;
        }

        public String defaultClusteringName()
        {
            while (true)
            {
                String candidate = DEFAULT_CLUSTERING_NAME + clusteringIndex;
                ++clusteringIndex;
                if (usedNames.add(candidate))
                    return candidate;
            }
        }

        public String defaultCompactValueName()
        {
            while (true)
            {
                String candidate = compactIndex == 0 ? DEFAULT_COMPACT_VALUE_NAME : DEFAULT_COMPACT_VALUE_NAME + compactIndex;
                ++compactIndex;
                if (usedNames.add(candidate))
                    return candidate;
            }
        }
    }
}
