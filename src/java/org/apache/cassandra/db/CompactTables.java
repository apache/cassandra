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

import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.SuperColumnCompatibility;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.EmptyType;
import org.apache.cassandra.db.marshal.UTF8Type;

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
 * SuperColumn families handling and detailed format description can be found in {@code SuperColumnCompatibility}.
 */
public abstract class CompactTables
{
    private CompactTables() {}

    public static ColumnDefinition getCompactValueColumn(PartitionColumns columns)
    {
        assert columns.regulars.simpleColumnCount() == 1 && columns.regulars.complexColumnCount() == 0;
        return columns.regulars.getSimple(0);
    }

    public static AbstractType<?> columnDefinitionComparator(String kind, boolean isSuper, AbstractType<?> rawComparator, AbstractType<?> rawSubComparator)
    {
        if (!"regular".equals(kind))
            return UTF8Type.instance;

        return isSuper ? rawSubComparator : rawComparator;
    }

    public static boolean hasEmptyCompactValue(CFMetaData metadata)
    {
        return metadata.compactValueColumn().type instanceof EmptyType;
    }

    public static DefaultNames defaultNameGenerator(Set<String> usedNames)
    {
        return new DefaultNames(new HashSet<String>(usedNames));
    }

    public static DefaultNames defaultNameGenerator(Iterable<ColumnDefinition> defs)
    {
        Set<String> usedNames = new HashSet<>();
        for (ColumnDefinition def : defs)
            usedNames.add(def.name.toString());
        return new DefaultNames(usedNames);
    }

    public static class DefaultNames
    {
        private static final String DEFAULT_PARTITION_KEY_NAME = "key";
        private static final String DEFAULT_CLUSTERING_NAME = "column";
        private static final String DEFAULT_COMPACT_VALUE_NAME = "value";

        private final Set<String> usedNames;
        private int partitionIndex = 0;
        private int clusteringIndex = 1;
        private int compactIndex = 0;

        private DefaultNames(Set<String> usedNames)
        {
            this.usedNames = usedNames;
        }

        public String defaultPartitionKeyName()
        {
            while (true)
            {
                // For compatibility sake, we call the first alias 'key' rather than 'key1'. This
                // is inconsistent with column alias, but it's probably not worth risking breaking compatibility now.
                String candidate = partitionIndex == 0 ? DEFAULT_PARTITION_KEY_NAME : DEFAULT_PARTITION_KEY_NAME + (partitionIndex + 1);
                ++partitionIndex;
                if (usedNames.add(candidate))
                    return candidate;
            }
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
