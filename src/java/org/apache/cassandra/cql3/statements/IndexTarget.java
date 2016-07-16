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
package org.apache.cassandra.cql3.statements;

import java.util.regex.Pattern;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;

public class IndexTarget
{
    public static final String TARGET_OPTION_NAME = "target";
    public static final String CUSTOM_INDEX_OPTION_NAME = "class_name";

    /**
     * The name of the option used to specify that the index is on the collection keys.
     */
    public static final String INDEX_KEYS_OPTION_NAME = "index_keys";

    /**
     * The name of the option used to specify that the index is on the collection (map) entries.
     */
    public static final String INDEX_ENTRIES_OPTION_NAME = "index_keys_and_values";

    /**
     * Regex for *unquoted* column names, anything which does not match this pattern must be a quoted name
     */
    private static final Pattern COLUMN_IDENTIFIER_PATTERN = Pattern.compile("[a-z_0-9]+");

    public final ColumnIdentifier column;
    public final boolean quoteName;
    public final Type type;

    public IndexTarget(ColumnIdentifier column, Type type)
    {
        this.column = column;
        this.type = type;

        // if the column name contains anything other than lower case alphanumerics
        // or underscores, then it must be quoted when included in the target string
        quoteName = !COLUMN_IDENTIFIER_PATTERN.matcher(column.toString()).matches();
    }

    public String asCqlString(CFMetaData cfm)
    {
        if (!cfm.getColumnDefinition(column).type.isCollection())
            return column.toCQLString();

        return String.format("%s(%s)", type.toString(), column.toCQLString());
    }

    public static class Raw
    {
        private final ColumnDefinition.Raw column;
        private final Type type;

        private Raw(ColumnDefinition.Raw column, Type type)
        {
            this.column = column;
            this.type = type;
        }

        public static Raw simpleIndexOn(ColumnDefinition.Raw c)
        {
            return new Raw(c, Type.SIMPLE);
        }

        public static Raw valuesOf(ColumnDefinition.Raw c)
        {
            return new Raw(c, Type.VALUES);
        }

        public static Raw keysOf(ColumnDefinition.Raw c)
        {
            return new Raw(c, Type.KEYS);
        }

        public static Raw keysAndValuesOf(ColumnDefinition.Raw c)
        {
            return new Raw(c, Type.KEYS_AND_VALUES);
        }

        public static Raw fullCollection(ColumnDefinition.Raw c)
        {
            return new Raw(c, Type.FULL);
        }

        public IndexTarget prepare(CFMetaData cfm)
        {
            // Until we've prepared the target column, we can't be certain about the target type
            // because (for backwards compatibility) an index on a collection's values uses the
            // same syntax as an index on a regular column (i.e. the 'values' in
            // 'CREATE INDEX on table(values(collection));' is optional). So we correct the target type
            // when the target column is a collection & the target type is SIMPLE.
            ColumnDefinition columnDef = column.prepare(cfm);
            Type actualType = (type == Type.SIMPLE && columnDef.type.isCollection()) ? Type.VALUES : type;
            return new IndexTarget(columnDef.name, actualType);
        }
    }

    public static enum Type
    {
        VALUES, KEYS, KEYS_AND_VALUES, FULL, SIMPLE;

        public String toString()
        {
            switch (this)
            {
                case KEYS: return "keys";
                case KEYS_AND_VALUES: return "entries";
                case FULL: return "full";
                case VALUES: return "values";
                case SIMPLE: return "";
                default: return "";
            }
        }

        public static Type fromString(String s)
        {
            if ("".equals(s))
                return SIMPLE;
            else if ("values".equals(s))
                return VALUES;
            else if ("keys".equals(s))
                return KEYS;
            else if ("entries".equals(s))
                return KEYS_AND_VALUES;
            else if ("full".equals(s))
                return FULL;

            throw new AssertionError("Unrecognized index target type " + s);
        }
    }
}
