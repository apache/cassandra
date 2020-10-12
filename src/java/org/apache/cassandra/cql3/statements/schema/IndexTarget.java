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
package org.apache.cassandra.cql3.statements.schema;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

public class IndexTarget
{
    public static final String TARGET_OPTION_NAME = "target";
    public static final String CUSTOM_INDEX_OPTION_NAME = "class_name";

    public final ColumnIdentifier column;
    public final Type type;

    public IndexTarget(ColumnIdentifier column, Type type)
    {
        this.column = column;
        this.type = type;
    }

    public String asCqlString()
    {
        return type == Type.SIMPLE
             ? column.toCQLString()
             : String.format("%s(%s)", type.toString(), column.toCQLString());
    }

    public static class Raw
    {
        private final ColumnIdentifier column;
        private final Type type;

        private Raw(ColumnIdentifier column, Type type)
        {
            this.column = column;
            this.type = type;
        }

        public static Raw simpleIndexOn(ColumnIdentifier c)
        {
            return new Raw(c, Type.SIMPLE);
        }

        public static Raw valuesOf(ColumnIdentifier c)
        {
            return new Raw(c, Type.VALUES);
        }

        public static Raw keysOf(ColumnIdentifier c)
        {
            return new Raw(c, Type.KEYS);
        }

        public static Raw keysAndValuesOf(ColumnIdentifier c)
        {
            return new Raw(c, Type.KEYS_AND_VALUES);
        }

        public static Raw fullCollection(ColumnIdentifier c)
        {
            return new Raw(c, Type.FULL);
        }

        public IndexTarget prepare(TableMetadata table)
        {
            // Until we've prepared the target column, we can't be certain about the target type
            // because (for backwards compatibility) an index on a collection's values uses the
            // same syntax as an index on a regular column (i.e. the 'values' in
            // 'CREATE INDEX on table(values(collection));' is optional). So we correct the target type
            // when the target column is a collection & the target type is SIMPLE.
            ColumnMetadata columnDef = table.getExistingColumn(column);
            Type actualType = (type == Type.SIMPLE && columnDef.type.isCollection()) ? Type.VALUES : type;
            return new IndexTarget(columnDef.name, actualType);
        }
    }

    public enum Type
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
    
    @Override
    public String toString()
    {
        return asCqlString();
    }
}
