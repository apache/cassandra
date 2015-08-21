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

import java.util.Map;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.schema.IndexMetadata;

public class IndexTarget
{
    public static final String CUSTOM_INDEX_OPTION_NAME = "class_name";

    /**
     * The name of the option used to specify that the index is on the collection keys.
     */
    public static final String INDEX_KEYS_OPTION_NAME = "index_keys";

    /**
     * The name of the option used to specify that the index is on the collection values.
     */
    public static final String INDEX_VALUES_OPTION_NAME = "index_values";

    /**
     * The name of the option used to specify that the index is on the collection (map) entries.
     */
    public static final String INDEX_ENTRIES_OPTION_NAME = "index_keys_and_values";

    public final ColumnIdentifier column;
    public final Type type;

    private IndexTarget(ColumnIdentifier column, Type type)
    {
        this.column = column;
        this.type = type;
    }

    public static class Raw
    {
        private final ColumnIdentifier.Raw column;
        private final Type type;

        private Raw(ColumnIdentifier.Raw column, Type type)
        {
            this.column = column;
            this.type = type;
        }

        public static Raw valuesOf(ColumnIdentifier.Raw c)
        {
            return new Raw(c, Type.VALUES);
        }

        public static Raw keysOf(ColumnIdentifier.Raw c)
        {
            return new Raw(c, Type.KEYS);
        }

        public static Raw keysAndValuesOf(ColumnIdentifier.Raw c)
        {
            return new Raw(c, Type.KEYS_AND_VALUES);
        }

        public static Raw fullCollection(ColumnIdentifier.Raw c)
        {
            return new Raw(c, Type.FULL);
        }

        public IndexTarget prepare(CFMetaData cfm)
        {
            return new IndexTarget(column.prepare(cfm), type);
        }
    }

    public static enum Type
    {
        VALUES, KEYS, KEYS_AND_VALUES, FULL;

        public String toString()
        {
            switch (this)
            {
                case KEYS: return "keys";
                case KEYS_AND_VALUES: return "entries";
                case FULL: return "full";
                default: return "values";
            }
        }

        public String indexOption()
        {
            switch (this)
            {
                case KEYS: return INDEX_KEYS_OPTION_NAME;
                case KEYS_AND_VALUES: return INDEX_ENTRIES_OPTION_NAME;
                case VALUES: return INDEX_VALUES_OPTION_NAME;
                default: throw new AssertionError();
            }
        }

        public static Type fromIndexMetadata(IndexMetadata index, CFMetaData cfm)
        {
            Map<String, String> options = index.options;
            if (options.containsKey(INDEX_KEYS_OPTION_NAME))
            {
                return KEYS;
            }
            else if (options.containsKey(INDEX_ENTRIES_OPTION_NAME))
            {
                return KEYS_AND_VALUES;
            }
            else
            {
                ColumnDefinition cd = index.indexedColumn(cfm);
                if (cd.type.isCollection() && !cd.type.isMultiCell())
                {
                    return FULL;
                }
                else
                {
                    return VALUES;
                }
            }
        }
    }
}
