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
package org.apache.cassandra.cql3;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.marshal.CollectionType;


public abstract class Relation {

    protected Type relationType;

    public static enum Type
    {
        EQ
        {
            public boolean allowsIndexQueryOn(ColumnDefinition columnDef)
            {
                return columnDef.isIndexed();
            }
        },
        LT,
        LTE,
        GTE,
        GT,
        IN,
        CONTAINS
        {
            public boolean allowsIndexQueryOn(ColumnDefinition columnDef)
            {
                return columnDef.isIndexed()
                        && columnDef.type.isCollection()
                        && (!((CollectionType<?>) columnDef.type).isMap()
                                || columnDef.hasIndexOption(SecondaryIndex.INDEX_VALUES_OPTION_NAME));
            }
        },
        CONTAINS_KEY
        {
            public boolean allowsIndexQueryOn(ColumnDefinition columnDef)
            {
                return columnDef.isIndexed()
                        && columnDef.type.isCollection()
                        && (!((CollectionType<?>) columnDef.type).isMap()
                                || columnDef.hasIndexOption(SecondaryIndex.INDEX_KEYS_OPTION_NAME));
            }
        },
        NEQ;

        /**
         * Checks if this relation type allow index queries on the specified column
         *
         * @param columnDef the column definition.
         * @return <code>true</code> if this relation type allow index queries on the specified column,
         * <code>false</code> otherwise.
         */
        public boolean allowsIndexQueryOn(ColumnDefinition columnDef)
        {
            return false;
        }

        @Override
        public String toString()
        {
            switch (this)
            {
                case EQ:
                    return "=";
                case LT:
                    return "<";
                case LTE:
                    return "<=";
                case GT:
                    return ">";
                case GTE:
                    return ">=";
                case NEQ:
                    return "!=";
                case CONTAINS_KEY:
                    return "CONTAINS KEY";
                default:
                    return this.name();
            }
        }
    }

    public Type operator()
    {
        return relationType;
    }

    public abstract boolean isMultiColumn();
}
