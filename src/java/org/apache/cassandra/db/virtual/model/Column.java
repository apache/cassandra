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

package org.apache.cassandra.db.virtual.model;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.cassandra.db.virtual.walker.RowWalker;

/**
 * Annotation to mark a walk order for a {@link RowWalker}. Annotation is used on a method that returns a value and
 * represents a column value for a row. The columns order is defined by the column type and the column name
 * in lexicographical order (case-insensitive) and the same way as in CQL (partition key columns first,
 * then clustering columns, then regular columns).
 * <p>
 * The column {@link Column.Type} type is the same as the existing CQL {@code ColumnMetadata.Kind} and is used to
 * define the column types for the virtual tables.
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.METHOD)
public @interface Column
{
    /**
     * The type of the column
     */
    Type type() default Type.REGULAR;

    /**
     * Be sure the order of the enum values is preserved as they are used in ordinal comparisons for the virtual tables.
     */
    enum Type
    {
        /**
         * The column is a partition key column.
         */
        PARTITION_KEY,
        /**
         * The column is a clustering column.
         */
        CLUSTERING,
        /**
         * The column is a regular column.
         */
        REGULAR
    }
}
