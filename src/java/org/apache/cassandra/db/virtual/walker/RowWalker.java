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

package org.apache.cassandra.db.virtual.walker;

import java.util.function.Supplier;

import org.apache.cassandra.db.virtual.model.Column;

/**
 * Utility class for quick iteration over row attributes and row values.
 * Walk order is defined by {@link Column} annotations and is the same as
 * in CQL (partition key columns first, then clustering columns, then regular columns).
 */
public interface RowWalker<R>
{
    /**
     * Returns the number of columns of the given type in the row.
     *
     * @param type the type of column
     * @return the number of columns of the given type in the row.
     */
    int count(Column.Type type);

    /**
     * Visit the metadata of the row.
     *
     * @param visitor the visitor to accept the metadata
     */
    void visitMeta(MetadataVisitor visitor);

    /**
     * Visit the row.
     *
     * @param row     the row to visit
     * @param visitor the visitor to accept the row
     */
    void visitRow(R row, RowMetadataVisitor visitor);

    /**
     * Visitor for metadata of the row.
     */
    interface MetadataVisitor
    {
        /**
         * Process the metadata.
         *
         * @param type  the type of column
         * @param name  the name of the column
         * @param clazz the class of the column
         * @param <T>   the type of the column
         */
        <T> void accept(Column.Type type, String name, Class<T> clazz);
    }

    /**
     * Visitor for a row value and its metadata.
     */
    interface RowMetadataVisitor
    {
        /**
         * Process the row metadata.
         *
         * @param type  the type of column
         * @param name  the name of the column
         * @param clazz the class of the column
         * @param value the value of the column
         * @param <T>   the type of the column
         */
        <T> void accept(Column.Type type, String name, Class<T> clazz, Supplier<T> value);
    }
}
