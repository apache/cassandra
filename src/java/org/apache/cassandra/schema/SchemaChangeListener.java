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
package org.apache.cassandra.schema;

import org.apache.cassandra.cql3.functions.UDAggregate;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.db.marshal.UserType;

public interface SchemaChangeListener
{
    default void onCreateKeyspace(KeyspaceMetadata keyspace)
    {
    }

    default void onCreateTable(TableMetadata table)
    {
    }

    default void onCreateView(ViewMetadata view)
    {
        onCreateTable(view.metadata);
    }

    default void onCreateType(UserType type)
    {
    }

    default void onCreateFunction(UDFunction function)
    {
    }

    default void onCreateAggregate(UDAggregate aggregate)
    {
    }

    default void onAlterKeyspace(KeyspaceMetadata before, KeyspaceMetadata after)
    {
    }

    default void onPreAlterTable(TableMetadata before, TableMetadata after)
    {
    }

    // the boolean flag indicates whether the change that triggered this event may have a substantive
    // impact on statements using the column family.
    default void onAlterTable(TableMetadata before, TableMetadata after, boolean affectStatements)
    {
    }

    default void onPreAlterView(ViewMetadata before, ViewMetadata after)
    {
    }

    default void onAlterView(ViewMetadata before, ViewMetadata after, boolean affectStatements)
    {
        onAlterTable(before.metadata, after.metadata, affectStatements);
    }

    default void onAlterType(UserType before, UserType after)
    {
    }

    default void onAlterFunction(UDFunction before, UDFunction after)
    {
    }

    default void onAlterAggregate(UDAggregate before, UDAggregate after)
    {
    }

    default void onDropKeyspace(KeyspaceMetadata keyspace, boolean dropData)
    {
    }

    default void onDropTable(TableMetadata table, boolean dropData)
    {
    }

    default void onDropView(ViewMetadata view, boolean dropData)
    {
        onDropTable(view.metadata, dropData);
    }

    default void onDropType(UserType type)
    {
    }

    default void onDropFunction(UDFunction function)
    {
    }

    default void onDropAggregate(UDAggregate aggregate)
    {
    }
}
