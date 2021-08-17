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

import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;

public abstract class SchemaChangeListener
{
    public void onCreateKeyspace(String keyspace)
    {
    }

    public void onCreateTable(String keyspace, String table)
    {
    }

    public void onCreateView(String keyspace, String view)
    {
        onCreateTable(keyspace, view);
    }

    public void onCreateType(String keyspace, String type)
    {
    }

    public void onCreateFunction(String keyspace, String function, List<AbstractType<?>> argumentTypes)
    {
    }

    public void onCreateAggregate(String keyspace, String aggregate, List<AbstractType<?>> argumentTypes)
    {
    }

    public void onAlterKeyspace(String keyspace)
    {
    }

    // the boolean flag indicates whether the change that triggered this event may have a substantive
    // impact on statements using the column family.
    public void onAlterTable(String keyspace, String table, boolean affectsStatements)
    {
    }

    public void onAlterView(String keyspace, String view, boolean affectsStataments)
    {
        onAlterTable(keyspace, view, affectsStataments);
    }

    public void onAlterType(String keyspace, String type)
    {
    }

    public void onAlterFunction(String keyspace, String function, List<AbstractType<?>> argumentTypes)
    {
    }

    public void onAlterAggregate(String keyspace, String aggregate, List<AbstractType<?>> argumentTypes)
    {
    }

    public void onDropKeyspace(String keyspace)
    {
    }

    public void onDropTable(String keyspace, String table)
    {
    }

    public void onDropView(String keyspace, String view)
    {
        onDropTable(keyspace, view);
    }

    public void onDropType(String keyspace, String type)
    {
    }

    public void onDropFunction(String keyspace, String function, List<AbstractType<?>> argumentTypes)
    {
    }

    public void onDropAggregate(String keyspace, String aggregate, List<AbstractType<?>> argumentTypes)
    {
    }
}
