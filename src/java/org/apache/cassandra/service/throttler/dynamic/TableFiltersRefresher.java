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

package org.apache.cassandra.service.throttler.dynamic;

import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaChangeListener;
import org.apache.cassandra.schema.TableMetadata;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class TableFiltersRefresher implements SchemaChangeListener
{
    private List<TableFilter> filters = new LinkedList<>();

    public static Set<String> getAllKeyspaceDotTables()
    {
        Set<String> result = new HashSet<>();
        Set<String> keyspaces = Schema.instance.getKeyspaces();
        for (String keyspace : keyspaces)
        {
            for (TableMetadata cf : Schema.instance.getTablesAndViews(keyspace))
            {
                result.add(String.format("%s.%s", keyspace, cf.name));
            }
        }
        return result;
    }

    public void addFilter(TableFilter filter)
    {
        filters.add(filter);
    }

    public void registerSchemaChangeListener()
    {
        Schema.instance.registerListener(this);
    }

    // TODO: (low priority) consider doing the refresh in an asynchrous way. This is because OnCreateTable adds to
    // the overall response time of the 'CREATE TABLE' query. If OnCreateTable take too long becausethe refresh
    // takse too long, the cql query can timeout. The refresh can take long when both of the followings are met:
    // 1. there are a huge number of tables
    // 2. the regex is quite complex
    // However, the chance of timeout is extremely low because:
    // 1. there are usually not too many tables
    // 2. most of the time the regex is simply an empty string. It is only during outage migation that we are going to
    // assign a non-empty value to the regex, and the value is very unlikely to be complex.
    @Override
    public void onCreateTable(TableMetadata table)
    {
        refresh();
    }

    @Override
    public void onDropTable(TableMetadata table, boolean dropData)
    {
        refresh();
    }

    @Override
    public void onDropKeyspace(KeyspaceMetadata keyspace, boolean dropData)
    {
        refresh();
    }

    public void refresh()
    {
        Set<String> keyspaceDotTables = getAllKeyspaceDotTables();
        for (TableFilter filter : filters)
        {
            filter.refresh(keyspaceDotTables);
        }
    }

    public String allFiltersToString()
    {
        StringBuilder sb = new StringBuilder();
        for (TableFilter filter : filters) {
            sb.append("{");
            sb.append(filter.toString());
            sb.append("} ");
        }
        return sb.toString();
    }
}
