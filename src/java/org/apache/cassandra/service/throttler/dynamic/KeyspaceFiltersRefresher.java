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

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class KeyspaceFiltersRefresher implements SchemaChangeListener
{
    private List<KeyspaceFilter> filters = new LinkedList<>();

    public void addFilter(KeyspaceFilter filter)
    {
        filters.add(filter);
    }

    public void registerSchemaChangeListener()
    {
        Schema.instance.registerListener(this);
    }

    public void onCreateKeyspace(KeyspaceMetadata keyspace)
    {
        refresh();
    }

    public void onDropKeyspace(KeyspaceMetadata keyspace, boolean dropData)
    {
        refresh();
    }

    public void refresh()
    {
        Set<String> keyspaces = Schema.instance.getKeyspaces();
        for (KeyspaceFilter filter : filters)
        {
            filter.refresh(keyspaces);
        }
    }

    public String allFiltersToString()
    {
        StringBuilder sb = new StringBuilder();
        for (KeyspaceFilter filter : filters) {
            sb.append("{");
            sb.append(filter.toString());
            sb.append("} ");
        }
        return sb.toString();
    }
}
