/**
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

package org.apache.cassandra.db;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.filter.SliceQueryFilter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

public class DefsTable
{
    public static final String MIGRATIONS_CF = "Migrations";
    public static final String SCHEMA_CF = "Schema";

    public static void dumpToStorage(UUID version) throws IOException
    {
        String versionKey = version.toString();
        long now = System.currentTimeMillis();
        RowMutation rm = new RowMutation(Table.DEFINITIONS, versionKey);
        for (String tableName : DatabaseDescriptor.getNonSystemTables())
        {
            KSMetaData ks = DatabaseDescriptor.getTableDefinition(tableName);
            rm.add(new QueryPath(SCHEMA_CF, null, ks.name.getBytes()), KSMetaData.serialize(ks), now);
        }
        rm.apply();
    }

    public static Collection<KSMetaData> loadFromStorage(UUID version) throws IOException
    {
        Table defs = Table.open(Table.DEFINITIONS);
        ColumnFamilyStore cfStore = defs.getColumnFamilyStore(SCHEMA_CF);
        SliceQueryFilter filter = new SliceQueryFilter(version.toString(), new QueryPath(SCHEMA_CF), "".getBytes(), "".getBytes(), false, 1024);
        ColumnFamily cf = cfStore.getColumnFamily(filter);
        Collection<KSMetaData> tables = new ArrayList<KSMetaData>();
        for (IColumn col : cf.getSortedColumns())
        {
            String ksName = new String(col.name());
            KSMetaData ks = KSMetaData.deserialize(new ByteArrayInputStream(col.value()));
            tables.add(ks);
        }
        return tables;
    }
}
