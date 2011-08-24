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

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.avro.Schema;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.migration.Migration;
import org.apache.cassandra.io.SerDeUtils;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;

public class DefsTable
{
    // column name for the schema storing serialized keyspace definitions
    // NB: must be an invalid keyspace name
    public static final ByteBuffer DEFINITION_SCHEMA_COLUMN_NAME = ByteBufferUtil.bytes("Avro/Schema");

    /** dumps current keyspace definitions to storage */
    public static synchronized void dumpToStorage(UUID version) throws IOException
    {
        final ByteBuffer versionKey = Migration.toUTF8Bytes(version);

        // build a list of keyspaces
        Collection<String> ksnames = org.apache.cassandra.config.Schema.instance.getNonSystemTables();

        // persist keyspaces under new version
        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, versionKey);
        long now = System.currentTimeMillis();
        for (String ksname : ksnames)
        {
            KSMetaData ksm = org.apache.cassandra.config.Schema.instance.getTableDefinition(ksname);
            rm.add(new QueryPath(Migration.SCHEMA_CF, null, ByteBufferUtil.bytes(ksm.name)), SerDeUtils.serialize(ksm.toAvro()), now);
        }
        // add the schema
        rm.add(new QueryPath(Migration.SCHEMA_CF,
                             null,
                             DEFINITION_SCHEMA_COLUMN_NAME),
                             ByteBufferUtil.bytes(org.apache.cassandra.db.migration.avro.KsDef.SCHEMA$.toString()),
                             now);
        rm.apply();

        // apply new version
        rm = new RowMutation(Table.SYSTEM_TABLE, Migration.LAST_MIGRATION_KEY);
        rm.add(new QueryPath(Migration.SCHEMA_CF, null, Migration.LAST_MIGRATION_KEY),
               ByteBuffer.wrap(UUIDGen.decompose(version)),
               now);
        rm.apply();
    }

    /** loads a version of keyspace definitions from storage */
    public static synchronized Collection<KSMetaData> loadFromStorage(UUID version) throws IOException
    {
        DecoratedKey vkey = StorageService.getPartitioner().decorateKey(Migration.toUTF8Bytes(version));
        Table defs = Table.open(Table.SYSTEM_TABLE);
        ColumnFamilyStore cfStore = defs.getColumnFamilyStore(Migration.SCHEMA_CF);
        QueryFilter filter = QueryFilter.getIdentityFilter(vkey, new QueryPath(Migration.SCHEMA_CF));
        ColumnFamily cf = cfStore.getColumnFamily(filter);
        IColumn avroschema = cf.getColumn(DEFINITION_SCHEMA_COLUMN_NAME);
        if (avroschema == null)
            // TODO: more polite way to handle this?
            throw new RuntimeException("Cannot read system table! Are you upgrading a pre-release version?");

        ByteBuffer value = avroschema.value();
        Schema schema = Schema.parse(ByteBufferUtil.string(value));

        // deserialize keyspaces using schema
        Collection<KSMetaData> keyspaces = new ArrayList<KSMetaData>();
        for (IColumn column : cf.getSortedColumns())
        {
            if (column.name().equals(DEFINITION_SCHEMA_COLUMN_NAME))
                continue;
            org.apache.cassandra.db.migration.avro.KsDef ks = SerDeUtils.deserialize(schema, column.value(), new org.apache.cassandra.db.migration.avro.KsDef());
            keyspaces.add(KSMetaData.fromAvro(ks));
        }
        return keyspaces;
    }
    
    /** gets all the files that belong to a given column family. */
    public static Set<File> getFiles(String table, final String cf)
    {
        Set<File> found = new HashSet<File>();
        for (String path : DatabaseDescriptor.getAllDataFileLocationsForTable(table))
        {
            File[] dbFiles = new File(path).listFiles(new FileFilter()
            {
                public boolean accept(File pathname)
                {
                    return pathname.getName().startsWith(cf + "-") && pathname.getName().endsWith(".db") && pathname.exists();
                }
            });
            found.addAll(Arrays.asList(dbFiles));
        }
        return found;
    }
}
