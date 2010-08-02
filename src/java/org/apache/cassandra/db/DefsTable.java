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

import org.apache.avro.Schema;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.migration.Migration;
import org.apache.cassandra.io.SerDeUtils;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static com.google.common.base.Charsets.UTF_8;

public class DefsTable
{
    // column name for the schema storing serialized keyspace definitions
    // NB: must be an invalid keyspace name
    public static final byte[] DEFINITION_SCHEMA_COLUMN_NAME = "Avro/Schema".getBytes(UTF_8);

    /** dumps current keyspace definitions to storage */
    public static synchronized void dumpToStorage(UUID version) throws IOException
    {
        final byte[] versionKey = Migration.toUTF8Bytes(version);

        // build a list of keyspaces
        Collection<String> ksnames = DatabaseDescriptor.getNonSystemTables();

        // persist keyspaces under new version
        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, versionKey);
        TimestampClock now = new TimestampClock(System.currentTimeMillis());
        for (String ksname : ksnames)
        {
            KSMetaData ksm = DatabaseDescriptor.getTableDefinition(ksname);
            rm.add(new QueryPath(Migration.SCHEMA_CF, null, ksm.name.getBytes(UTF_8)), SerDeUtils.serialize(ksm.deflate()), now);
        }
        // add the schema
        rm.add(new QueryPath(Migration.SCHEMA_CF,
                             null,
                             DEFINITION_SCHEMA_COLUMN_NAME),
                             org.apache.cassandra.config.avro.KsDef.SCHEMA$.toString().getBytes(UTF_8),
                             now);
        rm.apply();

        // apply new version
        rm = new RowMutation(Table.SYSTEM_TABLE, Migration.LAST_MIGRATION_KEY);
        rm.add(new QueryPath(Migration.SCHEMA_CF, null, Migration.LAST_MIGRATION_KEY),
               UUIDGen.decompose(version),
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
        Schema schema = Schema.parse(new String(avroschema.value()));

        // deserialize keyspaces using schema
        Collection<KSMetaData> keyspaces = new ArrayList<KSMetaData>();
        for (IColumn column : cf.getSortedColumns())
        {
            if (Arrays.equals(column.name(), DEFINITION_SCHEMA_COLUMN_NAME))
                continue;
            org.apache.cassandra.config.avro.KsDef ks = SerDeUtils.<org.apache.cassandra.config.avro.KsDef>deserialize(schema, column.value());
            keyspaces.add(KSMetaData.inflate(ks));
        }
        return keyspaces;
    }
    
    /** gets all the files that belong to a given column family. */
    public static Collection<File> getFiles(String table, final String cf)
    {
        List<File> found = new ArrayList<File>();
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
