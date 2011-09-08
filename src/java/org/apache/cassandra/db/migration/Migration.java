/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.migration;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.net.MessagingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.io.SerDeUtils;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;

/**
 * A migration represents a single metadata mutation (cf dropped, added, etc.).  Migrations can be applied locally, or
 * serialized and sent to another machine where it can be applied there. Each migration has a version represented by
 * a TimeUUID that can be used to look up both the Migration itself (see getLocalMigrations) as well as a serialization
 * of the Keyspace definition that was modified.
 * 
 * There are three parts to a migration (think of it as a schema update):
 * 1. data is written to the schema cf.
 * 2. the migration is serialized to the migrations cf.
 * 3. updated models are applied to the cassandra instance.
 * 
 * Since steps 1, 2 and 3 are not committed atomically, care should be taken to ensure that a node/cluster is reasonably
 * quiescent with regard to the keyspace or columnfamily whose schema is being modified.
 * 
 * Each class that extends Migration is required to implement a no arg constructor, which will be used to inflate the
 * object from it's serialized form.
 */
public abstract class Migration
{
    protected static final Logger logger = LoggerFactory.getLogger(Migration.class);
    
    public static final String NAME_VALIDATOR_REGEX = "\\w+";
    public static final String MIGRATIONS_CF = "Migrations";
    public static final String SCHEMA_CF = "Schema";
    public static final ByteBuffer MIGRATIONS_KEY = ByteBufferUtil.bytes("Migrations Key");
    public static final ByteBuffer LAST_MIGRATION_KEY = ByteBufferUtil.bytes("Last Migration");
    
    protected RowMutation rm;
    protected UUID newVersion;
    protected UUID lastVersion;

    // the migration in column form, used when announcing to others
    private IColumn column;

    // cast of the schema at the time when migration was initialized
    protected final Schema schema = Schema.instance;

    /** Subclasses must have a matching constructor */
    protected Migration() { }

    Migration(UUID newVersion, UUID lastVersion)
    {
        this();
        this.newVersion = newVersion;
        this.lastVersion = lastVersion;
    }

    /** apply changes */
    public final void apply() throws IOException, ConfigurationException
    {
        // ensure migration is serial. don't apply unless the previous version matches.
        if (!schema.getVersion().equals(lastVersion))
            throw new ConfigurationException("Previous version mismatch. cannot apply.");
        if (newVersion.timestamp() <= lastVersion.timestamp())
            throw new ConfigurationException("New version timestamp is not newer than the current version timestamp.");
        // write to schema
        assert rm != null;
        if (!StorageService.instance.isClientMode())
        {
            rm.apply();

            long now = System.currentTimeMillis();
            ByteBuffer buf = serialize();
            RowMutation migration = new RowMutation(Table.SYSTEM_TABLE, MIGRATIONS_KEY);
            ColumnFamily cf = ColumnFamily.create(Table.SYSTEM_TABLE, MIGRATIONS_CF);
            column = new Column(ByteBuffer.wrap(UUIDGen.decompose(newVersion)), buf, now);
            cf.addColumn(column);
            migration.add(cf);
            migration.apply();
            
            // note that we're storing this in the system table, which is not replicated
            logger.info("Applying migration {} {}", newVersion.toString(), toString());
            migration = new RowMutation(Table.SYSTEM_TABLE, LAST_MIGRATION_KEY);
            migration.add(new QueryPath(SCHEMA_CF, null, LAST_MIGRATION_KEY), ByteBuffer.wrap(UUIDGen.decompose(newVersion)), now);
            migration.apply();

            // if we fail here, there will be schema changes in the CL that will get replayed *AFTER* the schema is loaded.
            // CassandraDaemon checks for this condition (the stored version will be greater than the loaded version)
            // and calls MigrationManager.applyMigrations(loaded version, stored version).
        
            // flush changes out of memtables so we don't need to rely on the commit log.
            ColumnFamilyStore[] schemaStores = new ColumnFamilyStore[] {
                Table.open(Table.SYSTEM_TABLE).getColumnFamilyStore(Migration.MIGRATIONS_CF),
                Table.open(Table.SYSTEM_TABLE).getColumnFamilyStore(Migration.SCHEMA_CF)
            };
            List<Future<?>> flushes = new ArrayList<Future<?>>();
            for (ColumnFamilyStore cfs : schemaStores)
                flushes.add(cfs.forceFlush());
            for (Future<?> f : flushes)
            {
                if (f == null)
                    // applying the migration triggered a flush independently
                    continue;
                try
                {
                    f.get();
                }
                catch (ExecutionException e)
                {
                    throw new IOException(e);
                }
                catch (InterruptedException e)
                {
                    throw new IOException(e);
                }
            }
        }
        
        applyModels(); 
    }

    /** send this migration immediately to existing nodes in the cluster.  apply() must be called first. */
    public final void announce()
    {
        assert !StorageService.instance.isClientMode();
        assert column != null;
        MigrationManager.announce(column);
        passiveAnnounce(); // keeps gossip in sync w/ what we just told everyone
    }

    public final void passiveAnnounce()
    {
        MigrationManager.passiveAnnounce(newVersion);
    }

    public static UUID getLastMigrationId()
    {
        DecoratedKey<?> dkey = StorageService.getPartitioner().decorateKey(LAST_MIGRATION_KEY);
        Table defs = Table.open(Table.SYSTEM_TABLE);
        ColumnFamilyStore cfStore = defs.getColumnFamilyStore(SCHEMA_CF);
        QueryFilter filter = QueryFilter.getNamesFilter(dkey, new QueryPath(SCHEMA_CF), LAST_MIGRATION_KEY);
        ColumnFamily cf = cfStore.getColumnFamily(filter);
        if (cf == null || cf.getColumnNames().size() == 0)
            return null;
        else
            return UUIDGen.getUUID(cf.getColumn(LAST_MIGRATION_KEY).value());
    }
    
    /** keep in mind that applyLive might happen on another machine */
    abstract void applyModels() throws IOException;
    
    /** Deflate this Migration into an Avro object. */
    public abstract void subdeflate(org.apache.cassandra.db.migration.avro.Migration mi);
    
    /** Inflate this Migration from an Avro object: called after the required no-arg constructor. */
    public abstract void subinflate(org.apache.cassandra.db.migration.avro.Migration mi);
    
    public UUID getVersion()
    {
        return newVersion;
    }

    /**
     * Definitions are serialized as a row with a UUID key, with a magical column named DEFINITION_SCHEMA_COLUMN_NAME
     * (containing the Avro Schema) and a column per keyspace. Each keyspace column contains a avro.KsDef object
     * encoded with the Avro schema.
     */
    final RowMutation makeDefinitionMutation(KSMetaData add, KSMetaData remove, UUID versionId) throws IOException
    {
        // collect all keyspaces, while removing 'remove' and adding 'add'
        List<KSMetaData> ksms = new ArrayList<KSMetaData>();
        for (String tableName : schema.getNonSystemTables())
        {
            if (remove != null && remove.name.equals(tableName) || add != null && add.name.equals(tableName))
                continue;
            ksms.add(schema.getTableDefinition(tableName));
        }
        if (add != null)
            ksms.add(add);

        // wrap in mutation
        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, toUTF8Bytes(versionId));
        long now = System.currentTimeMillis();
        // add a column for each keyspace
        for (KSMetaData ksm : ksms)
            rm.add(new QueryPath(SCHEMA_CF, null, ByteBufferUtil.bytes(ksm.name)), SerDeUtils.serialize(ksm.toAvro()), now);
        // add the schema
        rm.add(new QueryPath(SCHEMA_CF,
                             null,
                             DefsTable.DEFINITION_SCHEMA_COLUMN_NAME),
                             ByteBufferUtil.bytes(org.apache.cassandra.db.migration.avro.KsDef.SCHEMA$.toString()),
                             now);
        return rm;
    }
        
    public ByteBuffer serialize() throws IOException
    {
        // super deflate
        org.apache.cassandra.db.migration.avro.Migration mi = new org.apache.cassandra.db.migration.avro.Migration();
        mi.old_version = new org.apache.cassandra.utils.avro.UUID();
        mi.old_version.bytes(UUIDGen.decompose(lastVersion));
        mi.new_version = new org.apache.cassandra.utils.avro.UUID();
        mi.new_version.bytes(UUIDGen.decompose(newVersion));
        mi.classname = new org.apache.avro.util.Utf8(this.getClass().getName());
        // TODO: Avro RowMutation serialization?
        DataOutputBuffer dob = new DataOutputBuffer();
        try
        {
            RowMutation.serializer().serialize(rm, dob, MessagingService.version_);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        mi.row_mutation = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

        // sub deflate
        this.subdeflate(mi);

        // serialize
        return SerDeUtils.serializeWithSchema(mi);
    }

    public static Migration deserialize(ByteBuffer bytes, int version) throws IOException
    {
        // deserialize
        org.apache.cassandra.db.migration.avro.Migration mi = SerDeUtils.deserializeWithSchema(bytes, new org.apache.cassandra.db.migration.avro.Migration());

        // create an instance of the migration subclass
        Migration migration;
        try
        {
            Class<?> migrationClass = Class.forName(mi.classname.toString());
            Constructor<?> migrationConstructor = migrationClass.getDeclaredConstructor();
            migrationConstructor.setAccessible(true);
            migration = (Migration)migrationConstructor.newInstance();
        }
        catch (Exception e)
        {
            throw new RuntimeException("Invalid migration class: " + mi.classname.toString(), e);
        }
        
        // super inflate
        migration.lastVersion = UUIDGen.getUUID(ByteBuffer.wrap(mi.old_version.bytes()));
        migration.newVersion = UUIDGen.getUUID(ByteBuffer.wrap(mi.new_version.bytes()));
        try
        {
            migration.rm = RowMutation.serializer().deserialize(SerDeUtils.createDataInputStream(mi.row_mutation), version);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        // sub inflate
        migration.subinflate(mi);
        return migration;
    }
    
    /** load serialized migrations. */
    public static Collection<IColumn> getLocalMigrations(UUID start, UUID end)
    {
        DecoratedKey<?> dkey = StorageService.getPartitioner().decorateKey(MIGRATIONS_KEY);
        Table defs = Table.open(Table.SYSTEM_TABLE);
        ColumnFamilyStore cfStore = defs.getColumnFamilyStore(Migration.MIGRATIONS_CF);
        QueryFilter filter = QueryFilter.getSliceFilter(dkey,
                                                        new QueryPath(MIGRATIONS_CF),
                                                        ByteBuffer.wrap(UUIDGen.decompose(start)),
                                                        ByteBuffer.wrap(UUIDGen.decompose(end)),
                                                        false,
                                                        100);
        ColumnFamily cf = cfStore.getColumnFamily(filter);
        return cf.getSortedColumns();
    }
    
    public static ByteBuffer toUTF8Bytes(UUID version)
    {
        return ByteBufferUtil.bytes(version.toString());
    }
    
    public static boolean isLegalName(String s)
    {
        return s.matches(Migration.NAME_VALIDATOR_REGEX);
    }
}
