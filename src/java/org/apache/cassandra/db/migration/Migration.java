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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CompactionManager;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.utils.UUIDGen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

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
 * Each class that extends Migration is required to implement a constructor that takes a DataInputStream as its only
 * argument.  Also, each implementation must take care to ensure that its serialization can be deserialized.  For 
 * example, it is required that the class name be serialized first always.
 */
public abstract class Migration
{
    private static final Logger logger = LoggerFactory.getLogger(Migration.class);
    
    public static final String MIGRATIONS_CF = "Migrations";
    public static final String SCHEMA_CF = "Schema";
    public static final String MIGRATIONS_KEY = "Migrations Key";
    public static final String LAST_MIGRATION_KEY = "Last Migration";
    
    protected RowMutation rm;
    protected final UUID newVersion;
    protected UUID lastVersion;
    
    Migration(UUID newVersion, UUID lastVersion)
    {
        this.newVersion = newVersion;
        this.lastVersion = lastVersion;
    }
    
    /** apply changes */
    public final void apply() throws IOException
    {
        // ensure migration is serial. don't apply unless the previous version matches.
        if (!DatabaseDescriptor.getDefsVersion().equals(lastVersion))
            throw new IOException("Previous version mismatch. cannot apply.");
        // write to schema
        assert rm != null;
        rm.apply();
        
        // write migration.
        long now = System.currentTimeMillis();
        byte[] buf = getBytes();
        RowMutation migration = new RowMutation(Table.DEFINITIONS, MIGRATIONS_KEY);
        migration.add(new QueryPath(MIGRATIONS_CF, null, UUIDGen.decompose(newVersion)), buf, now);
        migration.apply();
        
        // note that we storing this in the system table, which is not replicated, instead of the definitions table, which is.
        logger.debug("Applying migration " + newVersion.toString());
        migration = new RowMutation(Table.DEFINITIONS, LAST_MIGRATION_KEY);
        migration.add(new QueryPath(SCHEMA_CF, null, LAST_MIGRATION_KEY.getBytes()), UUIDGen.decompose(newVersion), now);
        migration.apply();
        
        // flush changes out of memtables so we don't need to rely on the commit log.
        for (Future f : Table.open(Table.DEFINITIONS).flush())
        {
            try
            {
                f.get();
            }
            catch (InterruptedException e)
            {
                throw new IOException(e);
            }
            catch (ExecutionException e)
            {
                throw new IOException(e);
            }
        }
        
        applyModels(); 
    }
    
    public final void announce()
    {
        // immediate notification for esiting nodes.
        MigrationManager.announce(newVersion, Gossiper.instance.getLiveMembers());
        // this is for notifying nodes as they arrive in the cluster.
        Gossiper.instance.addLocalApplicationState(MigrationManager.MIGRATION_STATE, new ApplicationState(newVersion.toString()));
    }
    
    public static UUID getLastMigrationId()
    {
        Table defs = Table.open(Table.DEFINITIONS);
        ColumnFamilyStore cfStore = defs.getColumnFamilyStore(SCHEMA_CF);
        QueryFilter filter = QueryFilter.getNamesFilter(LAST_MIGRATION_KEY, new QueryPath(SCHEMA_CF), LAST_MIGRATION_KEY.getBytes());
        ColumnFamily cf = cfStore.getColumnFamily(filter);
        if (cf.getColumnNames().size() == 0)
            return null;
        else
            return UUIDGen.makeType1UUID(cf.getColumn(LAST_MIGRATION_KEY.getBytes()).value());
    }
    
    /** keep in mind that applyLive might happen on another machine */
    abstract void applyModels() throws IOException;
    
    /** serialize migration */
    public abstract ICompactSerializer getSerializer();
    
    private byte[] getBytes() throws IOException
    {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bout);
        dout.writeUTF(getClass().getName());
        getSerializer().serialize(this, dout);
        dout.close();
        return bout.toByteArray();
    }
    
    public UUID getVersion()
    {
        return newVersion;
    }
    
    static RowMutation makeDefinitionMutation(KSMetaData add, KSMetaData remove, UUID versionId) throws IOException
    {
        final long now = System.currentTimeMillis();
        RowMutation rm = new RowMutation(Table.DEFINITIONS, versionId.toString());
        if (remove != null)
            rm.delete(new QueryPath(SCHEMA_CF, null, remove.name.getBytes()), System.currentTimeMillis());
        if (add != null)
            rm.add(new QueryPath(SCHEMA_CF, null, add.name.getBytes()), KSMetaData.serialize(add), now);
        return rm;
    }
    
    static void cleanupDeadFiles(boolean wait)
    {
        Future cleanup = CompactionManager.instance.submitGraveyardCleanup();
        if (wait)
        {
            // notify the compaction manager that it needs to clean up the dropped cf files.
            try
            {
                cleanup.get();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
            catch (ExecutionException e)
            {
                throw new RuntimeException(e);
            }
        } 
    }
    
    /** deserialize any Migration. */
    public static Migration deserialize(InputStream in) throws IOException
    {
        DataInputStream din = new DataInputStream(in);
        String className = din.readUTF();
        try
        {
            Class migrationClass = Class.forName(className);
            Field serializerField = migrationClass.getDeclaredField("serializer");
            serializerField.setAccessible(true);
            ICompactSerializer serializer = (ICompactSerializer)serializerField.get(migrationClass);
            return (Migration)serializer.deserialize(din);
        }
        catch (Exception e)
        {
            throw new IOException(e);
        }
    }
    
    /** load serialized migrations. */
    public static Collection<IColumn> getLocalMigrations(UUID start, UUID end)
    {
        Table defs = Table.open(Table.DEFINITIONS);
        ColumnFamilyStore cfStore = defs.getColumnFamilyStore(Migration.MIGRATIONS_CF);
        QueryFilter filter = QueryFilter.getSliceFilter(Migration.MIGRATIONS_KEY, new QueryPath(MIGRATIONS_CF), UUIDGen.decompose(start), UUIDGen.decompose(end), null, false, 1000);
        ColumnFamily cf = cfStore.getColumnFamily(filter);
        return cf.getSortedColumns();
    }
    
}
