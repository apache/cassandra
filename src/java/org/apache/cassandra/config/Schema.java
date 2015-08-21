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
package org.apache.cassandra.config;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.functions.*;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.utils.ConcurrentBiMap;
import org.apache.cassandra.utils.Pair;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

public class Schema
{
    private static final Logger logger = LoggerFactory.getLogger(Schema.class);

    public static final Schema instance = new Schema();

    /* system keyspace names (the ones with LocalStrategy replication strategy) */
    public static final Set<String> SYSTEM_KEYSPACE_NAMES = ImmutableSet.of(SystemKeyspace.NAME, SchemaKeyspace.NAME);

    /**
     * longest permissible KS or CF name.  Our main concern is that filename not be more than 255 characters;
     * the filename will contain both the KS and CF names. Since non-schema-name components only take up
     * ~64 characters, we could allow longer names than this, but on Windows, the entire path should be not greater than
     * 255 characters, so a lower limit here helps avoid problems.  See CASSANDRA-4110.
     */
    public static final int NAME_LENGTH = 48;

    /* metadata map for faster keyspace lookup */
    private final Map<String, KeyspaceMetadata> keyspaces = new NonBlockingHashMap<>();

    /* Keyspace objects, one per keyspace. Only one instance should ever exist for any given keyspace. */
    private final Map<String, Keyspace> keyspaceInstances = new NonBlockingHashMap<>();

    /* metadata map for faster ColumnFamily lookup */
    private final ConcurrentBiMap<Pair<String, String>, UUID> cfIdMap = new ConcurrentBiMap<>();

    private volatile UUID version;

    // 59adb24e-f3cd-3e02-97f0-5b395827453f
    public static final UUID emptyVersion;

    static
    {
        try
        {
            emptyVersion = UUID.nameUUIDFromBytes(MessageDigest.getInstance("MD5").digest());
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new AssertionError();
        }
    }

    /**
     * Initialize empty schema object and load the hardcoded system tables
     */
    public Schema()
    {
        load(SchemaKeyspace.metadata());
        load(SystemKeyspace.metadata());
    }

    /**
     * @return whether or not the keyspace is a really system one (w/ LocalStrategy, unmodifiable, hardcoded)
     */
    public static boolean isSystemKeyspace(String keyspaceName)
    {
        return SYSTEM_KEYSPACE_NAMES.contains(keyspaceName.toLowerCase());
    }

    /**
     * load keyspace (keyspace) definitions, but do not initialize the keyspace instances.
     * Schema version may be updated as the result.
     */
    public Schema loadFromDisk()
    {
        return loadFromDisk(true);
    }

    /**
     * Load schema definitions from disk.
     *
     * @param updateVersion true if schema version needs to be updated
     */
    public Schema loadFromDisk(boolean updateVersion)
    {
        load(SchemaKeyspace.readSchemaFromSystemTables());
        if (updateVersion)
            updateVersion();
        return this;
    }

    /**
     * Load up non-system keyspaces
     *
     * @param keyspaceDefs The non-system keyspace definitions
     *
     * @return self to support chaining calls
     */
    public Schema load(Collection<KeyspaceMetadata> keyspaceDefs)
    {
        keyspaceDefs.forEach(this::load);
        return this;
    }

    /**
     * Load specific keyspace into Schema
     *
     * @param keyspaceDef The keyspace to load up
     *
     * @return self to support chaining calls
     */
    public Schema load(KeyspaceMetadata keyspaceDef)
    {
        keyspaceDef.tables.forEach(this::load);
        setKeyspaceMetadata(keyspaceDef);
        return this;
    }

    /**
     * Get keyspace instance by name
     *
     * @param keyspaceName The name of the keyspace
     *
     * @return Keyspace object or null if keyspace was not found
     */
    public Keyspace getKeyspaceInstance(String keyspaceName)
    {
        return keyspaceInstances.get(keyspaceName);
    }

    public ColumnFamilyStore getColumnFamilyStoreInstance(UUID cfId)
    {
        Pair<String, String> pair = cfIdMap.inverse().get(cfId);
        if (pair == null)
            return null;
        Keyspace instance = getKeyspaceInstance(pair.left);
        if (instance == null)
            return null;
        return instance.getColumnFamilyStore(cfId);
    }

    /**
     * Store given Keyspace instance to the schema
     *
     * @param keyspace The Keyspace instance to store
     *
     * @throws IllegalArgumentException if Keyspace is already stored
     */
    public void storeKeyspaceInstance(Keyspace keyspace)
    {
        if (keyspaceInstances.containsKey(keyspace.getName()))
            throw new IllegalArgumentException(String.format("Keyspace %s was already initialized.", keyspace.getName()));

        keyspaceInstances.put(keyspace.getName(), keyspace);
    }

    /**
     * Remove keyspace from schema
     *
     * @param keyspaceName The name of the keyspace to remove
     *
     * @return removed keyspace instance or null if it wasn't found
     */
    public Keyspace removeKeyspaceInstance(String keyspaceName)
    {
        return keyspaceInstances.remove(keyspaceName);
    }

    /**
     * Remove keyspace definition from system
     *
     * @param ksm The keyspace definition to remove
     */
    public void clearKeyspaceMetadata(KeyspaceMetadata ksm)
    {
        keyspaces.remove(ksm.name);
    }

    /**
     * Given a keyspace name and column family name, get the column family
     * meta data. If the keyspace name or column family name is not valid
     * this function returns null.
     *
     * @param keyspaceName The keyspace name
     * @param cfName The ColumnFamily name
     *
     * @return ColumnFamily Metadata object or null if it wasn't found
     */
    public CFMetaData getCFMetaData(String keyspaceName, String cfName)
    {
        assert keyspaceName != null;
        KeyspaceMetadata ksm = keyspaces.get(keyspaceName);
        return (ksm == null) ? null : ksm.tables.getNullable(cfName);
    }

    /**
     * Get ColumnFamily metadata by its identifier
     *
     * @param cfId The ColumnFamily identifier
     *
     * @return metadata about ColumnFamily
     */
    public CFMetaData getCFMetaData(UUID cfId)
    {
        Pair<String,String> cf = getCF(cfId);
        return (cf == null) ? null : getCFMetaData(cf.left, cf.right);
    }

    public CFMetaData getCFMetaData(Descriptor descriptor)
    {
        return getCFMetaData(descriptor.ksname, descriptor.cfname);
    }

    /**
     * Get metadata about keyspace by its name
     *
     * @param keyspaceName The name of the keyspace
     *
     * @return The keyspace metadata or null if it wasn't found
     */
    public KeyspaceMetadata getKSMetaData(String keyspaceName)
    {
        assert keyspaceName != null;
        return keyspaces.get(keyspaceName);
    }

    /**
     * @return collection of the non-system keyspaces
     */
    public List<String> getNonSystemKeyspaces()
    {
        return ImmutableList.copyOf(Sets.difference(keyspaces.keySet(), SYSTEM_KEYSPACE_NAMES));
    }

    /**
     * Get metadata about keyspace inner ColumnFamilies
     *
     * @param keyspaceName The name of the keyspace
     *
     * @return metadata about ColumnFamilies the belong to the given keyspace
     */
    public Tables getTables(String keyspaceName)
    {
        assert keyspaceName != null;
        KeyspaceMetadata ksm = keyspaces.get(keyspaceName);
        assert ksm != null;
        return ksm.tables;
    }

    /**
     * @return collection of the all keyspace names registered in the system (system and non-system)
     */
    public Set<String> getKeyspaces()
    {
        return keyspaces.keySet();
    }

    /**
     * Update (or insert) new keyspace definition
     *
     * @param ksm The metadata about keyspace
     */
    public void setKeyspaceMetadata(KeyspaceMetadata ksm)
    {
        assert ksm != null;

        keyspaces.put(ksm.name, ksm);
        Keyspace keyspace = getKeyspaceInstance(ksm.name);
        if (keyspace != null)
            keyspace.setMetadata(ksm);
    }

    /* ColumnFamily query/control methods */

    /**
     * @param cfId The identifier of the ColumnFamily to lookup
     * @return The (ksname,cfname) pair for the given id, or null if it has been dropped.
     */
    public Pair<String,String> getCF(UUID cfId)
    {
        return cfIdMap.inverse().get(cfId);
    }

    /**
     * @param cfId The identifier of the ColumnFamily to lookup
     * @return true if the CF id is a known one, false otherwise.
     */
    public boolean hasCF(UUID cfId)
    {
        return cfIdMap.containsValue(cfId);
    }

    /**
     * Lookup keyspace/ColumnFamily identifier
     *
     * @param ksName The keyspace name
     * @param cfName The ColumnFamily name
     *
     * @return The id for the given (ksname,cfname) pair, or null if it has been dropped.
     */
    public UUID getId(String ksName, String cfName)
    {
        return cfIdMap.get(Pair.create(ksName, cfName));
    }

    /**
     * Load individual ColumnFamily Definition to the schema
     * (to make ColumnFamily lookup faster)
     *
     * @param cfm The ColumnFamily definition to load
     */
    public void load(CFMetaData cfm)
    {
        Pair<String, String> key = Pair.create(cfm.ksName, cfm.cfName);

        if (cfIdMap.containsKey(key))
            throw new RuntimeException(String.format("Attempting to load already loaded table %s.%s", cfm.ksName, cfm.cfName));

        logger.debug("Adding {} to cfIdMap", cfm);
        cfIdMap.put(key, cfm.cfId);
    }

    /**
     * Used for ColumnFamily data eviction out from the schema
     *
     * @param cfm The ColumnFamily Definition to evict
     */
    public void unload(CFMetaData cfm)
    {
        cfIdMap.remove(Pair.create(cfm.ksName, cfm.cfName));
    }

    /* Function helpers */

    /**
     * Get all function overloads with the specified name
     *
     * @param name fully qualified function name
     * @return an empty list if the keyspace or the function name are not found;
     *         a non-empty collection of {@link Function} otherwise
     */
    public Collection<Function> getFunctions(FunctionName name)
    {
        if (!name.hasKeyspace())
            throw new IllegalArgumentException(String.format("Function name must be fully quallified: got %s", name));

        KeyspaceMetadata ksm = getKSMetaData(name.keyspace);
        return ksm == null
             ? Collections.emptyList()
             : ksm.functions.get(name);
    }

    /**
     * Find the function with the specified name
     *
     * @param name fully qualified function name
     * @param argTypes function argument types
     * @return an empty {@link Optional} if the keyspace or the function name are not found;
     *         a non-empty optional of {@link Function} otherwise
     */
    public Optional<Function> findFunction(FunctionName name, List<AbstractType<?>> argTypes)
    {
        if (!name.hasKeyspace())
            throw new IllegalArgumentException(String.format("Function name must be fully quallified: got %s", name));

        KeyspaceMetadata ksm = getKSMetaData(name.keyspace);
        return ksm == null
             ? Optional.empty()
             : ksm.functions.find(name, argTypes);
    }

    /* Version control */

    /**
     * @return current schema version
     */
    public UUID getVersion()
    {
        return version;
    }

    /**
     * Read schema from system keyspace and calculate MD5 digest of every row, resulting digest
     * will be converted into UUID which would act as content-based version of the schema.
     */
    public void updateVersion()
    {
        version = SchemaKeyspace.calculateSchemaDigest();
        SystemKeyspace.updateSchemaVersion(version);
    }

    /*
     * Like updateVersion, but also announces via gossip
     */
    public void updateVersionAndAnnounce()
    {
        updateVersion();
        MigrationManager.passiveAnnounce(version);
    }

    /**
     * Clear all KS/CF metadata and reset version.
     */
    public synchronized void clear()
    {
        for (String keyspaceName : getNonSystemKeyspaces())
        {
            KeyspaceMetadata ksm = getKSMetaData(keyspaceName);
            ksm.tables.forEach(this::unload);
            clearKeyspaceMetadata(ksm);
        }

        updateVersionAndAnnounce();
    }

    public void addKeyspace(KeyspaceMetadata ksm)
    {
        assert getKSMetaData(ksm.name) == null;
        load(ksm);

        Keyspace.open(ksm.name);
        MigrationManager.instance.notifyCreateKeyspace(ksm);
    }

    public void updateKeyspace(String ksName, KeyspaceParams newParams)
    {
        KeyspaceMetadata ksm = update(ksName, ks -> ks.withSwapped(newParams));
        MigrationManager.instance.notifyUpdateKeyspace(ksm);
    }

    public void dropKeyspace(String ksName)
    {
        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(ksName);
        String snapshotName = Keyspace.getTimestampedSnapshotName(ksName);

        CompactionManager.instance.interruptCompactionFor(ksm.tables, true);

        Keyspace keyspace = Keyspace.open(ksm.name);

        // remove all cfs from the keyspace instance.
        List<UUID> droppedCfs = new ArrayList<>();
        for (CFMetaData cfm : ksm.tables)
        {
            ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfm.cfName);

            unload(cfm);

            if (DatabaseDescriptor.isAutoSnapshot())
                cfs.snapshot(snapshotName);
            Keyspace.open(ksm.name).dropCf(cfm.cfId);

            droppedCfs.add(cfm.cfId);
        }

        // remove the keyspace from the static instances.
        Keyspace.clear(ksm.name);
        clearKeyspaceMetadata(ksm);

        keyspace.writeOrder.awaitNewBarrier();

        // force a new segment in the CL
        CommitLog.instance.forceRecycleAllSegments(droppedCfs);

        MigrationManager.instance.notifyDropKeyspace(ksm);
    }

    public void addTable(CFMetaData cfm)
    {
        assert getCFMetaData(cfm.ksName, cfm.cfName) == null;

        update(cfm.ksName, ks ->
        {
            load(cfm);

            // make sure it's init-ed w/ the old definitions first,
            // since we're going to call initCf on the new one manually
            Keyspace.open(cfm.ksName);

            return ks.withSwapped(ks.tables.with(cfm));
        });

        Keyspace.open(cfm.ksName).initCf(cfm.cfId, cfm.cfName, true);
        MigrationManager.instance.notifyCreateColumnFamily(cfm);
    }

    public void updateTable(String ksName, String tableName)
    {
        CFMetaData cfm = getCFMetaData(ksName, tableName);
        assert cfm != null;
        boolean columnsDidChange = cfm.reload();

        Keyspace keyspace = Keyspace.open(cfm.ksName);
        keyspace.getColumnFamilyStore(cfm.cfName).reload();
        MigrationManager.instance.notifyUpdateColumnFamily(cfm, columnsDidChange);
    }

    public void dropTable(String ksName, String tableName)
    {
        KeyspaceMetadata oldKsm = getKSMetaData(ksName);
        assert oldKsm != null;
        ColumnFamilyStore cfs = Keyspace.open(ksName).getColumnFamilyStore(tableName);
        assert cfs != null;

        // make sure all the indexes are dropped, or else.
        cfs.indexManager.markAllIndexesRemoved();

        // reinitialize the keyspace.
        CFMetaData cfm = oldKsm.tables.get(tableName).get();
        KeyspaceMetadata newKsm = oldKsm.withSwapped(oldKsm.tables.without(tableName));

        unload(cfm);
        setKeyspaceMetadata(newKsm);

        CompactionManager.instance.interruptCompactionFor(Collections.singleton(cfm), true);

        if (DatabaseDescriptor.isAutoSnapshot())
            cfs.snapshot(Keyspace.getTimestampedSnapshotName(cfs.name));
        Keyspace.open(ksName).dropCf(cfm.cfId);
        MigrationManager.instance.notifyDropColumnFamily(cfm);

        CommitLog.instance.forceRecycleAllSegments(Collections.singleton(cfm.cfId));
    }

    public void addType(UserType ut)
    {
        update(ut.keyspace, ks -> ks.withSwapped(ks.types.with(ut)));
        MigrationManager.instance.notifyCreateUserType(ut);
    }

    public void updateType(UserType ut)
    {
        update(ut.keyspace, ks -> ks.withSwapped(ks.types.without(ut.name).with(ut)));
        MigrationManager.instance.notifyUpdateUserType(ut);
    }

    public void dropType(UserType ut)
    {
        update(ut.keyspace, ks -> ks.withSwapped(ks.types.without(ut.name)));
        MigrationManager.instance.notifyDropUserType(ut);
    }

    public void addFunction(UDFunction udf)
    {
        update(udf.name().keyspace, ks -> ks.withSwapped(ks.functions.with(udf)));
        MigrationManager.instance.notifyCreateFunction(udf);
    }

    public void updateFunction(UDFunction udf)
    {
        update(udf.name().keyspace, ks -> ks.withSwapped(ks.functions.without(udf.name(), udf.argTypes()).with(udf)));
        MigrationManager.instance.notifyUpdateFunction(udf);
    }

    public void dropFunction(UDFunction udf)
    {
        update(udf.name().keyspace, ks -> ks.withSwapped(ks.functions.without(udf.name(), udf.argTypes())));
        MigrationManager.instance.notifyDropFunction(udf);
    }

    public void addAggregate(UDAggregate uda)
    {
        update(uda.name().keyspace, ks -> ks.withSwapped(ks.functions.with(uda)));
        MigrationManager.instance.notifyCreateAggregate(uda);
    }

    public void updateAggregate(UDAggregate uda)
    {
        update(uda.name().keyspace, ks -> ks.withSwapped(ks.functions.without(uda.name(), uda.argTypes()).with(uda)));
        MigrationManager.instance.notifyUpdateAggregate(uda);
    }

    public void dropAggregate(UDAggregate uda)
    {
        update(uda.name().keyspace, ks -> ks.withSwapped(ks.functions.without(uda.name(), uda.argTypes())));
        MigrationManager.instance.notifyDropAggregate(uda);
    }

    private synchronized KeyspaceMetadata update(String keyspaceName, java.util.function.Function<KeyspaceMetadata, KeyspaceMetadata> transformation)
    {
        KeyspaceMetadata current = getKSMetaData(keyspaceName);
        if (current == null)
            throw new IllegalStateException(String.format("Keyspace %s doesn't exist", keyspaceName));

        KeyspaceMetadata transformed = transformation.apply(current);
        setKeyspaceMetadata(transformed);

        return transformed;
    }
}
