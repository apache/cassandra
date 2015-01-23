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
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.functions.Functions;
import org.apache.cassandra.cql3.functions.UDAggregate;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.schema.LegacySchemaTables;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.utils.ConcurrentBiMap;
import org.apache.cassandra.utils.Pair;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

public class Schema
{
    private static final Logger logger = LoggerFactory.getLogger(Schema.class);

    public static final Schema instance = new Schema();

    /**
     * longest permissible KS or CF name.  Our main concern is that filename not be more than 255 characters;
     * the filename will contain both the KS and CF names. Since non-schema-name components only take up
     * ~64 characters, we could allow longer names than this, but on Windows, the entire path should be not greater than
     * 255 characters, so a lower limit here helps avoid problems.  See CASSANDRA-4110.
     */
    public static final int NAME_LENGTH = 48;

    /* metadata map for faster keyspace lookup */
    private final Map<String, KSMetaData> keyspaces = new NonBlockingHashMap<>();

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
        load(SystemKeyspace.definition());
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
        load(LegacySchemaTables.readSchemaFromSystemTables());
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
    public Schema load(Collection<KSMetaData> keyspaceDefs)
    {
        for (KSMetaData def : keyspaceDefs)
            load(def);

        return this;
    }

    /**
     * Load specific keyspace into Schema
     *
     * @param keyspaceDef The keyspace to load up
     *
     * @return self to support chaining calls
     */
    public Schema load(KSMetaData keyspaceDef)
    {
        for (CFMetaData cfm : keyspaceDef.cfMetaData().values())
            load(cfm);

        setKeyspaceDefinition(keyspaceDef);

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
    public void clearKeyspaceDefinition(KSMetaData ksm)
    {
        keyspaces.remove(ksm.name);
    }

    /**
     * Given a keyspace name & column family name, get the column family
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
        KSMetaData ksm = keyspaces.get(keyspaceName);
        return (ksm == null) ? null : ksm.cfMetaData().get(cfName);
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
    public KSMetaData getKSMetaData(String keyspaceName)
    {
        assert keyspaceName != null;
        return keyspaces.get(keyspaceName);
    }

    /**
     * @return collection of the non-system keyspaces
     */
    public List<String> getNonSystemKeyspaces()
    {
        return ImmutableList.copyOf(Sets.difference(keyspaces.keySet(), Collections.singleton(SystemKeyspace.NAME)));
    }

    /**
     * Get metadata about keyspace inner ColumnFamilies
     *
     * @param keyspaceName The name of the keyspace
     *
     * @return metadata about ColumnFamilies the belong to the given keyspace
     */
    public Map<String, CFMetaData> getKeyspaceMetaData(String keyspaceName)
    {
        assert keyspaceName != null;
        KSMetaData ksm = keyspaces.get(keyspaceName);
        assert ksm != null;
        return ksm.cfMetaData();
    }

    /**
     * @return collection of the all keyspace names registered in the system (system and non-system)
     */
    public Set<String> getKeyspaces()
    {
        return keyspaces.keySet();
    }

    /**
     * @return collection of the metadata about all keyspaces registered in the system (system and non-system)
     */
    public Collection<KSMetaData> getKeyspaceDefinitions()
    {
        return keyspaces.values();
    }

    /**
     * Update (or insert) new keyspace definition
     *
     * @param ksm The metadata about keyspace
     */
    public void setKeyspaceDefinition(KSMetaData ksm)
    {
        assert ksm != null;
        keyspaces.put(ksm.name, ksm);
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
    public void purge(CFMetaData cfm)
    {
        cfIdMap.remove(Pair.create(cfm.ksName, cfm.cfName));
        cfm.markPurged();
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
        version = LegacySchemaTables.calculateSchemaDigest();
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
            KSMetaData ksm = getKSMetaData(keyspaceName);
            for (CFMetaData cfm : ksm.cfMetaData().values())
                purge(cfm);
            clearKeyspaceDefinition(ksm);
        }

        updateVersionAndAnnounce();
    }

    public void addKeyspace(KSMetaData ksm)
    {
        assert getKSMetaData(ksm.name) == null;
        load(ksm);

        Keyspace.open(ksm.name);
        MigrationManager.instance.notifyCreateKeyspace(ksm);
    }

    public void updateKeyspace(String ksName)
    {
        KSMetaData oldKsm = getKSMetaData(ksName);
        assert oldKsm != null;
        KSMetaData newKsm = LegacySchemaTables.createKeyspaceFromName(ksName).cloneWith(oldKsm.cfMetaData().values(), oldKsm.userTypes);

        setKeyspaceDefinition(newKsm);

        Keyspace.open(ksName).createReplicationStrategy(newKsm);
        MigrationManager.instance.notifyUpdateKeyspace(newKsm);
    }

    public void dropKeyspace(String ksName)
    {
        KSMetaData ksm = Schema.instance.getKSMetaData(ksName);
        String snapshotName = Keyspace.getTimestampedSnapshotName(ksName);

        CompactionManager.instance.interruptCompactionFor(ksm.cfMetaData().values(), true);

        Keyspace keyspace = Keyspace.open(ksm.name);

        // remove all cfs from the keyspace instance.
        List<UUID> droppedCfs = new ArrayList<>();
        for (CFMetaData cfm : ksm.cfMetaData().values())
        {
            ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfm.cfName);

            purge(cfm);

            if (DatabaseDescriptor.isAutoSnapshot())
                cfs.snapshot(snapshotName);
            Keyspace.open(ksm.name).dropCf(cfm.cfId);

            droppedCfs.add(cfm.cfId);
        }

        // remove the keyspace from the static instances.
        Keyspace.clear(ksm.name);
        clearKeyspaceDefinition(ksm);

        keyspace.writeOrder.awaitNewBarrier();

        // force a new segment in the CL
        CommitLog.instance.forceRecycleAllSegments(droppedCfs);

        MigrationManager.instance.notifyDropKeyspace(ksm);
    }

    public void addTable(CFMetaData cfm)
    {
        assert getCFMetaData(cfm.ksName, cfm.cfName) == null;
        KSMetaData ksm = getKSMetaData(cfm.ksName).cloneWithTableAdded(cfm);

        logger.info("Loading {}", cfm);

        load(cfm);

        // make sure it's init-ed w/ the old definitions first,
        // since we're going to call initCf on the new one manually
        Keyspace.open(cfm.ksName);

        setKeyspaceDefinition(ksm);
        Keyspace.open(ksm.name).initCf(cfm.cfId, cfm.cfName, true);
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
        KSMetaData ksm = getKSMetaData(ksName);
        assert ksm != null;
        ColumnFamilyStore cfs = Keyspace.open(ksName).getColumnFamilyStore(tableName);
        assert cfs != null;

        // reinitialize the keyspace.
        CFMetaData cfm = ksm.cfMetaData().get(tableName);

        purge(cfm);
        setKeyspaceDefinition(ksm.cloneWithTableRemoved(cfm));

        CompactionManager.instance.interruptCompactionFor(Arrays.asList(cfm), true);

        if (DatabaseDescriptor.isAutoSnapshot())
            cfs.snapshot(Keyspace.getTimestampedSnapshotName(cfs.name));
        Keyspace.open(ksm.name).dropCf(cfm.cfId);
        MigrationManager.instance.notifyDropColumnFamily(cfm);

        CommitLog.instance.forceRecycleAllSegments(Collections.singleton(cfm.cfId));
    }

    public void addType(UserType ut)
    {
        KSMetaData ksm = getKSMetaData(ut.keyspace);
        assert ksm != null;

        logger.info("Loading {}", ut);

        ksm.userTypes.addType(ut);

        MigrationManager.instance.notifyCreateUserType(ut);
    }

    public void updateType(UserType ut)
    {
        KSMetaData ksm = getKSMetaData(ut.keyspace);
        assert ksm != null;

        logger.info("Updating {}", ut);

        ksm.userTypes.addType(ut);

        MigrationManager.instance.notifyUpdateUserType(ut);
    }

    public void dropType(UserType ut)
    {
        KSMetaData ksm = getKSMetaData(ut.keyspace);
        assert ksm != null;

        ksm.userTypes.removeType(ut);

        MigrationManager.instance.notifyDropUserType(ut);
    }

    public void addFunction(UDFunction udf)
    {
        logger.info("Loading {}", udf);

        Functions.addFunction(udf);

        MigrationManager.instance.notifyCreateFunction(udf);
    }

    public void updateFunction(UDFunction udf)
    {
        logger.info("Updating {}", udf);

        Functions.replaceFunction(udf);

        MigrationManager.instance.notifyUpdateFunction(udf);
    }

    public void dropFunction(UDFunction udf)
    {
        logger.info("Drop {}", udf);

        // TODO: this is kind of broken as this remove all overloads of the function name
        Functions.removeFunction(udf.name(), udf.argTypes());

        MigrationManager.instance.notifyDropFunction(udf);
    }

    public void addAggregate(UDAggregate udf)
    {
        logger.info("Loading {}", udf);

        Functions.addFunction(udf);

        MigrationManager.instance.notifyCreateAggregate(udf);
    }

    public void updateAggregate(UDAggregate udf)
    {
        logger.info("Updating {}", udf);

        Functions.replaceFunction(udf);

        MigrationManager.instance.notifyUpdateAggregate(udf);
    }

    public void dropAggregate(UDAggregate udf)
    {
        logger.info("Drop {}", udf);

        // TODO: this is kind of broken as this remove all overloads of the function name
        Functions.removeFunction(udf.name(), udf.argTypes());

        MigrationManager.instance.notifyDropAggregate(udf);
    }
}
