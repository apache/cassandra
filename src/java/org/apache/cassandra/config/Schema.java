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

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import com.google.common.collect.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.tracing.Tracing;
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

    /* metadata map for faster table lookup */
    private final Map<String, KSMetaData> tables = new NonBlockingHashMap<String, KSMetaData>();

    /* Table objects, one per keyspace. Only one instance should ever exist for any given keyspace. */
    private final Map<String, Table> tableInstances = new NonBlockingHashMap<String, Table>();

    /* metadata map for faster ColumnFamily lookup */
    private final BiMap<Pair<String, String>, UUID> cfIdMap = HashBiMap.create();
    // mapping from old ColumnFamily Id (Integer) to a new version which is UUID
    private final BiMap<Integer, UUID> oldCfIdMap = HashBiMap.create();

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
     * Initialize empty schema object
     */
    public Schema()
    {}

    /**
     * Load up non-system tables
     *
     * @param tableDefs The non-system table definitions
     *
     * @return self to support chaining calls
     */
    public Schema load(Collection<KSMetaData> tableDefs)
    {
        for (KSMetaData def : tableDefs)
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

        setTableDefinition(keyspaceDef);

        return this;
    }

    /**
     * Get table instance by name
     *
     * @param tableName The name of the table
     *
     * @return Table object or null if table was not found
     */
    public Table getTableInstance(String tableName)
    {
        return tableInstances.get(tableName);
    }

    /**
     * Store given Table instance to the schema
     *
     * @param table The Table instance to store
     *
     * @throws IllegalArgumentException if Table is already stored
     */
    public void storeTableInstance(Table table)
    {
        if (tableInstances.containsKey(table.name))
            throw new IllegalArgumentException(String.format("Table %s was already initialized.", table.name));

        tableInstances.put(table.name, table);
    }

    /**
     * Remove table from schema
     *
     * @param tableName The name of the table to remove
     *
     * @return removed table instance or null if it wasn't found
     */
    public Table removeTableInstance(String tableName)
    {
        return tableInstances.remove(tableName);
    }

    /**
     * Remove table definition from system
     *
     * @param ksm The table definition to remove
     */
    public void clearTableDefinition(KSMetaData ksm)
    {
        tables.remove(ksm.name);
    }

    /**
     * Given a table name & column family name, get the column family
     * meta data. If the table name or column family name is not valid
     * this function returns null.
     *
     * @param tableName The table name
     * @param cfName The ColumnFamily name
     *
     * @return ColumnFamily Metadata object or null if it wasn't found
     */
    public CFMetaData getCFMetaData(String tableName, String cfName)
    {
        assert tableName != null;
        KSMetaData ksm = tables.get(tableName);
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
     * Get type of the ColumnFamily but it's keyspace/name
     *
     * @param ksName The keyspace name
     * @param cfName The ColumnFamily name
     *
     * @return The type of the ColumnFamily
     */
    public ColumnFamilyType getColumnFamilyType(String ksName, String cfName)
    {
        assert ksName != null && cfName != null;
        CFMetaData cfMetaData = getCFMetaData(ksName, cfName);
        return (cfMetaData == null) ? null : cfMetaData.cfType;
    }

    /**
     * Get column comparator for ColumnFamily but it's keyspace/name
     *
     * @param ksName The keyspace name
     * @param cfName The ColumnFamily name
     *
     * @return The comparator of the ColumnFamily
     */
    public AbstractType<?> getComparator(String ksName, String cfName)
    {
        assert ksName != null;
        CFMetaData cfmd = getCFMetaData(ksName, cfName);
        if (cfmd == null)
            throw new IllegalArgumentException("Unknown ColumnFamily " + cfName + " in keyspace " + ksName);
        return cfmd.comparator;
    }

    /**
     * Get subComparator of the ColumnFamily
     *
     * @param ksName The keyspace name
     * @param cfName The ColumnFamily name
     *
     * @return The subComparator of the ColumnFamily
     */
    public AbstractType<?> getSubComparator(String ksName, String cfName)
    {
        assert ksName != null;
        return getCFMetaData(ksName, cfName).subcolumnComparator;
    }

    /**
     * Get value validator for specific column
     *
     * @param ksName The keyspace name
     * @param cfName The ColumnFamily name
     * @param column The name of the column
     *
     * @return value validator specific to the column or default (per-cf) one
     */
    public AbstractType<?> getValueValidator(String ksName, String cfName, ByteBuffer column)
    {
        return getCFMetaData(ksName, cfName).getValueValidator(column);
    }

    /**
     * Get metadata about table by its name
     *
     * @param table The name of the table
     *
     * @return The table metadata or null if it wasn't found
     */
    public KSMetaData getKSMetaData(String table)
    {
        assert table != null;
        return tables.get(table);
    }

    /**
     * @return collection of the non-system tables
     */
    public List<String> getNonSystemTables()
    {
        ImmutableSet<String> system = ImmutableSet.of(Table.SYSTEM_KS, Tracing.TRACE_KS);
        return ImmutableList.copyOf(Sets.difference(tables.keySet(), system));
    }

    /**
     * Get metadata about table by its name
     *
     * @param table The name of the table
     *
     * @return The table metadata or null if it wasn't found
     */
    public KSMetaData getTableDefinition(String table)
    {
        return getKSMetaData(table);
    }

    /**
     * Get metadata about table inner ColumnFamilies
     *
     * @param tableName The name of the table
     *
     * @return metadata about ColumnFamilies the belong to the given table
     */
    public Map<String, CFMetaData> getTableMetaData(String tableName)
    {
        assert tableName != null;
        KSMetaData ksm = tables.get(tableName);
        assert ksm != null;
        return ksm.cfMetaData();
    }

    /**
     * @return collection of the all table names registered in the system (system and non-system)
     */
    public Set<String> getTables()
    {
        return tables.keySet();
    }

    /**
     * @return collection of the metadata about all tables registered in the system (system and non-system)
     */
    public Collection<KSMetaData> getTableDefinitions()
    {
        return tables.values();
    }

    /**
     * Update (or insert) new table definition
     *
     * @param ksm The metadata about table
     */
    public void setTableDefinition(KSMetaData ksm)
    {
        assert ksm != null;
        tables.put(ksm.name, ksm);
    }

    /* ColumnFamily query/control methods */

    public void addOldCfIdMapping(Integer oldId, UUID newId)
    {
        if (oldId == null)
            return;

        oldCfIdMap.put(oldId, newId);
    }

    public UUID convertOldCfId(Integer oldCfId) throws UnknownColumnFamilyException
    {
        UUID cfId = oldCfIdMap.get(oldCfId);

        if (cfId == null)
            throw new UnknownColumnFamilyException("ColumnFamily identified by old " + oldCfId + " was not found.", null);

        return cfId;
    }

    public Integer convertNewCfId(UUID newCfId)
    {
        return oldCfIdMap.containsValue(newCfId) ? oldCfIdMap.inverse().get(newCfId) : null;
    }

    /**
     * @param cfId The identifier of the ColumnFamily to lookup
     * @return The (ksname,cfname) pair for the given id, or null if it has been dropped.
     */
    public Pair<String,String> getCF(UUID cfId)
    {
        return cfIdMap.inverse().get(cfId);
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
        return cfIdMap.get(new Pair<String, String>(ksName, cfName));
    }

    /**
     * Load individual ColumnFamily Definition to the schema
     * (to make ColumnFamily lookup faster)
     *
     * @param cfm The ColumnFamily definition to load
     *
     * @throws ConfigurationException if ColumnFamily was already loaded
     */
    public void load(CFMetaData cfm)
    {
        Pair<String, String> key = new Pair<String, String>(cfm.ksName, cfm.cfName);

        if (cfIdMap.containsKey(key))
            throw new RuntimeException(String.format("Attempting to load already loaded column family %s.%s", cfm.ksName, cfm.cfName));

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
        cfIdMap.remove(new Pair<String, String>(cfm.ksName, cfm.cfName));
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
     * Read schema from system table and calculate MD5 digest of every row, resulting digest
     * will be converted into UUID which would act as content-based version of the schema.
     */
    public void updateVersion()
    {
        try
        {
            MessageDigest versionDigest = MessageDigest.getInstance("MD5");

            for (Row row : SystemTable.serializedSchema())
            {
                if (row.cf == null || (row.cf.isMarkedForDelete() && row.cf.isEmpty()))
                    continue;

                row.cf.updateDigest(versionDigest);
            }

            version = UUID.nameUUIDFromBytes(versionDigest.digest());
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
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
        for (String table : getNonSystemTables())
        {
            KSMetaData ksm = getTableDefinition(table);
            for (CFMetaData cfm : ksm.cfMetaData().values())
                purge(cfm);
            clearTableDefinition(ksm);
        }

        updateVersionAndAnnounce();
    }
}
