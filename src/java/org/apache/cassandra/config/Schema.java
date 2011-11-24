/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.config;

import java.io.IOError;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.migration.Migration;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.utils.Pair;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.cliffc.high_scale_lib.NonBlockingHashMap;

public class Schema
{
    private static final Logger logger = LoggerFactory.getLogger(Schema.class);

    public static final UUID INITIAL_VERSION = new UUID(4096, 0); // has type nibble set to 1, everything else to zero.

    public static final Schema instance = new Schema(INITIAL_VERSION);

    private static final int MIN_CF_ID = 1000;
    private final AtomicInteger cfIdGen = new AtomicInteger(MIN_CF_ID);

    /* metadata map for faster table lookup */
    private final Map<String, KSMetaData> tables = new NonBlockingHashMap<String, KSMetaData>();

    /* Table objects, one per keyspace. Only one instance should ever exist for any given keyspace. */
    private final Map<String, Table> tableInstances = new NonBlockingHashMap<String, Table>();

    /* metadata map for faster ColumnFamily lookup */
    private final BiMap<Pair<String, String>, Integer> cfIdMap = HashBiMap.create();

    private volatile UUID version;

    /**
     * Initialize empty schema object with given version
     * @param initialVersion The initial version of the schema
     */
    public Schema(UUID initialVersion)
    {
        version = initialVersion;
    }

    /**
     * Load up non-system tables and set schema version to the given value
     *
     * @param tableDefs The non-system table definitions
     * @param version The version of the schema
     *
     * @return self to support chaining calls
     */
    public Schema load(Collection<KSMetaData> tableDefs, UUID version)
    {
        for (KSMetaData def : tableDefs)
        {
            if (!Migration.isLegalName(def.name))
                throw new RuntimeException("invalid keyspace name: " + def.name);

            for (CFMetaData cfm : def.cfMetaData().values())
            {
                if (!Migration.isLegalName(cfm.cfName))
                    throw new RuntimeException("invalid column family name: " + cfm.cfName);

                try
                {
                    load(cfm);
                }
                catch (ConfigurationException ex)
                {
                    throw new IOError(ex);
                }
            }

            setTableDefinition(def, version);
        }

        setVersion(version);

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
     * Remove table definition from system and update schema version
     *
     * @param ksm The table definition to remove
     * @param newVersion New version of the system
     */
    public void clearTableDefinition(KSMetaData ksm, UUID newVersion)
    {
        tables.remove(ksm.name);
        version = newVersion;
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
    public CFMetaData getCFMetaData(Integer cfId)
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
    public AbstractType getComparator(String ksName, String cfName)
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
    public AbstractType getSubComparator(String ksName, String cfName)
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
    public AbstractType getValueValidator(String ksName, String cfName, ByteBuffer column)
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
        List<String> tablesList = new ArrayList<String>(tables.keySet());
        tablesList.remove(Table.SYSTEM_TABLE);
        return Collections.unmodifiableList(tablesList);
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
     * Update (or insert) new table definition and change schema version
     *
     * @param ksm The metadata about table
     * @param newVersion New schema version
     */
    public void setTableDefinition(KSMetaData ksm, UUID newVersion)
    {
        if (ksm != null)
            tables.put(ksm.name, ksm);
        version = newVersion;
    }

    /**
     * Add a new system table
     * @param systemTable The metadata describing new system table
     */
    public void addSystemTable(KSMetaData systemTable)
    {
        tables.put(systemTable.name, systemTable);
    }

    /* ColumnFamily query/control methods */

    /**
     * @param cfId The identifier of the ColumnFamily to lookup
     * @return The (ksname,cfname) pair for the given id, or null if it has been dropped.
     */
    public Pair<String,String> getCF(Integer cfId)
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
    public Integer getId(String ksName, String cfName)
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
    public void load(CFMetaData cfm) throws ConfigurationException
    {
        Pair<String, String> key = new Pair<String, String>(cfm.ksName, cfm.cfName);

        if (cfIdMap.containsKey(key))
            throw new ConfigurationException("Attempt to assign id to existing column family.");

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

    /**
     * This gets called after initialization to make sure that id generation happens properly.
     */
    public void fixCFMaxId()
    {
        // never set it to less than 1000. this ensures that we have enough system CFids for future use.
        cfIdGen.set(cfIdMap.size() == 0 ? MIN_CF_ID : Math.max(Collections.max(cfIdMap.values()) + 1, MIN_CF_ID));
    }

    /**
     * @return identifier for the new ColumnFamily (called primarily by CFMetaData constructor)
     */
    public int nextCFId()
    {
        return cfIdGen.getAndIncrement();
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
     * Set new version of the schema
     * @param newVersion New version of the schema
     */
    public void setVersion(UUID newVersion)
    {
        version = newVersion;
    }
}
