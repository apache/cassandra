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
package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.cassandra.config.*;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.migration.Migration;
import org.apache.cassandra.db.migration.MigrationHelper;
import org.apache.cassandra.db.migration.avro.KsDef;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * SCHEMA_{KEYSPACES, COLUMNFAMILIES, COLUMNS}_CF are used to store Keyspace/ColumnFamily attributes to make schema
 * load/distribution easy, it replaces old mechanism when local migrations where serialized, stored in system.Migrations
 * and used for schema distribution.
 *
 * SCHEMA_KEYSPACES_CF layout:
 *
 * <key (AsciiType)>
 *   ascii => json_serialized_value
 *   ...
 * </key>
 *
 * Where <key> is a name of keyspace e.g. "ks".
 *
 * SCHEMA_COLUMNFAMILIES_CF layout:
 *
 * <key (AsciiType)>
 *     composite(ascii, ascii) => json_serialized_value
 * </key>
 *
 * Where <key> is a name of keyspace e.g. "ks"., first component of the column name is name of the ColumnFamily, last
 * component is the name of the ColumnFamily attribute.
 *
 * SCHEMA_COLUMNS_CF layout:
 *
 * <key (AsciiType)>
 *     composite(ascii, ascii, ascii) => json_serialized value
 * </key>
 *
 * Where <key> is a name of keyspace e.g. "ks".
 *
 * Column names where made composite to support 3-level nesting which represents following structure:
 * "ColumnFamily name":"column name":"column attribute" => "value"
 *
 * Example of schema (using CLI):
 *
 * schema_keyspaces
 * ----------------
 * RowKey: ks
 *  => (column=durable_writes, value=true, timestamp=1327061028312185000)
 *  => (column=name, value="ks", timestamp=1327061028312185000)
 *  => (column=replication_factor, value=0, timestamp=1327061028312185000)
 *  => (column=strategy_class, value="org.apache.cassandra.locator.NetworkTopologyStrategy", timestamp=1327061028312185000)
 *  => (column=strategy_options, value={"datacenter1":"1"}, timestamp=1327061028312185000)
 *
 * schema_columnfamilies
 * ---------------------
 * RowKey: ks
 *  => (column=cf:bloom_filter_fp_chance, value=0.0, timestamp=1327061105833119000)
 *  => (column=cf:caching, value="NONE", timestamp=1327061105833119000)
 *  => (column=cf:column_type, value="Standard", timestamp=1327061105833119000)
 *  => (column=cf:comment, value="ColumnFamily", timestamp=1327061105833119000)
 *  => (column=cf:default_validation_class, value="org.apache.cassandra.db.marshal.BytesType", timestamp=1327061105833119000)
 *  => (column=cf:gc_grace_seconds, value=864000, timestamp=1327061105833119000)
 *  => (column=cf:id, value=1000, timestamp=1327061105833119000)
 *  => (column=cf:key_alias, value="S0VZ", timestamp=1327061105833119000)
 *  ... part of the output omitted.
 *
 * schema_columns
 * --------------
 * RowKey: ks
 *  => (column=cf:c:index_name, value=null, timestamp=1327061105833119000)
 *  => (column=cf:c:index_options, value=null, timestamp=1327061105833119000)
 *  => (column=cf:c:index_type, value=null, timestamp=1327061105833119000)
 *  => (column=cf:c:name, value="aGVsbG8=", timestamp=1327061105833119000)
 *  => (column=cf:c:validation_class, value="org.apache.cassandra.db.marshal.AsciiType", timestamp=1327061105833119000)
 */
public class DefsTable
{
    private final static Logger logger = LoggerFactory.getLogger(DefsTable.class);

    // unbuffered decoders
    private final static DecoderFactory DIRECT_DECODERS = new DecoderFactory().configureDirectDecoder(true);

    // column name for the schema storing serialized keyspace definitions
    // NB: must be an invalid keyspace name
    public static final ByteBuffer DEFINITION_SCHEMA_COLUMN_NAME = ByteBufferUtil.bytes("Avro/Schema");

    /* dumps current keyspace definitions to storage */
    public static synchronized void dumpToStorage(Collection<KSMetaData> keyspaces) throws IOException
    {
        long timestamp = System.currentTimeMillis();

        for (KSMetaData ksMetaData : keyspaces)
            ksMetaData.toSchema(timestamp).apply();
    }

    /**
     * Load keyspace definitions for the system keyspace (system.SCHEMA_KEYSPACES_CF)
     *
     * @return Collection of found keyspace definitions
     *
     * @throws IOException if failed to read SCHEMA_KEYSPACES_CF
     */
    public static Collection<KSMetaData> loadFromTable() throws IOException
    {
        List<Row> serializedSchema = SystemTable.serializedSchema(SystemTable.SCHEMA_KEYSPACES_CF);

        List<KSMetaData> keyspaces = new ArrayList<KSMetaData>(serializedSchema.size());

        for (Row row : serializedSchema)
        {
            if (row.cf == null || row.cf.isEmpty() || row.cf.isMarkedForDelete())
                continue;

            keyspaces.add(KSMetaData.fromSchema(row, serializedColumnFamilies(row.key)));
        }

        return keyspaces;
    }

    private static Row serializedColumnFamilies(DecoratedKey ksNameKey)
    {
        ColumnFamilyStore cfsStore = SystemTable.schemaCFS(SystemTable.SCHEMA_COLUMNFAMILIES_CF);
        return new Row(ksNameKey, cfsStore.getColumnFamily(QueryFilter.getIdentityFilter(ksNameKey, new QueryPath(SystemTable.SCHEMA_COLUMNFAMILIES_CF))));
    }

    /**
     * Loads a version of keyspace definitions from storage (using old SCHEMA_CF as a data source)
     * Note: If definitions where found in SCHEMA_CF this method would load them into new schema handling table KEYSPACE_CF
     *
     * @param version The version of the latest migration.
     *
     * @return Collection of found keyspace definitions
     *
     * @throws IOException if failed to read SCHEMA_CF or failed to deserialize Avro schema
     */
    public static synchronized Collection<KSMetaData> loadFromStorage(UUID version) throws IOException
    {
        DecoratedKey vkey = StorageService.getPartitioner().decorateKey(toUTF8Bytes(version));
        Table defs = Table.open(Table.SYSTEM_TABLE);
        ColumnFamilyStore cfStore = defs.getColumnFamilyStore(Migration.SCHEMA_CF);
        ColumnFamily cf = cfStore.getColumnFamily(QueryFilter.getIdentityFilter(vkey, new QueryPath(Migration.SCHEMA_CF)));
        IColumn avroschema = cf.getColumn(DEFINITION_SCHEMA_COLUMN_NAME);

        Collection<KSMetaData> keyspaces = Collections.emptyList();

        if (avroschema != null)
        {
            ByteBuffer value = avroschema.value();
            org.apache.avro.Schema schema = org.apache.avro.Schema.parse(ByteBufferUtil.string(value));

            // deserialize keyspaces using schema
            Collection<IColumn> columns = cf.getSortedColumns();
            keyspaces = new ArrayList<KSMetaData>(columns.size());

            for (IColumn column : columns)
            {
                if (column.name().equals(DEFINITION_SCHEMA_COLUMN_NAME))
                    continue;
                KsDef ks = deserializeAvro(schema, column.value(), new KsDef());
                keyspaces.add(Avro.ksFromAvro(ks));
            }

            // store deserialized keyspaces into new place
            dumpToStorage(keyspaces);

            logger.info("Truncating deprecated system column families (migrations, schema)...");
            MigrationHelper.dropColumnFamily(Table.SYSTEM_TABLE, Migration.MIGRATIONS_CF, -1, false);
            MigrationHelper.dropColumnFamily(Table.SYSTEM_TABLE, Migration.SCHEMA_CF, -1, false);
        }

        return keyspaces;
    }

    /**
     * Merge remote schema in form of row mutations with local and mutate ks/cf metadata objects
     * (which also involves fs operations on add/drop ks/cf)
     *
     * @param data The data of the message from remote node with schema information
     * @param version The version of the message
     *
     * @throws ConfigurationException If one of metadata attributes has invalid value
     * @throws IOException If data was corrupted during transportation or failed to apply fs operations
     */
    public static void mergeRemoteSchema(byte[] data, int version) throws ConfigurationException, IOException
    {
        if (version < MessagingService.VERSION_11)
        {
            logger.error("Can't accept schema migrations from Cassandra versions previous to 1.1, please update first.");
            return;
        }

        // save current state of the schema
        Map<DecoratedKey, ColumnFamily> oldKeyspaces = SystemTable.getSchema(SystemTable.SCHEMA_KEYSPACES_CF);
        Map<DecoratedKey, ColumnFamily> oldColumnFamilies = SystemTable.getSchema(SystemTable.SCHEMA_COLUMNFAMILIES_CF);

        // apply remote mutations
        for (RowMutation mutation : MigrationManager.deserializeMigrationMessage(data, version))
            mutation.apply();

        if (!StorageService.instance.isClientMode())
            MigrationHelper.flushSchemaCFs();

        Schema.instance.updateVersionAndAnnounce();

        Set<String> keyspacesToDrop = mergeKeyspaces(oldKeyspaces, SystemTable.getSchema(SystemTable.SCHEMA_KEYSPACES_CF));
        mergeColumnFamilies(oldColumnFamilies, SystemTable.getSchema(SystemTable.SCHEMA_COLUMNFAMILIES_CF));

        // it is safe to drop a keyspace only when all nested ColumnFamilies where deleted
        for (String keyspaceToDrop : keyspacesToDrop)
            MigrationHelper.dropKeyspace(keyspaceToDrop, -1, false);
    }

    private static Set<String> mergeKeyspaces(Map<DecoratedKey, ColumnFamily> old, Map<DecoratedKey, ColumnFamily> updated)
            throws ConfigurationException, IOException
    {
        // calculate the difference between old and new states (note that entriesOnlyLeft() will be always empty)
        MapDifference<DecoratedKey, ColumnFamily> diff = Maps.difference(old, updated);

        /**
         * At first step we check if any new keyspaces were added.
         */
        for (Map.Entry<DecoratedKey, ColumnFamily> entry : diff.entriesOnlyOnRight().entrySet())
        {
            ColumnFamily ksAttrs = entry.getValue();

            // we don't care about nested ColumnFamilies here because those are going to be processed separately
            if (!ksAttrs.isEmpty())
            {
                KSMetaData ksm = KSMetaData.fromSchema(new Row(entry.getKey(), entry.getValue()), Collections.<CFMetaData>emptyList());
                MigrationHelper.addKeyspace(ksm, -1, false);
            }
        }

        /**
         * At second step we check if there were any keyspaces re-created, in this context
         * re-created means that they were previously deleted but still exist in the low-level schema as empty keys
         */

        Map<DecoratedKey, MapDifference.ValueDifference<ColumnFamily>> modifiedEntries = diff.entriesDiffering();

        // instead of looping over all modified entries and skipping processed keys all the time
        // we would rather store "left to process" items and iterate over them removing already met keys
        List<DecoratedKey> leftToProcess = new ArrayList<DecoratedKey>(modifiedEntries.size());

        for (Map.Entry<DecoratedKey, MapDifference.ValueDifference<ColumnFamily>> entry : modifiedEntries.entrySet())
        {
            ColumnFamily prevValue = entry.getValue().leftValue();
            ColumnFamily newValue = entry.getValue().rightValue();

            if (prevValue.isEmpty())
            {
                KSMetaData ksm = KSMetaData.fromSchema(new Row(entry.getKey(), newValue), Collections.<CFMetaData>emptyList());
                MigrationHelper.addKeyspace(ksm, -1, false);
                continue;
            }

            leftToProcess.add(entry.getKey());
        }

        if (leftToProcess.size() == 0)
            return Collections.emptySet();

        /**
         * At final step we updating modified keyspaces and saving keyspaces drop them later
         */

        Set<String> keyspacesToDrop = new HashSet<String>();

        for (DecoratedKey key : leftToProcess)
        {
            MapDifference.ValueDifference<ColumnFamily> valueDiff = modifiedEntries.get(key);

            ColumnFamily newState = valueDiff.rightValue();

            if (newState.isEmpty())
            {
                keyspacesToDrop.add(AsciiType.instance.getString(key.key));
            }
            else
            {
                KSMetaData ksm = KSMetaData.fromSchema(new Row(key, newState), Collections.<CFMetaData>emptyList());
                MigrationHelper.updateKeyspace(ksm, -1, false);
            }
        }

        return keyspacesToDrop;
    }

    private static void mergeColumnFamilies(Map<DecoratedKey, ColumnFamily> old, Map<DecoratedKey, ColumnFamily> updated)
            throws ConfigurationException, IOException
    {
        // calculate the difference between old and new states (note that entriesOnlyLeft() will be always empty)
        MapDifference<DecoratedKey, ColumnFamily> diff = Maps.difference(old, updated);

        // check if any new Keyspaces with ColumnFamilies were added.
        for (Map.Entry<DecoratedKey, ColumnFamily> entry : diff.entriesOnlyOnRight().entrySet())
        {
            ColumnFamily cfAttrs = entry.getValue();

            if (!cfAttrs.isEmpty())
            {
               Map<String, CFMetaData> cfDefs = KSMetaData.deserializeColumnFamilies(new Row(entry.getKey(), cfAttrs));

                for (CFMetaData cfDef : cfDefs.values())
                    MigrationHelper.addColumnFamily(cfDef, -1, false);
            }
        }

        // deal with modified ColumnFamilies (remember that all of the keyspace nested ColumnFamilies are put to the single row)
        Map<DecoratedKey, MapDifference.ValueDifference<ColumnFamily>> modifiedEntries = diff.entriesDiffering();

        for (DecoratedKey keyspace : modifiedEntries.keySet())
        {
            MapDifference.ValueDifference<ColumnFamily> valueDiff = modifiedEntries.get(keyspace);

            ColumnFamily prevValue = valueDiff.leftValue(); // state before external modification
            ColumnFamily newValue = valueDiff.rightValue(); // updated state
            Row newRow = new Row(keyspace, newValue);

            if (prevValue.isEmpty()) // whole keyspace was deleted and now it's re-created
            {
                for (CFMetaData cfm : KSMetaData.deserializeColumnFamilies(newRow).values())
                    MigrationHelper.addColumnFamily(cfm, -1, false);
            }
            else if (newValue.isEmpty()) // whole keyspace is deleted
            {
                for (CFMetaData cfm : KSMetaData.deserializeColumnFamilies(new Row(keyspace, prevValue)).values())
                    MigrationHelper.dropColumnFamily(cfm.ksName, cfm.cfName, -1, false);
            }
            else // has modifications in the nested ColumnFamilies, need to perform nested diff to determine what was really changed
            {
                String ksName = AsciiType.instance.getString(keyspace.key);

                Map<String, CFMetaData> oldCfDefs = new HashMap<String, CFMetaData>();
                for (CFMetaData cfm : Schema.instance.getKSMetaData(ksName).cfMetaData().values())
                    oldCfDefs.put(cfm.cfName, cfm);

                Map<String, CFMetaData> newCfDefs = KSMetaData.deserializeColumnFamilies(newRow);

                MapDifference<String, CFMetaData> cfDefDiff = Maps.difference(oldCfDefs, newCfDefs);

                for (CFMetaData cfDef : cfDefDiff.entriesOnlyOnRight().values())
                    MigrationHelper.addColumnFamily(cfDef, -1, false);

                for (CFMetaData cfDef : cfDefDiff.entriesOnlyOnLeft().values())
                    MigrationHelper.dropColumnFamily(cfDef.ksName, cfDef.cfName, -1, false);

                for (MapDifference.ValueDifference<CFMetaData> cfDef : cfDefDiff.entriesDiffering().values())
                    MigrationHelper.updateColumnFamily(cfDef.rightValue(), -1, false);
            }
        }
    }

    private static ByteBuffer toUTF8Bytes(UUID version)
    {
        return ByteBufferUtil.bytes(version.toString());
    }

    /**
     * Deserialize a single object based on the given Schema.
     *
     * @param writer writer's schema
     * @param bytes Array to deserialize from
     * @param ob An empty object to deserialize into (must not be null).
     *
     * @return serialized Avro object
     *
     * @throws IOException if deserialization failed
     */
    public static <T extends SpecificRecord> T deserializeAvro(org.apache.avro.Schema writer, ByteBuffer bytes, T ob) throws IOException
    {
        BinaryDecoder dec = DIRECT_DECODERS.createBinaryDecoder(ByteBufferUtil.getArray(bytes), null);
        SpecificDatumReader<T> reader = new SpecificDatumReader<T>(writer);
        reader.setExpected(ob.getSchema());
        return reader.read(ob, dec);
    }
}
