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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.config.UTMetaData;
import org.apache.cassandra.cql3.functions.Functions;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * SCHEMA_{KEYSPACES, COLUMNFAMILIES, COLUMNS}_CF are used to store Keyspace/ColumnFamily attributes to make schema
 * load/distribution easy, it replaces old mechanism when local migrations where serialized, stored in system.Migrations
 * and used for schema distribution.
 */
public class DefsTables
{
    private static final Logger logger = LoggerFactory.getLogger(DefsTables.class);

    /**
     * Load keyspace definitions for the system keyspace (system.SCHEMA_KEYSPACES_TABLE)
     *
     * @return Collection of found keyspace definitions
     */
    public static Collection<KSMetaData> loadFromKeyspace()
    {
        List<Row> serializedSchema = SystemKeyspace.serializedSchema(SystemKeyspace.SCHEMA_KEYSPACES_TABLE);

        List<KSMetaData> keyspaces = new ArrayList<>(serializedSchema.size());

        for (Row row : serializedSchema)
        {
            if (Schema.invalidSchemaRow(row) || Schema.ignoredSchemaRow(row))
                continue;

            keyspaces.add(KSMetaData.fromSchema(row, serializedColumnFamilies(row.key), serializedUserTypes(row.key)));
        }

        return keyspaces;
    }

    private static Row serializedColumnFamilies(DecoratedKey ksNameKey)
    {
        ColumnFamilyStore cfsStore = SystemKeyspace.schemaCFS(SystemKeyspace.SCHEMA_COLUMNFAMILIES_TABLE);
        return new Row(ksNameKey, cfsStore.getColumnFamily(QueryFilter.getIdentityFilter(ksNameKey,
                                                                                         SystemKeyspace.SCHEMA_COLUMNFAMILIES_TABLE,
                                                                                         System.currentTimeMillis())));
    }

    private static Row serializedUserTypes(DecoratedKey ksNameKey)
    {
        ColumnFamilyStore cfsStore = SystemKeyspace.schemaCFS(SystemKeyspace.SCHEMA_USER_TYPES_TABLE);
        return new Row(ksNameKey, cfsStore.getColumnFamily(QueryFilter.getIdentityFilter(ksNameKey,
                                                                                         SystemKeyspace.SCHEMA_USER_TYPES_TABLE,
                                                                                         System.currentTimeMillis())));
    }

    /**
     * Merge remote schema in form of mutations with local and mutate ks/cf metadata objects
     * (which also involves fs operations on add/drop ks/cf)
     *
     * @param mutations the schema changes to apply
     *
     * @throws ConfigurationException If one of metadata attributes has invalid value
     * @throws IOException If data was corrupted during transportation or failed to apply fs operations
     */
    public static synchronized void mergeSchema(Collection<Mutation> mutations) throws ConfigurationException, IOException
    {
        mergeSchemaInternal(mutations, true);
        Schema.instance.updateVersionAndAnnounce();
    }

    public static synchronized void mergeSchemaInternal(Collection<Mutation> mutations, boolean doFlush) throws IOException
    {
        // compare before/after schemas of the affected keyspaces only
        Set<String> keyspaces = new HashSet<>(mutations.size());
        for (Mutation mutation : mutations)
            keyspaces.add(ByteBufferUtil.string(mutation.key()));

        // current state of the schema
        Map<DecoratedKey, ColumnFamily> oldKeyspaces = SystemKeyspace.getSchema(SystemKeyspace.SCHEMA_KEYSPACES_TABLE, keyspaces);
        Map<DecoratedKey, ColumnFamily> oldColumnFamilies = SystemKeyspace.getSchema(SystemKeyspace.SCHEMA_COLUMNFAMILIES_TABLE, keyspaces);
        Map<DecoratedKey, ColumnFamily> oldTypes = SystemKeyspace.getSchema(SystemKeyspace.SCHEMA_USER_TYPES_TABLE, keyspaces);
        Map<DecoratedKey, ColumnFamily> oldFunctions = SystemKeyspace.getSchema(SystemKeyspace.SCHEMA_FUNCTIONS_TABLE);

        for (Mutation mutation : mutations)
            mutation.apply();

        if (doFlush)
            flushSchemaCFs();

        // with new data applied
        Map<DecoratedKey, ColumnFamily> newKeyspaces = SystemKeyspace.getSchema(SystemKeyspace.SCHEMA_KEYSPACES_TABLE, keyspaces);
        Map<DecoratedKey, ColumnFamily> newColumnFamilies = SystemKeyspace.getSchema(SystemKeyspace.SCHEMA_COLUMNFAMILIES_TABLE, keyspaces);
        Map<DecoratedKey, ColumnFamily> newTypes = SystemKeyspace.getSchema(SystemKeyspace.SCHEMA_USER_TYPES_TABLE, keyspaces);
        Map<DecoratedKey, ColumnFamily> newFunctions = SystemKeyspace.getSchema(SystemKeyspace.SCHEMA_FUNCTIONS_TABLE);

        Set<String> keyspacesToDrop = mergeKeyspaces(oldKeyspaces, newKeyspaces);
        mergeColumnFamilies(oldColumnFamilies, newColumnFamilies);
        mergeTypes(oldTypes, newTypes);
        mergeFunctions(oldFunctions, newFunctions);

        // it is safe to drop a keyspace only when all nested ColumnFamilies where deleted
        for (String keyspaceToDrop : keyspacesToDrop)
            dropKeyspace(keyspaceToDrop);
    }

    private static Set<String> mergeKeyspaces(Map<DecoratedKey, ColumnFamily> before, Map<DecoratedKey, ColumnFamily> after)
    {
        List<Row> created = new ArrayList<>();
        List<String> altered = new ArrayList<>();
        Set<String> dropped = new HashSet<>();

        /*
         * - we don't care about entriesOnlyOnLeft() or entriesInCommon(), because only the changes are of interest to us
         * - of all entriesOnlyOnRight(), we only care about ones that have live columns; it's possible to have a ColumnFamily
         *   there that only has the top-level deletion, if:
         *      a) a pushed DROP KEYSPACE change for a keyspace hadn't ever made it to this node in the first place
         *      b) a pulled dropped keyspace that got dropped before it could find a way to this node
         * - of entriesDiffering(), we don't care about the scenario where both pre and post-values have zero live columns:
         *   that means that a keyspace had been recreated and dropped, and the recreated keyspace had never found a way
         *   to this node
         */
        MapDifference<DecoratedKey, ColumnFamily> diff = Maps.difference(before, after);

        for (Map.Entry<DecoratedKey, ColumnFamily> entry : diff.entriesOnlyOnRight().entrySet())
            if (entry.getValue().hasColumns())
                created.add(new Row(entry.getKey(), entry.getValue()));

        for (Map.Entry<DecoratedKey, MapDifference.ValueDifference<ColumnFamily>> entry : diff.entriesDiffering().entrySet())
        {
            String keyspaceName = AsciiType.instance.compose(entry.getKey().getKey());

            ColumnFamily pre  = entry.getValue().leftValue();
            ColumnFamily post = entry.getValue().rightValue();

            if (pre.hasColumns() && post.hasColumns())
                altered.add(keyspaceName);
            else if (pre.hasColumns())
                dropped.add(keyspaceName);
            else if (post.hasColumns()) // a (re)created keyspace
                created.add(new Row(entry.getKey(), post));
        }

        for (Row row : created)
            addKeyspace(KSMetaData.fromSchema(row, Collections.<CFMetaData>emptyList(), new UTMetaData()));
        for (String name : altered)
            updateKeyspace(name);
        return dropped;
    }

    // see the comments for mergeKeyspaces()
    private static void mergeColumnFamilies(Map<DecoratedKey, ColumnFamily> before, Map<DecoratedKey, ColumnFamily> after)
    {
        List<CFMetaData> created = new ArrayList<>();
        List<CFMetaData> altered = new ArrayList<>();
        List<CFMetaData> dropped = new ArrayList<>();

        MapDifference<DecoratedKey, ColumnFamily> diff = Maps.difference(before, after);

        for (Map.Entry<DecoratedKey, ColumnFamily> entry : diff.entriesOnlyOnRight().entrySet())
            if (entry.getValue().hasColumns())
                created.addAll(KSMetaData.deserializeColumnFamilies(new Row(entry.getKey(), entry.getValue())).values());

        for (Map.Entry<DecoratedKey, MapDifference.ValueDifference<ColumnFamily>> entry : diff.entriesDiffering().entrySet())
        {
            String keyspaceName = AsciiType.instance.compose(entry.getKey().getKey());

            ColumnFamily pre  = entry.getValue().leftValue();
            ColumnFamily post = entry.getValue().rightValue();

            if (pre.hasColumns() && post.hasColumns())
            {
                MapDifference<String, CFMetaData> delta =
                        Maps.difference(Schema.instance.getKSMetaData(keyspaceName).cfMetaData(),
                                        KSMetaData.deserializeColumnFamilies(new Row(entry.getKey(), post)));

                dropped.addAll(delta.entriesOnlyOnLeft().values());
                created.addAll(delta.entriesOnlyOnRight().values());
                Iterables.addAll(altered, Iterables.transform(delta.entriesDiffering().values(), new Function<MapDifference.ValueDifference<CFMetaData>, CFMetaData>()
                {
                    public CFMetaData apply(MapDifference.ValueDifference<CFMetaData> pair)
                    {
                        return pair.rightValue();
                    }
                }));
            }
            else if (pre.hasColumns())
            {
                dropped.addAll(Schema.instance.getKSMetaData(keyspaceName).cfMetaData().values());
            }
            else if (post.hasColumns())
            {
                created.addAll(KSMetaData.deserializeColumnFamilies(new Row(entry.getKey(), post)).values());
            }
        }

        for (CFMetaData cfm : created)
            addColumnFamily(cfm);
        for (CFMetaData cfm : altered)
            updateColumnFamily(cfm.ksName, cfm.cfName);
        for (CFMetaData cfm : dropped)
            dropColumnFamily(cfm.ksName, cfm.cfName);
    }

    // see the comments for mergeKeyspaces()
    private static void mergeTypes(Map<DecoratedKey, ColumnFamily> before, Map<DecoratedKey, ColumnFamily> after)
    {
        List<UserType> created = new ArrayList<>();
        List<UserType> altered = new ArrayList<>();
        List<UserType> dropped = new ArrayList<>();

        MapDifference<DecoratedKey, ColumnFamily> diff = Maps.difference(before, after);

        // New keyspace with types
        for (Map.Entry<DecoratedKey, ColumnFamily> entry : diff.entriesOnlyOnRight().entrySet())
            if (entry.getValue().hasColumns())
                created.addAll(UTMetaData.fromSchema(new Row(entry.getKey(), entry.getValue())).values());

        for (Map.Entry<DecoratedKey, MapDifference.ValueDifference<ColumnFamily>> entry : diff.entriesDiffering().entrySet())
        {
            String keyspaceName = AsciiType.instance.compose(entry.getKey().getKey());

            ColumnFamily pre  = entry.getValue().leftValue();
            ColumnFamily post = entry.getValue().rightValue();

            if (pre.hasColumns() && post.hasColumns())
            {
                MapDifference<ByteBuffer, UserType> delta =
                        Maps.difference(Schema.instance.getKSMetaData(keyspaceName).userTypes.getAllTypes(),
                                        UTMetaData.fromSchema(new Row(entry.getKey(), post)));

                dropped.addAll(delta.entriesOnlyOnLeft().values());
                created.addAll(delta.entriesOnlyOnRight().values());
                Iterables.addAll(altered, Iterables.transform(delta.entriesDiffering().values(), new Function<MapDifference.ValueDifference<UserType>, UserType>()
                {
                    public UserType apply(MapDifference.ValueDifference<UserType> pair)
                    {
                        return pair.rightValue();
                    }
                }));
            }
            else if (pre.hasColumns())
            {
                dropped.addAll(Schema.instance.getKSMetaData(keyspaceName).userTypes.getAllTypes().values());
            }
            else if (post.hasColumns())
            {
                created.addAll(UTMetaData.fromSchema(new Row(entry.getKey(), post)).values());
            }
        }

        for (UserType type : created)
            addType(type);
        for (UserType type : altered)
            updateType(type);
        for (UserType type : dropped)
            dropType(type);
    }

    // see the comments for mergeKeyspaces()
    private static void mergeFunctions(Map<DecoratedKey, ColumnFamily> before, Map<DecoratedKey, ColumnFamily> after)
    {
        List<UDFunction> created = new ArrayList<>();
        List<UDFunction> altered = new ArrayList<>();
        List<UDFunction> dropped = new ArrayList<>();

        MapDifference<DecoratedKey, ColumnFamily> diff = Maps.difference(before, after);

        // New keyspace with functions
        for (Map.Entry<DecoratedKey, ColumnFamily> entry : diff.entriesOnlyOnRight().entrySet())
            if (entry.getValue().hasColumns())
                created.addAll(UDFunction.fromSchema(new Row(entry.getKey(), entry.getValue())).values());

        for (Map.Entry<DecoratedKey, MapDifference.ValueDifference<ColumnFamily>> entry : diff.entriesDiffering().entrySet())
        {
            ColumnFamily pre = entry.getValue().leftValue();
            ColumnFamily post = entry.getValue().rightValue();

            if (pre.hasColumns() && post.hasColumns())
            {
                MapDifference<Composite, UDFunction> delta =
                        Maps.difference(UDFunction.fromSchema(new Row(entry.getKey(), pre)),
                                        UDFunction.fromSchema(new Row(entry.getKey(), post)));

                dropped.addAll(delta.entriesOnlyOnLeft().values());
                created.addAll(delta.entriesOnlyOnRight().values());
                Iterables.addAll(altered, Iterables.transform(delta.entriesDiffering().values(), new Function<MapDifference.ValueDifference<UDFunction>, UDFunction>()
                {
                    public UDFunction apply(MapDifference.ValueDifference<UDFunction> pair)
                    {
                        return pair.rightValue();
                    }
                }));
            }
            else if (pre.hasColumns())
            {
                dropped.addAll(UDFunction.fromSchema(new Row(entry.getKey(), pre)).values());
            }
            else if (post.hasColumns())
            {
                created.addAll(UDFunction.fromSchema(new Row(entry.getKey(), post)).values());
            }
        }

        for (UDFunction udf : created)
            addFunction(udf);
        for (UDFunction udf : altered)
            updateFunction(udf);
        for (UDFunction udf : dropped)
            dropFunction(udf);
    }

    private static void addKeyspace(KSMetaData ksm)
    {
        assert Schema.instance.getKSMetaData(ksm.name) == null;
        Schema.instance.load(ksm);

        Keyspace.open(ksm.name);
        MigrationManager.instance.notifyCreateKeyspace(ksm);
    }

    private static void addColumnFamily(CFMetaData cfm)
    {
        assert Schema.instance.getCFMetaData(cfm.ksName, cfm.cfName) == null;
        KSMetaData ksm = Schema.instance.getKSMetaData(cfm.ksName);
        ksm = KSMetaData.cloneWith(ksm, Iterables.concat(ksm.cfMetaData().values(), Collections.singleton(cfm)));

        logger.info("Loading {}", cfm);

        Schema.instance.load(cfm);

        // make sure it's init-ed w/ the old definitions first,
        // since we're going to call initCf on the new one manually
        Keyspace.open(cfm.ksName);

        Schema.instance.setKeyspaceDefinition(ksm);
        Keyspace.open(ksm.name).initCf(cfm.cfId, cfm.cfName, true);
        MigrationManager.instance.notifyCreateColumnFamily(cfm);
    }

    private static void addType(UserType ut)
    {
        KSMetaData ksm = Schema.instance.getKSMetaData(ut.keyspace);
        assert ksm != null;

        logger.info("Loading {}", ut);

        ksm.userTypes.addType(ut);

        MigrationManager.instance.notifyCreateUserType(ut);
    }

    private static void addFunction(UDFunction udf)
    {
        logger.info("Loading {}", udf);

        Functions.addFunction(udf);

        MigrationManager.instance.notifyCreateFunction(udf);
    }

    private static void updateKeyspace(String ksName)
    {
        KSMetaData oldKsm = Schema.instance.getKSMetaData(ksName);
        assert oldKsm != null;
        KSMetaData newKsm = KSMetaData.cloneWith(oldKsm.reloadAttributes(), oldKsm.cfMetaData().values());

        Schema.instance.setKeyspaceDefinition(newKsm);

        Keyspace.open(ksName).createReplicationStrategy(newKsm);
        MigrationManager.instance.notifyUpdateKeyspace(newKsm);
    }

    private static void updateColumnFamily(String ksName, String cfName)
    {
        CFMetaData cfm = Schema.instance.getCFMetaData(ksName, cfName);
        assert cfm != null;
        cfm.reload();

        Keyspace keyspace = Keyspace.open(cfm.ksName);
        keyspace.getColumnFamilyStore(cfm.cfName).reload();
        MigrationManager.instance.notifyUpdateColumnFamily(cfm);
    }

    private static void updateType(UserType ut)
    {
        KSMetaData ksm = Schema.instance.getKSMetaData(ut.keyspace);
        assert ksm != null;

        logger.info("Updating {}", ut);

        ksm.userTypes.addType(ut);

        MigrationManager.instance.notifyUpdateUserType(ut);
    }

    private static void updateFunction(UDFunction udf)
    {
        logger.info("Updating {}", udf);

        Functions.replaceFunction(udf);

        MigrationManager.instance.notifyUpdateFunction(udf);
    }

    private static void dropKeyspace(String ksName)
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

            Schema.instance.purge(cfm);

            if (DatabaseDescriptor.isAutoSnapshot())
                cfs.snapshot(snapshotName);
            Keyspace.open(ksm.name).dropCf(cfm.cfId);

            droppedCfs.add(cfm.cfId);
        }

        // remove the keyspace from the static instances.
        Keyspace.clear(ksm.name);
        Schema.instance.clearKeyspaceDefinition(ksm);

        keyspace.writeOrder.awaitNewBarrier();

        // force a new segment in the CL
        CommitLog.instance.forceRecycleAllSegments(droppedCfs);

        MigrationManager.instance.notifyDropKeyspace(ksm);
    }

    private static void dropColumnFamily(String ksName, String cfName)
    {
        KSMetaData ksm = Schema.instance.getKSMetaData(ksName);
        assert ksm != null;
        ColumnFamilyStore cfs = Keyspace.open(ksName).getColumnFamilyStore(cfName);
        assert cfs != null;

        // reinitialize the keyspace.
        CFMetaData cfm = ksm.cfMetaData().get(cfName);

        Schema.instance.purge(cfm);
        Schema.instance.setKeyspaceDefinition(makeNewKeyspaceDefinition(ksm, cfm));

        CompactionManager.instance.interruptCompactionFor(Arrays.asList(cfm), true);

        if (DatabaseDescriptor.isAutoSnapshot())
            cfs.snapshot(Keyspace.getTimestampedSnapshotName(cfs.name));
        Keyspace.open(ksm.name).dropCf(cfm.cfId);
        MigrationManager.instance.notifyDropColumnFamily(cfm);

        CommitLog.instance.forceRecycleAllSegments(Collections.singleton(cfm.cfId));
    }

    private static void dropType(UserType ut)
    {
        KSMetaData ksm = Schema.instance.getKSMetaData(ut.keyspace);
        assert ksm != null;

        ksm.userTypes.removeType(ut);

        MigrationManager.instance.notifyDropUserType(ut);
    }

    private static void dropFunction(UDFunction udf)
    {
        logger.info("Drop {}", udf);

        // TODO: this is kind of broken as this remove all overloads of the function name
        Functions.removeFunction(udf.name(), udf.argTypes());

        MigrationManager.instance.notifyDropFunction(udf);
    }

    private static KSMetaData makeNewKeyspaceDefinition(KSMetaData ksm, CFMetaData toExclude)
    {
        // clone ksm but do not include the new def
        List<CFMetaData> newCfs = new ArrayList<>(ksm.cfMetaData().values());
        newCfs.remove(toExclude);
        assert newCfs.size() == ksm.cfMetaData().size() - 1;
        return KSMetaData.cloneWith(ksm, newCfs);
    }

    private static void flushSchemaCFs()
    {
        for (String cf : SystemKeyspace.ALL_SCHEMA_TABLES)
            SystemKeyspace.forceBlockingFlush(cf);
    }
}
