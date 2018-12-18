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
package org.apache.cassandra.schema;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Sets;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.functions.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.schema.KeyspaceMetadata.KeyspaceDiff;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Pair;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import static java.lang.String.format;

import static com.google.common.collect.Iterables.size;

public final class Schema
{
    public static final Schema instance = new Schema();

    private volatile Keyspaces keyspaces = Keyspaces.none();

    // UUID -> mutable metadata ref map. We have to update these in place every time a table changes.
    private final Map<TableId, TableMetadataRef> metadataRefs = new NonBlockingHashMap<>();

    // (keyspace name, index name) -> mutable metadata ref map. We have to update these in place every time an index changes.
    private final Map<Pair<String, String>, TableMetadataRef> indexMetadataRefs = new NonBlockingHashMap<>();

    // Keyspace objects, one per keyspace. Only one instance should ever exist for any given keyspace.
    private final Map<String, Keyspace> keyspaceInstances = new NonBlockingHashMap<>();

    private volatile UUID version;

    private final List<SchemaChangeListener> changeListeners = new CopyOnWriteArrayList<>();

    /**
     * Initialize empty schema object and load the hardcoded system tables
     */
    private Schema()
    {
        if (DatabaseDescriptor.isDaemonInitialized() || DatabaseDescriptor.isToolInitialized())
        {
            load(SchemaKeyspace.metadata());
            load(SystemKeyspace.metadata());
        }
    }

    /**
     * load keyspace (keyspace) definitions, but do not initialize the keyspace instances.
     * Schema version may be updated as the result.
     */
    public void loadFromDisk()
    {
        loadFromDisk(true);
    }

    /**
     * Load schema definitions from disk.
     *
     * @param updateVersion true if schema version needs to be updated
     */
    public void loadFromDisk(boolean updateVersion)
    {
        SchemaDiagnostics.schemataLoading(this);
        SchemaKeyspace.fetchNonSystemKeyspaces().forEach(this::load);
        if (updateVersion)
            updateVersion();
        SchemaDiagnostics.schemataLoaded(this);
    }

    /**
     * Update (or insert) new keyspace definition
     *
     * @param ksm The metadata about keyspace
     */
    synchronized public void load(KeyspaceMetadata ksm)
    {
        KeyspaceMetadata previous = keyspaces.getNullable(ksm.name);

        if (previous == null)
            loadNew(ksm);
        else
            reload(previous, ksm);

        keyspaces = keyspaces.withAddedOrUpdated(ksm);
    }

    private void loadNew(KeyspaceMetadata ksm)
    {
        ksm.tablesAndViews()
           .forEach(metadata -> metadataRefs.put(metadata.id, new TableMetadataRef(metadata)));

        ksm.tables
           .indexTables()
           .forEach((name, metadata) -> indexMetadataRefs.put(Pair.create(ksm.name, name), new TableMetadataRef(metadata)));

        SchemaDiagnostics.metadataInitialized(this, ksm);
    }

    private void reload(KeyspaceMetadata previous, KeyspaceMetadata updated)
    {
        Keyspace keyspace = getKeyspaceInstance(updated.name);
        if (null != keyspace)
            keyspace.setMetadata(updated);

        Tables.TablesDiff tablesDiff = Tables.diff(previous.tables, updated.tables);
        Views.ViewsDiff viewsDiff = Views.diff(previous.views, updated.views);

        MapDifference<String, TableMetadata> indexesDiff = previous.tables.indexesDiff(updated.tables);

        // clean up after removed entries
        tablesDiff.dropped.forEach(table -> metadataRefs.remove(table.id));
        viewsDiff.dropped.forEach(view -> metadataRefs.remove(view.metadata.id));

        indexesDiff.entriesOnlyOnLeft()
                   .values()
                   .forEach(indexTable -> indexMetadataRefs.remove(Pair.create(indexTable.keyspace, indexTable.indexName().get())));

        // load up new entries
        tablesDiff.created.forEach(table -> metadataRefs.put(table.id, new TableMetadataRef(table)));
        viewsDiff.created.forEach(view -> metadataRefs.put(view.metadata.id, new TableMetadataRef(view.metadata)));

        indexesDiff.entriesOnlyOnRight()
                   .values()
                   .forEach(indexTable -> indexMetadataRefs.put(Pair.create(indexTable.keyspace, indexTable.indexName().get()), new TableMetadataRef(indexTable)));

        // refresh refs to updated ones
        tablesDiff.altered.forEach(diff -> metadataRefs.get(diff.after.id).set(diff.after));
        viewsDiff.altered.forEach(diff -> metadataRefs.get(diff.after.metadata.id).set(diff.after.metadata));

        indexesDiff.entriesDiffering()
                   .values()
                   .stream()
                   .map(MapDifference.ValueDifference::rightValue)
                   .forEach(indexTable -> indexMetadataRefs.get(Pair.create(indexTable.keyspace, indexTable.indexName().get())).set(indexTable));

        SchemaDiagnostics.metadataReloaded(this, previous, updated, tablesDiff, viewsDiff, indexesDiff);
    }

    public void registerListener(SchemaChangeListener listener)
    {
        changeListeners.add(listener);
    }

    @SuppressWarnings("unused")
    public void unregisterListener(SchemaChangeListener listener)
    {
        changeListeners.remove(listener);
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

    public ColumnFamilyStore getColumnFamilyStoreInstance(TableId id)
    {
        TableMetadata metadata = getTableMetadata(id);
        if (metadata == null)
            return null;

        Keyspace instance = getKeyspaceInstance(metadata.keyspace);
        if (instance == null)
            return null;

        return instance.hasColumnFamilyStore(metadata.id)
             ? instance.getColumnFamilyStore(metadata.id)
             : null;
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
    synchronized void unload(KeyspaceMetadata ksm)
    {
        keyspaces = keyspaces.without(ksm.name);

        ksm.tablesAndViews()
           .forEach(t -> metadataRefs.remove(t.id));

        ksm.tables
           .indexTables()
           .keySet()
           .forEach(name -> indexMetadataRefs.remove(Pair.create(ksm.name, name)));

        SchemaDiagnostics.metadataRemoved(this, ksm);
    }

    public int getNumberOfTables()
    {
        return keyspaces.stream().mapToInt(k -> size(k.tablesAndViews())).sum();
    }

    public ViewMetadata getView(String keyspaceName, String viewName)
    {
        assert keyspaceName != null;
        KeyspaceMetadata ksm = keyspaces.getNullable(keyspaceName);
        return (ksm == null) ? null : ksm.views.getNullable(viewName);
    }

    /**
     * Get metadata about keyspace by its name
     *
     * @param keyspaceName The name of the keyspace
     *
     * @return The keyspace metadata or null if it wasn't found
     */
    public KeyspaceMetadata getKeyspaceMetadata(String keyspaceName)
    {
        assert keyspaceName != null;
        KeyspaceMetadata keyspace = keyspaces.getNullable(keyspaceName);
        return null != keyspace ? keyspace : VirtualKeyspaceRegistry.instance.getKeyspaceMetadataNullable(keyspaceName);
    }

    private Set<String> getNonSystemKeyspacesSet()
    {
        return Sets.difference(keyspaces.names(), SchemaConstants.LOCAL_SYSTEM_KEYSPACE_NAMES);
    }

    /**
     * @return collection of the non-system keyspaces (note that this count as system only the
     * non replicated keyspaces, so keyspace like system_traces which are replicated are actually
     * returned. See getUserKeyspace() below if you don't want those)
     */
    public ImmutableList<String> getNonSystemKeyspaces()
    {
        return ImmutableList.copyOf(getNonSystemKeyspacesSet());
    }

    /**
     * @return a collection of keyspaces that do not use LocalStrategy for replication
     */
    public List<String> getNonLocalStrategyKeyspaces()
    {
        return keyspaces.stream()
                        .filter(keyspace -> keyspace.params.replication.klass != LocalStrategy.class)
                        .map(keyspace -> keyspace.name)
                        .collect(Collectors.toList());
    }

    /**
     * @return collection of the user defined keyspaces
     */
    public List<String> getUserKeyspaces()
    {
        return ImmutableList.copyOf(Sets.difference(getNonSystemKeyspacesSet(), SchemaConstants.REPLICATED_SYSTEM_KEYSPACE_NAMES));
    }

    /**
     * Get metadata about keyspace inner ColumnFamilies
     *
     * @param keyspaceName The name of the keyspace
     *
     * @return metadata about ColumnFamilies the belong to the given keyspace
     */
    public Iterable<TableMetadata> getTablesAndViews(String keyspaceName)
    {
        assert keyspaceName != null;
        KeyspaceMetadata ksm = keyspaces.getNullable(keyspaceName);
        assert ksm != null;
        return ksm.tablesAndViews();
    }

    /**
     * @return collection of the all keyspace names registered in the system (system and non-system)
     */
    public Set<String> getKeyspaces()
    {
        return keyspaces.names();
    }

    /* TableMetadata/Ref query/control methods */

    /**
     * Given a keyspace name and table/view name, get the table metadata
     * reference. If the keyspace name or table/view name is not present
     * this method returns null.
     *
     * @return TableMetadataRef object or null if it wasn't found
     */
    public TableMetadataRef getTableMetadataRef(String keyspace, String table)
    {
        TableMetadata tm = getTableMetadata(keyspace, table);
        return tm == null
             ? null
             : metadataRefs.get(tm.id);
    }

    public TableMetadataRef getIndexTableMetadataRef(String keyspace, String index)
    {
        return indexMetadataRefs.get(Pair.create(keyspace, index));
    }

    Map<Pair<String, String>, TableMetadataRef> getIndexTableMetadataRefs()
    {
        return indexMetadataRefs;
    }

    /**
     * Get Table metadata by its identifier
     *
     * @param id table or view identifier
     *
     * @return metadata about Table or View
     */
    public TableMetadataRef getTableMetadataRef(TableId id)
    {
        return metadataRefs.get(id);
    }

    public TableMetadataRef getTableMetadataRef(Descriptor descriptor)
    {
        return getTableMetadataRef(descriptor.ksname, descriptor.cfname);
    }

    Map<TableId, TableMetadataRef> getTableMetadataRefs()
    {
        return metadataRefs;
    }

    /**
     * Given a keyspace name and table name, get the table
     * meta data. If the keyspace name or table name is not valid
     * this function returns null.
     *
     * @param keyspace The keyspace name
     * @param table The table name
     *
     * @return TableMetadata object or null if it wasn't found
     */
    public TableMetadata getTableMetadata(String keyspace, String table)
    {
        assert keyspace != null;
        assert table != null;

        KeyspaceMetadata ksm = getKeyspaceMetadata(keyspace);
        return ksm == null
             ? null
             : ksm.getTableOrViewNullable(table);
    }

    @Nullable
    public TableMetadata getTableMetadata(TableId id)
    {
        TableMetadata table = keyspaces.getTableOrViewNullable(id);
        return null != table ? table : VirtualKeyspaceRegistry.instance.getTableMetadataNullable(id);
    }

    public TableMetadata validateTable(String keyspaceName, String tableName)
    {
        if (tableName.isEmpty())
            throw new InvalidRequestException("non-empty table is required");

        KeyspaceMetadata keyspace = getKeyspaceMetadata(keyspaceName);
        if (keyspace == null)
            throw new KeyspaceNotDefinedException(format("keyspace %s does not exist", keyspaceName));

        TableMetadata metadata = keyspace.getTableOrViewNullable(tableName);
        if (metadata == null)
            throw new InvalidRequestException(format("table %s does not exist", tableName));

        return metadata;
    }

    public TableMetadata getTableMetadata(Descriptor descriptor)
    {
        return getTableMetadata(descriptor.ksname, descriptor.cfname);
    }

    /**
     * @throws UnknownTableException if the table couldn't be found in the metadata
     */
    public TableMetadata getExistingTableMetadata(TableId id) throws UnknownTableException
    {
        TableMetadata metadata = getTableMetadata(id);
        if (metadata != null)
            return metadata;

        String message =
            String.format("Couldn't find table with id %s. If a table was just created, this is likely due to the schema"
                          + "not being fully propagated.  Please wait for schema agreement on table creation.",
                          id);
        throw new UnknownTableException(message, id);
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
            throw new IllegalArgumentException(String.format("Function name must be fully qualified: got %s", name));

        KeyspaceMetadata ksm = getKeyspaceMetadata(name.keyspace);
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

        KeyspaceMetadata ksm = getKeyspaceMetadata(name.keyspace);
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
     * Checks whether the given schema version is the same as the current local schema.
     */
    public boolean isSameVersion(UUID schemaVersion)
    {
        return schemaVersion != null && schemaVersion.equals(version);
    }

    /**
     * Checks whether the current schema is empty.
     */
    public boolean isEmpty()
    {
        return SchemaConstants.emptyVersion.equals(version);
    }

    /**
     * Read schema from system keyspace and calculate MD5 digest of every row, resulting digest
     * will be converted into UUID which would act as content-based version of the schema.
     */
    public void updateVersion()
    {
        version = SchemaKeyspace.calculateSchemaDigest();
        SystemKeyspace.updateSchemaVersion(version);
        SchemaDiagnostics.versionUpdated(this);
    }

    /*
     * Like updateVersion, but also announces via gossip
     */
    public void updateVersionAndAnnounce()
    {
        updateVersion();
        passiveAnnounceVersion();
    }

    /**
     * Announce my version passively over gossip.
     * Used to notify nodes as they arrive in the cluster.
     */
    private void passiveAnnounceVersion()
    {
        Gossiper.instance.addLocalApplicationState(ApplicationState.SCHEMA, StorageService.instance.valueFactory.schema(version));
        SchemaDiagnostics.versionAnnounced(this);
    }

    /**
     * Clear all KS/CF metadata and reset version.
     */
    public synchronized void clear()
    {
        getNonSystemKeyspaces().forEach(k -> unload(getKeyspaceMetadata(k)));
        updateVersionAndAnnounce();
        SchemaDiagnostics.schemataCleared(this);
    }

    /*
     * Reload schema from local disk. Useful if a user made changes to schema tables by hand, or has suspicion that
     * in-memory representation got out of sync somehow with what's on disk.
     */
    public synchronized void reloadSchemaAndAnnounceVersion()
    {
        Keyspaces before = keyspaces.filter(k -> !SchemaConstants.isLocalSystemKeyspace(k.name));
        Keyspaces after = SchemaKeyspace.fetchNonSystemKeyspaces();
        merge(Keyspaces.diff(before, after));
        updateVersionAndAnnounce();
    }

    /**
     * Merge remote schema in form of mutations with local and mutate ks/cf metadata objects
     * (which also involves fs operations on add/drop ks/cf)
     *
     * @param mutations the schema changes to apply
     *
     * @throws ConfigurationException If one of metadata attributes has invalid value
     */
    synchronized void mergeAndAnnounceVersion(Collection<Mutation> mutations)
    {
        merge(mutations);
        updateVersionAndAnnounce();
    }

    public synchronized TransformationResult transform(SchemaTransformation transformation, boolean locally, long now)
    {
        KeyspacesDiff diff;
        try
        {
            Keyspaces before = keyspaces;
            Keyspaces after = transformation.apply(before);
            diff = Keyspaces.diff(before, after);
        }
        catch (RuntimeException e)
        {
            return new TransformationResult(e);
        }

        if (diff.isEmpty())
            return new TransformationResult(diff, Collections.emptyList());

        Collection<Mutation> mutations = SchemaKeyspace.convertSchemaDiffToMutations(diff, now);
        SchemaKeyspace.applyChanges(mutations);

        merge(diff);
        updateVersion();
        if (!locally)
            passiveAnnounceVersion();

        return new TransformationResult(diff, mutations);
    }

    public static final class TransformationResult
    {
        public final boolean success;
        public final RuntimeException exception;
        public final KeyspacesDiff diff;
        public final Collection<Mutation> mutations;

        private TransformationResult(boolean success, RuntimeException exception, KeyspacesDiff diff, Collection<Mutation> mutations)
        {
            this.success = success;
            this.exception = exception;
            this.diff = diff;
            this.mutations = mutations;
        }

        TransformationResult(RuntimeException exception)
        {
            this(false, exception, null, null);
        }

        TransformationResult(KeyspacesDiff diff, Collection<Mutation> mutations)
        {
            this(true, null, diff, mutations);
        }
    }

    synchronized void merge(Collection<Mutation> mutations)
    {
        // only compare the keyspaces affected by this set of schema mutations
        Set<String> affectedKeyspaces = SchemaKeyspace.affectedKeyspaces(mutations);

        // fetch the current state of schema for the affected keyspaces only
        Keyspaces before = keyspaces.filter(k -> affectedKeyspaces.contains(k.name));

        // apply the schema mutations
        SchemaKeyspace.applyChanges(mutations);

        // apply the schema mutations and fetch the new versions of the altered keyspaces
        Keyspaces after = SchemaKeyspace.fetchKeyspaces(affectedKeyspaces);

        merge(Keyspaces.diff(before, after));
    }

    private void merge(KeyspacesDiff diff)
    {
        diff.dropped.forEach(this::dropKeyspace);
        diff.created.forEach(this::createKeyspace);
        diff.altered.forEach(this::alterKeyspace);
    }

    private void alterKeyspace(KeyspaceDiff delta)
    {
        SchemaDiagnostics.keyspaceAltering(this, delta);

        // drop tables and views
        delta.views.dropped.forEach(this::dropView);
        delta.tables.dropped.forEach(this::dropTable);

        load(delta.after);

        // add tables and views
        delta.tables.created.forEach(this::createTable);
        delta.views.created.forEach(this::createView);

        // update tables and views
        delta.tables.altered.forEach(diff -> alterTable(diff.after));
        delta.views.altered.forEach(diff -> alterView(diff.after));

        // deal with all added, and altered views
        Keyspace.open(delta.after.name).viewManager.reload(true);

        // notify on everything dropped
        delta.udas.dropped.forEach(uda -> notifyDropAggregate((UDAggregate) uda));
        delta.udfs.dropped.forEach(udf -> notifyDropFunction((UDFunction) udf));
        delta.views.dropped.forEach(this::notifyDropView);
        delta.tables.dropped.forEach(this::notifyDropTable);
        delta.types.dropped.forEach(this::notifyDropType);

        // notify on everything created
        delta.types.created.forEach(this::notifyCreateType);
        delta.tables.created.forEach(this::notifyCreateTable);
        delta.views.created.forEach(this::notifyCreateView);
        delta.udfs.created.forEach(udf -> notifyCreateFunction((UDFunction) udf));
        delta.udas.created.forEach(uda -> notifyCreateAggregate((UDAggregate) uda));

        // notify on everything altered
        if (!delta.before.params.equals(delta.after.params))
            notifyAlterKeyspace(delta.before, delta.after);
        delta.types.altered.forEach(diff -> notifyAlterType(diff.before, diff.after));
        delta.tables.altered.forEach(diff -> notifyAlterTable(diff.before, diff.after));
        delta.views.altered.forEach(diff -> notifyAlterView(diff.before, diff.after));
        delta.udfs.altered.forEach(diff -> notifyAlterFunction(diff.before, diff.after));
        delta.udas.altered.forEach(diff -> notifyAlterAggregate(diff.before, diff.after));
        SchemaDiagnostics.keyspaceAltered(this, delta);
    }

    private void createKeyspace(KeyspaceMetadata keyspace)
    {
        SchemaDiagnostics.keyspaceCreating(this, keyspace);
        load(keyspace);
        Keyspace.open(keyspace.name);

        notifyCreateKeyspace(keyspace);
        keyspace.types.forEach(this::notifyCreateType);
        keyspace.tables.forEach(this::notifyCreateTable);
        keyspace.views.forEach(this::notifyCreateView);
        keyspace.functions.udfs().forEach(this::notifyCreateFunction);
        keyspace.functions.udas().forEach(this::notifyCreateAggregate);
        SchemaDiagnostics.keyspaceCreated(this, keyspace);
    }

    private void dropKeyspace(KeyspaceMetadata keyspace)
    {
        SchemaDiagnostics.keyspaceDroping(this, keyspace);
        keyspace.views.forEach(this::dropView);
        keyspace.tables.forEach(this::dropTable);

        // remove the keyspace from the static instances.
        Keyspace.clear(keyspace.name);
        unload(keyspace);
        Keyspace.writeOrder.awaitNewBarrier();

        keyspace.functions.udas().forEach(this::notifyDropAggregate);
        keyspace.functions.udfs().forEach(this::notifyDropFunction);
        keyspace.views.forEach(this::notifyDropView);
        keyspace.tables.forEach(this::notifyDropTable);
        keyspace.types.forEach(this::notifyDropType);
        notifyDropKeyspace(keyspace);
        SchemaDiagnostics.keyspaceDroped(this, keyspace);
    }

    private void dropView(ViewMetadata metadata)
    {
        Keyspace.open(metadata.keyspace()).viewManager.dropView(metadata.name());
        dropTable(metadata.metadata);
    }

    private void dropTable(TableMetadata metadata)
    {
        SchemaDiagnostics.tableDropping(this, metadata);
        ColumnFamilyStore cfs = Keyspace.open(metadata.keyspace).getColumnFamilyStore(metadata.name);
        assert cfs != null;
        // make sure all the indexes are dropped, or else.
        cfs.indexManager.markAllIndexesRemoved();
        CompactionManager.instance.interruptCompactionFor(Collections.singleton(metadata), (sstable) -> true, true);
        if (DatabaseDescriptor.isAutoSnapshot())
            cfs.snapshot(Keyspace.getTimestampedSnapshotNameWithPrefix(cfs.name, ColumnFamilyStore.SNAPSHOT_DROP_PREFIX));
        CommitLog.instance.forceRecycleAllSegments(Collections.singleton(metadata.id));
        Keyspace.open(metadata.keyspace).dropCf(metadata.id);
        SchemaDiagnostics.tableDropped(this, metadata);
    }

    private void createTable(TableMetadata table)
    {
        SchemaDiagnostics.tableCreating(this, table);
        Keyspace.open(table.keyspace).initCf(metadataRefs.get(table.id), true);
        SchemaDiagnostics.tableCreated(this, table);
    }

    private void createView(ViewMetadata view)
    {
        Keyspace.open(view.keyspace()).initCf(metadataRefs.get(view.metadata.id), true);
    }

    private void alterTable(TableMetadata updated)
    {
        SchemaDiagnostics.tableAltering(this, updated);
        Keyspace.open(updated.keyspace).getColumnFamilyStore(updated.name).reload();
        SchemaDiagnostics.tableAltered(this, updated);
    }

    private void alterView(ViewMetadata updated)
    {
        Keyspace.open(updated.keyspace()).getColumnFamilyStore(updated.name()).reload();
    }

    private void notifyCreateKeyspace(KeyspaceMetadata ksm)
    {
        changeListeners.forEach(l -> l.onCreateKeyspace(ksm.name));
    }

    private void notifyCreateTable(TableMetadata metadata)
    {
        changeListeners.forEach(l -> l.onCreateTable(metadata.keyspace, metadata.name));
    }

    private void notifyCreateView(ViewMetadata view)
    {
        changeListeners.forEach(l -> l.onCreateView(view.keyspace(), view.name()));
    }

    private void notifyCreateType(UserType ut)
    {
        changeListeners.forEach(l -> l.onCreateType(ut.keyspace, ut.getNameAsString()));
    }

    private void notifyCreateFunction(UDFunction udf)
    {
        changeListeners.forEach(l -> l.onCreateFunction(udf.name().keyspace, udf.name().name, udf.argTypes()));
    }

    private void notifyCreateAggregate(UDAggregate udf)
    {
        changeListeners.forEach(l -> l.onCreateAggregate(udf.name().keyspace, udf.name().name, udf.argTypes()));
    }

    private void notifyAlterKeyspace(KeyspaceMetadata before, KeyspaceMetadata after)
    {
        changeListeners.forEach(l -> l.onAlterKeyspace(after.name));
    }

    private void notifyAlterTable(TableMetadata before, TableMetadata after)
    {
        boolean changeAffectedPreparedStatements = before.changeAffectsPreparedStatements(after);
        changeListeners.forEach(l -> l.onAlterTable(after.keyspace, after.name, changeAffectedPreparedStatements));
    }

    private void notifyAlterView(ViewMetadata before, ViewMetadata after)
    {
        boolean changeAffectedPreparedStatements = before.metadata.changeAffectsPreparedStatements(after.metadata);
        changeListeners.forEach(l ->l.onAlterView(after.keyspace(), after.name(), changeAffectedPreparedStatements));
    }

    private void notifyAlterType(UserType before, UserType after)
    {
        changeListeners.forEach(l -> l.onAlterType(after.keyspace, after.getNameAsString()));
    }

    private void notifyAlterFunction(UDFunction before, UDFunction after)
    {
        changeListeners.forEach(l -> l.onAlterFunction(after.name().keyspace, after.name().name, after.argTypes()));
    }

    private void notifyAlterAggregate(UDAggregate before, UDAggregate after)
    {
        changeListeners.forEach(l -> l.onAlterAggregate(after.name().keyspace, after.name().name, after.argTypes()));
    }

    private void notifyDropKeyspace(KeyspaceMetadata ksm)
    {
        changeListeners.forEach(l -> l.onDropKeyspace(ksm.name));
    }

    private void notifyDropTable(TableMetadata metadata)
    {
        changeListeners.forEach(l -> l.onDropTable(metadata.keyspace, metadata.name));
    }

    private void notifyDropView(ViewMetadata view)
    {
        changeListeners.forEach(l -> l.onDropView(view.keyspace(), view.name()));
    }

    private void notifyDropType(UserType ut)
    {
        changeListeners.forEach(l -> l.onDropType(ut.keyspace, ut.getNameAsString()));
    }

    private void notifyDropFunction(UDFunction udf)
    {
        changeListeners.forEach(l -> l.onDropFunction(udf.name().keyspace, udf.name().name, udf.argTypes()));
    }

    private void notifyDropAggregate(UDAggregate udf)
    {
        changeListeners.forEach(l -> l.onDropAggregate(udf.name().keyspace, udf.name().name, udf.argTypes()));
    }


    /**
     * Converts the given schema version to a string. Returns {@code unknown}, if {@code version} is {@code null}
     * or {@code "(empty)"}, if {@code version} refers to an {@link SchemaConstants#emptyVersion empty) schema.
     */
    public static String schemaVersionToString(UUID version)
    {
        return version == null
               ? "unknown"
               : SchemaConstants.emptyVersion.equals(version)
                 ? "(empty)"
                 : version.toString();
    }
}
