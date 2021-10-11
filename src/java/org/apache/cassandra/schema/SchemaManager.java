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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MapDifference;
import org.apache.commons.lang3.ObjectUtils;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import org.apache.cassandra.cql3.functions.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.schema.KeyspaceMetadata.KeyspaceDiff;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.schema.SchemaTransformation.SchemaTransformationResult;
import org.apache.cassandra.service.StorageService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;

import static com.google.common.collect.Iterables.size;
import static org.apache.cassandra.config.DatabaseDescriptor.isDaemonInitialized;
import static org.apache.cassandra.config.DatabaseDescriptor.isToolInitialized;

public final class SchemaManager implements SchemaProvider
{
    private static final Logger logger = LoggerFactory.getLogger(SchemaManager.class);

    public static final String FORCE_LOAD_LOCAL_KEYSPACES_PROP = "cassandra.schema.force_load_local_keyspaces";
    private static final boolean FORCE_LOAD_LOCAL_KEYSPACES = Boolean.getBoolean(FORCE_LOAD_LOCAL_KEYSPACES_PROP);

    public static final SchemaManager instance = new SchemaManager();

    private volatile Keyspaces sharedKeyspaces = Keyspaces.none();

    private final LocalKeyspaces localKeyspaces;

    private final TableMetadataRefCache tableMetadataRefCache = new TableMetadataRefCache();

    // Keyspace objects, one per keyspace. Only one instance should ever exist for any given keyspace.
    // We operate on futures because we need to achieve atomic initialization with at-most-once semantics for
    // loadFunction. Although it seems that this is a valid case for using ConcurrentHashMap.computeIfAbsent,
    // we should not use it because we have no knowledge about the loadFunction and in fact that load function may
    // do some nested calls to getOrCreateKeyspaceInstance, also using different threads, and in a blocking manner.
    // This may lead to a deadlock. The documentation of ConcurrentHashMap says that manipulating other keys inside
    // the lambda passed to the computeIfAbsent method is prohibited.
    private final ConcurrentMap<String, CompletableFuture<Keyspace>> keyspaceInstances = new NonBlockingHashMap<>();

    private volatile UUID version;

    private final SchemaChangeNotifier schemaChangeNotifier = new SchemaChangeNotifier();

    /**
     * Initialize empty schema object and load the hardcoded system tables
     */
    private SchemaManager()
    {
        this.localKeyspaces = new LocalKeyspaces(FORCE_LOAD_LOCAL_KEYSPACES || isDaemonInitialized() || isToolInitialized());
        this.localKeyspaces.getAll().forEach(this::loadNew);
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
        SchemaDiagnostics.schemaLoading(this);
        SchemaKeyspace.fetchNonSystemKeyspaces().forEach(this::load);
        if (updateVersion)
            updateVersion();
        SchemaDiagnostics.schemaLoaded(this);
    }

    /**
     * Update (or insert) new keyspace definition
     *
     * @param ksm The metadata about keyspace
     */
    synchronized public void load(KeyspaceMetadata ksm)
    {
        Preconditions.checkArgument(!SchemaConstants.isLocalSystemKeyspace(ksm.name));
        KeyspaceMetadata previous = sharedKeyspaces.getNullable(ksm.name);

        if (previous == null)
            loadNew(ksm);
        else
            reload(previous, ksm);

        sharedKeyspaces = sharedKeyspaces.withAddedOrUpdated(ksm);
    }

    private void loadNew(KeyspaceMetadata ksm)
    {
        tableMetadataRefCache.addNewRefs(ksm);

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

        tableMetadataRefCache.updateRefs(previous, updated);

        SchemaDiagnostics.metadataReloaded(this, previous, updated, tablesDiff, viewsDiff, indexesDiff);
    }

    public void registerListener(SchemaChangeListener listener)
    {
        schemaChangeNotifier.registerListener(listener);
    }

    @SuppressWarnings("unused")
    public void unregisterListener(SchemaChangeListener listener)
    {
        schemaChangeNotifier.unregisterListener(listener);
    }

    /**
     * Get keyspace instance by name
     *
     * @param keyspaceName The name of the keyspace
     * @return Keyspace object or null if keyspace was not found, or if the keyspace has not completed construction yet
     */
    @Override
    public Keyspace getKeyspaceInstance(String keyspaceName)
    {
        CompletableFuture<Keyspace> future = keyspaceInstances.get(keyspaceName);
        if (future != null && future.isDone())
            return future.join();
        else
            return null;
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

    @Override
    public Keyspace getOrCreateKeyspaceInstance(String keyspaceName, Supplier<Keyspace> loadFunction)
    {
        CompletableFuture<Keyspace> future = keyspaceInstances.get(keyspaceName);
        if (future == null)
        {
            CompletableFuture<Keyspace> empty = new CompletableFuture<>();
            future = keyspaceInstances.putIfAbsent(keyspaceName, empty);
            if (future == null)
            {
                // We managed to create an entry for the keyspace. Now initialize it.
                future = empty;
                try
                {
                    empty.complete(loadFunction.get());
                }
                catch (Throwable t)
                {
                    empty.completeExceptionally(new Throwable(t));
                    // Remove future so that construction can be retried later
                    keyspaceInstances.remove(keyspaceName, future);
                }
            }
            // Else some other thread beat us to it, but we now have the reference to the future which we can wait for.
        }

        // Most of the time the keyspace will be ready and this will complete immediately. If it is being created
        // concurrently, wait for that process to complete.
        return future.join();
    }

    /**
     * Remove keyspace from schema. This puts a temporary entry in the map that throws an exception when queried.
     * When the metadata is also deleted, that temporary entry must also be deleted using clearKeyspaceInstance below.
     *
     * @param keyspaceName The name of the keyspace to remove
     */
    private void removeKeyspaceInstance(String keyspaceName, Consumer<Keyspace> unloadFunction)
    {
        CompletableFuture<Keyspace> droppedFuture = new CompletableFuture<>();
        droppedFuture.completeExceptionally(new KeyspaceNotDefinedException(keyspaceName));

        CompletableFuture<Keyspace> existingFuture = keyspaceInstances.put(keyspaceName, droppedFuture);
        if (existingFuture == null || existingFuture.isCompletedExceptionally())
            return;

        Keyspace instance = existingFuture.join();
        unloadFunction.accept(instance);

        CompletableFuture<Keyspace> future = keyspaceInstances.remove(keyspaceName);
        assert future == droppedFuture;
    }

    /**
     * @deprecated use {@link #sharedAndLocalKeyspaces()}
     */
    @Deprecated
    public Keyspaces snapshot()
    {
        return sharedAndLocalKeyspaces();
    }

    public Keyspaces sharedAndLocalKeyspaces()
    {
        return Keyspaces.builder().add(localKeyspaces.getAll()).add(sharedKeyspaces).build();
    }

    public Keyspaces sharedKeyspaces()
    {
        return sharedKeyspaces;
    }

    /**
     * Remove keyspace definition from system
     *
     * @param ksm The keyspace definition to remove
     */
    synchronized void unload(KeyspaceMetadata ksm)
    {
        sharedKeyspaces = sharedKeyspaces.without(ksm.name);

        tableMetadataRefCache.removeRefs(ksm);

        SchemaDiagnostics.metadataRemoved(this, ksm);
    }

    public int getNumberOfTables()
    {
        return snapshot().stream().mapToInt(k -> size(k.tablesAndViews())).sum();
    }

    public ViewMetadata getView(String keyspaceName, String viewName)
    {
        assert keyspaceName != null;
        KeyspaceMetadata ksm = snapshot().getNullable(keyspaceName);
        return (ksm == null) ? null : ksm.views.getNullable(viewName);
    }

    /**
     * Get metadata about keyspace by its name
     *
     * @param keyspaceName The name of the keyspace
     *
     * @return The keyspace metadata or null if it wasn't found
     */
    @Override
    public KeyspaceMetadata getKeyspaceMetadata(String keyspaceName)
    {
        assert keyspaceName != null;
        KeyspaceMetadata keyspace = snapshot().getNullable(keyspaceName);
        return null != keyspace ? keyspace : VirtualKeyspaceRegistry.instance.getKeyspaceMetadataNullable(keyspaceName);
    }

    /**
     * Returns all non-local keyspaces, that is, all but {@link SchemaConstants#LOCAL_SYSTEM_KEYSPACE_NAMES}
     * or virtual keyspaces.
     * @deprecated use {@link #sharedKeyspaces()}
     */
    @Deprecated
    public Keyspaces getNonSystemKeyspaces()
    {
        return sharedKeyspaces;
    }

    /**
     * Returns all non-local keyspaces whose replication strategy is not {@link LocalStrategy}.
     */
    public Keyspaces getNonLocalStrategyKeyspaces()
    {
        return sharedKeyspaces.filter(keyspace -> keyspace.params.replication.klass != LocalStrategy.class);
    }

    /**
     * Returns keyspaces that partition data across the ring.
     */
    public Keyspaces getPartitionedKeyspaces()
    {
        return sharedKeyspaces.filter(keyspace -> Keyspace.open(keyspace.name).getReplicationStrategy().isPartitioned());
    }

    /**
     * Returns user keyspaces, that is all but {@link SchemaConstants#LOCAL_SYSTEM_KEYSPACE_NAMES},
     * {@link SchemaConstants#REPLICATED_SYSTEM_KEYSPACE_NAMES} or virtual keyspaces.
     */
    public Keyspaces getUserKeyspaces()
    {
        return sharedKeyspaces.without(SchemaConstants.REPLICATED_SYSTEM_KEYSPACE_NAMES);
    }

    /**
     * Returns non-internal keyspaces
     */
    public Keyspaces getNonInternalKeyspaces()
    {
        return getUserKeyspaces().filter(ks -> !SchemaConstants.isInternalKeyspace(ks.name));
    }

    /**
     * Get metadata about keyspace inner ColumnFamilies
     *
     * @param keyspaceName The name of the keyspace
     * @return metadata about ColumnFamilies the belong to the given keyspace
     */
    public Iterable<TableMetadata> getTablesAndViews(String keyspaceName)
    {
        Preconditions.checkNotNull(keyspaceName);
        KeyspaceMetadata ksm = ObjectUtils.getFirstNonNull(() -> sharedKeyspaces.getNullable(keyspaceName),
                                                           () -> localKeyspaces.get(keyspaceName));
        Preconditions.checkNotNull(ksm, "Keyspace %s not found", keyspaceName);
        return ksm.tablesAndViews();
    }

    /**
     * @return collection of the all keyspace names registered in the system (system and non-system)
     */
    public ImmutableSet<String> getKeyspaces()
    {
        return snapshot().names();
    }

    public Keyspaces getLocalKeyspaces()
    {
        return Keyspaces.builder().add(localKeyspaces.getAll()).build();
    }

    /* TableMetadata/Ref query/control methods */

    /**
     * Given a keyspace name and table/view name, get the table metadata
     * reference. If the keyspace name or table/view name is not present
     * this method returns null.
     *
     * @return TableMetadataRef object or null if it wasn't found
     */
    @Override
    public TableMetadataRef getTableMetadataRef(String keyspace, String table)
    {
        return tableMetadataRefCache.getTableMetadataRef(keyspace, table);
    }

    public TableMetadataRef getIndexTableMetadataRef(String keyspace, String index)
    {
        return tableMetadataRefCache.getIndexTableMetadataRef(keyspace, index);
    }

    /**
     * Get Table metadata by its identifier
     *
     * @param id table or view identifier
     * @return metadata about Table or View
     */
    @Override
    public TableMetadataRef getTableMetadataRef(TableId id)
    {
        return tableMetadataRefCache.getTableMetadataRef(id);
    }

    @Override
    public TableMetadataRef getTableMetadataRef(Descriptor descriptor)
    {
        return getTableMetadataRef(descriptor.ksname, descriptor.cfname);
    }

    /**
     * Given a keyspace name and table name, get the table
     * meta data. If the keyspace name or table name is not valid
     * this function returns null.
     *
     * @param keyspace The keyspace name
     * @param table    The table name
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

    @Override
    public TableMetadata getTableMetadata(TableId id)
    {
        return ObjectUtils.getFirstNonNull(() -> sharedKeyspaces.getTableOrViewNullable(id),
                                           () -> localKeyspaces.getTableOrView(id),
                                           () -> VirtualKeyspaceRegistry.instance.getTableMetadataNullable(id));
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
     * @param name     fully qualified function name
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
        getNonSystemKeyspaces().forEach(this::unload);
        updateVersionAndAnnounce();
        SchemaDiagnostics.schemaCleared(this);
    }

    /*
     * Reload schema from local disk. Useful if a user made changes to schema tables by hand, or has suspicion that
     * in-memory representation got out of sync somehow with what's on disk.
     */
    public synchronized void reloadSchemaAndAnnounceVersion()
    {
        Keyspaces before = snapshot().filter(k -> !SchemaConstants.isLocalSystemKeyspace(k.name));
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
    public synchronized void mergeAndAnnounceVersion(Collection<Mutation> mutations)
    {
        merge(mutations);
        updateVersionAndAnnounce();
    }

    public synchronized SchemaTransformationResult transform(SchemaTransformation transformation, boolean locally, long now)
    {
        KeyspacesDiff diff;
        Keyspaces before = snapshot();
        Keyspaces after = transformation.apply(before);
        diff = Keyspaces.diff(before, after);

        if (diff.isEmpty())
            return new SchemaTransformationResult(new SharedSchema(before), new SharedSchema(after), diff, Collections.emptyList());

        Collection<Mutation> mutations = SchemaKeyspace.convertSchemaDiffToMutations(diff, now);
        SchemaKeyspace.applyChanges(mutations);

        merge(diff);
        updateVersion();
        if (!locally)
            passiveAnnounceVersion();

        return new SchemaTransformationResult(new SharedSchema(before), new SharedSchema(after), diff, mutations);
    }

    synchronized void merge(Collection<Mutation> mutations)
    {
        // only compare the keyspaces affected by this set of schema mutations
        Set<String> affectedKeyspaces = SchemaKeyspace.affectedKeyspaces(mutations);

        // fetch the current state of schema for the affected keyspaces only
        Keyspaces before = snapshot().filter(k -> affectedKeyspaces.contains(k.name));

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

        boolean initialized = Keyspace.isInitialized();

        Keyspace keyspace = initialized ? getKeyspaceInstance(delta.before.name) : null;
        if (initialized)
        {
            assert keyspace != null;
            assert delta.before.name.equals(delta.after.name);

            // drop tables and views
            delta.views.dropped.forEach(v -> dropView(keyspace, v));
            delta.tables.dropped.forEach(t -> dropTable(keyspace, t));
        }

        load(delta.after);

        if (initialized)
        {
            // add tables and views
            delta.tables.created.forEach(t -> createTable(keyspace, t));
            delta.views.created.forEach(v -> createView(keyspace, v));

            // update tables and views
            delta.tables.altered.forEach(diff -> alterTable(keyspace, diff.after));
            delta.views.altered.forEach(diff -> alterView(keyspace, diff.after));

            // deal with all added, and altered views
            Keyspace.open(delta.after.name).viewManager.reload(true);
        }

        schemaChangeNotifier.notifyKeyspaceAltered(delta);
        SchemaDiagnostics.keyspaceAltered(this, delta);
    }

    private void createKeyspace(KeyspaceMetadata keyspace)
    {
        SchemaDiagnostics.keyspaceCreating(this, keyspace);
        load(keyspace);
        if (Keyspace.isInitialized())
        {
            Keyspace.open(keyspace.name);
        }

        schemaChangeNotifier.notifyKeyspaceCreated(keyspace);
        SchemaDiagnostics.keyspaceCreated(this, keyspace);
    }

    private void dropKeyspace(KeyspaceMetadata keyspace)
    {
        SchemaDiagnostics.keyspaceDropping(this, keyspace);

        boolean initialized = Keyspace.isInitialized();
        Keyspace ks = initialized ? getKeyspaceInstance(keyspace.name) : null;
        if (initialized)
        {
            if (ks == null)
                return;

            keyspace.views.forEach(v -> dropView(ks, v));
            keyspace.tables.forEach(t -> dropTable(ks, t));

            // remove the keyspace from the static instances
            removeKeyspaceInstance(keyspace.name, Keyspace::unload);
        }

        unload(keyspace);

        if (initialized)
        {
            Keyspace.writeOrder.awaitNewBarrier();
        }

        schemaChangeNotifier.notifyKeyspaceDropped(keyspace);
        SchemaDiagnostics.keyspaceDropped(this, keyspace);
    }

    private void dropView(Keyspace keyspace, ViewMetadata metadata)
    {
        keyspace.viewManager.dropView(metadata.name());
        dropTable(keyspace, metadata.metadata);
    }

    /**
     *
     * @param keyspace
     * @param metadata
     */
    private void dropTable(Keyspace keyspace, TableMetadata metadata)
    {
        SchemaDiagnostics.tableDropping(this, metadata);
        keyspace.dropCf(metadata.id);
        SchemaDiagnostics.tableDropped(this, metadata);
    }

    private void createTable(Keyspace keyspace, TableMetadata table)
    {
        SchemaDiagnostics.tableCreating(this, table);
        keyspace.initCf(tableMetadataRefCache.getTableMetadataRef(table.id), true);
        SchemaDiagnostics.tableCreated(this, table);
    }

    private void createView(Keyspace keyspace, ViewMetadata view)
    {
        SchemaDiagnostics.tableCreating(this, view.metadata);
        keyspace.initCf(tableMetadataRefCache.getTableMetadataRef(view.metadata.id), true);
        SchemaDiagnostics.tableCreated(this, view.metadata);
    }

    private void alterTable(Keyspace keyspace, TableMetadata updated)
    {
        SchemaDiagnostics.tableAltering(this, updated);
        keyspace.getColumnFamilyStore(updated.name).reload();
        SchemaDiagnostics.tableAltered(this, updated);
    }

    private void alterView(Keyspace keyspace, ViewMetadata updated)
    {
        SchemaDiagnostics.tableAltering(this, updated.metadata);
        keyspace.getColumnFamilyStore(updated.name()).reload();
        SchemaDiagnostics.tableAltered(this, updated.metadata);
    }
}
