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

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.UserFunction;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.KeyspaceNotDefinedException;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.schema.KeyspaceMetadata.KeyspaceDiff;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.schema.SchemaTransformation.SchemaTransformationResult;
import org.apache.cassandra.service.PendingRangeCalculatorService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Awaitable;
import org.apache.cassandra.utils.concurrent.LoadingMap;

import static com.google.common.collect.Iterables.size;
import static java.lang.String.format;
import static org.apache.cassandra.config.DatabaseDescriptor.isDaemonInitialized;
import static org.apache.cassandra.config.DatabaseDescriptor.isToolInitialized;

/**
 * Manages shared schema, keyspace instances and table metadata refs. Provides methods to initialize, modify and query
 * both the shared and local schema, as well as to register listeners.
 * <p>
 * This class should be the only entity used to query and manage schema. Internal details should not be access in
 * production code (would be great if they were not accessed in the test code as well).
 * <p>
 * TL;DR: All modifications are made using the implementation of {@link SchemaUpdateHandler} obtained from the provided
 * factory. After each modification, the internally managed table metadata refs and keyspaces instances are updated and
 * notifications are sent to the registered listeners.
 * When the schema change is applied by the update handler (regardless it is initiated locally or received from outside),
 * the registered callback is executed which performs the remaining updates for tables metadata refs and keyspace
 * instances (see {@link #mergeAndUpdateVersion(SchemaTransformationResult, boolean)}).
 */
public class Schema implements SchemaProvider
{
    private static final Logger logger = LoggerFactory.getLogger(Schema.class);

    public static final Schema instance = new Schema();

    private volatile Keyspaces distributedKeyspaces = Keyspaces.none();

    private final Keyspaces localKeyspaces;

    private volatile TableMetadataRefCache tableMetadataRefCache = TableMetadataRefCache.EMPTY;

    // Keyspace objects, one per keyspace. Only one instance should ever exist for any given keyspace.
    // We operate on loading map because we need to achieve atomic initialization with at-most-once semantics for
    // loadFunction. Although it seems that this is a valid case for using ConcurrentHashMap.computeIfAbsent,
    // we should not use it because we have no knowledge about the loadFunction and in fact that load function may
    // do some nested calls to maybeAddKeyspaceInstance, also using different threads, and in a blocking manner.
    // This may lead to a deadlock. The documentation of ConcurrentHashMap says that manipulating other keys inside
    // the lambda passed to the computeIfAbsent method is prohibited.
    private final LoadingMap<String, Keyspace> keyspaceInstances = new LoadingMap<>();

    private volatile UUID version = SchemaConstants.emptyVersion;

    private final SchemaChangeNotifier schemaChangeNotifier = new SchemaChangeNotifier();

    public final SchemaUpdateHandler updateHandler;

    private final boolean online;

    /**
     * Initialize empty schema object and load the hardcoded system tables
     */
    private Schema()
    {
        this.online = isDaemonInitialized();
        this.localKeyspaces = (CassandraRelevantProperties.FORCE_LOAD_LOCAL_KEYSPACES.getBoolean() || isDaemonInitialized() || isToolInitialized())
                              ? Keyspaces.of(SchemaKeyspace.metadata(), SystemKeyspace.metadata())
                              : Keyspaces.none();

        this.localKeyspaces.forEach(this::loadNew);
        this.updateHandler = SchemaUpdateHandlerFactoryProvider.instance.get().getSchemaUpdateHandler(online, this::mergeAndUpdateVersion);
    }

    @VisibleForTesting
    public Schema(boolean online, Keyspaces localKeyspaces, SchemaUpdateHandler updateHandler)
    {
        this.online = online;
        this.localKeyspaces = localKeyspaces;
        this.updateHandler = updateHandler;
    }

    public void startSync()
    {
        logger.debug("Starting update handler");
        updateHandler.start();
    }

    public boolean waitUntilReady(Duration timeout)
    {
        logger.debug("Waiting for update handler to be ready...");
        return updateHandler.waitUntilReady(timeout);
    }

    /**
     * Load keyspaces definitions from local storage, see {@link SchemaUpdateHandler#reset(boolean)}.
     */
    public void loadFromDisk()
    {
        SchemaDiagnostics.schemaLoading(this);
        updateHandler.reset(true);
        SchemaDiagnostics.schemaLoaded(this);
    }

    /**
     * Update (or insert) new keyspace definition
     *
     * @param ksm The metadata about keyspace
     */
    private synchronized void load(KeyspaceMetadata ksm)
    {
        Preconditions.checkArgument(!SchemaConstants.isLocalSystemKeyspace(ksm.name));
        KeyspaceMetadata previous = distributedKeyspaces.getNullable(ksm.name);

        if (previous == null)
            loadNew(ksm);
        else
            reload(previous, ksm);

        distributedKeyspaces = distributedKeyspaces.withAddedOrUpdated(ksm);
    }

    private synchronized void loadNew(KeyspaceMetadata ksm)
    {
        this.tableMetadataRefCache = tableMetadataRefCache.withNewRefs(ksm);

        SchemaDiagnostics.metadataInitialized(this, ksm);
    }

    private synchronized void reload(KeyspaceMetadata previous, KeyspaceMetadata updated)
    {
        Keyspace keyspace = getKeyspaceInstance(updated.name);
        if (null != keyspace)
            keyspace.setMetadata(updated);

        Tables.TablesDiff tablesDiff = Tables.diff(previous.tables, updated.tables);
        Views.ViewsDiff viewsDiff = Views.diff(previous.views, updated.views);

        MapDifference<String, TableMetadata> indexesDiff = previous.tables.indexesDiff(updated.tables);

        this.tableMetadataRefCache = tableMetadataRefCache.withUpdatedRefs(previous, updated);

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
     *
     * @return Keyspace object or null if keyspace was not found
     */
    @Override
    public Keyspace getKeyspaceInstance(String keyspaceName)
    {
        return keyspaceInstances.getIfReady(keyspaceName);
    }

    /**
     * Returns {@link ColumnFamilyStore} by the table identifier. Note that though, if called for {@link TableMetadata#id},
     * when metadata points to a secondary index table, the {@link TableMetadata#id} denotes the identifier of the main
     * table, not the index table. Thus, this method will return CFS of the main table rather than, probably expected,
     * CFS for the index backing table.
     */
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
    public Keyspace maybeAddKeyspaceInstance(String keyspaceName, Supplier<Keyspace> loadFunction)
    {
        return keyspaceInstances.blockingLoadIfAbsent(keyspaceName, loadFunction);
    }

    private Keyspace maybeRemoveKeyspaceInstance(String keyspaceName, Consumer<Keyspace> unloadFunction)
    {
        try
        {
            return keyspaceInstances.blockingUnloadIfPresent(keyspaceName, unloadFunction);
        }
        catch (LoadingMap.UnloadExecutionException e)
        {
            throw new AssertionError("Failed to unload the keyspace " + keyspaceName, e);
        }
    }

    public Keyspaces distributedKeyspaces()
    {
        return distributedKeyspaces;
    }

    /**
     * Compute the largest gc grace seconds amongst all the tables
     * @return the largest gcgs.
     */
    public int largestGcgs()
    {
        return Streams.concat(distributedKeyspaces.stream(), localKeyspaces.stream())
                      .flatMap(ksm -> ksm.tables.stream())
                      .mapToInt(tm -> tm.params.gcGraceSeconds)
                      .max()
                      .orElse(Integer.MIN_VALUE);
    }

    /**
     * Remove keyspace definition from system
     *
     * @param ksm The keyspace definition to remove
     */
    private synchronized void unload(KeyspaceMetadata ksm)
    {
        distributedKeyspaces = distributedKeyspaces.without(ksm.name);

        this.tableMetadataRefCache = tableMetadataRefCache.withRemovedRefs(ksm);

        SchemaDiagnostics.metadataRemoved(this, ksm);
    }

    public int getNumberOfTables()
    {
        return Streams.concat(distributedKeyspaces.stream(), localKeyspaces.stream())
                      .mapToInt(k -> size(k.tablesAndViews()))
                      .sum();
    }

    public ViewMetadata getView(String keyspaceName, String viewName)
    {
        assert keyspaceName != null;
        KeyspaceMetadata ksm = distributedKeyspaces.getNullable(keyspaceName);
        ksm = ksm != null ? ksm : localKeyspaces.getNullable(keyspaceName);
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
        KeyspaceMetadata ksm = distributedKeyspaces.getNullable(keyspaceName);
        ksm = ksm != null ? ksm : localKeyspaces.getNullable(keyspaceName);
        return null != ksm ? ksm : VirtualKeyspaceRegistry.instance.getKeyspaceMetadataNullable(keyspaceName);
    }

    /**
     * Returns user keyspaces, that is all but {@link SchemaConstants#LOCAL_SYSTEM_KEYSPACE_NAMES},
     * {@link SchemaConstants#REPLICATED_SYSTEM_KEYSPACE_NAMES} or virtual keyspaces.
     */
    public Sets.SetView<String> getUserKeyspaces()
    {
        return Sets.difference(distributedKeyspaces.names(), SchemaConstants.REPLICATED_SYSTEM_KEYSPACE_NAMES);
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
        KeyspaceMetadata ksm = ObjectUtils.getFirstNonNull(() -> distributedKeyspaces.getNullable(keyspaceName),
                                                           () -> localKeyspaces.getNullable(keyspaceName));
        Preconditions.checkNotNull(ksm, "Keyspace %s not found", keyspaceName);
        return ksm.tablesAndViews();
    }

    /**
     * @return a set of local and distributed keyspace names; it does not include virtual keyspaces
     */
    public Sets.SetView<String> getKeyspaces()
    {
        return Sets.union(distributedKeyspaces.names(), localKeyspaces.names());
    }

    public Keyspaces getLocalKeyspaces()
    {
        return localKeyspaces;
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
        return ObjectUtils.getFirstNonNull(() -> distributedKeyspaces.getTableOrViewNullable(id),
                                           () -> localKeyspaces.getTableOrViewNullable(id),
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
     * Get all user-defined function overloads with the specified name.
     *
     * @param name fully qualified function name
     * @return an empty list if the keyspace or the function name are not found;
     *         a non-empty collection of {@link UserFunction} otherwise
     */
    public Collection<UserFunction> getUserFunctions(FunctionName name)
    {
        if (!name.hasKeyspace())
            throw new IllegalArgumentException(String.format("Function name must be fully qualified: got %s", name));

        KeyspaceMetadata ksm = getKeyspaceMetadata(name.keyspace);
        return ksm == null
               ? Collections.emptyList()
               : ksm.userFunctions.get(name);
    }

    /**
     * Find the function with the specified name and arguments.
     *
     * @param name     fully qualified function name
     * @param argTypes function argument types
     * @return an empty {@link Optional} if the keyspace or the function name are not found;
     *         a non-empty optional of {@link Function} otherwise
     */
    public Optional<UserFunction> findUserFunction(FunctionName name, List<AbstractType<?>> argTypes)
    {
        if (!name.hasKeyspace())
            throw new IllegalArgumentException(String.format("Function name must be fully quallified: got %s", name));

        return Optional.ofNullable(getKeyspaceMetadata(name.keyspace))
                       .flatMap(ksm -> ksm.userFunctions.find(name, argTypes));
    }

    /* Version control */

    /**
     * Returns the current schema version. Although, if the schema is being updated while the method was called, it
     * can return a stale version which does not correspond to the current keyspaces metadata. It is because the schema
     * version is unknown for the partially applied changes and is updated after the entire schema change is completed.
     * <p>
     * This method should be used only internally by {@link Schema} or {@link SchemaUpdateHandler} implementations.
     * Please use {@link #getDistributedSchemaBlocking()} to get schema version consistently in other cases.
     */
    public UUID getVersion()
    {
        return version;
    }

    /**
     * Returns the current keyspaces metadata and version synchronouly. If the schema is in the middle of a multistep
     * transformation, the method blocks until the update is completed.
     */
    public synchronized DistributedSchema getDistributedSchemaBlocking()
    {
        return new DistributedSchema(distributedKeyspaces, version);
    }

    /**
     * Checks whether the given schema version is the same as the current local schema.
     * Note that this method is non-blocking and may use a stale schema version for comparison - see {@link #getVersion()}.
     */
    public boolean isSameVersion(UUID schemaVersion)
    {
        return schemaVersion != null && schemaVersion.equals(version);
    }

    /**
     * Checks whether the current schema is empty.
     * Note that this method is non-blocking and may use a stale schema version for comparison - see {@link #getVersion()}.
     */
    public boolean isEmpty()
    {
        return SchemaConstants.emptyVersion.equals(version);
    }

    /**
     * Read schema from system keyspace and calculate MD5 digest of every row, resulting digest
     * will be converted into UUID which would act as content-based version of the schema.
     *
     * See CASSANDRA-16856/16996. Make sure schema pulls are synchronized to prevent concurrent schema pull/writes
     */
    private synchronized void updateVersion(UUID version)
    {
        this.version = version;
        SchemaDiagnostics.versionUpdated(this);
    }

    /**
     * When we receive {@link SchemaTransformationResult} in a callback invocation, the transformation result includes
     * pre-transformation and post-transformation schema metadata and versions, and a diff between them. Basically
     * we expect that the local image of the schema metadata ({@link #distributedKeyspaces}) and version ({@link #version})
     * are the same as pre-transformation. However, it might not always be true because some changes might not be
     * applied completely due to some errors. This methods is to emit warning in such case and recalculate diff so that
     * it contains the changes between the local schema image ({@link #distributedKeyspaces} and the post-transformation
     * schema. That recalculation allows the following updates in the callback to recover the schema.
     *
     * @param result the incoming transformation result
     * @return recalculated transformation result if needed, otherwise the provided incoming result
     */
    private synchronized SchemaTransformationResult localDiff(SchemaTransformationResult result)
    {
        Keyspaces localBefore = distributedKeyspaces;
        UUID localVersion = version;
        boolean needNewDiff = false;

        if (!Objects.equals(localBefore, result.before.getKeyspaces()))
        {
            logger.info("Schema was different to what we expected: {}", Keyspaces.diff(result.before.getKeyspaces(), localBefore));
            needNewDiff = true;
        }

        if (!Objects.equals(localVersion, result.before.getVersion()))
        {
            logger.info("Schema version was different to what we expected: {} != {}", result.before.getVersion(), localVersion);
            needNewDiff = true;
        }

        if (needNewDiff)
            return new SchemaTransformationResult(new DistributedSchema(localBefore, localVersion),
                                                  result.after,
                                                  Keyspaces.diff(localBefore, result.after.getKeyspaces()));

        return result;
    }

    /*
     * Reload schema from local disk. Useful if a user made changes to schema tables by hand, or has suspicion that
     * in-memory representation got out of sync somehow with what's on disk.
     */
    public void reloadSchemaAndAnnounceVersion()
    {
        updateHandler.reset(true);
    }

    /**
     * Merge remote schema in form of mutations with local and mutate ks/cf metadata objects
     * (which also involves fs operations on add/drop ks/cf)
     *
     * @throws ConfigurationException If one of metadata attributes has invalid value
     */
    @VisibleForTesting
    public synchronized void mergeAndUpdateVersion(SchemaTransformationResult result, boolean dropData)
    {
        result = localDiff(result);
        assert result.after.getKeyspaces().stream().noneMatch(ksm -> ksm.params.replication.klass == LocalStrategy.class) : "LocalStrategy should not be used";
        schemaChangeNotifier.notifyPreChanges(result);
        merge(result.diff, dropData);
        updateVersion(result.after.getVersion());
        if (online)
            SystemKeyspace.updateSchemaVersion(result.after.getVersion());
    }

    public SchemaTransformationResult transform(SchemaTransformation transformation)
    {
        return transform(transformation, false);
    }

    public SchemaTransformationResult transform(SchemaTransformation transformation, boolean local)
    {
        return updateHandler.apply(transformation, local);
    }

    /**
     * Clear all locally stored schema information and fetch schema from another node.
     * Called by user (via JMX) who wants to get rid of schema disagreement.
     */
    public void resetLocalSchema()
    {
        logger.debug("Clearing local schema...");

        if (Gossiper.instance.getLiveMembers().stream().allMatch(ep -> FBUtilities.getBroadcastAddressAndPort().equals(ep)))
            throw new InvalidRequestException("Cannot reset local schema when there are no other live nodes");

        Awaitable clearCompletion = updateHandler.clear();
        try
        {
            if (!clearCompletion.await(StorageService.SCHEMA_DELAY_MILLIS, TimeUnit.MILLISECONDS))
            {
                throw new RuntimeException("Schema reset failed - no schema received from other nodes");
            }
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Failed to reset schema - the thread has been interrupted");
        }
        SchemaDiagnostics.schemaCleared(this);
        logger.info("Local schema reset completed");
    }

    private void merge(KeyspacesDiff diff, boolean removeData)
    {
        diff.dropped.forEach(keyspace -> dropKeyspace(keyspace, removeData));
        diff.created.forEach(this::createKeyspace);
        diff.altered.forEach(delta -> alterKeyspace(delta, removeData));
    }

    private void alterKeyspace(KeyspaceDiff delta, boolean dropData)
    {
        SchemaDiagnostics.keyspaceAltering(this, delta);

        boolean initialized = Keyspace.isInitialized();

        Keyspace keyspace = initialized ? getKeyspaceInstance(delta.before.name) : null;
        if (initialized)
        {
            assert keyspace != null;
            assert delta.before.name.equals(delta.after.name);

            // drop tables and views
            delta.views.dropped.forEach(v -> dropView(keyspace, v, dropData));
            delta.tables.dropped.forEach(t -> dropTable(keyspace, t, dropData));
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
            Keyspace.open(delta.after.name, this, true).viewManager.reload(true);
        }

        schemaChangeNotifier.notifyKeyspaceAltered(delta, dropData);
        SchemaDiagnostics.keyspaceAltered(this, delta);
    }

    private void createKeyspace(KeyspaceMetadata keyspace)
    {
        SchemaDiagnostics.keyspaceCreating(this, keyspace);
        load(keyspace);
        if (Keyspace.isInitialized())
        {
            Keyspace.open(keyspace.name, this, true);
        }

        schemaChangeNotifier.notifyKeyspaceCreated(keyspace);
        SchemaDiagnostics.keyspaceCreated(this, keyspace);

        // If keyspace has been added, we need to recalculate pending ranges to make sure
        // we send mutations to the correct set of bootstrapping nodes. Refer CASSANDRA-15433.
        if (keyspace.params.replication.klass != LocalStrategy.class && Keyspace.isInitialized())
        {
            PendingRangeCalculatorService.calculatePendingRanges(Keyspace.open(keyspace.name, this, true).getReplicationStrategy(), keyspace.name);
        }
    }

    private void dropKeyspace(KeyspaceMetadata keyspaceMetadata, boolean dropData)
    {
        SchemaDiagnostics.keyspaceDropping(this, keyspaceMetadata);

        boolean initialized = Keyspace.isInitialized();
        Keyspace keyspace = initialized ? Keyspace.open(keyspaceMetadata.name, this, false) : null;
        if (initialized)
        {
            if (keyspace == null)
                return;

            keyspaceMetadata.views.forEach(v -> dropView(keyspace, v, dropData));
            keyspaceMetadata.tables.forEach(t -> dropTable(keyspace, t, dropData));

            // remove the keyspace from the static instances
            Keyspace unloadedKeyspace = maybeRemoveKeyspaceInstance(keyspaceMetadata.name, ks -> {
                ks.unload(dropData);
                unload(keyspaceMetadata);
            });
            assert unloadedKeyspace == keyspace;

            Keyspace.writeOrder.awaitNewBarrier();
        }
        else
        {
            unload(keyspaceMetadata);
        }

        schemaChangeNotifier.notifyKeyspaceDropped(keyspaceMetadata, dropData);
        SchemaDiagnostics.keyspaceDropped(this, keyspaceMetadata);
    }

    private void dropView(Keyspace keyspace, ViewMetadata metadata, boolean dropData)
    {
        keyspace.viewManager.dropView(metadata.name());
        dropTable(keyspace, metadata.metadata, dropData);
    }

    private void dropTable(Keyspace keyspace, TableMetadata metadata, boolean dropData)
    {
        SchemaDiagnostics.tableDropping(this, metadata);
        keyspace.dropCf(metadata.id, dropData);
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

    public Map<UUID, Set<InetAddressAndPort>> getOutstandingSchemaVersions()
    {
        return updateHandler instanceof DefaultSchemaUpdateHandler
               ? ((DefaultSchemaUpdateHandler) updateHandler).getOutstandingSchemaVersions()
               : Collections.emptyMap();
    }

}