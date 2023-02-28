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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.transformations.AlterSchema;

import static com.google.common.collect.Iterables.size;
import static org.apache.cassandra.config.DatabaseDescriptor.isDaemonInitialized;
import static org.apache.cassandra.config.DatabaseDescriptor.isToolInitialized;

/**
 * Manages shared schema, keyspace instances and table metadata refs. Provides methods to initialize, modify and query
 * both the shared and local schema, as well as to register listeners.
 * <p>
 * This class should be the only entity used to query and manage schema. Internal details should not be access in
 * production code (would be great if they were not accessed in the test code as well).
 * <p>
 * TL;DR: All modifications are made using the implementation of SchemaUpdateHandle obtained from the provided
 * factory. After each modification, the internally managed table metadata refs and keyspaces instances are updated and
 * notifications are sent to the registered listeners.
 * When the schema change is applied by the update handler (regardless it is initiated locally or received from outside),
 * the registered callback is executed which performs the remaining updates for tables metadata refs and keyspace
 * instances (see mergeAndUpdateVersion(SchemaTransformationResult).
 */
public final class Schema implements SchemaProvider
{
    private static final Logger logger = LoggerFactory.getLogger(Schema.class);

    private static final boolean FORCE_LOAD_LOCAL_KEYSPACES = CassandraRelevantProperties.FORCE_LOAD_LOCAL_KEYSPACES.getBoolean();

    public static SchemaProvider instance = initialize();

    private static Schema initialize()
    {
        Schema schema = new Schema();

        if ((FORCE_LOAD_LOCAL_KEYSPACES || isDaemonInitialized() || isToolInitialized()))
        {
            for (KeyspaceMetadata ks : schema.localKeyspaces)
            {
                schema.localKeyspaceInstances.put(ks.name, new LazyVariable<>(() -> Keyspace.forSchema(ks.name, schema)));
            }
        }
        return schema;
    }

    private final Keyspaces localKeyspaces;
    private final SchemaChangeNotifier schemaChangeNotifier = new SchemaChangeNotifier();
    private final Map<String, Supplier<Keyspace>> localKeyspaceInstances = new HashMap<>();

    /**
     * Initialize empty schema object and load the hardcoded system tables
     */
    private Schema()
    {
        this.localKeyspaces = Keyspaces.of(SchemaKeyspace.metadata(), SystemKeyspace.metadata());
    }

    /**
     * Add entries to system_schema.* for the hardcoded system keyspaces
     *
     * See CASSANDRA-16856/16996. Make sure schema pulls are synchronized to prevent concurrent schema pull/writes
     */
    public synchronized void saveSystemKeyspace()
    {
        SchemaKeyspace.saveSystemKeyspacesSchema();
    }

    @Override
    public void registerListener(SchemaChangeListener listener)
    {
        schemaChangeNotifier.registerListener(listener);
    }

    @Override
    public void unregisterListener(SchemaChangeListener listener)
    {
        schemaChangeNotifier.unregisterListener(listener);
    }

    @Override
    public SchemaChangeNotifier schemaChangeNotifier()
    {
        return schemaChangeNotifier;
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
        if (SchemaConstants.isVirtualSystemKeyspace(keyspaceName))
            return null;
        else if (SchemaConstants.isLocalSystemKeyspace(keyspaceName))
            return localKeyspaceInstances.get(keyspaceName).get();
        else
            return ClusterMetadata.current().schema.getKeyspace(keyspaceName);
    }

    public Keyspaces distributedAndLocalKeyspaces()
    {
        // TODO reimplement perf fix from CASSANDRA-18921
        return Keyspaces.NONE.with(localKeyspaces).with(distributedKeyspaces());
    }

    @Override
    public Keyspaces distributedKeyspaces()
    {
        ClusterMetadata metadata = ClusterMetadata.currentNullable();
        if (metadata == null)
            return Keyspaces.NONE;

        return metadata.schema.getKeyspaces();
    }

    public Keyspaces localKeyspaces()
    {
        return localKeyspaces;
    }

    public int getNumberOfTables()
    {
        return distributedAndLocalKeyspaces().stream().mapToInt(k -> size(k.tablesAndViews())).sum();
    }

    public Optional<TableMetadata> getIndexMetadata(String keyspace, String index)
    {
        assert keyspace != null;
        assert index != null;

        KeyspaceMetadata ksm = getKeyspaceMetadata(keyspace);
        if (ksm == null)
            return Optional.empty();

        return ksm.getIndexMetadata(index);
    }

    public ViewMetadata getView(String keyspaceName, String viewName)
    {
        assert keyspaceName != null;
        KeyspaceMetadata ksm = distributedAndLocalKeyspaces().getNullable(keyspaceName);
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
        KeyspaceMetadata keyspace;
        if (SchemaConstants.isLocalSystemKeyspace(keyspaceName))
            keyspace = localKeyspaces.getNullable(keyspaceName);
        else
            keyspace = distributedKeyspaces().getNullable(keyspaceName);

        return null != keyspace ? keyspace : VirtualKeyspaceRegistry.instance.getKeyspaceMetadataNullable(keyspaceName);
    }

    /**
     * Returns all non-local keyspaces whose replication strategy is not {@link LocalStrategy}.
     */
    public Keyspaces getNonLocalStrategyKeyspaces()
    {
        return distributedKeyspaces().filter(keyspace -> keyspace.params.replication.klass != LocalStrategy.class);
    }

    /**
     * Returns user keyspaces, that is all but {@link SchemaConstants#LOCAL_SYSTEM_KEYSPACE_NAMES},
     * {@link SchemaConstants#REPLICATED_SYSTEM_KEYSPACE_NAMES} or virtual keyspaces.
     */
    public Keyspaces getUserKeyspaces()
    {
        return distributedKeyspaces().without(SchemaConstants.REPLICATED_SYSTEM_KEYSPACE_NAMES);
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
        KeyspaceMetadata ksm = ObjectUtils.getFirstNonNull(() -> distributedKeyspaces().getNullable(keyspaceName),
                                                           () -> localKeyspaces.getNullable(keyspaceName));
        Preconditions.checkNotNull(ksm, "Keyspace %s not found", keyspaceName);
        return ksm.tablesAndViews();
    }

    /**
     * @return collection of the all keyspace names registered in the system (system and non-system)
     */
    public ImmutableSet<String> getKeyspaces()
    {
        return ImmutableSet.copyOf(distributedAndLocalKeyspaces().names());
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
        return ObjectUtils.getFirstNonNull(() -> localKeyspaces.getTableOrViewNullable(id),
                                           () -> distributedKeyspaces().getTableOrViewNullable(id),
                                           () -> VirtualKeyspaceRegistry.instance.getTableMetadataNullable(id));

    }

    public TableMetadata getTableMetadata(Descriptor descriptor)
    {
        return getTableMetadata(descriptor.ksname, descriptor.cfname);
    }

    @Override
    public ClusterMetadata submit(SchemaTransformation transformation)
    {
        logger.debug("Submitting schema transformation {}", transformation);

        // result of this execution can be either a complete failure/timeout, or a success, but together with a log of
        // operations that have to be applied before we can do anything
        // TODO perhaps we should change the retry predicate to be a bit smarter and examine the state of metadata
        //      when retrying.
        return ClusterMetadataService.instance().commit(new AlterSchema(transformation, this),
                                                        (metadata) -> true,
                                                        (metadata) -> metadata,
                                                        (metadata, reason) -> {
                                                            throw new InvalidRequestException(reason);
                                                        });
    }

    // We need to lazy-initialize schema for test purposes: since column families are initialized
    // eagerly, if local schema initialization is attempted before commit log instance is started,
    // cf initialization will fail to grab a current commit log position.
    //
    // This could be further improved by providing a special Schema instance for tests, or
    // using supplier with precomputed value for regular code path, and lazy variable for tests.
    //
    // TODO: it _might_ be useful to avoid eagerly initializing distributed keyspaces, especially in
    // when the number of tables is large.
    private final static class LazyVariable<T> implements Supplier<T>
    {
        private final AtomicReference<Object> ref;
        private final Supplier<T> run;

        private LazyVariable(Supplier<T> run)
        {
            this.ref = new AtomicReference<>(null);
            this.run = run;
        }

        public T get()
        {
            Object v = ref.get();
            if (v == null)
            {
                Sentinel sentinel = new Sentinel();

                if (ref.compareAndSet(null, sentinel))
                {
                    try
                    {
                        v = run.get();
                    }
                    catch (Throwable t)
                    {
                        ref.compareAndSet(sentinel, null);
                        throw t;
                    }
                    boolean result = ref.compareAndSet(sentinel, v);
                    assert result;
                }
                else
                {
                    return get();
                }
            }
            else
            {
                while (v instanceof Sentinel)
                {
                    if (((Sentinel) v).thread == Thread.currentThread())
                    {
                        throw new RuntimeException("Looks like we have a deadlock. Check sentinel for the original call.",
                                                   ((Sentinel) v).throwable);
                    }
                    v = ref.get();
                    LockSupport.parkNanos(100);
                }
            }
            return (T) v;
        }
    }

    private static final class Sentinel
    {
        private final Thread thread;
        private final Throwable throwable;

        private Sentinel()
        {
            this(Thread.currentThread(), new RuntimeException("Sentinel call") {
                private final StackTraceElement[] trace = Thread.currentThread().getStackTrace();

                @Override
                public StackTraceElement[] getStackTrace()
                {
                    return trace;
                }
            });
        }

        private Sentinel(Thread thread, Throwable throwable)
        {
            this.thread = thread;
            this.throwable = throwable;
        }
    }
}