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

package org.apache.cassandra.io.sstable;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.RowUpdateBuilder;
import org.apache.cassandra.cql3.RowUpdateBuilder.RegularRowUpdateBuilder;
import org.apache.cassandra.cql3.functions.types.TypeCodec;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.schema.CreateIndexStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTypeStatement;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaTransformation;
import org.apache.cassandra.schema.SchemaTransformations;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.schema.UserFunctions;
import org.apache.cassandra.schema.Views;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.transformations.AlterSchema;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.JavaDriverUtils;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

public class HarrySSTableWriter implements Closeable
{
    public static final ByteBuffer UNSET_VALUE = ByteBufferUtil.UNSET_BYTE_BUFFER;

    static
    {
        CassandraRelevantProperties.FORCE_LOAD_LOCAL_KEYSPACES.setBoolean(true);
        DatabaseDescriptor.clientInitialization(false);
        // Partitioner is not set in client mode.
        if (DatabaseDescriptor.getPartitioner() == null)
            DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        ClusterMetadataService.initializeForClients();
    }

    private final AbstractSSTableSimpleWriter writer;

    private HarrySSTableWriter(AbstractSSTableSimpleWriter writer)
    {
        this.writer = writer;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public HarrySSTableWriter addRow(String cql, Object... values) throws IOException
    {
        ModificationStatement statement = prepare(cql);
        List<ColumnSpecification> boundNames = statement.getBindVariables();
        // TODO: avoid materializing this
        List<TypeCodec<Object>> typeCodecs = boundNames.stream()
                                                       .map(bn -> JavaDriverUtils.codecFor(JavaDriverUtils.driverType(bn.type)))
                                                       .collect(Collectors.toList());

        int size = Math.min(values.length, boundNames.size());
        List<ByteBuffer> rawValues = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
        {
            Object value = values[i];
            rawValues.add(serialize(value, typeCodecs.get(i), boundNames.get(i)));
        }

        return rawAddRow(statement, rawValues, boundNames);
    }

    private ModificationStatement prepare(String cql)
    {
        ModificationStatement.Parsed statement = QueryProcessor.parseStatement(cql,
                                                                               ModificationStatement.Parsed.class,
                                                                               "INSERT/UPDATE/DELETE");
        ClientState state = ClientState.forInternalCalls();
        ModificationStatement preparedModificationStatement = statement.prepare(state);
        preparedModificationStatement.validate(state);

        if (preparedModificationStatement.hasConditions())
            throw new IllegalArgumentException("Conditional statements are not supported");
        if (preparedModificationStatement.isCounter())
            throw new IllegalArgumentException("Counter modification statements are not supported");
        if (preparedModificationStatement.getBindVariables().isEmpty())
            throw new IllegalArgumentException("Provided preparedModificationStatement statement has no bind variables");

        return preparedModificationStatement;
    }

    /**
     * Adds a new row to the writer given already serialized values.
     * <p>
     * This is a shortcut for {@code rawAddRow(Arrays.asList(values))}.
     *
     * @param values the row values (corresponding to the bind variables of the
     *               modification statement used when creating by this writer) as binary.
     * @return this writer.
     */
    public HarrySSTableWriter rawAddRow(ModificationStatement modificationStatement, List<ByteBuffer> values, List<ColumnSpecification> boundNames) throws InvalidRequestException, IOException
    {
        if (values.size() != boundNames.size())
            throw new InvalidRequestException(String.format("Invalid number of arguments, expecting %d values but got %d", boundNames.size(), values.size()));

        QueryOptions options = QueryOptions.forInternalCalls(null, values);
        ClientState state = ClientState.forInternalCalls();
        List<ByteBuffer> keys = modificationStatement.buildPartitionKeyNames(options, state);

        long now = currentTimeMillis();
        // Note that we asks indexes to not validate values (the last 'false' arg below) because that triggers a 'Keyspace.open'
        // and that forces a lot of initialization that we don't want.
        RowUpdateBuilder params = new RegularRowUpdateBuilder(modificationStatement.metadata,
                                                              ClientState.forInternalCalls(),
                                                              options,
                                                              modificationStatement.getTimestamp(TimeUnit.MILLISECONDS.toMicros(now), options),
                                                              options.getNowInSec((int) TimeUnit.MILLISECONDS.toSeconds(now)),
                                                              modificationStatement.getTimeToLive(options),
                                                              Collections.emptyMap());

        try
        {
            if (modificationStatement.hasSlices())
            {
                Slices slices = modificationStatement.createSlices(options);

                for (ByteBuffer key : keys)
                {
                    for (Slice slice : slices)
                        modificationStatement.addUpdateForKey(writer.getUpdateFor(key), slice, params);
                }
            }
            else
            {
                NavigableSet<Clustering<?>> clusterings = modificationStatement.createClustering(options, state);

                for (ByteBuffer key : keys)
                {
                    for (Clustering clustering : clusterings)
                        modificationStatement.addUpdateForKey(writer.getUpdateFor(key), clustering, params);
                }
            }
            return this;
        }
        catch (SSTableSimpleUnsortedWriter.SyncException e)
        {
            // If we use a BufferedWriter and had a problem writing to disk, the IOException has been
            // wrapped in a SyncException (see BufferedWriter below). We want to extract that IOE.
            throw (IOException) e.getCause();
        }
    }

    /**
     * Close this writer.
     * <p>
     * This method should be called, otherwise the produced sstables are not
     * guaranteed to be complete (and won't be in practice).
     */
    public void close() throws IOException
    {
        writer.close();
    }

    private ByteBuffer serialize(Object value, TypeCodec codec, ColumnSpecification columnSpecification)
    {
        if (value == null || value == UNSET_VALUE)
            return (ByteBuffer) value;

        try
        {
            return codec.serialize(value, ProtocolVersion.CURRENT);
        }
        catch (ClassCastException cce)
        {
            // For backwards-compatibility with consumers that may be passing
            // an Integer for a Date field, for example.
            return ((AbstractType) columnSpecification.type).decompose(value);
        }
    }

    /**
     * A Builder for a CQLSSTableWriter object.
     */
    public static class Builder
    {
        private static final Logger logger = LoggerFactory.getLogger(Builder.class);
        private static final long DEFAULT_BUFFER_SIZE_IN_MIB_FOR_UNSORTED = 128L;

        protected SSTableFormat<?, ?> format = null;

        private final List<CreateTypeStatement.Raw> typeStatements;
        private final List<CreateIndexStatement.Raw> indexStatements;

        private File directory;
        private CreateTableStatement.Raw schemaStatement;
        private IPartitioner partitioner;
        private boolean sorted = false;
        private long maxSSTableSizeInMiB = -1L;
        private boolean buildIndexes = true;
        private Consumer<Collection<SSTableReader>> sstableProducedListener;
        private boolean openSSTableOnProduced = false;

        protected Builder()
        {
            this.typeStatements = new ArrayList<>();
            this.indexStatements = new ArrayList<>();
        }

        /**
         * The directory where to write the sstables.
         * <p>
         * This is a mandatory option.
         *
         * @param directory the directory to use, which should exists and be writable.
         * @return this builder.
         * @throws IllegalArgumentException if {@code directory} doesn't exist or is not writable.
         */
        public Builder inDirectory(String directory)
        {
            return inDirectory(new File(directory));
        }

        /**
         * The directory where to write the sstables (mandatory option).
         * <p>
         * This is a mandatory option.
         *
         * @param directory the directory to use, which should exists and be writable.
         * @return this builder.
         * @throws IllegalArgumentException if {@code directory} doesn't exist or is not writable.
         */
        public Builder inDirectory(File directory)
        {
            if (!directory.exists())
                throw new IllegalArgumentException(directory + " doesn't exists");
            if (!directory.isWritable())
                throw new IllegalArgumentException(directory + " exists but is not writable");

            this.directory = directory;
            return this;
        }

        public Builder withType(String typeDefinition) throws SyntaxException
        {
            typeStatements.add(QueryProcessor.parseStatement(typeDefinition, CreateTypeStatement.Raw.class, "CREATE TYPE"));
            return this;
        }

        /**
         * The schema (CREATE TABLE statement) for the table for which sstable are to be created.
         * <p>
         * Please note that the provided CREATE TABLE statement <b>must</b> use a fully-qualified
         * table name, one that include the keyspace name.
         * <p>
         * This is a mandatory option.
         *
         * @param schema the schema of the table for which sstables are to be created.
         * @return this builder.
         * @throws IllegalArgumentException if {@code schema} is not a valid CREATE TABLE statement
         *                                  or does not have a fully-qualified table name.
         */
        public Builder forTable(String schema)
        {
            this.schemaStatement = QueryProcessor.parseStatement(schema, CreateTableStatement.Raw.class, "CREATE TABLE");
            return this;
        }

        /**
         * The schema (CREATE INDEX statement) for index to be created for the table. Only SAI indexes are supported.
         *
         * @param indexes CQL statements representing SAI indexes to be created.
         * @return this builder
         */
        public Builder withIndexes(String... indexes)
        {
            for (String index : indexes)
                indexStatements.add(QueryProcessor.parseStatement(index, CreateIndexStatement.Raw.class, "CREATE INDEX"));

            return this;
        }

        /**
         * The partitioner to use.
         * <p>
         * By default, {@code Murmur3Partitioner} will be used. If this is not the partitioner used
         * by the cluster for which the SSTables are created, you need to use this method to
         * provide the correct partitioner.
         *
         * @param partitioner the partitioner to use.
         * @return this builder.
         */
        public Builder withPartitioner(IPartitioner partitioner)
        {
            this.partitioner = partitioner;
            return this;
        }

        /**
         * Defines the maximum SSTable size in mebibytes when using the sorted writer.
         * By default, i.e. not specified, there is no maximum size limit for the produced SSTable
         *
         * @param size the maximum sizein mebibytes of each individual SSTable allowed
         * @return this builder
         */
        public Builder withMaxSSTableSizeInMiB(int size)
        {
            if (size <= 0)
            {
                logger.warn("A non-positive value for maximum SSTable size is specified, " +
                            "which disables the size limiting effectively. Please supply a positive value in order " +
                            "to enforce size limiting for the produced SSTables.");
            }
            this.maxSSTableSizeInMiB = size;
            return this;
        }

        /**
         * The size of the buffer to use.
         * <p>
         * This defines how much data will be buffered before being written as
         * a new SSTable. This corresponds roughly to the data size that will have the created
         * sstable.
         * <p>
         * The default is 128MiB, which should be reasonable for a 1GiB heap. If you experience
         * OOM while using the writer, you should lower this value.
         *
         * @param size the size to use in MiB.
         * @return this builder.
         * @deprecated This method is deprecated in favor of the new withMaxSSTableSizeInMiB(int size)
         */
        @Deprecated(since = "5.0")
        public Builder withBufferSizeInMiB(int size)
        {
            return withMaxSSTableSizeInMiB(size);
        }

        /**
         * The size of the buffer to use.
         * <p>
         * This defines how much data will be buffered before being written as
         * a new SSTable. This corresponds roughly to the data size that will have the created
         * sstable.
         * <p>
         * The default is 128MiB, which should be reasonable for a 1GiB heap. If you experience
         * OOM while using the writer, you should lower this value.
         *
         * @param size the size to use in MiB.
         * @return this builder.
         * @deprecated This method is deprecated in favor of the new withBufferSizeInMiB(int size). See CASSANDRA-17675
         */
        @Deprecated(since = "4.1")
        public Builder withBufferSizeInMB(int size)
        {
            return withBufferSizeInMiB(size);
        }

        /**
         * Creates a CQLSSTableWriter that expects sorted inputs.
         * <p>
         * If this option is used, the resulting writer will expect rows to be
         * added in SSTable sorted order (and an exception will be thrown if that
         * is not the case during modification). The SSTable sorted order means that
         * rows are added such that their partition key respect the partitioner
         * order.
         * <p>
         * You should thus only use this option is you know that you can provide
         * the rows in order, which is rarely the case. If you can provide the
         * rows in order however, using this sorted might be more efficient.
         * <p>
         * Note that if used, some option like withBufferSizeInMiB will be ignored.
         *
         * @return this builder.
         */
        public Builder sorted()
        {
            this.sorted = true;
            return this;
        }

        /**
         * Whether indexes should be built and serialized to disk along data. Defaults to true.
         *
         * @param buildIndexes true if indexes should be built, false otherwise
         * @return this builder
         */
        public Builder withBuildIndexes(boolean buildIndexes)
        {
            this.buildIndexes = buildIndexes;
            return this;
        }

        /**
         * Set the listener to receive notifications on sstable produced
         * <p>
         * Note that if listener is registered, the sstables are opened into {@link SSTableReader}.
         * The consumer is responsible for releasing the {@link SSTableReader}
         *
         * @param sstableProducedListener receives the produced sstables
         * @return this builder
         */
        public Builder withSSTableProducedListener(Consumer<Collection<SSTableReader>> sstableProducedListener)
        {
            this.sstableProducedListener = sstableProducedListener;
            return this;
        }

        /**
         * Whether the produced sstable should be open or not.
         * By default, the writer does not open the produced sstables
         *
         * @return this builder
         */
        public Builder openSSTableOnProduced()
        {
            this.openSSTableOnProduced = true;
            return this;
        }

        public HarrySSTableWriter build()
        {
            if (directory == null)
                throw new IllegalStateException("No ouptut directory specified, you should provide a directory with inDirectory()");
            if (schemaStatement == null)
                throw new IllegalStateException("Missing schema, you should provide the schema for the SSTable to create with forTable()");

            Preconditions.checkState(Sets.difference(SchemaConstants.LOCAL_SYSTEM_KEYSPACE_NAMES, Schema.instance.getKeyspaces()).isEmpty(),
                                     "Local keyspaces were not loaded. If this is running as a client, please make sure to add %s=true system property.",
                                     CassandraRelevantProperties.FORCE_LOAD_LOCAL_KEYSPACES.getKey());

            // Assign the default max SSTable size if not defined in builder
            if (isMaxSSTableSizeUnset())
            {
                maxSSTableSizeInMiB = sorted ? -1L : DEFAULT_BUFFER_SIZE_IN_MIB_FOR_UNSORTED;
            }

            synchronized (HarrySSTableWriter.class)
            {
                String keyspaceName = schemaStatement.keyspace();
                String tableName = schemaStatement.table();

                Schema.instance.submit(SchemaTransformations.addKeyspace(KeyspaceMetadata.create(keyspaceName,
                                                                                                 KeyspaceParams.simple(1),
                                                                                                 Tables.none(),
                                                                                                 Views.none(),
                                                                                                 Types.none(),
                                                                                                 UserFunctions.none()), true));

                KeyspaceMetadata ksm = KeyspaceMetadata.create(keyspaceName,
                                                               KeyspaceParams.simple(1),
                                                               Tables.none(),
                                                               Views.none(),
                                                               Types.none(),
                                                               UserFunctions.none());

                TableMetadata tableMetadata = Schema.instance.getTableMetadata(keyspaceName, tableName);
                if (tableMetadata == null)
                {
                    Types types = createTypes(keyspaceName);
                    Schema.instance.submit(SchemaTransformations.addTypes(types, true));
                    tableMetadata = createTable(types, ksm.userFunctions);
                    Schema.instance.submit(SchemaTransformations.addTable(tableMetadata, true));

                    if (buildIndexes && !indexStatements.isEmpty())
                    {
                        // we need to commit keyspace metadata first so applyIndexes sees that keyspace from TCM
                        commitKeyspaceMetadata(ksm.withSwapped(ksm.tables.with(tableMetadata)));
                        applyIndexes(keyspaceName);
                    }

                    KeyspaceMetadata keyspaceMetadata = ClusterMetadata.current().schema.getKeyspaceMetadata(keyspaceName);
                    tableMetadata = keyspaceMetadata.tables.getNullable(tableName);

                    Schema.instance.submit(SchemaTransformations.addTable(tableMetadata, true));
                }

                KeyspaceMetadata keyspaceMetadata = ClusterMetadata.current().schema.getKeyspaceMetadata(keyspaceName);
                Keyspace keyspace = Keyspace.mockKS(keyspaceMetadata);
                Directories directories = new Directories(tableMetadata, Collections.singleton(new Directories.DataDirectory(new File(directory.toPath()))));
                ColumnFamilyStore cfs = ColumnFamilyStore.createColumnFamilyStore(keyspace,
                                                                tableName,
                                                                tableMetadata,
                                                                directories,
                                                                false,
                                                                false);

                keyspace.initCfCustom(cfs);

                // this is the empty directory / leftover from times we initialized ColumnFamilyStore
                // it will automatically create directories for keyspace and table on disk after initialization
                // we set that directory to the destination of generated SSTables so we just remove empty directories here
                try
                {
                    new File(directory, keyspaceName).deleteRecursive();
                }
                catch (UncheckedIOException ex)
                {
                    if (!(ex.getCause() instanceof NoSuchFileException))
                    {
                        throw ex;
                    }
                }

                TableMetadataRef ref = tableMetadata.ref;
                AbstractSSTableSimpleWriter writer = sorted
                                                     ? new SSTableSimpleWriter(directory, ref, cfs.metadata.get().regularAndStaticColumns(), maxSSTableSizeInMiB)
                                                     : new SSTableSimpleUnsortedWriter(directory, ref, cfs.metadata.get().regularAndStaticColumns(), maxSSTableSizeInMiB);

                if (format != null)
                    writer.setSSTableFormatType(format);

                if (buildIndexes && !indexStatements.isEmpty() && cfs != null)
                {
                    StorageAttachedIndexGroup saiGroup = StorageAttachedIndexGroup.getIndexGroup(cfs);
                    if (saiGroup != null)
                        writer.addIndexGroup(saiGroup);
                }

                if (sstableProducedListener != null)
                    writer.setSSTableProducedListener(sstableProducedListener);

                writer.setShouldOpenProducedSSTable(openSSTableOnProduced);

                return new HarrySSTableWriter(writer);
            }
        }

        private boolean isMaxSSTableSizeUnset()
        {
            return maxSSTableSizeInMiB <= 0;
        }

        private Types createTypes(String keyspace)
        {
            Types.RawBuilder builder = Types.rawBuilder(keyspace);
            for (CreateTypeStatement.Raw st : typeStatements)
                st.addToRawBuilder(builder);
            return builder.build();
        }

        /**
         * Applies any provided index definitions to the target table
         *
         * @param keyspaceName name of the keyspace to apply indexes for
         * @return table metadata reflecting applied indexes
         */
        private void applyIndexes(String keyspaceName)
        {
            ClientState state = ClientState.forInternalCalls();

            for (CreateIndexStatement.Raw statement : indexStatements)
            {
                Keyspaces keyspaces = statement.prepare(state).apply(ClusterMetadata.current());
                commitKeyspaceMetadata(keyspaces.getNullable(keyspaceName));
            }
        }

        private void commitKeyspaceMetadata(KeyspaceMetadata keyspaceMetadata)
        {
            SchemaTransformation schemaTransformation = new SchemaTransformation()
            {
                @Override
                public Keyspaces apply(ClusterMetadata metadata)
                {
                    return metadata.schema.getKeyspaces().withAddedOrUpdated(keyspaceMetadata);
                }

                @Override
                public boolean compatibleWith(ClusterMetadata metadata)
                {
                    return true;
                }
            };
            ClusterMetadataService.instance().commit(new AlterSchema(schemaTransformation));
        }

        /**
         * Creates the table according to schema statement
         *
         * @param types types this table should be created with
         */
        private TableMetadata createTable(Types types, UserFunctions functions)
        {
            ClientState state = ClientState.forInternalCalls();
            CreateTableStatement statement = schemaStatement.prepare(state);
            statement.validate(ClientState.forInternalCalls());

            TableMetadata.Builder builder = statement.builder(types, functions);
            if (partitioner != null)
                builder.partitioner(partitioner);

            return builder.build();
        }
    }
}
