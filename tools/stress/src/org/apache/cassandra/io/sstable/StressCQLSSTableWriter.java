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
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ArrayUtils;

import org.antlr.runtime.RecognitionException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLFragmentParser;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.CqlParser;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.UpdateParameters;
import org.apache.cassandra.cql3.functions.types.TypeCodec;
import org.apache.cassandra.cql3.statements.UpdateStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTypeStatement;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTransformations;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.JavaDriverUtils;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

/**
 * Utility to write SSTables.
 * <p>
 * Typical usage looks like:
 * <pre>
 *   String type = CREATE TYPE myKs.myType (a int, b int)";
 *   String schema = "CREATE TABLE myKs.myTable ("
 *                 + "  k int PRIMARY KEY,"
 *                 + "  v1 text,"
 *                 + "  v2 int,"
 *                 + "  v3 myType,"
 *                 + ")";
 *   String insert = "INSERT INTO myKs.myTable (k, v1, v2, v3) VALUES (?, ?, ?, ?)";
 *
 *   // Creates a new writer. You need to provide at least the directory where to write the created sstable,
 *   // the schema for the sstable to write and a (prepared) insert statement to use. If you do not use the
 *   // default partitioner (Murmur3Partitioner), you will also need to provide the partitioner in use, see
 *   // StressCQLSSTableWriter.Builder for more details on the available options.
 *   StressCQLSSTableWriter writer = StressCQLSSTableWriter.builder()
 *                                             .inDirectory("path/to/directory")
 *                                             .withType(type)
 *                                             .forTable(schema)
 *                                             .using(insert).build();
 *
 *   UserType myType = writer.getUDType("myType");
 *   // Adds a nember of rows to the resulting sstable
 *   writer.addRow(0, "test1", 24, myType.newValue().setInt("a", 10).setInt("b", 20));
 *   writer.addRow(1, "test2", null, null);
 *   writer.addRow(2, "test3", 42, myType.newValue().setInt("a", 30).setInt("b", 40));
 *
 *   // Close the writer, finalizing the sstable
 *   writer.close();
 * </pre>
 *
 * Please note that {@code StressCQLSSTableWriter} is <b>not</b> thread-safe (multiple threads cannot access the
 * same instance). It is however safe to use multiple instances in parallel (even if those instance write
 * sstables for the same table).
 */
public class StressCQLSSTableWriter implements Closeable
{
    public static final ByteBuffer UNSET_VALUE = ByteBufferUtil.UNSET_BYTE_BUFFER;

    static
    {
        DatabaseDescriptor.clientInitialization(false);
        // Partitioner is not set in client mode.
        if (DatabaseDescriptor.getPartitioner() == null)
            DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    private final AbstractSSTableSimpleWriter writer;
    private final UpdateStatement insert;
    private final List<ColumnSpecification> boundNames;
    private final List<TypeCodec> typeCodecs;
    private final ColumnFamilyStore cfs;

    private StressCQLSSTableWriter(ColumnFamilyStore cfs, AbstractSSTableSimpleWriter writer, UpdateStatement insert, List<ColumnSpecification> boundNames)
    {
        this.cfs = cfs;
        this.writer = writer;
        this.insert = insert;
        this.boundNames = boundNames;
        this.typeCodecs = boundNames.stream().map(bn ->  JavaDriverUtils.codecFor(JavaDriverUtils.driverType(bn.type)))
                                             .collect(Collectors.toList());
    }

    /**
     * Returns a new builder for a StressCQLSSTableWriter.
     *
     * @return the new builder.
     */
    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * Adds a new row to the writer.
     * <p>
     * This is a shortcut for {@code addRow(Arrays.asList(values))}.
     *
     * @param values the row values (corresponding to the bind variables of the
     * insertion statement used when creating by this writer).
     * @return this writer.
     */
    public StressCQLSSTableWriter addRow(Object... values)
    throws InvalidRequestException, IOException
    {
        return addRow(Arrays.asList(values));
    }

    /**
     * Adds a new row to the writer.
     * <p>
     * Each provided value type should correspond to the types of the CQL column
     * the value is for. The correspondance between java type and CQL type is the
     * same one than the one documented at
     * www.datastax.com/drivers/java/2.0/apidocs/com/datastax/driver/core/DataType.Name.html#asJavaClass().
     * <p>
     * If you prefer providing the values directly as binary, use
     * {@link #rawAddRow} instead.
     *
     * @param values the row values (corresponding to the bind variables of the
     * insertion statement used when creating by this writer).
     * @return this writer.
     */
    public StressCQLSSTableWriter addRow(List<Object> values)
    throws InvalidRequestException, IOException
    {
        int size = Math.min(values.size(), boundNames.size());
        List<ByteBuffer> rawValues = new ArrayList<>(size);

        for (int i = 0; i < size; i++)
        {
            Object value = values.get(i);
            rawValues.add(serialize(value, typeCodecs.get(i)));
        }

        return rawAddRow(rawValues);
    }

    /**
     * Adds a new row to the writer.
     * <p>
     * This is equivalent to the other addRow methods, but takes a map whose
     * keys are the names of the columns to add instead of taking a list of the
     * values in the order of the insert statement used during construction of
     * this write.
     * <p>
     * Please note that the column names in the map keys must be in lowercase unless
     * the declared column name is a
     * <a href="http://cassandra.apache.org/doc/cql3/CQL.html#identifiers">case-sensitive quoted identifier</a>
     * (in which case the map key must use the exact case of the column).
     *
     * @param values a map of colum name to column values representing the new
     * row to add. Note that if a column is not part of the map, it's value will
     * be {@code null}. If the map contains keys that does not correspond to one
     * of the column of the insert statement used when creating this writer, the
     * the corresponding value is ignored.
     * @return this writer.
     */
    public StressCQLSSTableWriter addRow(Map<String, Object> values)
    throws InvalidRequestException, IOException
    {
        int size = boundNames.size();
        List<ByteBuffer> rawValues = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
        {
            ColumnSpecification spec = boundNames.get(i);
            Object value = values.get(spec.name.toString());
            rawValues.add(serialize(value, typeCodecs.get(i)));
        }
        return rawAddRow(rawValues);
    }

    /**
     * Adds a new row to the writer given already serialized values.
     *
     * @param values the row values (corresponding to the bind variables of the
     * insertion statement used when creating by this writer) as binary.
     * @return this writer.
     */
    public StressCQLSSTableWriter rawAddRow(ByteBuffer... values)
    throws InvalidRequestException, IOException
    {
        return rawAddRow(Arrays.asList(values));
    }

    /**
     * Adds a new row to the writer given already serialized values.
     * <p>
     * This is a shortcut for {@code rawAddRow(Arrays.asList(values))}.
     *
     * @param values the row values (corresponding to the bind variables of the
     * insertion statement used when creating by this writer) as binary.
     * @return this writer.
     */
    public StressCQLSSTableWriter rawAddRow(List<ByteBuffer> values)
    throws InvalidRequestException, IOException
    {
        if (values.size() != boundNames.size())
            throw new InvalidRequestException(String.format("Invalid number of arguments, expecting %d values but got %d", boundNames.size(), values.size()));

        QueryOptions options = QueryOptions.forInternalCalls(null, values);
        ClientState state = ClientState.forInternalCalls();
        List<ByteBuffer> keys = insert.buildPartitionKeyNames(options, state);
        SortedSet<Clustering<?>> clusterings = insert.createClustering(options, state);

        long now = currentTimeMillis();
        // Note that we asks indexes to not validate values (the last 'false' arg below) because that triggers a 'Keyspace.open'
        // and that forces a lot of initialization that we don't want.
        UpdateParameters params = new UpdateParameters(insert.metadata(),
                                                       insert.updatedColumns(),
                                                       ClientState.forInternalCalls(),
                                                       options,
                                                       insert.getTimestamp(TimeUnit.MILLISECONDS.toMicros(now), options),
                                                       (int) TimeUnit.MILLISECONDS.toSeconds(now),
                                                       insert.getTimeToLive(options),
                                                       Collections.emptyMap());

        try
        {
            for (ByteBuffer key : keys)
            {
                for (Clustering<?> clustering : clusterings)
                    insert.addUpdateForKey(writer.getUpdateFor(key), clustering, params);
            }
            return this;
        }
        catch (SSTableSimpleUnsortedWriter.SyncException e)
        {
            // If we use a BufferedWriter and had a problem writing to disk, the IOException has been
            // wrapped in a SyncException (see BufferedWriter below). We want to extract that IOE.
            throw (IOException)e.getCause();
        }
    }

    /**
     * Adds a new row to the writer given already serialized values.
     * <p>
     * This is equivalent to the other rawAddRow methods, but takes a map whose
     * keys are the names of the columns to add instead of taking a list of the
     * values in the order of the insert statement used during construction of
     * this write.
     *
     * @param values a map of colum name to column values representing the new
     * row to add. Note that if a column is not part of the map, it's value will
     * be {@code null}. If the map contains keys that does not correspond to one
     * of the column of the insert statement used when creating this writer, the
     * the corresponding value is ignored.
     * @return this writer.
     */
    public StressCQLSSTableWriter rawAddRow(Map<String, ByteBuffer> values)
    throws InvalidRequestException, IOException
    {
        int size = Math.min(values.size(), boundNames.size());
        List<ByteBuffer> rawValues = new ArrayList<>(size);
        for (int i = 0; i < size; i++) 
        {
            ColumnSpecification spec = boundNames.get(i);
            rawValues.add(values.get(spec.name.toString()));
        }
        return rawAddRow(rawValues);
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

    private ByteBuffer serialize(Object value, TypeCodec codec)
    {
        if (value == null || value == UNSET_VALUE)
            return (ByteBuffer) value;

        return codec.serialize(value, ProtocolVersion.CURRENT);
    }
    /**
     * The writer loads data in directories corresponding to how they laid out on the server.
     * <p>
     * {keyspace}/{table-cfid}/
     *
     * This method can be used to fetch the innermost directory with the sstable components
     * @return The directory containing the sstable components
     */
    public File getInnermostDirectory()
    {
        return cfs.getDirectories().getDirectoryForNewSSTables().toJavaIOFile();
    }

    /**
     * A Builder for a StressCQLSSTableWriter object.
     */
    public static class Builder
    {
        private final List<File> directoryList;
        private ColumnFamilyStore cfs;

        protected SSTableFormat<?, ?> format = null;

        private Boolean makeRangeAware = false;

        private CreateTableStatement.Raw schemaStatement;
        private final List<CreateTypeStatement.Raw> typeStatements;
        private UpdateStatement.ParsedInsert insertStatement;
        private IPartitioner partitioner;

        private boolean sorted = false;
        private long bufferSizeInMiB = 128;

        protected Builder()
        {
            this.typeStatements = new ArrayList<>();
            this.directoryList = new ArrayList<>();
        }

        /**
         * The directory where to write the sstables.
         * <p>
         * This is a mandatory option.
         *
         * @param directory the directory to use, which should exists and be writable.
         * @return this builder.
         *
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
         * @param directory the directory to use, which should exist and be writable.
         * @return this builder.
         *
         * @throws IllegalArgumentException if {@code directory} doesn't exist or is not writable.
         */
        public Builder inDirectory(File directory)
        {
            if (!directory.exists())
                throw new IllegalArgumentException(directory + " doesn't exists");
            if (!directory.canWrite())
                throw new IllegalArgumentException(directory + " exists but is not writable");

            directoryList.add(directory);
            return this;
        }

        /**
         * A pre-instanciated ColumnFamilyStore
         * <p>
         * This is can be used in place of inDirectory and forTable
         *
         * @see #inDirectory(File)
         *
         * @param cfs the list of directories to use, which should exist and be writable.
         * @return this builder.
         *
         * @throws IllegalArgumentException if a directory doesn't exist or is not writable.
         */
        public Builder withCfs(ColumnFamilyStore cfs)
        {
            this.cfs = cfs;
            return this;
        }


        public Builder withType(String typeDefinition) throws SyntaxException
        {
            typeStatements.add(parseStatement(typeDefinition, CreateTypeStatement.Raw.class, "CREATE TYPE"));
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
         *
         * @throws IllegalArgumentException if {@code schema} is not a valid CREATE TABLE statement
         * or does not have a fully-qualified table name.
         */
        public Builder forTable(String schema)
        {
            this.schemaStatement = parseStatement(schema, CreateTableStatement.Raw.class, "CREATE TABLE");
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
         * Specify if the sstable writer should be vnode range aware.
         * This will create a sstable per vnode range.
         *
         * @param makeRangeAware
         * @return
         */
        public Builder rangeAware(boolean makeRangeAware)
        {
            this.makeRangeAware = makeRangeAware;
            return this;
        }

        /**
         * The INSERT statement defining the order of the values to add for a given CQL row.
         * <p>
         * Please note that the provided INSERT statement <b>must</b> use a fully-qualified
         * table name, one that include the keyspace name. Morewover, said statement must use
         * bind variables since it is those bind variables that will be bound to values by the
         * resulting writer.
         * <p>
         * This is a mandatory option, and this needs to be called after foTable().
         *
         * @param insert an insertion statement that defines the order
         * of column values to use.
         * @return this builder.
         *
         * @throws IllegalArgumentException if {@code insertStatement} is not a valid insertion
         * statement, does not have a fully-qualified table name or have no bind variables.
         */
        public Builder using(String insert)
        {
            this.insertStatement = parseStatement(insert, UpdateStatement.ParsedInsert.class, "INSERT");
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
         */
        public Builder withBufferSizeInMiB(int size)
        {
            this.bufferSizeInMiB = size;
            return this;
        }

        /**
         * This method is deprecated in favor of the new withBufferSizeInMiB(int size)
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
         */
        public Builder withBufferSizeInMB(int size)
        {
            return withBufferSizeInMiB(size);
        }

        /**
         * Creates a StressCQLSSTableWriter that expects sorted inputs.
         * <p>
         * If this option is used, the resulting writer will expect rows to be
         * added in SSTable sorted order (and an exception will be thrown if that
         * is not the case during insertion). The SSTable sorted order means that
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

        public StressCQLSSTableWriter build()
        {
            if (directoryList.isEmpty() && cfs == null)
                throw new IllegalStateException("No output directories specified, you should provide a directory with inDirectory()");
            if (schemaStatement == null && cfs == null)
                throw new IllegalStateException("Missing schema, you should provide the schema for the SSTable to create with forTable()");
            if (insertStatement == null)
                throw new IllegalStateException("No insert statement specified, you should provide an insert statement through using()");

            synchronized (StressCQLSSTableWriter.class)
            {
                if (cfs == null)
                    cfs = createOfflineTable(schemaStatement, typeStatements, directoryList);

                if (partitioner == null)
                    partitioner = cfs.getPartitioner();

                UpdateStatement preparedInsert = prepareInsert();
                AbstractSSTableSimpleWriter writer = sorted
                                                     ? new SSTableSimpleWriter(cfs.getDirectories().getDirectoryForNewSSTables(), cfs.metadata, preparedInsert.updatedColumns(), -1)
                                                     : new SSTableSimpleUnsortedWriter(cfs.getDirectories().getDirectoryForNewSSTables(), cfs.metadata, preparedInsert.updatedColumns(), bufferSizeInMiB);

                if (format != null)
                    writer.setSSTableFormatType(format);

                writer.setRangeAwareWriting(makeRangeAware);

                return new StressCQLSSTableWriter(cfs, writer, preparedInsert, preparedInsert.getBindVariables());
            }
        }

        private static Types createTypes(String keyspace, List<CreateTypeStatement.Raw> typeStatements)
        {
            KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(keyspace);
            Types.RawBuilder builder = Types.rawBuilder(keyspace);
            for (CreateTypeStatement.Raw st : typeStatements)
                st.addToRawBuilder(builder);

            return builder.build();
        }

        public static ColumnFamilyStore createOfflineTable(String schema, List<File> directoryList)
        {
            return createOfflineTable(parseStatement(schema, CreateTableStatement.Raw.class, "CREATE TABLE"), Collections.EMPTY_LIST, directoryList);
        }

        /**
         * Creates the table according to schema statement
         * with specified data directories
         */
        public static ColumnFamilyStore createOfflineTable(CreateTableStatement.Raw schemaStatement, List<CreateTypeStatement.Raw> typeStatements, List<File> directoryList)
        {
            String keyspace = schemaStatement.keyspace();

            Schema.instance.transform(SchemaTransformations.addKeyspace(KeyspaceMetadata.create(keyspace, KeyspaceParams.simple(1)), true));

            Types types = createTypes(keyspace, typeStatements);
            Schema.instance.transform(SchemaTransformations.addTypes(types, true));

            KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(keyspace);

            TableMetadata tableMetadata = ksm.tables.getNullable(schemaStatement.table());
            if (tableMetadata != null)
                return Schema.instance.getColumnFamilyStoreInstance(tableMetadata.id);

            ClientState state = ClientState.forInternalCalls();
            CreateTableStatement statement = schemaStatement.prepare(state);
            statement.validate(state);

            //Build metadata with a portable tableId
            tableMetadata = statement.builder(ksm.types)
                                     .id(deterministicId(schemaStatement.keyspace(), schemaStatement.table()))
                                     .build();

            Keyspace.setInitialized();
            Directories directories = new Directories(tableMetadata, directoryList.stream().map(f -> new Directories.DataDirectory(new org.apache.cassandra.io.util.File(f.toPath()))).collect(Collectors.toList()));

            Keyspace ks = Keyspace.openWithoutSSTables(keyspace);
            ColumnFamilyStore cfs =  ColumnFamilyStore.createColumnFamilyStore(ks, tableMetadata.name, TableMetadataRef.forOfflineTools(tableMetadata), directories, false, false, true);

            ks.initCfCustom(cfs);
            Schema.instance.transform(SchemaTransformations.addTable(tableMetadata, true));

            return cfs;
        }

        private static TableId deterministicId(String keyspace, String table)
        {
            return TableId.fromUUID(UUID.nameUUIDFromBytes(ArrayUtils.addAll(keyspace.getBytes(), table.getBytes())));
        }

        /**
         * Prepares insert statement for writing data to SSTable
         *
         * @return prepared Insert statement and it's bound names
         */
        private UpdateStatement prepareInsert()
        {
            ClientState state = ClientState.forInternalCalls();
            CQLStatement cqlStatement = insertStatement.prepare(state);
            UpdateStatement insert = (UpdateStatement) cqlStatement;
            insert.validate(state);

            if (insert.hasConditions())
                throw new IllegalArgumentException("Conditional statements are not supported");
            if (insert.isCounter())
                throw new IllegalArgumentException("Counter update statements are not supported");
            if (insert.getBindVariables().isEmpty())
                throw new IllegalArgumentException("Provided insert statement has no bind variables");

            return insert;
        }
    }

    public static <T extends CQLStatement.Raw> T parseStatement(String query, Class<T> klass, String type)
    {
        try
        {
            CQLStatement.Raw stmt = CQLFragmentParser.parseAnyUnhandled(CqlParser::query, query);

            if (!stmt.getClass().equals(klass))
                throw new IllegalArgumentException("Invalid query, must be a " + type + " statement but was: " + stmt.getClass());

            return klass.cast(stmt);
        }
        catch (RecognitionException | RequestValidationException e)
        {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }
}
