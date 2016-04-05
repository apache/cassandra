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

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UpdateParameters;
import org.apache.cassandra.cql3.functions.UDHelper;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.cql3.statements.CreateTypeStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.cql3.statements.UpdateStatement;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

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
 *   // CQLSSTableWriter.Builder for more details on the available options.
 *   CQLSSTableWriter writer = CQLSSTableWriter.builder()
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
 * Please note that {@code CQLSSTableWriter} is <b>not</b> thread-safe (multiple threads cannot access the
 * same instance). It is however safe to use multiple instances in parallel (even if those instance write
 * sstables for the same table).
 */
public class CQLSSTableWriter implements Closeable
{
    static
    {
        Config.setClientMode(true);
        // Partitioner is not set in client mode.
        if (DatabaseDescriptor.getPartitioner() == null)
            DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    private final AbstractSSTableSimpleWriter writer;
    private final UpdateStatement insert;
    private final List<ColumnSpecification> boundNames;

    private CQLSSTableWriter(AbstractSSTableSimpleWriter writer, UpdateStatement insert, List<ColumnSpecification> boundNames)
    {
        this.writer = writer;
        this.insert = insert;
        this.boundNames = boundNames;
    }

    /**
     * Returns a new builder for a CQLSSTableWriter.
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
    public CQLSSTableWriter addRow(Object... values)
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
    public CQLSSTableWriter addRow(List<Object> values)
    throws InvalidRequestException, IOException
    {
        int size = Math.min(values.size(), boundNames.size());
        List<ByteBuffer> rawValues = new ArrayList<>(size);

        for (int i = 0; i < size; i++)
        {
            TypeCodec typeCodec = UDHelper.codecFor(UDHelper.driverType(boundNames.get(i).type));
            rawValues.add(values.get(i) == null ? null : typeCodec.serialize(values.get(i),
                                                                             ProtocolVersion.NEWEST_SUPPORTED));
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
    public CQLSSTableWriter addRow(Map<String, Object> values)
    throws InvalidRequestException, IOException
    {
        int size = boundNames.size();
        List<ByteBuffer> rawValues = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
        {
            ColumnSpecification spec = boundNames.get(i);
            Object value = values.get(spec.name.toString());
            rawValues.add(value == null ? null : ((AbstractType) spec.type).decompose(value));
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
    public CQLSSTableWriter rawAddRow(ByteBuffer... values)
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
    public CQLSSTableWriter rawAddRow(List<ByteBuffer> values)
    throws InvalidRequestException, IOException
    {
        if (values.size() != boundNames.size())
            throw new InvalidRequestException(String.format("Invalid number of arguments, expecting %d values but got %d", boundNames.size(), values.size()));

        QueryOptions options = QueryOptions.forInternalCalls(null, values);
        List<ByteBuffer> keys = insert.buildPartitionKeyNames(options);
        SortedSet<Clustering> clusterings = insert.createClustering(options);

        long now = System.currentTimeMillis() * 1000;
        // Note that we asks indexes to not validate values (the last 'false' arg below) because that triggers a 'Keyspace.open'
        // and that forces a lot of initialization that we don't want.
        UpdateParameters params = new UpdateParameters(insert.cfm,
                                                       insert.updatedColumns(),
                                                       options,
                                                       insert.getTimestamp(now, options),
                                                       insert.getTimeToLive(options),
                                                       Collections.<DecoratedKey, Partition>emptyMap());

        try
        {
            for (ByteBuffer key : keys)
            {
                for (Clustering clustering : clusterings)
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
    public CQLSSTableWriter rawAddRow(Map<String, ByteBuffer> values)
    throws InvalidRequestException, IOException
    {
        int size = Math.min(values.size(), boundNames.size());
        List<ByteBuffer> rawValues = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            ColumnSpecification spec = boundNames.get(i);
            rawValues.add(values.get(spec.name.toString()));
        }
        return rawAddRow(rawValues);
    }

    /**
     * Returns the User Defined type, used in this SSTable Writer, that can
     * be used to create UDTValue instances.
     *
     * @param dataType name of the User Defined type
     * @return user defined type
     */
    public com.datastax.driver.core.UserType getUDType(String dataType)
    {
        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(insert.keyspace());
        UserType userType = ksm.types.getNullable(ByteBufferUtil.bytes(dataType));
        return (com.datastax.driver.core.UserType) UDHelper.driverType(userType);
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

    /**
     * A Builder for a CQLSSTableWriter object.
     */
    public static class Builder
    {
        private File directory;

        protected SSTableFormat.Type formatType = null;

        private CreateTableStatement.RawStatement schemaStatement;
        private final List<CreateTypeStatement> typeStatements;
        private UpdateStatement.ParsedInsert insertStatement;
        private IPartitioner partitioner;

        private boolean sorted = false;
        private long bufferSizeInMB = 128;

        protected Builder() {
            this.typeStatements = new ArrayList<>();
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
         * @param directory the directory to use, which should exists and be writable.
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

            this.directory = directory;
            return this;
        }

        public Builder withType(String typeDefinition) throws SyntaxException
        {
            typeStatements.add(parseStatement(typeDefinition, CreateTypeStatement.class, "CREATE TYPE"));
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
            this.schemaStatement = parseStatement(schema, CreateTableStatement.RawStatement.class, "CREATE TABLE");
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
         * a new SSTable. This correspond roughly to the data size that will have the created
         * sstable.
         * <p>
         * The default is 128MB, which should be reasonable for a 1GB heap. If you experience
         * OOM while using the writer, you should lower this value.
         *
         * @param size the size to use in MB.
         * @return this builder.
         */
        public Builder withBufferSizeInMB(int size)
        {
            this.bufferSizeInMB = size;
            return this;
        }

        /**
         * Creates a CQLSSTableWriter that expects sorted inputs.
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
         * Note that if used, some option like withBufferSizeInMB will be ignored.
         *
         * @return this builder.
         */
        public Builder sorted()
        {
            this.sorted = true;
            return this;
        }

        @SuppressWarnings("resource")
        public CQLSSTableWriter build()
        {
            if (directory == null)
                throw new IllegalStateException("No ouptut directory specified, you should provide a directory with inDirectory()");
            if (schemaStatement == null)
                throw new IllegalStateException("Missing schema, you should provide the schema for the SSTable to create with forTable()");
            if (insertStatement == null)
                throw new IllegalStateException("No insert statement specified, you should provide an insert statement through using()");

            synchronized (CQLSSTableWriter.class)
            {
                String keyspace = schemaStatement.keyspace();

                if (Schema.instance.getKSMetaData(keyspace) == null)
                    Schema.instance.load(KeyspaceMetadata.create(keyspace, KeyspaceParams.simple(1)));

                createTypes(keyspace);
                CFMetaData cfMetaData = createTable(keyspace);
                Pair<UpdateStatement, List<ColumnSpecification>> preparedInsert = prepareInsert();

                AbstractSSTableSimpleWriter writer = sorted
                                                     ? new SSTableSimpleWriter(directory, cfMetaData, preparedInsert.left.updatedColumns())
                                                     : new SSTableSimpleUnsortedWriter(directory, cfMetaData, preparedInsert.left.updatedColumns(), bufferSizeInMB);

                if (formatType != null)
                    writer.setSSTableFormatType(formatType);

                return new CQLSSTableWriter(writer, preparedInsert.left, preparedInsert.right);
            }
        }

        private void createTypes(String keyspace)
        {
            KeyspaceMetadata ksm = Schema.instance.getKSMetaData(keyspace);
            Types.RawBuilder builder = Types.rawBuilder(keyspace);
            for (CreateTypeStatement st : typeStatements)
                st.addToRawBuilder(builder);

            ksm = ksm.withSwapped(builder.build());
            Schema.instance.setKeyspaceMetadata(ksm);
        }
        /**
         * Creates the table according to schema statement
         *
         * @param keyspace name of the keyspace where table should be created
         */
        private CFMetaData createTable(String keyspace)
        {
            KeyspaceMetadata ksm = Schema.instance.getKSMetaData(keyspace);

            CFMetaData cfMetaData = ksm.tables.getNullable(schemaStatement.columnFamily());
            if (cfMetaData == null)
            {
                CreateTableStatement statement = (CreateTableStatement) schemaStatement.prepare(ksm.types).statement;
                statement.validate(ClientState.forInternalCalls());

                cfMetaData = statement.getCFMetaData();

                Schema.instance.load(cfMetaData);
                Schema.instance.setKeyspaceMetadata(ksm.withSwapped(ksm.tables.with(cfMetaData)));
            }

            if (partitioner != null)
                return cfMetaData.copy(partitioner);
            else
                return cfMetaData;
        }

        /**
         * Prepares insert statement for writing data to SSTable
         *
         * @return prepared Insert statement and it's bound names
         */
        private Pair<UpdateStatement, List<ColumnSpecification>> prepareInsert()
        {
            ParsedStatement.Prepared cqlStatement = insertStatement.prepare();
            UpdateStatement insert = (UpdateStatement) cqlStatement.statement;
            insert.validate(ClientState.forInternalCalls());

            if (insert.hasConditions())
                throw new IllegalArgumentException("Conditional statements are not supported");
            if (insert.isCounter())
                throw new IllegalArgumentException("Counter update statements are not supported");
            if (cqlStatement.boundNames.isEmpty())
                throw new IllegalArgumentException("Provided insert statement has no bind variables");

            return Pair.create(insert, cqlStatement.boundNames);
        }
    }

    private static <T extends ParsedStatement> T parseStatement(String query, Class<T> klass, String type)
    {
        try
        {
            ParsedStatement stmt = QueryProcessor.parseStatement(query);

            if (!stmt.getClass().equals(klass))
                throw new IllegalArgumentException("Invalid query, must be a " + type + " statement but was: " + stmt.getClass());

            return klass.cast(stmt);
        }
        catch (RequestValidationException e)
        {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }
}
