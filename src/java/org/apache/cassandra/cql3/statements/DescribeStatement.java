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
package org.apache.cassandra.cql3.statements;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import com.googlecode.concurrenttrees.common.Iterables;
import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.UTName;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.UDAggregate;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.db.KeyspaceNotDefinedException;
import org.apache.cassandra.db.SchemaCQLHelper;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static java.lang.String.format;

/**
 * Implements the foundations for all concrete {@code DESCRIBE} statement implementations.
 *
 * <p>
 * Returns a result set that consists of a single column {@code schema_part} of type {@code text}
 * and represents a part of the whole {@code DESCRIBE} result.
 * The get the whole DDL as a single string, the contents of the {@code schema_part} column of all
 * rows must be concatenated on the client side. Whitespaces (incl newlines) must not be inserted
 * by a client between each rows' {@code schema_part} value as the row separation is arbitrary and
 * can happen anywhere in the whole result output.
 * </p>
 *
 * <p>
 * Paging is implemented to avoid result sets that are too big.
 * The paging state must not be interpreted on the client side.
 * Only row-level paging is supported. Byte-level paging will result in an error.
 * If the schema changes between two pages, the following page will return an error, as the
 * consistency and correctness of the whole output can no longer be guaranteed.
 * </p>
 *
 * Syntax description (copied from {@code cqlsh}):
 *
 * PLEASE KEEP THIS IN SYNC WITH {@code cqlsh.Shell#do_describe} IN {@code bin/cqlsh.py}
 * AND {@code resources/cassandra/bin/cqlsh.py} !
 *
 * <pre><code>
 *         </code>{@link TheKeyspaces DESCRIBE KEYSPACES}<code>
 *
 *           Output the names of all keyspaces.
 *
 *         </code>{@link Keyspace DESCRIBE [ONLY] KEYSPACE [&lt;keyspacename&gt;] [WITH INTERNALS]}<code>
 *
 *           Output CQL commands that could be used to recreate the given keyspace,
 *           and the objects in it (such as tables, types, functions, etc.).
 *           In some cases, as the CQL interface matures, there will be some metadata
 *           about a keyspace that is not representable with CQL. That metadata will not be shown.
 *
 *           The '<keyspacename>' argument may be omitted, in which case the current
 *           keyspace will be described.
 *
 *           If WITH INTERNALS is specified, the output contains the table IDs and is
 *           adopted to represent the DDL necessary to "re-create" dropped columns.
 *
 *           If ONLY is specified, only the DDL to recreate the keyspace will be created.
 *           All keyspace elements, like tables, types, functions, etc will be omitted.
 *
 *         </code>{@link Tables DESCRIBE TABLES}<code>
 *
 *           Output the names of all tables in the current keyspace, or in all
 *           keyspaces if there is no current keyspace.
 *
 *         </code>{@link Table DESCRIBE TABLE [&lt;keyspace&gt;.]&lt;tablename&gt; [WITH INTERNALS]}<code>
 *
 *           Output CQL commands that could be used to recreate the given table.
 *           In some cases, as above, there may be table metadata which is not
 *           representable and which will not be shown.
 *
 *           If WITH INTERNALS is specified, the output contains the table ID and is
 *           adopted to represent the DDL necessary to "re-create" dropped columns.
 *
 *         </code>{@link Index DESCRIBE INDEX &lt;indexname&gt;}<code>
 *
 *           Output the CQL command that could be used to recreate the given index.
 *           In some cases, there may be index metadata which is not representable
 *           and which will not be shown.
 *
 *         </code>{@link View DESCRIBE MATERIALIZED VIEW &lt;viewname&gt; [WITH INTERNALS]}<code>
 *
 *           Output the CQL command that could be used to recreate the given materialized view.
 *           In some cases, there may be materialized view metadata which is not representable
 *           and which will not be shown.
 *
 *           If WITH INTERNALS is specified, the output contains the table ID and is
 *           adopted to represent the DDL necessary to "re-create" dropped columns.
 *
 *         </code>{@link Cluster DESCRIBE CLUSTER}<code>
 *
 *           Output information about the connected Cassandra cluster, such as the
 *           cluster name, and the partitioner and snitch in use. When you are
 *           connected to a non-system keyspace, also shows endpoint-range
 *           ownership information for the Cassandra ring.
 *
 *         </code>{@link TheSchema DESCRIBE [FULL] SCHEMA [WITH INTERNALS]}<code>
 *
 *           Output CQL commands that could be used to recreate the entire (non-system) schema.
 *           Works as though "DESCRIBE KEYSPACE k" was invoked for each non-system keyspace
 *           k. Use DESCRIBE FULL SCHEMA to include the system keyspaces.
 *
 *           If WITH INTERNALS is specified, the output contains the table IDs and is
 *           adopted to represent the DDL necessary to "re-create" dropped columns.
 *
 *         </code>{@link Types DESCRIBE TYPES}<code>
 *
 *           Output the names of all user-defined-types in the current keyspace, or in all
 *           keyspaces if there is no current keyspace.
 *
 *         </code>{@link Type DESCRIBE TYPE [&lt;keyspace&gt;.]&lt;type&gt;}<code>
 *
 *           Output the CQL command that could be used to recreate the given user-defined-type.
 *
 *         </code>{@link Functions DESCRIBE FUNCTIONS}<code>
 *
 *           Output the names of all user-defined-functions in the current keyspace, or in all
 *           keyspaces if there is no current keyspace.
 *
 *         </code>{@link Function DESCRIBE FUNCTION [&lt;keyspace&gt;.]&lt;function&gt;}<code>
 *
 *           Output the CQL command that could be used to recreate the given user-defined-function.
 *
 *         </code>{@link Aggregates DESCRIBE AGGREGATES}<code>
 *
 *           Output the names of all user-defined-aggregates in the current keyspace, or in all
 *           keyspaces if there is no current keyspace.
 *
 *         </code>{@link Aggregate DESCRIBE AGGREGATE [&lt;keyspace&gt;.]&lt;aggregate&gt;}<code>
 *
 *           Output the CQL command that could be used to recreate the given user-defined-aggregate.
 *
 *         </code>{@link Generic DESCRIBE &lt;objname&gt; [WITH INTERNALS]}<code>
 *
 *           Output CQL commands that could be used to recreate the entire object schema,
 *           where object can be either a keyspace or a table or an index or a materialized
 *           view (in this order).
 *
 *           If WITH INTERNALS is specified and &lt;objname&gt; represents a keyspace, table
 *           materialized view, the output contains the table IDs and is
 *           adopted to represent the DDL necessary to "re-create" dropped columns.
 *
 *           &lt;objname&gt; (obviously) cannot be any of the "describe what" qualifiers like
 *           "cluster", "table", etc.
 * </code></pre>
 */
public abstract class DescribeStatement extends CQLStatement.Raw implements CQLStatement
{
    private static final int LINE_WIDTH = 80;

    private static final String KS = "system";
    private static final String CF = "describe";
    private static final List<ColumnSpecification> metadata =
            ImmutableList.of(new ColumnSpecification(KS, CF, new ColumnIdentifier("schema_part", true), UTF8Type.instance));

    /**
     * "Magic version" for the paging state.
     */
    private static final int PAGING_STATE_VERSION = 0x0001;

    static final String SCHEMA_CHANGED_WHILE_PAGING_MESSAGE = "The schema has changed since the previous page of the DESCRIBE statement result. " +
                                                              "Please retry the DESCRIBE statement.";

    /**
     * Helper functionality for unit tests to inject a "mocked" schema.
     * Delegates to {@link DefaultSchemaAccess} in production.
     */
    @VisibleForTesting
    static volatile SchemaAccess schema = new DefaultSchemaAccess();

    protected final SchemaSnapshot snapshot;

    protected boolean includeInternalDetails;

    protected DescribeStatement()
    {
        this.snapshot = schema.snapshot();
    }

    public void withInternalDetails()
    {
        this.includeInternalDetails = true;
    }

    @Override
    public CQLStatement prepare(ClientState clientState) throws RequestValidationException
    {
        return this;
    }

    public List<ColumnSpecification> getBindVariables()
    {
        return Collections.emptyList();
    }

    @Override
    public void authorize(ClientState state)
    {
    }

    @Override
    public void validate(ClientState state)
    {
    }

    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.DESCRIBE);
    }

    @Override
    public final ResultMessage execute(QueryState state, QueryOptions options, long queryStartNanoTime) throws RequestValidationException, RequestExecutionException
    {
        return executeLocally(state, options);
    }

    @Override
    public ResultMessage executeLocally(QueryState state, QueryOptions options)
    {
        PagingState pagingState = options.getPagingState();

        // The paging implemented here uses some arbitray row number as the partition-key for paging,
        // which is used to skip/limit the result from the Java Stream. This works good enough for
        // reasonably sized schemas. Even a 'DESCRIBE SCHEMA' for an abnormally schema with 10000 tables
        // completes within a few seconds. This seems good enough for now. Once Cassandra actually supports
        // more than a few hundred tables, the implementation here should be reconsidered.
        //
        // Paging is only supported on row-level.
        //
        // The "partition key" in the paging-state contains a serialized object:
        //   (short) version, currently 0x0001
        //   (long) row offset
        //   (vint bytes) serialized schema hash (currently the result of Keyspaces.hashCode())
        //

        long offset = -1L;

        if (pagingState != null)
        {
            try (DataInputBuffer dib = new DataInputBuffer(pagingState.partitionKey, false))
            {
                if (dib.readShort() != PAGING_STATE_VERSION)
                {
                    // DESCRIBE paging state version number
                    throw new InvalidRequestException("Incompatible paging state");
                }
                offset = dib.readLong();
                ByteBuffer hash = ByteBufferUtil.readWithVIntLength(dib);
                if (!snapshot.schemaHash().equals(hash))
                {
                    // The schema has changed since the received paging state has been generated.
                    throw new InvalidRequestException(SCHEMA_CHANGED_WHILE_PAGING_MESSAGE);
                }
            }
            catch (IOException e)
            {
                throw new InvalidRequestException("Invalid paging state.", e);
            }
        }

        int pageSize = options.getPageSize();

        // Create the actual stream of strings that represents the output of the DESCRIBE command.
        // Each individual String in the stream represents a row in the result set.
        Stream<String> stringStream = describe(state.getClientState());

        if (offset > 0L)
            stringStream = stringStream.skip(offset);
        if (pageSize > 0)
            stringStream = stringStream.limit(pageSize);

        // make sure that offset+pageSize are not negative
        offset = Math.max(0L, offset);
        pageSize = Math.max(0, pageSize);

        List<List<ByteBuffer>> rows = stringStream.map(str -> str + "\n")
                                                  .map(UTF8Type.instance::decompose)
                                                  // map to a single-column row
                                                  .map(Collections::singletonList)
                                                  // collect the rows for the result set
                                                  .collect(Collectors.toList());

        if (offset == 0L && rows.isEmpty())
        {
            // Some implementations need to be "informed" that the name pattern resolved to no object (i.e. "object not found").
            onEmpty();
        }

        ResultSet.ResultMetadata resultMetadata = new ResultSet.ResultMetadata(metadata, pagingState);
        ResultSet result = new ResultSet(resultMetadata, rows);

        if (pageSize > 0 && rows.size() >= pageSize)
        {
            ByteBuffer pagingData;
            try (DataOutputBuffer dob = new DataOutputBuffer())
            {
                dob.writeShort(PAGING_STATE_VERSION);
                dob.writeLong(offset + pageSize);
                ByteBufferUtil.writeWithVIntLength(snapshot.schemaHash(), dob);
                pagingData = dob.asNewBuffer();
            }
            catch (IOException e)
            {
                throw new InvalidRequestException("Invalid paging state.", e);
            }

            // set paging result w/ paging state
            result.metadata.setHasMorePages(new PagingState(pagingData,
                                          null,
                                          1, // we actually don't know
                                          0));
        }

        return new ResultMessage.Rows(result);
    }

    /**
     * Some entity types, UDFs and UDAs, that can resolve a name to multiple entities need a special callback
     * to handle the not-found-case and emit a proper error.
     *
     * So instread of doing an expensive test before invocation of {@link #describe(ClientState)}, we just invoke
     * this method when {@link #describe(ClientState)} returned no data so implementations can act accordingly in
     * a way that is compatible to what cqlsh (or the python driver) did in earlier versions.
     */
    protected void onEmpty()
    {
    }

    /**
     * Execute the actual DESCRIBE implementation. It is up to the implementation how many elements are returned
     * in the stream and how long the individual strings are. The implementation must however respect the usual
     * protocol limitations - and be gentle and do not return super big strings.
     *
     * A newline character is appended to each {@code String} from the result stream.
     *
     * Note: Paging via {@link Stream#limit(long) Stream.limit()}/{@link Stream#skip(long) Stream.skip()} is
     * pretty efficient.
     */
    protected abstract Stream<String> describe(ClientState state);

    /**
     * Helper function that returns either the given {@code keyspace}, if it is not {@code null}, or the keyspace
     * in {@link QueryState#getClientState() QueryState.getClientState().getRawKeyspace()}.
     *
     * Intentionally {@code protected} to allow future implementations that are not implemented in this class or
     * package.
     */
    protected final String keyspaceOrClientKeyspace(String keyspace, ClientState state)
    {
        return keyspace != null ? keyspace : state.getRawKeyspace();
    }

    /**
     * Helper function to retrieve a stream with the "effective" keyspaces. If a keyspace is "used" in
     * {@link QueryState#getClientState() QueryState.getClientState()}, only that one will be returned. Otherwise
     * the function returns all keyspaces.
     *
     * The result is sorted to be deterministic, which is important for paging.
     *
     * Intentionally {@code protected} to allow future implementations that are not implemented in this class or
     * package.
     */
    @SuppressWarnings("WeakerAccess")
    protected final Stream<String> keyspaceNames(ClientState state)
    {
        String keyspace = keyspaceOrClientKeyspace(null, state);

        return (keyspace == null ? snapshot.rawKeyspaceNames()
                                 : Stream.of(keyspace))
                .sorted();
    }

    /**
     * Returns the metadata for the given keyspace or throws a {@link KeyspaceNotDefinedException} exception.
     *
     * Intentionally {@code protected} to allow future implementations that are not implemented in this class or
     * package.
     */
    @SuppressWarnings("WeakerAccess")
    protected KeyspaceMetadata validateKeyspace(String ks)
    {
        KeyspaceMetadata ksm = snapshot.keyspaceMetadata(ks);

        if (ksm == null)
            throw new KeyspaceNotDefinedException(format("Keyspace '%s' not found", ks));

        return ksm;
    }

    /**
     * Returns the metadata for the given table or throws a {@link InvalidRequestException} exception.
     *
     * Intentionally {@code protected} to allow future implementations that are not implemented in this class or
     * package.
     */
    protected TableMetadata validateTable(String ks, String tab)
    {
        if (tab.isEmpty())
            throw new InvalidRequestException("Non-empty table name is required");

        KeyspaceMetadata ksm = validateKeyspace(ks);

        TableMetadata tm = ksm.getTableOrViewNullable(tab);
        if (tm == null)
            throw new InvalidRequestException(format("Table '%s' not found in keyspace '%s'", tab, ksm.name));

        return tm;
    }

    /**
     * Abstracted access to schema to allow querying both the in-memory schema and the schema as persisted on disk
     * for support case investigation (not implemented yet).
     *
     * An implementation to describe the schema on disk would need to make {@link org.apache.cassandra.schema.SchemaKeyspace#fetchNonSystemKeyspaces}
     * public, use the returned {@link org.apache.cassandra.schema.Keyspaces} object to implement the methods of this
     * interface.
     *
     * Also used for tests.
     */
    interface SchemaAccess
    {
        SchemaSnapshot snapshot();
    }

    /**
     * Retrieves an immutable snapshot of the schema.
     * In production code, the underlying data structure is {@link Keyspace}, which is immutable.
     */
    interface SchemaSnapshot
    {
        /**
         * Used to check whether the schema changed between to native-protocol-pages of a DESCRIBE statement.
         * If the schema changed between two pages, the output of the whole DESCRIBE statement would be broken,
         * therefore we error out if the schema changes.
         */
        ByteBuffer schemaHash();

        /**
         * Retrieve all keyspace names, including virtual keyspaces.
         */
        Stream<String> rawKeyspaceNames();

        KeyspaceMetadata keyspaceMetadata(String ks);
    }

    /**
     * Production implementation that delegates to {@link org.apache.cassandra.schema.Schema#instance Schema.instance}.
     */
    private static class DefaultSchemaAccess implements SchemaAccess
    {
        public SchemaSnapshot snapshot()
        {
            return new DefaultSchemaSnapshot(Schema.instance.snapshot(), Schema.instance.getVersion());
        }

        private static final class DefaultSchemaSnapshot implements SchemaSnapshot
        {
            private final Keyspaces keyspaces;
            private final UUID schemaVersion;

            private DefaultSchemaSnapshot(Keyspaces keyspaces, UUID schemaVersion)
            {
                this.keyspaces = keyspaces;
                this.schemaVersion = schemaVersion;
            }

            @Override
            public ByteBuffer schemaHash()
            {
                // Virtual keyspaces are setup during startup, so we probably do not need to care about
                // including the virtual keyspaces here.
                String prodVersion = FBUtilities.getReleaseVersionString();
                ByteBuffer bb = ByteBuffer.allocate(16 + prodVersion.length());
                bb.putLong(schemaVersion.getMostSignificantBits());
                bb.putLong(schemaVersion.getLeastSignificantBits());
                bb.put(prodVersion.getBytes(StandardCharsets.UTF_8));
                bb.flip();
                return bb;
            }

            public Stream<String> rawKeyspaceNames()
            {
                return Stream.concat(keyspaces.stream().map(ksm -> ksm.name),
                                     Iterables.toList(VirtualKeyspaceRegistry.instance.virtualKeyspacesMetadata()).stream().map(ksm -> ksm.name));
            }

            public KeyspaceMetadata keyspaceMetadata(String ks)
            {
                KeyspaceMetadata ksm = keyspaces.getNullable(ks);
                if (ksm != null)
                    return ksm;
                // virtual keyspaces
                return Schema.instance.getKeyspaceMetadata(ks);
            }
        }
    }

    public static abstract class NamedDescribeStatement<NT> extends DescribeStatement
    {
        protected NT name;

        protected NamedDescribeStatement(NT name)
        {
            this.name = name;
        }
    }

    public static abstract class QualifiedNameDescribeStatement extends NamedDescribeStatement<QualifiedName>
    {
        protected QualifiedNameDescribeStatement(QualifiedName name)
        {
            super(name);
        }

        protected void ensureExplicitOrClientKeyspace(ClientState state)
        {
            if (!name.hasKeyspace())
            {
                QualifiedName n = new QualifiedName();
                String ks = keyspaceOrClientKeyspace(null, state);
                if (ks == null)
                    throw new InvalidRequestException("No keyspace specified and no current keyspace");
                n.setKeyspace(ks, true);
                n.setName(name.getName(), true);
                name = n;
            }
        }
    }

    /**
     * Implementation of {@code DESCRIBE KEYSPACES}, list all (accessible) keyspace names.
     *
     * See javadoc of {@link DescribeStatement} for the syntax description.
     *
     * Named {@code TheKeyspaces} to avoid the name clash with the schema's {@link Keyspaces} class.
     */
    public static class TheKeyspaces extends DescribeStatement
    {
        @Override
        public void authorize(ClientState state)
        {
            // We iterate over the whole schema and filter out stuff that the caller has no access to. So no need
            // to check access here.
        }

        @Override
        protected Stream<String> describe(ClientState state)
        {
            Stream<String> resultStream = keyspaceNames(state)
                    .sorted()
                    .map(ColumnIdentifier::maybeQuote);

            resultStream = FBUtilities.columnize(resultStream, LINE_WIDTH);

            resultStream = Stream.concat(Stream.of(""),
                                         resultStream);

            return resultStream;
        }
    }

    /**
     * Base implementation for all {@code DESCRIBE} statements that list element names per keyspaces,
     * like {@code DESCRIBE TABLES}, {@code DESCRIBE TYPES}, {@code DESCRIBE FUNCTIONS}, {@code DESCRIBE AGGREGATES}.
     *
     * {@link Listing} does the common per-keyspace part and delegates the specific describe-operation via
     * {@link #perKeyspace(KeyspaceMetadata)} to the implementation.
     *
     * The output is columnized using a line width of {@link #LINE_WIDTH} (= {@value LINE_WIDTH}).
     */
    public static abstract class Listing extends DescribeStatement
    {
        @Override
        public void authorize(ClientState state)
        {
            // We iterate over the whole schema and filter out stuff that the caller has no access to. So no need
            // to check access here.
        }

        protected abstract Stream<String> perKeyspace(KeyspaceMetadata ksm);

        @Override
        protected Stream<String> describe(ClientState state)
        {

            Stream<KeyspaceMetadata> result = keyspaceNames(state).map(snapshot::keyspaceMetadata)
                                                                  .filter(Objects::nonNull);

            // For the "all keyspaces" case (i.e. when there is no "USE"d keyspace), create a header for
            // each keyspace that looks like the following.
            //
            // Keyspace some_keyspace_name
            // ---------------------------
            //
            String rawKeyspace = state.getRawKeyspace();

            return result.flatMap(ksm ->
                                  {
                                      Stream<String> resultStream = FBUtilities.columnize(perKeyspace(ksm), LINE_WIDTH);

                                      String header = rawKeyspace == null
                                                      ? perKeyspaceHeader(ksm)
                                                      : "";

                                      return Stream.concat(Stream.of(header),
                                                           resultStream);
                                  });
        }

        private String perKeyspaceHeader(KeyspaceMetadata ksm)
        {
            String h = "Keyspace " + ColumnIdentifier.maybeQuote(ksm.name);
            return "\n" +
                   h + "\n" +
                   Strings.repeat("-", h.length());
        }
    }

    /**
     * Implementation of {@code DESCRIBE TABLES}.
     *
     * See javadoc of {@link DescribeStatement} for the syntax description.
     */
    public static class Tables extends Listing
    {
        @Override
        protected Stream<String> perKeyspace(KeyspaceMetadata ksm)
        {
            return StreamSupport.stream(ksm.tables.spliterator(), false)
                                .map(tm -> tm.name)
                                .sorted()
                                .map(ColumnIdentifier::maybeQuote);
        }
    }

    /**
     * Implementation of {@code DESCRIBE TYPES}.
     *
     * See javadoc of {@link DescribeStatement} for the syntax description.
     */
    public static class Types extends Listing
    {
        @Override
        protected Stream<String> perKeyspace(KeyspaceMetadata ksm)
        {
            return StreamSupport.stream(ksm.types.spliterator(), false)
                                .map(UserType::getNameAsString)
                                .sorted()
                                .map(ColumnIdentifier::maybeQuote);
        }
    }

    /**
     * Implementation of {@code DESCRIBE FUNCTIONS}.
     *
     * See javadoc of {@link DescribeStatement} for the syntax description.
     */
    public static class Functions extends Listing
    {
        @Override
        protected Stream<String> perKeyspace(KeyspaceMetadata ksm)
        {
            return ksm.functions.udfs()
                                .map(UDFunction::toCQLString)
                                .sorted();
        }
    }

    /**
     * Implementation of {@code DESCRIBE AGGREGATES}.
     *
     * See javadoc of {@link DescribeStatement} for the syntax description.
     */
    public static class Aggregates extends Listing
    {
        @Override
        protected Stream<String> perKeyspace(KeyspaceMetadata ksm)
        {
            return ksm.functions.udas()
                                .map(UDAggregate::toCQLString)
                                .sorted();
        }
    }

    /**
     * Implementation of {@code DESCRIBE [FULL] SCHEMA}.
     *
     * See javadoc of {@link DescribeStatement} for the syntax description.
     *
     * Named {@code TheSchema} to avoid the name clash with the schema's {@link Schema} class.
     */
    public static class TheSchema extends Keyspace
    {
        // include system keyspaces
        private final boolean includeSystemKeyspaces;

        public TheSchema(boolean includeSystemKeyspaces)
        {
            super();
            this.includeSystemKeyspaces = includeSystemKeyspaces;
        }

        @Override
        public void authorize(ClientState state)
        {
            // We iterate over the whole schema and filter out stuff that the caller has no access to. So no need
            // to check access here.
        }

        @Override
        protected Stream<KeyspaceMetadata> keyspaces(ClientState state)
        {
            return snapshot.rawKeyspaceNames()
                           .filter(ks -> includeSystemKeyspaces || (!SchemaConstants.isLocalSystemKeyspace(ks) && !SchemaConstants.isReplicatedSystemKeyspace(ks)))
                           .sorted()
                           .map(snapshot::keyspaceMetadata)
                           .filter(Objects::nonNull);
        }
    }

    /**
     * Implementation of {@code DESCRIBE KEYSPACE}.
     *
     * See javadoc of {@link DescribeStatement} for the syntax description.
     */
    public static class Keyspace extends NamedDescribeStatement<String>
    {
        private final boolean onlyKeyspaceDefinition;

        protected Keyspace()
        {
            super(null);
            this.onlyKeyspaceDefinition = false;
        }

        public Keyspace(String name, boolean onlyKeyspaceDefinition)
        {
            super(name);
            this.onlyKeyspaceDefinition = onlyKeyspaceDefinition;
        }

        @Override
        public void authorize(ClientState state)
        {
            name = keyspaceOrClientKeyspace(name, state);
            if (name == null)
                throw new InvalidRequestException("No keyspace specified and no current keyspace");
        }

        protected Stream<KeyspaceMetadata> keyspaces(ClientState state)
        {
            KeyspaceMetadata ksm = validateKeyspace(name);
            return Stream.of(ksm);
        }

        @Override
        protected Stream<String> describe(ClientState state)
        {
            return keyspaces(state).flatMap(this::describe);
        }

        private Stream<String> describe(KeyspaceMetadata object)
        {
            // CREATE KEYSPACE

            Stream<String> s = Stream.of("",
                                         SchemaCQLHelper.getKeyspaceMetadataAsCQL(object));

            if (!onlyKeyspaceDefinition)
            {
                // CREATE TYPE
                s = Stream.concat(s, Stream.of(""));
                LinkedHashSet<ByteBuffer> types = new LinkedHashSet<>();
                StreamSupport.stream(object.types.spliterator(), false)
                             .sorted(Comparator.comparing(UserType::toCQLString))
                             .forEach(udt -> SchemaCQLHelper.findUserType(udt, types));
                s = Stream.concat(s, types.stream()
                                          .map(name -> {
                                              Optional<UserType> type = object.types.get(name);
                                              if (type.isPresent())
                                                  return SchemaCQLHelper.toCQL(type.get()) + '\n';

                                              String typeName = UTF8Type.instance.getString(name);
                                              // This really shouldn't happen, but if it does (a bug), we can at least dump what we know about and
                                              // log an error to tell users they will have to fill the gaps. We also include the error as a CQL
                                              // comment in the output of this method (in place of the missing statement) in case the user see it
                                              // more there.
                                              return String.format("// ERROR: user type %s is part of keyspace %s definition but its "
                                                                   + "definition was missing", typeName, object.name);
                                          }));

                // CREATE FUNCTION
                s = Stream.concat(s, object.functions
                        .udfs()
                        .sorted(Comparator.comparing(UDFunction::toString))
                        .map(udf -> SchemaCQLHelper.getFunctionAsCQL(udf) + '\n'));

                // CREATE AGGREGATE
                s = Stream.concat(s, object.functions
                        .udas()
                        .sorted(Comparator.comparing(UDAggregate::toString))
                        .map(uda -> SchemaCQLHelper.getAggregateAsCQL(uda) + '\n'));

                // CREATE TABLE
                s = Stream.concat(s, StreamSupport.stream(object.tables.spliterator(), false)
                                                  .sorted(Comparator.comparing(tm -> tm.name))
                                                  .flatMap(tm -> {

                                                      // CREATE TABLE
                                                      // includes CREATE TYPE/DROP TYPE for dropped columns with UDTs
                                                      Stream<String> r = SchemaCQLHelper.reCreateStatements(tm,
                                                                                                            object.types,
                                                                                                            includeInternalDetails,
                                                                                                            includeInternalDetails,
                                                                                                            false,
                                                                                                            false);

                                                      // CREATE INDEX
                                                      r = Stream.concat(r, SchemaCQLHelper.getIndexesAsCQL(tm));

                                                      // CREATE MATERIALIZED VIEW
                                                      r = Stream.concat(r, StreamSupport.stream(object.views.spliterator(), false)
                                                                                        .filter(v -> v.baseTableId.equals(tm.id))
                                                                                        .sorted(Comparator.comparing(v -> v.name()))
                                                                                        .flatMap(v -> SchemaCQLHelper.reCreateStatements(v.metadata,
                                                                                                                                         object.types,
                                                                                                                                         includeInternalDetails,
                                                                                                                                         includeInternalDetails,
                                                                                                                                         false,
                                                                                                                                         false)));

                                                      return r;
                                                  }));
            }

            return s;
        }
    }

    /**
     * Implementation of {@code DESCRIBE TABLE}.
     *
     * See javadoc of {@link DescribeStatement} for the syntax description.
     */
    public static class Table extends QualifiedNameDescribeStatement
    {
        public Table(QualifiedName name)
        {
            super(name);
        }

        @Override
        public void authorize(ClientState state)
        {
            ensureExplicitOrClientKeyspace(state);
        }

        @Override
        protected Stream<String> describe(ClientState state)
        {
            TableMetadata tableMetadata = validateTable(name.getKeyspace(), name.getName());

            return Stream.concat(Stream.of(""),
                                 Stream.of(tableMetadata)
                                       .flatMap(tm -> SchemaCQLHelper.reCreateStatements(tm,
                                                                                         validateKeyspace(name.getKeyspace()).types,
                                                                                         includeInternalDetails,
                                                                                         includeInternalDetails,
                                                                                         false,
                                                                                         true)));
        }
    }

    /**
     * Implementation of {@code DESCRIBE INDEX}.
     *
     * See javadoc of {@link DescribeStatement} for the syntax description.
     */
    public static class Index extends QualifiedNameDescribeStatement
    {
        public Index(QualifiedName name)
        {
            super(name);
        }

        @Override
        public void authorize(ClientState state)
        {
            ensureExplicitOrClientKeyspace(state);
        }

        @Override
        protected Stream<String> describe(ClientState state)
        {
            KeyspaceMetadata ksm = validateKeyspace(name.getKeyspace());
            TableMetadata tm = ksm.findIndexedTable(name.getName())
                                  .orElseThrow(() -> new InvalidRequestException(format("Table for existing index '%s' not found in '%s'", name.getName(), name.getKeyspace())));
            return tm.indexes.get(name.getName())
                             .map(idx -> Pair.create(tm, idx))
                             .map(Stream::of)
                             .orElseThrow(() -> new InvalidRequestException(format("Index '%s' not found in '%s'", name.getName(), name.getKeyspace())))
                             .flatMap(tableIndex -> Stream.of("",
                                                              SchemaCQLHelper.toCQL(tableIndex.left, tableIndex.right)));
        }
    }

    /**
     * Implementation of {@code DESCRIBE MATERIALIZED VIEW}.
     *
     * See javadoc of {@link DescribeStatement} for the syntax description.
     */
    public static class View extends QualifiedNameDescribeStatement
    {
        public View(QualifiedName name)
        {
            super(name);
        }

        @Override
        public void authorize(ClientState state)
        {
            ensureExplicitOrClientKeyspace(state);
        }

        @Override
        protected Stream<String> describe(ClientState state)
        {
            KeyspaceMetadata ksm = validateKeyspace(name.getKeyspace());
            return ksm.views.get(name.getName())
                            .map(Stream::of)
                            .orElseThrow(() -> new InvalidRequestException(format("Materialized view '%s' not found in '%s'", name.getName(), name.getKeyspace())))
                            .flatMap(view -> Stream.concat(Stream.of(""),
                                                           Stream.of(view)
                                                                 .flatMap(vtm -> SchemaCQLHelper.reCreateStatements(vtm.metadata,
                                                                                                                    ksm.types,
                                                                                                                    includeInternalDetails,
                                                                                                                    includeInternalDetails,
                                                                                                                    false,
                                                                                                                    true))));
        }
    }

    /**
     * Implementation of {@code DESCRIBE TYPE}.
     *
     * See javadoc of {@link DescribeStatement} for the syntax description.
     */
    public static class Type extends NamedDescribeStatement<UTName>
    {
        public Type(UTName name)
        {
            super(name);
        }

        @Override
        public void authorize(ClientState state)
        {
            if (!name.hasKeyspace()) {
                String ks = state.getRawKeyspace();
                if (ks == null)
                    throw new InvalidRequestException("No keyspace specified and no current keyspace");
                name = new UTName(ColumnIdentifier.getInterned(ks, true),
                                  ColumnIdentifier.getInterned(name.getStringTypeName(), true));
            }
        }

        @Override
        protected Stream<String> describe(ClientState state)
        {
            KeyspaceMetadata ksm = validateKeyspace(name.getKeyspace());
            return ksm.types.get(name.getUserTypeName())
                            .map(Stream::of)
                            .orElseThrow(() -> new InvalidRequestException(format("User defined type '%s' not found in '%s'", name.getStringTypeName(), name.getKeyspace())))
                            .flatMap(udt -> Stream.of("",
                                                      SchemaCQLHelper.toCQL(udt)));
        }
    }

    /**
     * Implementation of {@code DESCRIBE FUNCTION}.
     *
     * See javadoc of {@link DescribeStatement} for the syntax description.
     */
    protected abstract static class AbstractFunction extends NamedDescribeStatement<FunctionName>
    {
        protected AbstractFunction(FunctionName name)
        {
            super(name);
        }

        @Override
        public void authorize(ClientState state)
        {
            if (!name.hasKeyspace())
            {
                String ks = state.getRawKeyspace();
                if (ks == null)
                    throw new InvalidRequestException("No keyspace specified and no current keyspace");
                name = new FunctionName(ks, name.name);
            }
        }
    }

    /**
     * Implementation of {@code DESCRIBE FUNCTION}.
     *
     * See javadoc of {@link DescribeStatement} for the syntax description.
     */
    public static class Function extends AbstractFunction
    {
        public Function(FunctionName name)
        {
            super(name);
        }

        @Override
        protected void onEmpty()
        {
            throw new InvalidRequestException(format("User defined function '%s' not found in '%s'", name.name, name.keyspace));
        }

        @Override
        protected Stream<String> describe(ClientState state)
        {
            return validateKeyspace(name.keyspace).functions.udfs()
                                                            .filter(udf -> udf.name().equals(name))
                                                            .flatMap(udf -> Stream.of("",
                                                                                      SchemaCQLHelper.getFunctionAsCQL(udf)));
        }
    }

    /**
     * Implementation of {@code DESCRIBE AGGREGATE}.
     *
     * See javadoc of {@link DescribeStatement} for the syntax description.
     */
    public static class Aggregate extends AbstractFunction
    {
        public Aggregate(FunctionName name)
        {
            super(name);
        }

        @Override
        protected void onEmpty()
        {
            throw new InvalidRequestException(format("User defined aggregate '%s' not found in '%s'", name.name, name.keyspace));
        }

        @Override
        protected Stream<String> describe(ClientState state)
        {
            return validateKeyspace(name.keyspace).functions.udas()
                                                            .filter(uda -> uda.name().equals(name))
                                                            .flatMap(uda -> Stream.of("",
                                                                                      SchemaCQLHelper.getAggregateAsCQL(uda)));
        }
    }

    /**
     * Implementation of the generic {@code DESCRIBE ...}.
     *
     * See javadoc of {@link DescribeStatement} for the syntax description.
     */
    public static class Generic extends NamedDescribeStatement<QualifiedName>
    {
        public Generic(QualifiedName name)
        {
            super(name);
        }

        private DescribeStatement delegate;

        private void resolve(ClientState state)
        {
            // from cqlsh help: "keyspace or a table or an index or a materialized view (in this order)."
            if (!name.hasKeyspace())
            {
                // try name.getName() as keyspace
                if (snapshot.keyspaceMetadata(name.getName()) != null)
                {
                    delegateResolved(new Keyspace(name.getName(), false), state);
                    return;
                }

                String ks = keyspaceOrClientKeyspace(null, state);
                if (ks == null)
                {
                    delegateResolved(new Keyspace(name.getName(), false), state);
                    return;
                }
                else
                    name.setKeyspace(ks, true);
            }

            KeyspaceMetadata keyspaceMetadata = snapshot.keyspaceMetadata(name.getKeyspace());
            if (keyspaceMetadata.tables.getNullable(name.getName()) != null)
            {
                delegateResolved(new Table(name), state);
                return;
            }

            Optional<TableMetadata> indexed = keyspaceMetadata.findIndexedTable(name.getName());
            if (indexed.isPresent())
            {
                Optional<IndexMetadata> index = indexed.get().indexes.get(name.getName());
                if (index.isPresent())
                {
                    delegateResolved(new Index(name), state);
                    return;
                }
            }

            if (keyspaceMetadata.views.getNullable(name.getName()) != null)
            {
                delegateResolved(new View(name), state);
                return;
            }

            throw new InvalidRequestException(format("'%s' not found in keyspace '%s'", name.getName(), name.getKeyspace()));
        }

        private void delegateResolved(DescribeStatement d, ClientState state)
        {
            delegate = d;
            delegate.includeInternalDetails = includeInternalDetails;
            delegate.authorize(state);
        }

        @Override
        public void authorize(ClientState state)
        {
            resolve(state);

            delegate.authorize(state);
        }

        @Override
        protected void onEmpty()
        {
            delegate.onEmpty();
        }

        @SuppressWarnings("unchecked")
        @Override
        protected Stream<String> describe(ClientState state)
        {
            return delegate.describe(state);
        }
    }

    /**
     * Implementation of the generic {@code DESCRIBE CLUSTER}.
     *
     * See javadoc of {@link DescribeStatement} for the syntax description.
     */
    public static class Cluster extends DescribeStatement
    {
        public Cluster()
        {
            super();
        }

        @Override
        public void authorize(ClientState state)
        {
        }

        @Override
        protected Stream<String> describe(ClientState state)
        {
            Stream.Builder<String> output = Stream.<String>builder()
                    .add("")
                    .add(format("Cluster: %s", DatabaseDescriptor.getClusterName()))
                    .add(format("Partitioner: %s", trimIfPresent(DatabaseDescriptor.getPartitionerName(), "org.apache.cassandra.dht.")))
                    .add(format("Snitch: %s", trimIfPresent(DatabaseDescriptor.getEndpointSnitch().getClass().getName(), "org.apache.cassandra.locator.")))
                    .add("");

            String useKs = keyspaceOrClientKeyspace(null, state);
            if (useKs != null && !SchemaConstants.isLocalSystemKeyspace(useKs))
            {
                output.add("Range ownership:");
                StorageService.instance.getRangeToAddressMap(useKs)
                                       .entrySet()
                                       .stream()
                                       .sorted(Comparator.comparing(Map.Entry::getKey))
                                       .map(rangeOwners -> format(" %39s  [%s]",
                                                                  rangeOwners.getKey().right,
                                                                  rangeOwners.getValue().stream().map(r -> r.endpoint().toString()).collect(Collectors.joining(", "))))
                                       .forEach(output::add);
                output.add("");
            }
            return output.build();
        }

        private String trimIfPresent(String src, String begin)
        {
            if (src.startsWith(begin))
                return src.substring(begin.length());
            return src;
        }
    }
}
