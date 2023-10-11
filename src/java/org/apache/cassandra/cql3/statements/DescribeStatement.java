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
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.db.KeyspaceNotDefinedException;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import static java.lang.String.format;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotEmpty;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

/**
 * The differents <code>DESCRIBE</code> statements parsed from a CQL statement.
 */
public abstract class DescribeStatement<T> extends CQLStatement.Raw implements CQLStatement
{
    private static final String KS = "system";
    private static final String CF = "describe";

    /**
     * The columns returned by the describe queries that only list elements names (e.g. DESCRIBE KEYSPACES, DESCRIBE TABLES...)
     */
    private static final List<ColumnSpecification> LIST_METADATA =
            ImmutableList.of(new ColumnSpecification(KS, CF, new ColumnIdentifier("keyspace_name", true), UTF8Type.instance),
                             new ColumnSpecification(KS, CF, new ColumnIdentifier("type", true), UTF8Type.instance),
                             new ColumnSpecification(KS, CF, new ColumnIdentifier("name", true), UTF8Type.instance));

    /**
     * The columns returned by the describe queries that returns the CREATE STATEMENT for the different elements (e.g. DESCRIBE KEYSPACE, DESCRIBE TABLE ...)
     */
    private static final List<ColumnSpecification> ELEMENT_METADATA =
            ImmutableList.<ColumnSpecification>builder().addAll(LIST_METADATA)
                                                        .add(new ColumnSpecification(KS, CF, new ColumnIdentifier("create_statement", true), UTF8Type.instance))
                                                        .build();

    /**
     * "Magic version" for the paging state.
     */
    private static final int PAGING_STATE_VERSION = 0x0001;

    static final String SCHEMA_CHANGED_WHILE_PAGING_MESSAGE = "The schema has changed since the previous page of the DESCRIBE statement result. " +
                                                              "Please retry the DESCRIBE statement.";

    private boolean includeInternalDetails;

    public final void withInternalDetails()
    {
        this.includeInternalDetails = true;
    }

    @Override
    public final CQLStatement prepare(ClientState clientState) throws RequestValidationException
    {
        return this;
    }

    public final List<ColumnSpecification> getBindVariables()
    {
        return Collections.emptyList();
    }

    @Override
    public final void authorize(ClientState state)
    {
    }

    @Override
    public final void validate(ClientState state)
    {
    }

    public final AuditLogContext getAuditLogContext()
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
        Keyspaces keyspaces = Schema.instance.distributedKeyspaces();
        UUID schemaVersion = Schema.instance.getVersion();

        keyspaces = Keyspaces.builder()
                             .add(keyspaces)
                             .add(Schema.instance.getLocalKeyspaces())
                             .add(VirtualKeyspaceRegistry.instance.virtualKeyspacesMetadata())
                             .build();

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

        long offset = getOffset(pagingState, schemaVersion);
        int pageSize = options.getPageSize();

        Stream<? extends T> stream = describe(state.getClientState(), keyspaces);

        if (offset > 0L)
            stream = stream.skip(offset);
        if (pageSize > 0)
            stream = stream.limit(pageSize);

        List<List<ByteBuffer>> rows = stream.map(e -> toRow(e, includeInternalDetails))
                                            .collect(Collectors.toList());

        ResultSet.ResultMetadata resultMetadata = new ResultSet.ResultMetadata(metadata(state.getClientState()));
        ResultSet result = new ResultSet(resultMetadata, rows);

        if (pageSize > 0 && rows.size() == pageSize)
        {
            result.metadata.setHasMorePages(getPagingState(offset + pageSize, schemaVersion));
        }

        return new ResultMessage.Rows(result);
    }

    /**
     * Returns the columns of the {@code ResultMetadata}
     */
    protected abstract List<ColumnSpecification> metadata(ClientState state);

    private PagingState getPagingState(long nextPageOffset, UUID schemaVersion)
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            out.writeShort(PAGING_STATE_VERSION);
            out.writeUTF(FBUtilities.getReleaseVersionString());
            out.write(UUIDGen.decompose(schemaVersion));
            out.writeLong(nextPageOffset);

            return new PagingState(out.asNewBuffer(),
                                   null,
                                   Integer.MAX_VALUE,
                                   Integer.MAX_VALUE);
        }
        catch (IOException e)
        {
            throw new InvalidRequestException("Invalid paging state.", e);
        }
    }

    private long getOffset(PagingState pagingState, UUID schemaVersion)
    {
        if (pagingState == null)
            return 0L;

        try (DataInputBuffer in = new DataInputBuffer(pagingState.partitionKey, false))
        {
            checkTrue(in.readShort() == PAGING_STATE_VERSION, "Incompatible paging state");

            final String pagingStateServerVersion = in.readUTF();
            final String releaseVersion = FBUtilities.getReleaseVersionString();
            checkTrue(pagingStateServerVersion.equals(releaseVersion),
                      "The server version of the paging state %s is different from the one of the server %s",
                      pagingStateServerVersion,
                      releaseVersion);

            byte[] bytes = new byte[UUIDGen.UUID_LEN];
            in.read(bytes);
            UUID version = UUIDGen.getUUID(ByteBuffer.wrap(bytes));
            checkTrue(schemaVersion.equals(version), SCHEMA_CHANGED_WHILE_PAGING_MESSAGE);

            return in.readLong();
        }
        catch (IOException e)
        {
            throw new InvalidRequestException("Invalid paging state.", e);
        }
    }

    protected abstract List<ByteBuffer> toRow(T element, boolean withInternals);

    /**
     * Returns the schema elements that must be part of the output.
     */
    protected abstract Stream<? extends T> describe(ClientState state, Keyspaces keyspaces);

    /**
     * Returns the metadata for the given keyspace or throws a {@link KeyspaceNotDefinedException} exception.
     */
    private static KeyspaceMetadata validateKeyspace(String ks, Keyspaces keyspaces)
    {
        return keyspaces.get(ks)
                        .orElseThrow(() -> new KeyspaceNotDefinedException(format("'%s' not found in keyspaces", ks)));
    }

    /**
     * {@code DescribeStatement} implementation used for describe queries that only list elements names.
     */
    public static final class Listing extends DescribeStatement<SchemaElement>
    {
        private final java.util.function.Function<KeyspaceMetadata, Stream<? extends SchemaElement>> elementsProvider;

        public Listing(java.util.function.Function<KeyspaceMetadata, Stream<? extends SchemaElement>> elementsProvider)
        {
            this.elementsProvider = elementsProvider;
        }

        @Override
        protected Stream<? extends SchemaElement> describe(ClientState state, Keyspaces keyspaces)
        {
            String keyspace = state.getRawKeyspace();
            Stream<KeyspaceMetadata> stream = keyspace == null ? keyspaces.stream().sorted(SchemaElement.NAME_COMPARATOR)
                                                               : Stream.of(validateKeyspace(keyspace, keyspaces));

            return stream.flatMap(k -> elementsProvider.apply(k).sorted(SchemaElement.NAME_COMPARATOR));
        }

        @Override
        protected List<ColumnSpecification> metadata(ClientState state)
        {
            return LIST_METADATA;
        }

        @Override
        protected List<ByteBuffer> toRow(SchemaElement element, boolean withInternals)
        {
            return ImmutableList.of(bytes(element.elementKeyspaceQuotedIfNeeded()),
                                    bytes(element.elementType().toString()),
                                    bytes(element.elementNameQuotedIfNeeded()));
        }
    }

    /**
     * Creates a {@link DescribeStatement} for {@code DESCRIBE TABLES}.
     */
    public static DescribeStatement<SchemaElement> tables()
    {
        return new Listing(ks -> ks.tables.stream());
    }

    /**
     * Creates a {@link DescribeStatement} for {@code DESCRIBE TYPES}.
     */
    public static DescribeStatement<SchemaElement> types()
    {
        return new Listing(ks -> ks.types.stream());
    }

    /**
     * Creates a {@link DescribeStatement} for {@code DESCRIBE FUNCTIONS}.
     */
    public static DescribeStatement<SchemaElement> functions()
    {
        return new Listing(ks -> ks.userFunctions.udfs());
    }

    /**
     * Creates a {@link DescribeStatement} for {@code DESCRIBE AGGREGATES}.
     */
    public static DescribeStatement<SchemaElement> aggregates()
    {
        return new Listing(ks -> ks.userFunctions.udas());
    }

    /**
     * Creates a {@link DescribeStatement} for {@code DESCRIBE KEYSPACES}.
     */
    public static DescribeStatement<SchemaElement> keyspaces()
    {
        return new DescribeStatement<SchemaElement>()
        {
            @Override
            protected Stream<? extends SchemaElement> describe(ClientState state, Keyspaces keyspaces)
            {
                return keyspaces.stream().sorted(SchemaElement.NAME_COMPARATOR);
            }

            @Override
            protected List<ColumnSpecification> metadata(ClientState state)
            {
                return LIST_METADATA;
            }

            @Override
            protected List<ByteBuffer> toRow(SchemaElement element, boolean withInternals)
            {
                return ImmutableList.of(bytes(element.elementKeyspaceQuotedIfNeeded()),
                                        bytes(element.elementType().toString()),
                                        bytes(element.elementNameQuotedIfNeeded()));
            }
        };
    }

    /**
     * Creates a {@link DescribeStatement} for {@code DESCRIBE [FULL] SCHEMA}.
     */
    public static DescribeStatement<SchemaElement> schema(boolean includeSystemKeyspaces)
    {
        return new DescribeStatement<SchemaElement>()
        {
            @Override
            protected Stream<? extends SchemaElement> describe(ClientState state, Keyspaces keyspaces)
            {
                return keyspaces.stream()
                                .filter(ks -> includeSystemKeyspaces || !SchemaConstants.isSystemKeyspace(ks.name))
                                .sorted(SchemaElement.NAME_COMPARATOR)
                                .flatMap(ks -> getKeyspaceElements(ks, false));
            }

            @Override
            protected List<ColumnSpecification> metadata(ClientState state)
            {
                return ELEMENT_METADATA;
            }

            @Override
            protected List<ByteBuffer> toRow(SchemaElement element, boolean withInternals)
            {
                return ImmutableList.of(bytes(element.elementKeyspaceQuotedIfNeeded()),
                                        bytes(element.elementType().toString()),
                                        bytes(element.elementNameQuotedIfNeeded()),
                                        bytes(element.toCqlString(withInternals, false)));
            }
        };
    }

    /**
     * {@code DescribeStatement} implementation used for describe queries for a single schema element.
     */
    public static class Element extends DescribeStatement<SchemaElement>
    {
        /**
         * The keyspace name
         */
        private final String keyspace;

        /**
         * The element name
         */
        private final String name;

        private final BiFunction<KeyspaceMetadata, String, Stream<? extends SchemaElement>> elementsProvider;

        public Element(String keyspace, String name, BiFunction<KeyspaceMetadata, String, Stream<? extends SchemaElement>> elementsProvider)
        {
            this.keyspace = keyspace;
            this.name = name;
            this.elementsProvider = elementsProvider;
        }

        @Override
        protected Stream<? extends SchemaElement> describe(ClientState state, Keyspaces keyspaces)
        {
            String ks = keyspace == null ? checkNotNull(state.getRawKeyspace(), "No keyspace specified and no current keyspace")
                                         : keyspace;

            return elementsProvider.apply(validateKeyspace(ks, keyspaces), name);
        }

        @Override
        protected List<ColumnSpecification> metadata(ClientState state)
        {
            return ELEMENT_METADATA;
        }

        @Override
        protected List<ByteBuffer> toRow(SchemaElement element, boolean withInternals)
        {
            return ImmutableList.of(bytes(element.elementKeyspaceQuotedIfNeeded()),
                                    bytes(element.elementType().toString()),
                                    bytes(element.elementNameQuotedIfNeeded()),
                                    bytes(element.toCqlString(withInternals, false)));
        }
    }

    /**
     * Creates a {@link DescribeStatement} for {@code DESCRIBE KEYSPACE}.
     */
    public static DescribeStatement<SchemaElement> keyspace(String keyspace, boolean onlyKeyspaceDefinition)
    {
        return new Element(keyspace, null, (ks, t) -> getKeyspaceElements(ks, onlyKeyspaceDefinition));
    }

    private static Stream<? extends SchemaElement> getKeyspaceElements(KeyspaceMetadata ks, boolean onlyKeyspace)
    {
        Stream<? extends SchemaElement> s = Stream.of(ks);

        if (!onlyKeyspace)
        {
            s = Stream.concat(s, ks.types.sortedStream());
            s = Stream.concat(s, ks.userFunctions.udfs().sorted(SchemaElement.NAME_COMPARATOR));
            s = Stream.concat(s, ks.userFunctions.udas().sorted(SchemaElement.NAME_COMPARATOR));
            s = Stream.concat(s, ks.tables.stream().sorted(SchemaElement.NAME_COMPARATOR)
                                                   .flatMap(tm -> getTableElements(ks, tm)));
        }

        return s;
    }

    private static Stream<? extends SchemaElement> getTableElements(KeyspaceMetadata ks, TableMetadata table)
    {
        Stream<? extends SchemaElement> s = Stream.of(table);
        s = Stream.concat(s, table.indexes.stream()
                                          .map(i -> toDescribable(table, i))
                                          .sorted(SchemaElement.NAME_COMPARATOR));
        s = Stream.concat(s, ks.views.stream(table.id)
                                     .sorted(SchemaElement.NAME_COMPARATOR));
        return s;
    }

    /**
     * Creates a {@link DescribeStatement} for {@code DESCRIBE TABLE}.
     */
    public static DescribeStatement<SchemaElement> table(String keyspace, String name)
    {
        return new Element(keyspace, name, (ks, t) -> {

            TableMetadata table = checkNotNull(ks.getTableNullable(t),
                                               "Table '%s' not found in keyspace '%s'", t, ks.name);

            return Stream.concat(Stream.of(table), table.indexes.stream()
                                                                .map(index -> toDescribable(table, index))
                                                                .sorted(SchemaElement.NAME_COMPARATOR));
        });
    }

    /**
     * Creates a {@link DescribeStatement} for {@code DESCRIBE INDEX}.
     */
    public static DescribeStatement<SchemaElement> index(String keyspace, String name)
    {
        return new Element(keyspace, name, (ks, index) -> {

            TableMetadata tm = ks.findIndexedTable(index)
                                 .orElseThrow(() -> invalidRequest("Table for existing index '%s' not found in '%s'",
                                                                   index,
                                                                   ks.name));
            return tm.indexes.get(index)
                             .map(i -> toDescribable(tm, i))
                             .map(Stream::of)
                             .orElseThrow(() -> invalidRequest("Index '%s' not found in '%s'", index, ks.name));
        });
    }

    /**
     * Creates a {@link DescribeStatement} for {@code DESCRIBE MATERIALIZED VIEW}.
     */
    public static DescribeStatement<SchemaElement> view(String keyspace, String name)
    {
        return new Element(keyspace, name, (ks, view) -> {

            return ks.views.get(view)
                           .map(Stream::of)
                           .orElseThrow(() -> invalidRequest("Materialized view '%s' not found in '%s'", view, ks.name));
        });
    }

    /**
     * Creates a {@link DescribeStatement} for {@code DESCRIBE TYPE}.
     */
    public static DescribeStatement<SchemaElement> type(String keyspace, String name)
    {
        return new Element(keyspace, name, (ks, type) -> {

            return ks.types.get(ByteBufferUtil.bytes(type))
                           .map(Stream::of)
                           .orElseThrow(() -> invalidRequest("User defined type '%s' not found in '%s'",
                                                             type,
                                                             ks.name));
        });
    }

    /**
     * Creates a {@link DescribeStatement} for {@code DESCRIBE FUNCTION}.
     */
    public static DescribeStatement<SchemaElement> function(String keyspace, String name)
    {
        return new Element(keyspace, name, (ks, n) -> {

            return checkNotEmpty(ks.userFunctions.getUdfs(new FunctionName(ks.name, n)),
                                 "User defined function '%s' not found in '%s'", n, ks.name).stream()
                                                                                             .sorted(SchemaElement.NAME_COMPARATOR);
        });
    }

    /**
     * Creates a {@link DescribeStatement} for {@code DESCRIBE FUNCTION}.
     */
    public static DescribeStatement<SchemaElement> aggregate(String keyspace, String name)
    {
        return new Element(keyspace, name, (ks, n) -> {

            return checkNotEmpty(ks.userFunctions.getUdas(new FunctionName(ks.name, n)),
                                 "User defined aggregate '%s' not found in '%s'", n, ks.name).stream()
                                                                                              .sorted(SchemaElement.NAME_COMPARATOR);
        });
    }

    private static SchemaElement toDescribable(TableMetadata table, IndexMetadata index)
    {
        return new SchemaElement()
                {
                    @Override
                    public SchemaElementType elementType()
                    {
                        return SchemaElementType.INDEX;
                    }

                    @Override
                    public String elementKeyspace()
                    {
                        return table.keyspace;
                    }

                    @Override
                    public String elementName()
                    {
                        return index.name;
                    }

                    @Override
                    public String toCqlString(boolean withInternals, boolean ifNotExists)
                    {
                        return index.toCqlString(table, ifNotExists);
                    }
                };
    }

    /**
     * Creates a {@link DescribeStatement} for the generic {@code DESCRIBE ...}.
     */
    public static DescribeStatement<SchemaElement> generic(String keyspace, String name)
    {
        return new DescribeStatement<SchemaElement>()
        {
            private DescribeStatement<SchemaElement> delegate;

            private DescribeStatement<SchemaElement> resolve(ClientState state, Keyspaces keyspaces)
            {
                String ks = keyspace;

                // from cqlsh help: "keyspace or a table or an index or a materialized view (in this order)."
                if (keyspace == null)
                {
                    if (keyspaces.containsKeyspace(name))
                        return keyspace(name, false);

                    String rawKeyspace = state.getRawKeyspace();
                    ks = rawKeyspace == null ? name : rawKeyspace;
                }

                KeyspaceMetadata keyspaceMetadata = validateKeyspace(ks, keyspaces);

                if (keyspaceMetadata.tables.getNullable(name) != null)
                    return table(ks, name);

                Optional<TableMetadata> indexed = keyspaceMetadata.findIndexedTable(name);
                if (indexed.isPresent())
                {
                    Optional<IndexMetadata> index = indexed.get().indexes.get(name);
                    if (index.isPresent())
                        return index(ks, name);
                }

                if (keyspaceMetadata.views.getNullable(name) != null)
                    return view(ks, name);

                throw invalidRequest("'%s' not found in keyspace '%s'", name, ks);
            }

            @Override
            protected Stream<? extends SchemaElement> describe(ClientState state, Keyspaces keyspaces)
            {
                delegate = resolve(state, keyspaces);
                return delegate.describe(state, keyspaces);
            }

            @Override
            protected List<ColumnSpecification> metadata(ClientState state)
            {
                return delegate.metadata(state);
            }

            @Override
            protected List<ByteBuffer> toRow(SchemaElement element, boolean withInternals)
            {
                return delegate.toRow(element, withInternals);
            }
        };
    }

    /**
     * Creates a {@link DescribeStatement} for {@code DESCRIBE CLUSTER}.
     */
    public static DescribeStatement<List<Object>> cluster()
    {
        return new DescribeStatement<List<Object>>()
        {
            /**
             * The column index of the cluster name
             */
            private static final int CLUSTER_NAME_INDEX = 0;

            /**
             * The column index of the partitioner name
             */
            private static final int PARTITIONER_NAME_INDEX = 1;

            /**
             * The column index of the snitch class
             */
            private static final int SNITCH_CLASS_INDEX = 2;

            /**
             * The range ownerships index
             */
            private static final int RANGE_OWNERSHIPS_INDEX = 3;

            @Override
            protected Stream<List<Object>> describe(ClientState state, Keyspaces keyspaces)
            {
                List<Object> list = new ArrayList<Object>();
                list.add(DatabaseDescriptor.getClusterName());
                list.add(trimIfPresent(DatabaseDescriptor.getPartitionerName(), "org.apache.cassandra.dht."));
                list.add(trimIfPresent(DatabaseDescriptor.getEndpointSnitch().getClass().getName(),
                                            "org.apache.cassandra.locator."));

                String useKs = state.getRawKeyspace();
                if (mustReturnsRangeOwnerships(useKs))
                {
                    list.add(StorageService.instance.getRangeToAddressMap(useKs)
                                                    .entrySet()
                                                    .stream()
                                                    .sorted(Comparator.comparing(Map.Entry::getKey))
                                                    .collect(Collectors.toMap(e -> e.getKey().right.toString(),
                                                                              e -> e.getValue()
                                                                                    .stream()
                                                                                    .map(r -> r.endpoint().toString())
                                                                                    .collect(Collectors.toList()))));
                }
                return Stream.of(list);
            }

            private boolean mustReturnsRangeOwnerships(String useKs)
            {
                return useKs != null && !SchemaConstants.isLocalSystemKeyspace(useKs) && !SchemaConstants.isSystemKeyspace(useKs);
            }

            @Override
            protected List<ColumnSpecification> metadata(ClientState state)
            {
                ImmutableList.Builder<ColumnSpecification> builder = ImmutableList.builder();
                builder.add(new ColumnSpecification(KS, CF, new ColumnIdentifier("cluster", true), UTF8Type.instance),
                                        new ColumnSpecification(KS, CF, new ColumnIdentifier("partitioner", true), UTF8Type.instance),
                                        new ColumnSpecification(KS, CF, new ColumnIdentifier("snitch", true), UTF8Type.instance));

                if (mustReturnsRangeOwnerships(state.getRawKeyspace()))
                    builder.add(new ColumnSpecification(KS, CF, new ColumnIdentifier("range_ownership", true), MapType.getInstance(UTF8Type.instance,
                                                                                                                                   ListType.getInstance(UTF8Type.instance, false), false)));

                return builder.build();
            }

            @Override
            protected List<ByteBuffer> toRow(List<Object> elements, boolean withInternals)
            {
                ImmutableList.Builder<ByteBuffer> builder = ImmutableList.builder();

                builder.add(UTF8Type.instance.decompose((String) elements.get(CLUSTER_NAME_INDEX)),
                            UTF8Type.instance.decompose((String) elements.get(PARTITIONER_NAME_INDEX)),
                            UTF8Type.instance.decompose((String) elements.get(SNITCH_CLASS_INDEX)));

                if (elements.size() > 3)
                {
                    MapType<String, List<String>> rangeOwnershipType = MapType.getInstance(UTF8Type.instance,
                                                                                           ListType.getInstance(UTF8Type.instance, false),
                                                                                           false);

                    builder.add(rangeOwnershipType.decompose((Map<String, List<String>>) elements.get(RANGE_OWNERSHIPS_INDEX)));
                }

                return builder.build();
            }

            private String trimIfPresent(String src, String begin)
            {
                if (src.startsWith(begin))
                    return src.substring(begin.length());
                return src;
            }
        };
    }
}
