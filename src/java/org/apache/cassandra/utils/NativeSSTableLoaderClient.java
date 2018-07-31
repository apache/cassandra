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
package org.apache.cassandra.utils;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.stream.Collectors;

import com.datastax.driver.core.*;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.ColumnMetadata.ClusteringOrder;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.Token.TokenFactory;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.schema.TableMetadata;

import org.apache.cassandra.schema.CQLTypeParser;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.schema.Types;

public class NativeSSTableLoaderClient extends SSTableLoader.Client
{
    protected final Map<String, TableMetadataRef> tables;
    private final Collection<InetSocketAddress> hosts;
    private final int port;
    private final int storagePort;
    private final AuthProvider authProvider;
    private final SSLOptions sslOptions;
    private final boolean allowServerPortDiscovery;


    public NativeSSTableLoaderClient(Collection<InetSocketAddress> hosts, int nativePort, int storagePort, String username, String password, SSLOptions sslOptions, boolean allowServerPortDiscovery)
    {
        this(hosts, nativePort, storagePort, new PlainTextAuthProvider(username, password), sslOptions, allowServerPortDiscovery);
    }

    public NativeSSTableLoaderClient(Collection<InetSocketAddress> hosts, int nativePort, int storagePort, AuthProvider authProvider, SSLOptions sslOptions, boolean allowServerPortDiscovery)
    {
        super();
        this.tables = new HashMap<>();
        this.hosts = hosts;
        this.port = nativePort;
        this.authProvider = authProvider;
        this.sslOptions = sslOptions;
        this.allowServerPortDiscovery = allowServerPortDiscovery;
        this.storagePort = storagePort;
    }

    public void init(String keyspace)
    {
        Set<InetAddress> hostAddresses = hosts.stream().map(host -> host.getAddress()).collect(Collectors.toSet());
        Cluster.Builder builder = Cluster.builder().addContactPoints(hostAddresses).withPort(port).allowBetaProtocolVersion();

        if (allowServerPortDiscovery)
            builder = builder.allowServerPortDiscovery();

        if (sslOptions != null)
            builder.withSSL(sslOptions);
        if (authProvider != null)
            builder = builder.withAuthProvider(authProvider);

        try (Cluster cluster = builder.build(); Session session = cluster.connect())
        {

            Metadata metadata = cluster.getMetadata();

            Set<TokenRange> tokenRanges = metadata.getTokenRanges();

            IPartitioner partitioner = FBUtilities.newPartitioner(metadata.getPartitioner());
            TokenFactory tokenFactory = partitioner.getTokenFactory();

            for (TokenRange tokenRange : tokenRanges)
            {
                Set<Host> endpoints = metadata.getReplicas(Metadata.quote(keyspace), tokenRange);
                Range<Token> range = new Range<>(tokenFactory.fromString(tokenRange.getStart().getValue().toString()),
                                                 tokenFactory.fromString(tokenRange.getEnd().getValue().toString()));
                for (Host endpoint : endpoints)
                {
                    int portToUse;
                    if (allowServerPortDiscovery)
                    {
                        portToUse = endpoint.getBroadcastAddressOptPort().portOrElse(storagePort);
                    }
                    else
                    {
                        portToUse = storagePort;
                    }
                    addRangeForEndpoint(range, InetAddressAndPort.getByNameOverrideDefaults(endpoint.getAddress().getHostAddress(), portToUse));
                }
            }

            Types types = fetchTypes(keyspace, session);

            tables.putAll(fetchTables(keyspace, session, partitioner, types));
            // We only need the TableMetadata for the views, so we only load that.
            tables.putAll(fetchViews(keyspace, session, partitioner, types));
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    public TableMetadataRef getTableMetadata(String tableName)
    {
        return tables.get(tableName);
    }

    @Override
    public void setTableMetadata(TableMetadataRef cfm)
    {
        tables.put(cfm.name, cfm);
    }

    private static Types fetchTypes(String keyspace, Session session)
    {
        String query = String.format("SELECT * FROM %s.%s WHERE keyspace_name = ?", SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspace.TYPES);

        Types.RawBuilder types = Types.rawBuilder(keyspace);
        for (Row row : session.execute(query, keyspace))
        {
            String name = row.getString("type_name");
            List<String> fieldNames = row.getList("field_names", String.class);
            List<String> fieldTypes = row.getList("field_types", String.class);
            types.add(name, fieldNames, fieldTypes);
        }
        return types.build();
    }

    /*
     * The following is a slightly simplified but otherwise duplicated version of
     * SchemaKeyspace.createTableFromTableRowAndColumnRows().
     * It might be safer to have a simple wrapper of the driver ResultSet/Row implementing
     * UntypedResultSet/UntypedResultSet.Row and reuse the original method.
     *
     * Note: It is not safe for this class to use static methods from SchemaKeyspace (static final fields are ok)
     * as that triggers initialization of the class, which fails in client mode.
     */
    private static Map<String, TableMetadataRef> fetchTables(String keyspace, Session session, IPartitioner partitioner, Types types)
    {
        Map<String, TableMetadataRef> tables = new HashMap<>();
        String query = String.format("SELECT * FROM %s.%s WHERE keyspace_name = ?", SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspace.TABLES);

        for (Row row : session.execute(query, keyspace))
        {
            String name = row.getString("table_name");
            tables.put(name, createTableMetadata(keyspace, session, partitioner, false, row, name, types));
        }

        return tables;
    }

    private static Map<String, TableMetadataRef> fetchViews(String keyspace, Session session, IPartitioner partitioner, Types types)
    {
        Map<String, TableMetadataRef> tables = new HashMap<>();
        String query = String.format("SELECT * FROM %s.%s WHERE keyspace_name = ?", SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspace.VIEWS);

        for (Row row : session.execute(query, keyspace))
        {
            String name = row.getString("view_name");
            tables.put(name, createTableMetadata(keyspace, session, partitioner, true, row, name, types));
        }

        return tables;
    }

    private static TableMetadataRef createTableMetadata(String keyspace,
                                                        Session session,
                                                        IPartitioner partitioner,
                                                        boolean isView,
                                                        Row row,
                                                        String name,
                                                        Types types)
    {
        TableMetadata.Builder builder = TableMetadata.builder(keyspace, name, TableId.fromUUID(row.getUUID("id")))
                                                     .partitioner(partitioner);

        if (!isView)
            builder.flags(TableMetadata.Flag.fromStringSet(row.getSet("flags", String.class)));

        String columnsQuery = String.format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND table_name = ?",
                                            SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                            SchemaKeyspace.COLUMNS);

        for (Row colRow : session.execute(columnsQuery, keyspace, name))
            builder.addColumn(createDefinitionFromRow(colRow, keyspace, name, types));

        String droppedColumnsQuery = String.format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND table_name = ?",
                                                   SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                                   SchemaKeyspace.DROPPED_COLUMNS);
        Map<ByteBuffer, DroppedColumn> droppedColumns = new HashMap<>();
        for (Row colRow : session.execute(droppedColumnsQuery, keyspace, name))
        {
            DroppedColumn droppedColumn = createDroppedColumnFromRow(colRow, keyspace, name);
            droppedColumns.put(droppedColumn.column.name.bytes, droppedColumn);
        }
        builder.droppedColumns(droppedColumns);

        return TableMetadataRef.forOfflineTools(builder.build());
    }

    private static ColumnMetadata createDefinitionFromRow(Row row, String keyspace, String table, Types types)
    {
        ClusteringOrder order = ClusteringOrder.valueOf(row.getString("clustering_order").toUpperCase());
        AbstractType<?> type = CQLTypeParser.parse(keyspace, row.getString("type"), types);
        if (order == ClusteringOrder.DESC)
            type = ReversedType.getInstance(type);

        ColumnIdentifier name = new ColumnIdentifier(row.getBytes("column_name_bytes"), row.getString("column_name"));

        int position = row.getInt("position");
        org.apache.cassandra.schema.ColumnMetadata.Kind kind = ColumnMetadata.Kind.valueOf(row.getString("kind").toUpperCase());
        return new ColumnMetadata(keyspace, table, name, type, position, kind);
    }

    private static DroppedColumn createDroppedColumnFromRow(Row row, String keyspace, String table)
    {
        String name = row.getString("column_name");
        AbstractType<?> type = CQLTypeParser.parse(keyspace, row.getString("type"), Types.none());
        ColumnMetadata.Kind kind = ColumnMetadata.Kind.valueOf(row.getString("kind").toUpperCase());
        ColumnMetadata column = new ColumnMetadata(keyspace, table, ColumnIdentifier.getInterned(name, true), type, ColumnMetadata.NO_POSITION, kind);
        long droppedTime = row.getTimestamp("dropped_time").getTime();
        return new DroppedColumn(column, droppedTime);
    }
}
