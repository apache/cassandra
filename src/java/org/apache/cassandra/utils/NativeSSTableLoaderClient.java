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
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.*;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.ColumnDefinition.ClusteringOrder;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.Token.TokenFactory;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.schema.CQLTypeParser;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.schema.Types;

public class NativeSSTableLoaderClient extends SSTableLoader.Client
{
    protected final Map<String, CFMetaData> tables;
    private final Collection<InetAddress> hosts;
    private final int port;
    private final AuthProvider authProvider;
    private final SSLOptions sslOptions;


    public NativeSSTableLoaderClient(Collection<InetAddress> hosts, int port, String username, String password, SSLOptions sslOptions)
    {
        this(hosts, port, new PlainTextAuthProvider(username, password), sslOptions);
    }

    public NativeSSTableLoaderClient(Collection<InetAddress> hosts, int port, AuthProvider authProvider, SSLOptions sslOptions)
    {
        super();
        this.tables = new HashMap<>();
        this.hosts = hosts;
        this.port = port;
        this.authProvider = authProvider;
        this.sslOptions = sslOptions;
    }

    public void init(String keyspace)
    {
        Cluster.Builder builder = Cluster.builder().addContactPoints(hosts).withPort(port);
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
                    addRangeForEndpoint(range, endpoint.getBroadcastAddress());
            }

            Types types = fetchTypes(keyspace, session);

            tables.putAll(fetchTables(keyspace, session, partitioner, types));
            // We only need the CFMetaData for the views, so we only load that.
            tables.putAll(fetchViews(keyspace, session, partitioner, types));
        }
    }

    public CFMetaData getTableMetadata(String tableName)
    {
        return tables.get(tableName);
    }

    @Override
    public void setTableMetadata(CFMetaData cfm)
    {
        tables.put(cfm.cfName, cfm);
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
    private static Map<String, CFMetaData> fetchTables(String keyspace, Session session, IPartitioner partitioner, Types types)
    {
        Map<String, CFMetaData> tables = new HashMap<>();
        String query = String.format("SELECT * FROM %s.%s WHERE keyspace_name = ?", SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspace.TABLES);

        for (Row row : session.execute(query, keyspace))
        {
            String name = row.getString("table_name");
            tables.put(name, createTableMetadata(keyspace, session, partitioner, false, row, name, types));
        }

        return tables;
    }

    /*
     * In the case where we are creating View CFMetaDatas, we
     */
    private static Map<String, CFMetaData> fetchViews(String keyspace, Session session, IPartitioner partitioner, Types types)
    {
        Map<String, CFMetaData> tables = new HashMap<>();
        String query = String.format("SELECT * FROM %s.%s WHERE keyspace_name = ?", SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspace.VIEWS);

        for (Row row : session.execute(query, keyspace))
        {
            String name = row.getString("view_name");
            tables.put(name, createTableMetadata(keyspace, session, partitioner, true, row, name, types));
        }

        return tables;
    }

    private static CFMetaData createTableMetadata(String keyspace,
                                                  Session session,
                                                  IPartitioner partitioner,
                                                  boolean isView,
                                                  Row row,
                                                  String name,
                                                  Types types)
    {
        UUID id = row.getUUID("id");
        Set<CFMetaData.Flag> flags = isView ? Collections.emptySet() : CFMetaData.flagsFromStrings(row.getSet("flags", String.class));

        boolean isSuper = flags.contains(CFMetaData.Flag.SUPER);
        boolean isCounter = flags.contains(CFMetaData.Flag.COUNTER);
        boolean isDense = flags.contains(CFMetaData.Flag.DENSE);
        boolean isCompound = isView || flags.contains(CFMetaData.Flag.COMPOUND);

        String columnsQuery = String.format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND table_name = ?",
                                            SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                            SchemaKeyspace.COLUMNS);

        List<ColumnDefinition> defs = new ArrayList<>();
        for (Row colRow : session.execute(columnsQuery, keyspace, name))
            defs.add(createDefinitionFromRow(colRow, keyspace, name, types));

        CFMetaData metadata = CFMetaData.create(keyspace,
                                                name,
                                                id,
                                                isDense,
                                                isCompound,
                                                isSuper,
                                                isCounter,
                                                isView,
                                                defs,
                                                partitioner);

        String droppedColumnsQuery = String.format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND table_name = ?",
                                                   SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                                   SchemaKeyspace.DROPPED_COLUMNS);
        Map<ByteBuffer, CFMetaData.DroppedColumn> droppedColumns = new HashMap<>();
        for (Row colRow : session.execute(droppedColumnsQuery, keyspace, name))
        {
            CFMetaData.DroppedColumn droppedColumn = createDroppedColumnFromRow(colRow, keyspace);
            droppedColumns.put(UTF8Type.instance.decompose(droppedColumn.name), droppedColumn);
        }
        metadata.droppedColumns(droppedColumns);

        return metadata;
    }

    private static ColumnDefinition createDefinitionFromRow(Row row, String keyspace, String table, Types types)
    {
        ClusteringOrder order = ClusteringOrder.valueOf(row.getString("clustering_order").toUpperCase());
        AbstractType<?> type = CQLTypeParser.parse(keyspace, row.getString("type"), types);
        if (order == ClusteringOrder.DESC)
            type = ReversedType.getInstance(type);

        ColumnIdentifier name = new ColumnIdentifier(row.getBytes("column_name_bytes"), row.getString("column_name"));

        int position = row.getInt("position");
        ColumnDefinition.Kind kind = ColumnDefinition.Kind.valueOf(row.getString("kind").toUpperCase());
        return new ColumnDefinition(keyspace, table, name, type, position, kind);
    }

    private static CFMetaData.DroppedColumn createDroppedColumnFromRow(Row row, String keyspace)
    {
        String name = row.getString("column_name");
        AbstractType<?> type = CQLTypeParser.parse(keyspace, row.getString("type"), Types.none());
        long droppedTime = TimeUnit.MILLISECONDS.toMicros(row.getTimestamp("dropped_time").getTime());
        return new CFMetaData.DroppedColumn(name, type, droppedTime);
    }
}
