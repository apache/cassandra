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
import java.util.*;

import com.datastax.driver.core.*;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.schema.SchemaKeyspace;

public class NativeSSTableLoaderClient extends SSTableLoader.Client
{
    protected final Map<String, CFMetaData> tables;
    private final Collection<InetAddress> hosts;
    private final int port;
    private final String username;
    private final String password;
    private final SSLOptions sslOptions;

    public NativeSSTableLoaderClient(Collection<InetAddress> hosts, int port, String username, String password, SSLOptions sslOptions)
    {
        super();
        this.tables = new HashMap<>();
        this.hosts = hosts;
        this.port = port;
        this.username = username;
        this.password = password;
        this.sslOptions = sslOptions;
    }

    public void init(String keyspace)
    {
        Cluster.Builder builder = Cluster.builder().addContactPoints(hosts).withPort(port);
        if (sslOptions != null)
            builder.withSSL(sslOptions);
        if (username != null && password != null)
            builder = builder.withCredentials(username, password);

        try (Cluster cluster = builder.build(); Session session = cluster.connect())
        {

            Metadata metadata = cluster.getMetadata();

            setPartitioner(metadata.getPartitioner());

            Set<TokenRange> tokenRanges = metadata.getTokenRanges();

            Token.TokenFactory tokenFactory = getPartitioner().getTokenFactory();

            for (TokenRange tokenRange : tokenRanges)
            {
                Set<Host> endpoints = metadata.getReplicas(keyspace, tokenRange);
                Range<Token> range = new Range<>(tokenFactory.fromString(tokenRange.getStart().getValue().toString()),
                                                 tokenFactory.fromString(tokenRange.getEnd().getValue().toString()));
                for (Host endpoint : endpoints)
                    addRangeForEndpoint(range, endpoint.getAddress());
            }

            tables.putAll(fetchTablesMetadata(keyspace, session));
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

    /*
     * The following is a slightly simplified but otherwise duplicated version of
     * SchemaKeyspace.createTableFromTableRowAndColumnRows().
     * It might be safer to have a simple wrapper of the driver ResultSet/Row implementing
     * UntypedResultSet/UntypedResultSet.Row and reuse the original method.
     */
    private static Map<String, CFMetaData> fetchTablesMetadata(String keyspace, Session session)
    {
        Map<String, CFMetaData> tables = new HashMap<>();
        String query = String.format("SELECT * FROM %s.%s WHERE keyspace_name = ?", SchemaKeyspace.NAME, SchemaKeyspace.TABLES);

        for (Row row : session.execute(query, keyspace))
        {
            String name = row.getString("table_name");
            UUID id = row.getUUID("id");

            Set<CFMetaData.Flag> flags = row.isNull("flags")
                                       ? Collections.emptySet()
                                       : SchemaKeyspace.flagsFromStrings(row.getSet("flags", String.class));

            boolean isSuper = flags.contains(CFMetaData.Flag.SUPER);
            boolean isCounter = flags.contains(CFMetaData.Flag.COUNTER);
            boolean isDense = flags.contains(CFMetaData.Flag.DENSE);
            boolean isCompound = flags.contains(CFMetaData.Flag.COMPOUND);

            String columnsQuery = String.format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND table_name = ?",
                                                SchemaKeyspace.NAME,
                                                SchemaKeyspace.COLUMNS);

            List<ColumnDefinition> defs = new ArrayList<>();
            for (Row colRow : session.execute(columnsQuery, keyspace, name))
                defs.add(createDefinitionFromRow(colRow, keyspace, name));

            tables.put(name, CFMetaData.create(keyspace, name, id, isDense, isCompound, isSuper, isCounter, defs));
        }

        return tables;
    }

    private static ColumnDefinition createDefinitionFromRow(Row row, String keyspace, String table)
    {
        ColumnIdentifier name = ColumnIdentifier.getInterned(row.getBytes("column_name_bytes"), row.getString("column_name"));

        ColumnDefinition.Kind kind = ColumnDefinition.Kind.valueOf(row.getString("type").toUpperCase());

        Integer componentIndex = null;
        if (!row.isNull("component_index"))
            componentIndex = row.getInt("component_index");

        AbstractType<?> validator = TypeParser.parse(row.getString("validator"));

        return new ColumnDefinition(keyspace, table, name, validator, null, null, null, componentIndex, kind);
    }
}
