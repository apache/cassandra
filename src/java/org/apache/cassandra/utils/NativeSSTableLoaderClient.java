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
import org.apache.cassandra.db.CompactTables;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.schema.LegacySchemaTables;

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

    private static Map<String, CFMetaData> fetchTablesMetadata(String keyspace, Session session)
    {
        Map<String, CFMetaData> tables = new HashMap<>();

        String query = String.format("SELECT columnfamily_name, cf_id, type, comparator, subcomparator, is_dense, default_validator FROM %s.%s WHERE keyspace_name = '%s'",
                                     SystemKeyspace.NAME,
                                     LegacySchemaTables.COLUMNFAMILIES,
                                     keyspace);


        // The following is a slightly simplified but otherwise duplicated version of LegacySchemaTables.createTableFromTableRowAndColumnRows. It might
        // be safer to have a simple wrapper of the driver ResultSet/Row implementing UntypedResultSet/UntypedResultSet.Row and reuse the original method.
        for (Row row : session.execute(query))
        {
            String name = row.getString("columnfamily_name");
            UUID id = row.getUUID("cf_id");
            boolean isSuper = row.getString("type").toLowerCase().equals("super");
            AbstractType rawComparator = TypeParser.parse(row.getString("comparator"));
            AbstractType subComparator = row.isNull("subcomparator")
                                       ? null
                                       : TypeParser.parse(row.getString("subcomparator"));
            boolean isDense = row.getBool("is_dense");
            boolean isCompound = rawComparator instanceof CompositeType;

            AbstractType<?> defaultValidator = TypeParser.parse(row.getString("default_validator"));
            boolean isCounter =  defaultValidator instanceof CounterColumnType;
            boolean isCQLTable = !isSuper && !isDense && isCompound;

            String columnsQuery = String.format("SELECT column_name, component_index, type, validator FROM %s.%s WHERE keyspace_name='%s' AND columnfamily_name='%s'",
                                                SystemKeyspace.NAME,
                                                LegacySchemaTables.COLUMNS,
                                                keyspace,
                                                name);

            List<ColumnDefinition> defs = new ArrayList<>();
            for (Row colRow : session.execute(columnsQuery))
                defs.add(createDefinitionFromRow(colRow, keyspace, name, rawComparator, subComparator, isSuper, isCQLTable));

            tables.put(name, CFMetaData.create(keyspace, name, id, isDense, isCompound, isSuper, isCounter, defs));
        }

        return tables;
    }

    // A slightly simplified version of LegacySchemaTables.
    private static ColumnDefinition createDefinitionFromRow(Row row,
                                                            String keyspace,
                                                            String table,
                                                            AbstractType<?> rawComparator,
                                                            AbstractType<?> rawSubComparator,
                                                            boolean isSuper,
                                                            boolean isCQLTable)
    {
        ColumnDefinition.Kind kind = LegacySchemaTables.deserializeKind(row.getString("type"));

        Integer componentIndex = null;
        if (!row.isNull("component_index"))
            componentIndex = row.getInt("component_index");

        // Note: we save the column name as string, but we should not assume that it is an UTF8 name, we
        // we need to use the comparator fromString method
        AbstractType<?> comparator = isCQLTable
                                   ? UTF8Type.instance
                                   : CompactTables.columnDefinitionComparator(kind, isSuper, rawComparator, rawSubComparator);
        ColumnIdentifier name = ColumnIdentifier.getInterned(comparator.fromString(row.getString("column_name")), comparator);

        AbstractType<?> validator = TypeParser.parse(row.getString("validator"));

        return new ColumnDefinition(keyspace, table, name, validator, null, null, null, componentIndex, kind);
    }
}
