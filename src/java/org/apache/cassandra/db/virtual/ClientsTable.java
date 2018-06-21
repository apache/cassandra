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
package org.apache.cassandra.db.virtual;

import java.net.InetSocketAddress;

import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ConnectedClient;

public final class ClientsTable extends AbstractVirtualTable
{

    private static final String ADDRESS = "address";
    private static final String PORT = "port";
    private static final String HOSTNAME = "hostname";
    private static final String PROTOCOL_VERSION = "protocol_version";
    private static final String USER = "username";
    private static final String REQUESTS = "requests";
    private static final String SSL = "ssl_enabled";
    private static final String SSL_PROTOCOL = "ssl_protocol";
    private static final String SSL_CIPHER_SUITE = "ssl_cipher_suite";
    private static final String DRIVER_VERSION = "driver_version";
    private static final String DRIVER_NAME = "driver_name";

    ClientsTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "clients")
                .comment("currently connected clients")
                .kind(TableMetadata.Kind.VIRTUAL)
                .addPartitionKeyColumn(ADDRESS, InetAddressType.instance)
                .addClusteringColumn(PORT, Int32Type.instance)
                .addRegularColumn(HOSTNAME, UTF8Type.instance)
                .addRegularColumn(PROTOCOL_VERSION, Int32Type.instance)
                .addRegularColumn(SSL, BooleanType.instance)
                .addRegularColumn(SSL_CIPHER_SUITE, UTF8Type.instance)
                .addRegularColumn(SSL_PROTOCOL, UTF8Type.instance)
                .addRegularColumn(DRIVER_NAME, UTF8Type.instance)
                .addRegularColumn(DRIVER_VERSION, UTF8Type.instance)
                .addRegularColumn(REQUESTS, LongType.instance)
                .addRegularColumn(USER, UTF8Type.instance)
                .build());
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        for(ConnectedClient c : ClientMetrics.instance.getClients())
        {
            InetSocketAddress host = c.remoteAddress();
            result.row(host.getAddress(), host.getPort())
                .column(HOSTNAME, host.getHostName())
                .column(REQUESTS, c.requestCount())
                .column(USER, c.username().orElse(null))
                .column(SSL, c.sslEnabled())
                .column(SSL_CIPHER_SUITE, c.sslCipherSuite().orElse(null))
                .column(SSL_PROTOCOL, c.sslProtocol().orElse(null))
                .column(DRIVER_NAME, c.driverName().orElse(null))
                .column(DRIVER_VERSION, c.driverVersion().orElse(null))
                .column(PROTOCOL_VERSION, c.protocolVersion());

        }

        return result;
    }
}
