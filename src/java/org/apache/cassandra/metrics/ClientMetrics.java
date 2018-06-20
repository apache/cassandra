/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.metrics;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import org.apache.cassandra.transport.Connection;
import org.apache.cassandra.transport.Server;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.google.common.collect.ImmutableMap;


public class ClientMetrics
{
    private static final MetricNameFactory factory = new DefaultNameFactory("Client");

    public static final String USER = "user";
    public static final String ADDRESS = "address";
    public static final String VERSION = "version";
    public static final String KEYSPACE = "keyspace";
    public static final String PROTOCOL = "protocol";
    public static final String CIPHER = "cipher";
    public static final String DRIVER_VERSION = "driverVersion";
    public static final String DRIVER_NAME = "driverName";
    public static final String SSL = "ssl";
    public static final String REQUESTS = "requests";

    public static final ClientMetrics instance = new ClientMetrics();
    public boolean initialized = false;

    private Collection<Server> servers;

    private ClientMetrics()
    {
    }

    public List<Connection.View> getConnectionStates()
    {
        if (servers == null)
            return Collections.emptyList();
        List<Connection.View> connections = new ArrayList<>();
        for (Server s : servers)
        {
            connections.addAll(s.getConnectionStates());
        }
        return connections;
    }

    public int getConnectedNativeClients()
    {
        int ret = 0;
        for (Server server : servers)
            ret += server.getConnectedClients();
        return ret;
    }

    public Map<String, Integer> getConnectedNativeClientsByUser()
    {
        Map<String, Integer> result = new HashMap<>();
        for (Server server : servers)
        {
            for (Entry<String, Integer> e : server.getConnectedClientsByUser().entrySet())
            {
                String user = e.getKey();
                result.put(user, result.getOrDefault(user, 0) + e.getValue());
            }
        }
        return result;
    }

    public <T> Gauge<T> addGauge(String name, final Callable<T> provider)
    {
        return Metrics.register(factory.createMetricName(name), (Gauge<T>) () -> {
            try
            {
                return provider.call();
            } catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        });
    }

    public Meter addMeter(String name)
    {
        return Metrics.meter(factory.createMetricName(name));
    }

    public synchronized void init(Collection<Server> servers)
    {
        this.servers = servers;
        if (initialized) return;
        initialized = true;

        // register metrics
        addGauge("connectedNativeClients", () -> getConnectedNativeClients());
        addGauge("connectedNativeClientsByUser", () -> getConnectedNativeClientsByUser());
        addGauge("connections", () ->
        {
            List<Map<String, String>> result = new ArrayList<>();
            for (Server server : servers)
            {
                for (Connection.View connection : server.getConnectionStates())
                {
                    result.add(new ImmutableMap.Builder<String,String>()
                            .put(USER, connection.getUser())
                            .put(ADDRESS, connection.getAddress().toString())
                            .put(VERSION, String.valueOf(connection.getVersion()))
                            .put(REQUESTS, String.valueOf(connection.getRequests()))
                            .put(SSL, Boolean.toString(connection.sslEnabled()))
                            .put(DRIVER_NAME, connection.getDriverName().orElse("undefined"))
                            .put(DRIVER_VERSION, connection.getDriverVersion().orElse("undefined"))
                            .put(CIPHER, connection.getSSLCipher().orElse("undefined"))
                            .put(PROTOCOL, connection.getSSLProtocol().orElse("undefined"))
                            .put(KEYSPACE, connection.getKeyspace().orElse(""))
                            .build());
                }
            }
            return result;
        });
        addGauge("clientsByProtocolVersion", () ->
        {
            List<Map<String, String>> result = new ArrayList<>();
            for (Server server : servers)
            {
                result.addAll(server.getClientsByProtocolVersion());
            }
            return result;
        });
    }

}
