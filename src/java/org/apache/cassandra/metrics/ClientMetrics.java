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

import java.util.*;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import org.apache.cassandra.transport.ClientStat;
import org.apache.cassandra.transport.ConnectedClient;
import org.apache.cassandra.transport.Server;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public final class ClientMetrics
{
    public static final ClientMetrics instance = new ClientMetrics();

    private static final MetricNameFactory factory = new DefaultNameFactory("Client");

    private volatile boolean initialized = false;
    private Collection<Server> servers = Collections.emptyList();

    private Meter authSuccess;
    private Meter authFailure;

    private ClientMetrics()
    {
    }

    public void markAuthSuccess()
    {
        authSuccess.mark();
    }

    public void markAuthFailure()
    {
        authFailure.mark();
    }

    public List<ConnectedClient> allConnectedClients()
    {
        List<ConnectedClient> clients = new ArrayList<>();

        for (Server server : servers)
            clients.addAll(server.getConnectedClients());

        return clients;
    }

    public synchronized void init(Collection<Server> servers)
    {
        if (initialized)
            return;

        this.servers = servers;

        registerGauge("connectedNativeClients",       this::countConnectedClients);
        registerGauge("connectedNativeClientsByUser", this::countConnectedClientsByUser);
        registerGauge("connections",                  this::connectedClients);
        registerGauge("clientsByProtocolVersion",     this::recentClientStats);

        authSuccess = registerMeter("AuthSuccess");
        authFailure = registerMeter("AuthFailure");

        initialized = true;
    }

    private int countConnectedClients()
    {
        int count = 0;

        for (Server server : servers)
            count += server.countConnectedClients();

        return count;
    }

    private Map<String, Integer> countConnectedClientsByUser()
    {
        Map<String, Integer> counts = new HashMap<>();

        for (Server server : servers)
        {
            server.countConnectedClientsByUser()
                  .forEach((username, count) -> counts.put(username, counts.getOrDefault(username, 0) + count));
        }

        return counts;
    }

    private List<Map<String, String>> connectedClients()
    {
        List<Map<String, String>> clients = new ArrayList<>();

        for (Server server : servers)
            for (ConnectedClient client : server.getConnectedClients())
                clients.add(client.asMap());

        return clients;
    }

    private List<Map<String, String>> recentClientStats()
    {
        List<Map<String, String>> stats = new ArrayList<>();

        for (Server server : servers)
            for (ClientStat stat : server.recentClientStats())
                stats.add(stat.asMap());

        stats.sort(Comparator.comparing(map -> map.get(ClientStat.PROTOCOL_VERSION)));

        return stats;
    }

    private <T> Gauge<T> registerGauge(String name, Gauge<T> gauge)
    {
        return Metrics.register(factory.createMetricName(name), gauge);
    }

    private Meter registerMeter(String name)
    {
        return Metrics.meter(factory.createMetricName(name));
    }
}
