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

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import org.apache.cassandra.transport.Server;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;


public class ClientMetrics
{
    private static final MetricNameFactory factory = new DefaultNameFactory("Client");
    public static final ClientMetrics instance = new ClientMetrics();

    private volatile boolean initialized = false;

    private Collection<Server> servers = Collections.emptyList();

    private AtomicInteger pausedConnections;
    private Gauge<Integer> pausedConnectionsGauge;
    private Meter requestDiscarded;

    private ClientMetrics()
    {
    }

    public void pauseConnection() { pausedConnections.incrementAndGet(); }
    public void unpauseConnection() { pausedConnections.decrementAndGet(); }
    public void markRequestDiscarded() { requestDiscarded.mark(); }

    public synchronized void init(Collection<Server> servers)
    {
        if (initialized)
            return;

        this.servers = servers;

        registerGauge("connectedNativeClients", this::countConnectedClients);

        pausedConnections = new AtomicInteger();
        pausedConnectionsGauge = registerGauge("PausedConnections", pausedConnections::get);
        requestDiscarded = registerMeter("RequestDiscarded");

        initialized = true;
    }

    public void addCounter(String name, final Callable<Integer> provider)
    {
        Metrics.register(factory.createMetricName(name), (Gauge<Integer>) () -> {
            try
            {
                return provider.call();
            } catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        });
    }

    private int countConnectedClients()
    {
        int count = 0;

        for (Server server : servers)
            count += server.getConnectedClients();

        return count;
    }

    private <T> Gauge<T> registerGauge(String name, Gauge<T> gauge)
    {
        return Metrics.register(factory.createMetricName(name), gauge);
    }

    public Meter registerMeter(String name)
    {
        return Metrics.meter(factory.createMetricName(name));
    }
}
