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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import com.google.common.annotations.VisibleForTesting;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Reservoir;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.transport.ClientResourceLimits;
import org.apache.cassandra.transport.ClientStat;
import org.apache.cassandra.transport.ConnectedClient;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.transport.ServerConnection;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public final class ClientMetrics
{
    public static final ClientMetrics instance = new ClientMetrics();

    private static final MetricNameFactory factory = new DefaultNameFactory("Client");

    private volatile boolean initialized = false;
    private Collection<Server> servers = Collections.emptyList();

    @VisibleForTesting
    Meter authSuccess;

    @VisibleForTesting
    final Map<String, Meter> authSuccessByMode = new HashMap<>();

    @VisibleForTesting
    Meter authFailure;

    @VisibleForTesting
    final Map<String, Meter> authFailureByMode = new HashMap<>();

    @VisibleForTesting
    Gauge<Integer> connectedNativeClients;

    @VisibleForTesting
    Gauge<Integer> encryptedConnectedNativeClients;

    @VisibleForTesting
    Gauge<Integer> unencryptedConnectedNativeClients;

    @VisibleForTesting
    Gauge<Map<String, Integer>> connectedNativeClientsByUser;

    @VisibleForTesting
    final Map<String, Gauge<Integer>> connectedNativeClientsByAuthMode = new HashMap<>();

    private AtomicInteger pausedConnections;

    @SuppressWarnings({ "unused", "FieldCanBeLocal" })
    private Gauge<Integer> pausedConnectionsGauge;
    
    private Meter requestDiscarded;
    private Meter requestDispatched;

    private Meter protocolException;
    private Meter unknownException;

    private static final String AUTH_SUCCESS = "AuthSuccess";

    private static final String AUTH_FAILURE = "AuthFailure";

    private static final String CONNECTED_NATIVE_CLIENTS = "ConnectedNativeClients";

    private ClientMetrics()
    {
    }

    /**
     * @deprecated by {@link #markAuthSuccess(String)}
     */
    @Deprecated(since="5.1", forRemoval = true)
    public void markAuthSuccess()
    {
        markAuthSuccess(null);
    }

    public void markAuthSuccess(String mode)
    {
        markAuthMeter(authSuccess, authSuccessByMode, mode);
    }

    /**
     * @deprecated by {@link #markAuthFailure(String)}
     */
    @Deprecated(since="5.1", forRemoval = true)
    public void markAuthFailure()
    {
        authFailure.mark();
    }

    public void markAuthFailure(String mode)
    {
        markAuthMeter(authFailure, authFailureByMode, mode);
    }

    private void markAuthMeter(Meter meter, Map<String, Meter> meterMap, String mode)
    {
        meter.mark();

        // If there is a mode and that mode is present in the meterMap, mark it.
        // If there is no meter for this mode, the IAuthenticator must have surfaced a mode that it doesn't
        // list in 'getSupportedAuthenticationModes', in this case, just don't mark.
        Optional.ofNullable(mode)
                .flatMap((m) -> Optional.ofNullable(meterMap.get(m)))
                .ifPresent(Meter::mark);
    }

    public void pauseConnection() { pausedConnections.incrementAndGet(); }
    public void unpauseConnection() { pausedConnections.decrementAndGet(); }

    public void markRequestDiscarded() { requestDiscarded.mark(); }
    public void markRequestDispatched() { requestDispatched.mark(); }

    public List<ConnectedClient> allConnectedClients()
    {
        List<ConnectedClient> clients = new ArrayList<>();

        for (Server server : servers)
            clients.addAll(server.getConnectedClients());

        return clients;
    }

    public void markProtocolException()
    {
        protocolException.mark();
    }

    public void markUnknownException()
    {
        unknownException.mark();
    }

    public synchronized void init(Collection<Server> servers)
    {
        if (initialized)
            return;

        this.servers = servers;

        // deprecated the lower-cased initial letter metric names in 4.0
        connectedNativeClients = registerGauge(CONNECTED_NATIVE_CLIENTS, "connectedNativeClients", this::countConnectedClients);
        connectedNativeClientsByUser = registerGauge("ConnectedNativeClientsByUser", "connectedNativeClientsByUser", this::countConnectedClientsByUser);
        registerGauge("Connections", "connections", this::connectedClients);
        registerGauge("ClientsByProtocolVersion", "clientsByProtocolVersion", this::recentClientStats);
        registerGauge("RequestsSize", ClientResourceLimits::getCurrentGlobalUsage);

        Reservoir ipUsageReservoir = ClientResourceLimits.ipUsageReservoir();
        Metrics.register(factory.createMetricName("RequestsSizeByIpDistribution"),
                         new Histogram(ipUsageReservoir)
        {
             public long getCount()
             {
                 return ipUsageReservoir.size();
             }
        });

        authSuccess = registerMeter("AuthSuccess");
        authFailure = registerMeter("AuthFailure");

        // For each of SSL, non-SSL register a gauge:
        encryptedConnectedNativeClients = registerGauge(new DefaultNameFactory("Client", "Encrypted"), CONNECTED_NATIVE_CLIENTS, () -> countConnectedClients((ServerConnection::isSSL)));
        unencryptedConnectedNativeClients = registerGauge(new DefaultNameFactory("Client", "Unencrypted"), CONNECTED_NATIVE_CLIENTS, () -> countConnectedClients(((ServerConnection connection) -> !connection.isSSL())));

        // for each supported authentication mode, register a meter for success and failures.
        IAuthenticator authenticator = DatabaseDescriptor.getAuthenticator();
        for (IAuthenticator.AuthenticationMode mode : authenticator.getSupportedAuthenticationModes())
        {
            String modeString = mode.toString();
            MetricNameFactory factory = new DefaultNameFactory("Client", modeString);
            authSuccessByMode.put(modeString, registerMeter(factory, AUTH_SUCCESS));
            authFailureByMode.put(modeString, registerMeter(factory, AUTH_FAILURE));

            Gauge<Integer> clients = registerGauge(factory, CONNECTED_NATIVE_CLIENTS, () -> countConnectedClients((ServerConnection connection) -> {
                AuthenticatedUser user = connection.getClientState().getUser();
                return Optional.ofNullable(user)
                               .map(u -> mode.equals(u.getAuthenticationMode()))
                               .orElse(false);
            }));
            connectedNativeClientsByAuthMode.put(modeString, clients);
        }

        pausedConnections = new AtomicInteger();
        pausedConnectionsGauge = registerGauge("PausedConnections", pausedConnections::get);
        requestDiscarded = registerMeter("RequestDiscarded");
        requestDispatched = registerMeter("RequestDispatched");

        protocolException = registerMeter("ProtocolException");
        unknownException = registerMeter("UnknownException");

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

    private int countConnectedClients(Predicate<ServerConnection> predicate)
    {
        int count = 0;

        for (Server server : servers)
            count += server.countConnectedClients(predicate);

        return count;
    }

    private List<Map<String, String>> recentClientStats()
    {
        List<Map<String, String>> stats = new ArrayList<>();

        for (Server server : servers)
            for (ClientStat stat : server.recentClientStats())
                stats.add(new HashMap<>(stat.asMap())); // asMap returns guava, so need to convert to java for jmx

        stats.sort(Comparator.comparing(map -> map.get(ClientStat.PROTOCOL_VERSION)));

        return stats;
    }

    private <T> Gauge<T> registerGauge(String name, Gauge<T> gauge)
    {
        return registerGauge(factory, name, gauge);
    }

    private <T> Gauge<T> registerGauge(MetricNameFactory metricNameFactory, String name, Gauge<T> gauge)
    {
        return Metrics.register(metricNameFactory.createMetricName(name), gauge);
    }

    private <T> Gauge<T> registerGauge(String name, String deprecated, Gauge<T> gauge)
    {
        Gauge<T> registeredGauge = registerGauge(name, gauge);
        Metrics.registerMBean(registeredGauge, factory.createMetricName(deprecated).getMBeanName());
        return registeredGauge;
    }

    private Meter registerMeter(String name)
    {
        return registerMeter(factory, name);
    }

    private Meter registerMeter(MetricNameFactory metricNameFactory, String name)
    {
        return Metrics.meter(metricNameFactory.createMetricName(name));
    }
}
