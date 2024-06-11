/**
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
package org.apache.cassandra.stress.util;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import com.google.common.net.HostAndPort;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;
import com.datastax.shaded.netty.channel.socket.SocketChannel;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.stress.settings.StressSettings;

public class JavaDriverClient
{

    static
    {
        InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
    }

    public final List<String> hosts;
    public final int port;
    public final String username;
    public final String password;
    public final AuthProvider authProvider;
    public final int maxPendingPerConnection;
    public final int connectionsPerHost;

    private final ProtocolVersion protocolVersion;
    private final EncryptionOptions encryptionOptions;
    private Cluster cluster;
    private Session session;
    private final LoadBalancingPolicy loadBalancingPolicy;

    private static final ConcurrentMap<String, PreparedStatement> stmts = new ConcurrentHashMap<>();

    public JavaDriverClient(StressSettings settings, String host, int port)
    {
        this(settings, Collections.singletonList(host), port, new EncryptionOptions());
    }

    public JavaDriverClient(StressSettings settings, List<String> hosts, int port)
    {
        this(settings, hosts, port, new EncryptionOptions());
    }

    public JavaDriverClient(StressSettings settings, List<String> hosts, int port, EncryptionOptions encryptionOptions)
    {
        this.protocolVersion = settings.mode.protocolVersion;
        this.hosts = hosts;
        this.port = port;
        this.username = settings.mode.username;
        this.password = settings.mode.password;
        this.authProvider = settings.mode.authProvider;
        this.encryptionOptions = new EncryptionOptions(encryptionOptions).applyConfig();
        this.loadBalancingPolicy = loadBalancingPolicy(settings);
        this.connectionsPerHost = settings.mode.connectionsPerHost == null ? 8 : settings.mode.connectionsPerHost;

        int maxThreadCount = 0;
        if (settings.rate.auto)
            maxThreadCount = settings.rate.maxThreads;
        else
            maxThreadCount = settings.rate.threadCount;

        //Always allow enough pending requests so every thread can have a request pending
        //See https://issues.apache.org/jira/browse/CASSANDRA-7217
        int requestsPerConnection = (maxThreadCount / connectionsPerHost) + connectionsPerHost;

        maxPendingPerConnection = settings.mode.maxPendingPerConnection == null ? Math.max(128, requestsPerConnection ) : settings.mode.maxPendingPerConnection;
    }

    private LoadBalancingPolicy loadBalancingPolicy(StressSettings settings)
    {
        DCAwareRoundRobinPolicy.Builder policyBuilder = DCAwareRoundRobinPolicy.builder();
        if (settings.node.datacenter != null)
            policyBuilder.withLocalDc(settings.node.datacenter);

        LoadBalancingPolicy ret = null;
        if (settings.node.datacenter != null)
            ret = policyBuilder.build();

        if (settings.node.isWhiteList)
            ret = new WhiteListPolicy(ret == null ? policyBuilder.build() : ret, settings.node.resolveAll(settings.port.nativePort));

        return new TokenAwarePolicy(ret == null ? policyBuilder.build() : ret);
    }

    public PreparedStatement prepare(String query)
    {
        PreparedStatement stmt = stmts.get(query);
        if (stmt != null)
            return stmt;
        synchronized (stmts)
        {
            stmt = stmts.get(query);
            if (stmt != null)
                return stmt;
            stmt = getSession().prepare(query);
            stmts.put(query, stmt);
        }
        return stmt;
    }

    public void connect(ProtocolOptions.Compression compression) throws Exception
    {

        PoolingOptions poolingOpts = new PoolingOptions()
                                     .setConnectionsPerHost(HostDistance.LOCAL, connectionsPerHost, connectionsPerHost)
                                     .setMaxRequestsPerConnection(HostDistance.LOCAL, maxPendingPerConnection)
                                     .setNewConnectionThreshold(HostDistance.LOCAL, 100);

        List<InetSocketAddress> contacts = new ArrayList<>();
        for (String host : hosts)
        {
            HostAndPort hap = HostAndPort.fromString(host).withDefaultPort(port);
            InetSocketAddress contact = new InetSocketAddress(InetAddress.getByName(hap.getHost()), hap.getPort());
            contacts.add(contact);
        }

        Cluster.Builder clusterBuilder = Cluster.builder()
                                                .addContactPointsWithPorts(contacts)
                                                .withPoolingOptions(poolingOpts)
                                                .withoutJMXReporting()
                                                .withProtocolVersion(protocolVersion)
                                                .withoutMetrics(); // The driver uses metrics 3 with conflict with our version

        if (loadBalancingPolicy != null)
            clusterBuilder.withLoadBalancingPolicy(loadBalancingPolicy);
        clusterBuilder.withCompression(compression);
        if (encryptionOptions.getEnabled())
        {
            SSLContext sslContext;
            sslContext = SSLFactory.createSSLContext(encryptionOptions, true);

            // Temporarily override newSSLEngine to set accepted protocols until it is added to
            // RemoteEndpointAwareJdkSSLOptions.  See CASSANDRA-13325 and CASSANDRA-16362.
            RemoteEndpointAwareJdkSSLOptions sslOptions = new RemoteEndpointAwareJdkSSLOptions(sslContext, encryptionOptions.cipherSuitesArray())
            {
                @Override
                protected SSLEngine newSSLEngine(SocketChannel channel, InetSocketAddress remoteEndpoint)
                {
                    SSLEngine engine = super.newSSLEngine(channel, remoteEndpoint);

                    String[] acceptedProtocols = encryptionOptions.acceptedProtocolsArray();
                    if (acceptedProtocols != null && acceptedProtocols.length > 0)
                        engine.setEnabledProtocols(acceptedProtocols);

                    return engine;
                }
            };
            clusterBuilder.withSSL(sslOptions);
        }

        if (authProvider != null)
        {
            clusterBuilder.withAuthProvider(authProvider);
        }
        else if (username != null)
        {
            clusterBuilder.withCredentials(username, password);
        }

        cluster = clusterBuilder.build();
        Metadata metadata = cluster.getMetadata();
        System.out.printf(
                "Connected to cluster: %s, max pending requests per connection %d, max connections per host %d%n",
                metadata.getClusterName(),
                maxPendingPerConnection,
                connectionsPerHost);
        for (Host host : metadata.getAllHosts())
        {
            System.out.printf("Datacenter: %s; Host: %s; Rack: %s%n",
                    host.getDatacenter(), host.getAddress() + ":" + host.getSocketAddress().getPort(), host.getRack());
        }

        session = cluster.connect();
    }

    public Cluster getCluster()
    {
        return cluster;
    }

    public Session getSession()
    {
        return session;
    }

    public ResultSet execute(String query, org.apache.cassandra.db.ConsistencyLevel consistency)
    {
        SimpleStatement stmt = new SimpleStatement(query);

        if (consistency.isSerialConsistency())
            stmt.setSerialConsistencyLevel(from(consistency));
        else
            stmt.setConsistencyLevel(from(consistency));
        return getSession().execute(stmt);
    }

    public ResultSet executePrepared(PreparedStatement stmt, List<Object> queryParams, org.apache.cassandra.db.ConsistencyLevel consistency)
    {
        if (consistency.isSerialConsistency())
        {
            stmt.setSerialConsistencyLevel(from(consistency));
        }
        else
        {
            stmt.setConsistencyLevel(from(consistency));
        }
        BoundStatement bstmt = stmt.bind((Object[]) queryParams.toArray(new Object[queryParams.size()]));
        return getSession().execute(bstmt);
    }

    /**
     * Get ConsistencyLevel from a C* ConsistencyLevel. This exists in the Java Driver ConsistencyLevel,
     * but it is not public.
     *
     * @param cl
     * @return
     */
    public static ConsistencyLevel from(org.apache.cassandra.db.ConsistencyLevel cl)
    {
        switch (cl)
        {
            case ANY:
                return com.datastax.driver.core.ConsistencyLevel.ANY;
            case ONE:
                return com.datastax.driver.core.ConsistencyLevel.ONE;
            case TWO:
                return com.datastax.driver.core.ConsistencyLevel.TWO;
            case THREE:
                return com.datastax.driver.core.ConsistencyLevel.THREE;
            case QUORUM:
                return com.datastax.driver.core.ConsistencyLevel.QUORUM;
            case ALL:
                return com.datastax.driver.core.ConsistencyLevel.ALL;
            case LOCAL_QUORUM:
                return com.datastax.driver.core.ConsistencyLevel.LOCAL_QUORUM;
            case EACH_QUORUM:
                return com.datastax.driver.core.ConsistencyLevel.EACH_QUORUM;
            case LOCAL_ONE:
                return com.datastax.driver.core.ConsistencyLevel.LOCAL_ONE;
            case SERIAL:
                return com.datastax.driver.core.ConsistencyLevel.SERIAL;
            case LOCAL_SERIAL:
                return com.datastax.driver.core.ConsistencyLevel.LOCAL_SERIAL;
        }
        throw new AssertionError();
    }

    public void disconnect()
    {
        cluster.close();
    }
}
