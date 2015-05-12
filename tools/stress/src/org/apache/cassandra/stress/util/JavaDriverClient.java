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

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.net.ssl.SSLContext;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;
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

    public final String host;
    public final int port;
    public final String username;
    public final String password;
    public final AuthProvider authProvider;

    private final EncryptionOptions.ClientEncryptionOptions encryptionOptions;
    private Cluster cluster;
    private Session session;
    private final WhiteListPolicy whitelist;

    private static final ConcurrentMap<String, PreparedStatement> stmts = new ConcurrentHashMap<>();

    public JavaDriverClient(StressSettings settings, String host, int port)
    {
        this(settings, host, port, new EncryptionOptions.ClientEncryptionOptions());
    }

    public JavaDriverClient(StressSettings settings, String host, int port, EncryptionOptions.ClientEncryptionOptions encryptionOptions)
    {
        this.host = host;
        this.port = port;
        this.username = settings.mode.username;
        this.password = settings.mode.password;
        this.authProvider = settings.mode.authProvider;
        this.encryptionOptions = encryptionOptions;
        if (settings.node.isWhiteList)
            whitelist = new WhiteListPolicy(new DCAwareRoundRobinPolicy(), settings.node.resolveAll(settings.port.nativePort));
        else
            whitelist = null;
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
        Cluster.Builder clusterBuilder = Cluster.builder()
                                                .addContactPoint(host)
                                                .withPort(port)
                                                .withoutMetrics(); // The driver uses metrics 3 with conflict with our version
        if (whitelist != null)
            clusterBuilder.withLoadBalancingPolicy(whitelist);
        clusterBuilder.withCompression(compression);
        if (encryptionOptions.enabled)
        {
            SSLContext sslContext;
            sslContext = SSLFactory.createSSLContext(encryptionOptions, true);
            SSLOptions sslOptions = new SSLOptions(sslContext, encryptionOptions.cipher_suites);
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
        System.out.printf("Connected to cluster: %s%n",
                metadata.getClusterName());
        for (Host host : metadata.getAllHosts())
        {
            System.out.printf("Datatacenter: %s; Host: %s; Rack: %s%n",
                    host.getDatacenter(), host.getAddress(), host.getRack());
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
        stmt.setConsistencyLevel(from(consistency));
        return getSession().execute(stmt);
    }

    public ResultSet executePrepared(PreparedStatement stmt, List<Object> queryParams, org.apache.cassandra.db.ConsistencyLevel consistency)
    {

        stmt.setConsistencyLevel(from(consistency));
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
        }
        throw new AssertionError();
    }

    public void disconnect()
    {
        cluster.close();
    }
}
