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

import java.nio.ByteBuffer;
import java.util.List;
import javax.net.ssl.SSLContext;

import com.datastax.driver.core.*;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.utils.FBUtilities;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Slf4JLoggerFactory;

public class JavaDriverClient
{

    static
    {
        InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
    }

    public final String host;
    public final int port;
    private final EncryptionOptions.ClientEncryptionOptions encryptionOptions;
    private Cluster cluster;
    private Session session;

    public JavaDriverClient(String host, int port)
    {
        this(host, port, new EncryptionOptions.ClientEncryptionOptions());
    }

    public JavaDriverClient(String host, int port, EncryptionOptions.ClientEncryptionOptions encryptionOptions)
    {
        this.host = host;
        this.port = port;
        this.encryptionOptions = encryptionOptions;
    }

    public PreparedStatement prepare(String query)
    {
        return getSession().prepare(query);
    }

    public void connect(ProtocolOptions.Compression compression) throws Exception
    {
        Cluster.Builder clusterBuilder = Cluster.builder()
                                                .addContactPoint(host)
                                                .withPort(port)
                                                .withoutMetrics(); // The driver uses metrics 3 with conflict with our version
        clusterBuilder.withCompression(compression);
        if (encryptionOptions.enabled)
        {
            SSLContext sslContext;
            sslContext = SSLFactory.createSSLContext(encryptionOptions, true);
            SSLOptions sslOptions = new SSLOptions(sslContext, encryptionOptions.cipher_suites);
            clusterBuilder.withSSL(sslOptions);
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

    public ResultSet executePrepared(PreparedStatement stmt, List<ByteBuffer> queryParams, org.apache.cassandra.db.ConsistencyLevel consistency)
    {

        stmt.setConsistencyLevel(from(consistency));
        BoundStatement bstmt = stmt.bind((Object[]) queryParams.toArray(new ByteBuffer[queryParams.size()]));
        return getSession().execute(bstmt);
    }

    /**
     * Get ConsistencyLevel from a C* ConsistencyLevel. This exists in the Java Driver ConsistencyLevel,
     * but it is not public.
     *
     * @param cl
     * @return
     */
    ConsistencyLevel from(org.apache.cassandra.db.ConsistencyLevel cl)
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
        }
        throw new AssertionError();
    }

    public void disconnect()
    {
        cluster.close();
    }
}
