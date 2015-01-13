package org.apache.cassandra.hadoop.cql3;
/*
*
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
*
*/
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Arrays;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.google.common.base.Optional;

public class CqlConfigHelper
{
    private static final String INPUT_CQL_COLUMNS_CONFIG = "cassandra.input.columnfamily.columns";
    private static final String INPUT_CQL_PAGE_ROW_SIZE_CONFIG = "cassandra.input.page.row.size";
    private static final String INPUT_CQL_WHERE_CLAUSE_CONFIG = "cassandra.input.where.clause";
    private static final String INPUT_CQL = "cassandra.input.cql";

    private static final String USERNAME = "cassandra.username";
    private static final String PASSWORD = "cassandra.password";

    private static final String INPUT_NATIVE_PORT = "cassandra.input.native.port";
    private static final String INPUT_NATIVE_CORE_CONNECTIONS_PER_HOST = "cassandra.input.native.core.connections.per.host";
    private static final String INPUT_NATIVE_MAX_CONNECTIONS_PER_HOST = "cassandra.input.native.max.connections.per.host";
    private static final String INPUT_NATIVE_MIN_SIMULT_REQ_PER_CONNECTION = "cassandra.input.native.min.simult.reqs.per.connection"; 
    private static final String INPUT_NATIVE_MAX_SIMULT_REQ_PER_CONNECTION = "cassandra.input.native.max.simult.reqs.per.connection";
    private static final String INPUT_NATIVE_CONNECTION_TIMEOUT = "cassandra.input.native.connection.timeout";
    private static final String INPUT_NATIVE_READ_CONNECTION_TIMEOUT = "cassandra.input.native.read.connection.timeout";
    private static final String INPUT_NATIVE_RECEIVE_BUFFER_SIZE = "cassandra.input.native.receive.buffer.size";
    private static final String INPUT_NATIVE_SEND_BUFFER_SIZE = "cassandra.input.native.send.buffer.size";
    private static final String INPUT_NATIVE_SOLINGER = "cassandra.input.native.solinger";
    private static final String INPUT_NATIVE_TCP_NODELAY = "cassandra.input.native.tcp.nodelay";
    private static final String INPUT_NATIVE_REUSE_ADDRESS = "cassandra.input.native.reuse.address";
    private static final String INPUT_NATIVE_KEEP_ALIVE = "cassandra.input.native.keep.alive";
    private static final String INPUT_NATIVE_AUTH_PROVIDER = "cassandra.input.native.auth.provider";
    private static final String INPUT_NATIVE_SSL_TRUST_STORE_PATH = "cassandra.input.native.ssl.trust.store.path";
    private static final String INPUT_NATIVE_SSL_KEY_STORE_PATH = "cassandra.input.native.ssl.key.store.path";
    private static final String INPUT_NATIVE_SSL_TRUST_STORE_PASSWARD = "cassandra.input.native.ssl.trust.store.password";
    private static final String INPUT_NATIVE_SSL_KEY_STORE_PASSWARD = "cassandra.input.native.ssl.key.store.password";
    private static final String INPUT_NATIVE_SSL_CIPHER_SUITES = "cassandra.input.native.ssl.cipher.suites";

    private static final String INPUT_NATIVE_PROTOCOL_VERSION = "cassandra.input.native.protocol.version";

    private static final String OUTPUT_CQL = "cassandra.output.cql";
    
    /**
     * Set the CQL columns for the input of this job.
     *
     * @param conf Job configuration you are about to run
     * @param columns
     */
    public static void setInputColumns(Configuration conf, String columns)
    {
        if (columns == null || columns.isEmpty())
            return;
        
        conf.set(INPUT_CQL_COLUMNS_CONFIG, columns);
    }
    
    /**
     * Set the CQL query Limit for the input of this job.
     *
     * @param conf Job configuration you are about to run
     * @param cqlPageRowSize
     */
    public static void setInputCQLPageRowSize(Configuration conf, String cqlPageRowSize)
    {
        if (cqlPageRowSize == null)
        {
            throw new UnsupportedOperationException("cql page row size may not be null");
        }

        conf.set(INPUT_CQL_PAGE_ROW_SIZE_CONFIG, cqlPageRowSize);
    }

    /**
     * Set the CQL user defined where clauses for the input of this job.
     *
     * @param conf Job configuration you are about to run
     * @param clauses
     */
    public static void setInputWhereClauses(Configuration conf, String clauses)
    {
        if (clauses == null || clauses.isEmpty())
            return;
        
        conf.set(INPUT_CQL_WHERE_CLAUSE_CONFIG, clauses);
    }
  
    /**
     * Set the CQL prepared statement for the output of this job.
     *
     * @param conf Job configuration you are about to run
     * @param cql
     */
    public static void setOutputCql(Configuration conf, String cql)
    {
        if (cql == null || cql.isEmpty())
            return;
        
        conf.set(OUTPUT_CQL, cql);
    }

    public static void setInputCql(Configuration conf, String cql)
    {
        if (cql == null || cql.isEmpty())
            return;

        conf.set(INPUT_CQL, cql);
    }

    public static void setUserNameAndPassword(Configuration conf, String username, String password)
    {
        if (StringUtils.isNotBlank(username))
        {
            conf.set(INPUT_NATIVE_AUTH_PROVIDER, PlainTextAuthProvider.class.getName());
            conf.set(USERNAME, username);
            conf.set(PASSWORD, password);
        }
    }

    public static Optional<Integer> getInputCoreConnections(Configuration conf)
    {
        return getIntSetting(INPUT_NATIVE_CORE_CONNECTIONS_PER_HOST, conf);
    }

    public static Optional<Integer> getInputMaxConnections(Configuration conf)
    {
        return getIntSetting(INPUT_NATIVE_MAX_CONNECTIONS_PER_HOST, conf);
    }

    public static int getInputNativePort(Configuration conf)
    {
        return Integer.parseInt(conf.get(INPUT_NATIVE_PORT, "9042"));
    }

    public static Optional<Integer> getInputMinSimultReqPerConnections(Configuration conf)
    {
        return getIntSetting(INPUT_NATIVE_MIN_SIMULT_REQ_PER_CONNECTION, conf);
    }

    public static Optional<Integer> getInputMaxSimultReqPerConnections(Configuration conf)
    {
        return getIntSetting(INPUT_NATIVE_MAX_SIMULT_REQ_PER_CONNECTION, conf);
    }

    public static Optional<Integer> getInputNativeConnectionTimeout(Configuration conf)
    {
        return getIntSetting(INPUT_NATIVE_CONNECTION_TIMEOUT, conf);
    }

    public static Optional<Integer> getInputNativeReadConnectionTimeout(Configuration conf)
    {
        return getIntSetting(INPUT_NATIVE_READ_CONNECTION_TIMEOUT, conf);
    }

    public static Optional<Integer> getInputNativeReceiveBufferSize(Configuration conf)
    {
        return getIntSetting(INPUT_NATIVE_RECEIVE_BUFFER_SIZE, conf);
    }

    public static Optional<Integer> getInputNativeSendBufferSize(Configuration conf)
    {
        return getIntSetting(INPUT_NATIVE_SEND_BUFFER_SIZE, conf);
    }

    public static Optional<Integer> getInputNativeSolinger(Configuration conf)
    {
        return getIntSetting(INPUT_NATIVE_SOLINGER, conf);
    }

    public static Optional<Boolean> getInputNativeTcpNodelay(Configuration conf)
    {
        return getBooleanSetting(INPUT_NATIVE_TCP_NODELAY, conf);
    }

    public static Optional<Boolean> getInputNativeReuseAddress(Configuration conf)
    {
        return getBooleanSetting(INPUT_NATIVE_REUSE_ADDRESS, conf);
    }

    public static Optional<String> getInputNativeAuthProvider(Configuration conf)
    {
        return getStringSetting(INPUT_NATIVE_AUTH_PROVIDER, conf);
    }

    public static Optional<String> getInputNativeSSLTruststorePath(Configuration conf)
    {
        return getStringSetting(INPUT_NATIVE_SSL_TRUST_STORE_PATH, conf);
    }

    public static Optional<String> getInputNativeSSLKeystorePath(Configuration conf)
    {
        return getStringSetting(INPUT_NATIVE_SSL_KEY_STORE_PATH, conf);
    }

    public static Optional<String> getInputNativeSSLKeystorePassword(Configuration conf)
    {
        return getStringSetting(INPUT_NATIVE_SSL_KEY_STORE_PASSWARD, conf);
    }

    public static Optional<String> getInputNativeSSLTruststorePassword(Configuration conf)
    {
        return getStringSetting(INPUT_NATIVE_SSL_TRUST_STORE_PASSWARD, conf);
    }

    public static Optional<String> getInputNativeSSLCipherSuites(Configuration conf)
    {
        return getStringSetting(INPUT_NATIVE_SSL_CIPHER_SUITES, conf);
    }

    public static Optional<Boolean> getInputNativeKeepAlive(Configuration conf)
    {
        return getBooleanSetting(INPUT_NATIVE_KEEP_ALIVE, conf);
    }

    public static String getInputcolumns(Configuration conf)
    {
        return conf.get(INPUT_CQL_COLUMNS_CONFIG);
    }

    public static Optional<Integer> getInputPageRowSize(Configuration conf)
    {
        return getIntSetting(INPUT_CQL_PAGE_ROW_SIZE_CONFIG, conf);
    }

    public static String getInputWhereClauses(Configuration conf)
    {
        return conf.get(INPUT_CQL_WHERE_CLAUSE_CONFIG);
    }

    public static String getInputCql(Configuration conf)
    {
        return conf.get(INPUT_CQL);
    }

    public static String getOutputCql(Configuration conf)
    {
        return conf.get(OUTPUT_CQL);
    }

    private static Optional<Integer> getProtocolVersion(Configuration conf) {
        return getIntSetting(INPUT_NATIVE_PROTOCOL_VERSION, conf);
    }

    public static Cluster getInputCluster(String host, Configuration conf)
    {
        // this method has been left for backward compatibility
        return getInputCluster(new String[] {host}, conf);
    }

    public static Cluster getInputCluster(String[] hosts, Configuration conf)
    {
        int port = getInputNativePort(conf);
        Optional<AuthProvider> authProvider = getAuthProvider(conf);
        Optional<SSLOptions> sslOptions = getSSLOptions(conf);
        Optional<Integer> protocolVersion = getProtocolVersion(conf);
        LoadBalancingPolicy loadBalancingPolicy = getReadLoadBalancingPolicy(conf, hosts);
        SocketOptions socketOptions = getReadSocketOptions(conf);
        QueryOptions queryOptions = getReadQueryOptions(conf);
        PoolingOptions poolingOptions = getReadPoolingOptions(conf);
        
        Cluster.Builder builder = Cluster.builder()
                                         .addContactPoints(hosts)
                                         .withPort(port)
                                         .withCompression(ProtocolOptions.Compression.NONE);

        if (authProvider.isPresent())
            builder.withAuthProvider(authProvider.get());
        if (sslOptions.isPresent())
            builder.withSSL(sslOptions.get());

        if (protocolVersion.isPresent()) {
            builder.withProtocolVersion(protocolVersion.get());
        }
        builder.withLoadBalancingPolicy(loadBalancingPolicy)
               .withSocketOptions(socketOptions)
               .withQueryOptions(queryOptions)
               .withPoolingOptions(poolingOptions);

        return builder.build();
    }


    public static void setInputCoreConnections(Configuration conf, String connections)
    {
        conf.set(INPUT_NATIVE_CORE_CONNECTIONS_PER_HOST, connections);
    }

    public static void setInputMaxConnections(Configuration conf, String connections)
    {
        conf.set(INPUT_NATIVE_MAX_CONNECTIONS_PER_HOST, connections);
    }

    public static void setInputMinSimultReqPerConnections(Configuration conf, String reqs)
    {
        conf.set(INPUT_NATIVE_MIN_SIMULT_REQ_PER_CONNECTION, reqs);
    }

    public static void setInputMaxSimultReqPerConnections(Configuration conf, String reqs)
    {
        conf.set(INPUT_NATIVE_MAX_SIMULT_REQ_PER_CONNECTION, reqs);
    }    

    public static void setInputNativeConnectionTimeout(Configuration conf, String timeout)
    {
        conf.set(INPUT_NATIVE_CONNECTION_TIMEOUT, timeout);
    }

    public static void setInputNativeReadConnectionTimeout(Configuration conf, String timeout)
    {
        conf.set(INPUT_NATIVE_READ_CONNECTION_TIMEOUT, timeout);
    }

    public static void setInputNativeReceiveBufferSize(Configuration conf, String size)
    {
        conf.set(INPUT_NATIVE_RECEIVE_BUFFER_SIZE, size);
    }

    public static void setInputNativeSendBufferSize(Configuration conf, String size)
    {
        conf.set(INPUT_NATIVE_SEND_BUFFER_SIZE, size);
    }

    public static void setInputNativeSolinger(Configuration conf, String solinger)
    {
        conf.set(INPUT_NATIVE_SOLINGER, solinger);
    }

    public static void setInputNativeTcpNodelay(Configuration conf, String tcpNodelay)
    {
        conf.set(INPUT_NATIVE_TCP_NODELAY, tcpNodelay);
    }

    public static void setInputNativeAuthProvider(Configuration conf, String authProvider)
    {
        conf.set(INPUT_NATIVE_AUTH_PROVIDER, authProvider);
    }

    public static void setInputNativeSSLTruststorePath(Configuration conf, String path)
    {
        conf.set(INPUT_NATIVE_SSL_TRUST_STORE_PATH, path);
    } 

    public static void setInputNativeSSLKeystorePath(Configuration conf, String path)
    {
        conf.set(INPUT_NATIVE_SSL_KEY_STORE_PATH, path);
    }

    public static void setInputNativeSSLKeystorePassword(Configuration conf, String pass)
    {
        conf.set(INPUT_NATIVE_SSL_KEY_STORE_PASSWARD, pass);
    }

    public static void setInputNativeSSLTruststorePassword(Configuration conf, String pass)
    {
        conf.set(INPUT_NATIVE_SSL_TRUST_STORE_PASSWARD, pass);
    }

    public static void setInputNativeSSLCipherSuites(Configuration conf, String suites)
    {
        conf.set(INPUT_NATIVE_SSL_CIPHER_SUITES, suites);
    }

    public static void setInputNativeReuseAddress(Configuration conf, String reuseAddress)
    {
        conf.set(INPUT_NATIVE_REUSE_ADDRESS, reuseAddress);
    }

    public static void setInputNativeKeepAlive(Configuration conf, String keepAlive)
    {
        conf.set(INPUT_NATIVE_KEEP_ALIVE, keepAlive);
    }

    public static void setInputNativePort(Configuration conf, String port)
    {
        conf.set(INPUT_NATIVE_PORT, port);
    }

    private static PoolingOptions getReadPoolingOptions(Configuration conf)
    {
        Optional<Integer> coreConnections = getInputCoreConnections(conf);
        Optional<Integer> maxConnections = getInputMaxConnections(conf);
        Optional<Integer> maxSimultaneousRequests = getInputMaxSimultReqPerConnections(conf);
        Optional<Integer> minSimultaneousRequests = getInputMinSimultReqPerConnections(conf);
        
        PoolingOptions poolingOptions = new PoolingOptions();

        for (HostDistance hostDistance : Arrays.asList(HostDistance.LOCAL, HostDistance.REMOTE))
        {
            if (coreConnections.isPresent())
                poolingOptions.setCoreConnectionsPerHost(hostDistance, coreConnections.get());
            if (maxConnections.isPresent())
                poolingOptions.setMaxConnectionsPerHost(hostDistance, maxConnections.get());
            if (minSimultaneousRequests.isPresent())
                poolingOptions.setMinSimultaneousRequestsPerConnectionThreshold(hostDistance, minSimultaneousRequests.get());
            if (maxSimultaneousRequests.isPresent())
                poolingOptions.setMaxSimultaneousRequestsPerConnectionThreshold(hostDistance, maxSimultaneousRequests.get());
        }

        return poolingOptions;
    }  

    private static QueryOptions getReadQueryOptions(Configuration conf)
    {
        String CL = ConfigHelper.getReadConsistencyLevel(conf);
        Optional<Integer> fetchSize = getInputPageRowSize(conf);
        QueryOptions queryOptions = new QueryOptions();
        if (CL != null && !CL.isEmpty())
            queryOptions.setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.valueOf(CL));

        if (fetchSize.isPresent())
            queryOptions.setFetchSize(fetchSize.get());
        return queryOptions;
    }

    private static SocketOptions getReadSocketOptions(Configuration conf)
    {
        SocketOptions socketOptions = new SocketOptions();
        Optional<Integer> connectTimeoutMillis = getInputNativeConnectionTimeout(conf);
        Optional<Integer> readTimeoutMillis = getInputNativeReadConnectionTimeout(conf);
        Optional<Integer> receiveBufferSize = getInputNativeReceiveBufferSize(conf);
        Optional<Integer> sendBufferSize = getInputNativeSendBufferSize(conf);
        Optional<Integer> soLinger = getInputNativeSolinger(conf);
        Optional<Boolean> tcpNoDelay = getInputNativeTcpNodelay(conf);
        Optional<Boolean> reuseAddress = getInputNativeReuseAddress(conf);       
        Optional<Boolean> keepAlive = getInputNativeKeepAlive(conf);

        if (connectTimeoutMillis.isPresent())
            socketOptions.setConnectTimeoutMillis(connectTimeoutMillis.get());
        if (readTimeoutMillis.isPresent())
            socketOptions.setReadTimeoutMillis(readTimeoutMillis.get());
        if (receiveBufferSize.isPresent())
            socketOptions.setReceiveBufferSize(receiveBufferSize.get());
        if (sendBufferSize.isPresent())
            socketOptions.setSendBufferSize(sendBufferSize.get());
        if (soLinger.isPresent())
            socketOptions.setSoLinger(soLinger.get());
        if (tcpNoDelay.isPresent())
            socketOptions.setTcpNoDelay(tcpNoDelay.get());
        if (reuseAddress.isPresent())
            socketOptions.setReuseAddress(reuseAddress.get());
        if (keepAlive.isPresent())
            socketOptions.setKeepAlive(keepAlive.get());     

        return socketOptions;
    }

    private static LoadBalancingPolicy getReadLoadBalancingPolicy(Configuration conf, final String[] stickHosts)
    {
        return new LimitedLocalNodeFirstLocalBalancingPolicy(stickHosts);
    }

    private static Optional<AuthProvider> getAuthProvider(Configuration conf)
    {
        Optional<String> authProvider = getInputNativeAuthProvider(conf);
        if (!authProvider.isPresent())
            return Optional.absent();

        return Optional.of(getClientAuthProvider(authProvider.get(), conf));
    }

    private static Optional<SSLOptions> getSSLOptions(Configuration conf)
    {
        Optional<String> truststorePath = getInputNativeSSLTruststorePath(conf);
        Optional<String> keystorePath = getInputNativeSSLKeystorePath(conf);
        Optional<String> truststorePassword = getInputNativeSSLTruststorePassword(conf);
        Optional<String> keystorePassword = getInputNativeSSLKeystorePassword(conf);
        Optional<String> cipherSuites = getInputNativeSSLCipherSuites(conf);
        
        if (truststorePath.isPresent() && keystorePath.isPresent() && truststorePassword.isPresent() && keystorePassword.isPresent())
        {
            SSLContext context;
            try
            {
                context = getSSLContext(truststorePath.get(), truststorePassword.get(), keystorePath.get(), keystorePassword.get());
            }
            catch (UnrecoverableKeyException | KeyManagementException |
                    NoSuchAlgorithmException | KeyStoreException | CertificateException | IOException e)
            {
                throw new RuntimeException(e);
            }
            String[] css = SSLOptions.DEFAULT_SSL_CIPHER_SUITES;
            if (cipherSuites.isPresent())
                css = cipherSuites.get().split(",");
            return Optional.of(new SSLOptions(context,css));
        }
        return Optional.absent();
    }

    private static Optional<Integer> getIntSetting(String parameter, Configuration conf)
    {
        String setting = conf.get(parameter);
        if (setting == null)
            return Optional.absent();
        return Optional.of(Integer.parseInt(setting));  
    }

    private static Optional<Boolean> getBooleanSetting(String parameter, Configuration conf)
    {
        String setting = conf.get(parameter);
        if (setting == null)
            return Optional.absent();
        return Optional.of(Boolean.parseBoolean(setting));  
    }

    private static Optional<String> getStringSetting(String parameter, Configuration conf)
    {
        String setting = conf.get(parameter);
        if (setting == null)
            return Optional.absent();
        return Optional.of(setting);  
    }

    private static AuthProvider getClientAuthProvider(String factoryClassName, Configuration conf)
    {
        try
        {
            Class<?> c = Class.forName(factoryClassName);
            if (PlainTextAuthProvider.class.equals(c))
            {
                String username = getStringSetting(USERNAME, conf).or("");
                String password = getStringSetting(PASSWORD, conf).or("");
                return (AuthProvider) c.getConstructor(String.class, String.class)
                        .newInstance(username, password);
            }
            else
            {
                return (AuthProvider) c.newInstance();
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to instantiate auth provider:" + factoryClassName, e);
        }
    }

    private static SSLContext getSSLContext(String truststorePath, String truststorePassword, String keystorePath, String keystorePassword)
            throws NoSuchAlgorithmException, KeyStoreException, CertificateException, IOException, UnrecoverableKeyException, KeyManagementException
    {
        FileInputStream tsf = null;
        FileInputStream ksf = null;
        SSLContext ctx = null;
        try
        {
            tsf = new FileInputStream(truststorePath);
            ksf = new FileInputStream(keystorePath);
            ctx = SSLContext.getInstance("SSL");

            KeyStore ts = KeyStore.getInstance("JKS");
            ts.load(tsf, truststorePassword.toCharArray());
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ts);

            KeyStore ks = KeyStore.getInstance("JKS");
            ks.load(ksf, keystorePassword.toCharArray());
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, keystorePassword.toCharArray());

            ctx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
        }
        finally
        {
            FileUtils.closeQuietly(tsf);
            FileUtils.closeQuietly(ksf);
        }
        return ctx;
    }
}
