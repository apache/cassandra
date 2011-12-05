package org.apache.cassandra.security;

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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.util.Set;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;

import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

/**
 * A Factory for providing and setting up Client and Server SSL wrapped
 * Socket and ServerSocket
 */
public final class SSLFactory
{
    private static final Logger logger_ = LoggerFactory.getLogger(SSLFactory.class);

    public static SSLServerSocket getServerSocket(EncryptionOptions options, InetAddress address, int port) throws IOException
    {
        SSLContext ctx = createSSLContext(options);
        SSLServerSocket serverSocket = (SSLServerSocket)ctx.getServerSocketFactory().createServerSocket();
        serverSocket.setReuseAddress(true);
        String[] suits = filterCipherSuites(serverSocket.getSupportedCipherSuites(), options.cipher_suites);
        serverSocket.setEnabledCipherSuites(suits);
        serverSocket.bind(new InetSocketAddress(address, port), 100);
        return serverSocket;
    }

    /** Create a socket and connect */
    public static SSLSocket getSocket(EncryptionOptions options, InetAddress address, int port, InetAddress localAddress, int localPort) throws IOException
    {
        SSLContext ctx = createSSLContext(options);
        SSLSocket socket = (SSLSocket) ctx.getSocketFactory().createSocket(address, port, localAddress, localPort);
        String[] suits = filterCipherSuites(socket.getSupportedCipherSuites(), options.cipher_suites);
        socket.setEnabledCipherSuites(suits);
        return socket;
    }

    /** Just create a socket */
    public static SSLSocket getSocket(EncryptionOptions options) throws IOException
    {
        SSLContext ctx = createSSLContext(options);
        SSLSocket socket = (SSLSocket) ctx.getSocketFactory().createSocket();
        String[] suits = filterCipherSuites(socket.getSupportedCipherSuites(), options.cipher_suites);
        socket.setEnabledCipherSuites(suits);
        return socket;
    }

    private static SSLContext createSSLContext(EncryptionOptions options) throws IOException
    {
        FileInputStream tsf = new FileInputStream(options.truststore);
        FileInputStream ksf = new FileInputStream(options.keystore);
        SSLContext ctx;
        try
        {
            ctx = SSLContext.getInstance(options.protocol);
            TrustManagerFactory tmf;
            KeyManagerFactory kmf;

            tmf = TrustManagerFactory.getInstance(options.algorithm);
            KeyStore ts = KeyStore.getInstance(options.store_type);
            ts.load(tsf, options.truststore_password.toCharArray());
            tmf.init(ts);

            kmf = KeyManagerFactory.getInstance(options.algorithm);
            KeyStore ks = KeyStore.getInstance(options.store_type);
            ks.load(ksf, options.keystore_password.toCharArray());
            kmf.init(ks, options.keystore_password.toCharArray());

            ctx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

        }
        catch (Exception e)
        {
            throw new IOException("Error creating the initializing the SSL Context", e);
        }
        finally
        {
            FileUtils.closeQuietly(tsf);
            FileUtils.closeQuietly(ksf);
        }
        return ctx;
    }
    
    private static String[] filterCipherSuites(String[] supported, String[] desired)
    {
        Set<String> des = Sets.newHashSet(desired);
        Set<String> return_ = Sets.intersection(Sets.newHashSet(supported), des);
        if (des.size() > return_.size())
            logger_.warn("Filtering out {} as it isnt supported by the socket", StringUtils.join(Sets.difference(des, return_), ","));
        return return_.toArray(new String[return_.size()]);
    }
}
