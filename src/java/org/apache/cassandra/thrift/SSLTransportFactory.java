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
package org.apache.cassandra.thrift;

import com.google.common.collect.Sets;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TTransport;

import java.util.Map;
import java.util.Set;

public class SSLTransportFactory implements ITransportFactory
{
    public static final int DEFAULT_MAX_FRAME_SIZE = 15 * 1024 * 1024; // 15 MiB

    public static final String TRUSTSTORE = "enc.truststore";
    public static final String TRUSTSTORE_PASSWORD = "enc.truststore.password";
    public static final String KEYSTORE = "enc.keystore";
    public static final String KEYSTORE_PASSWORD = "enc.keystore.password";
    public static final String PROTOCOL = "enc.protocol";
    public static final String CIPHER_SUITES = "enc.cipher.suites";
    public static final int SOCKET_TIMEOUT = 0;

    private static final Set<String> SUPPORTED_OPTIONS = Sets.newHashSet(TRUSTSTORE,
                                                                         TRUSTSTORE_PASSWORD,
                                                                         KEYSTORE,
                                                                         KEYSTORE_PASSWORD,
                                                                         PROTOCOL,
                                                                         CIPHER_SUITES);

    private String truststore;
    private String truststorePassword;
    private String keystore;
    private String keystorePassword;
    private String protocol;
    private String[] cipherSuites;

    @Override
    @SuppressWarnings("resource")
    public TTransport openTransport(String host, int port) throws Exception
    {
        TSSLTransportFactory.TSSLTransportParameters params = new TSSLTransportFactory.TSSLTransportParameters(protocol, cipherSuites);
        params.setTrustStore(truststore, truststorePassword);
        if (null != keystore)
            params.setKeyStore(keystore, keystorePassword);
        TTransport trans = TSSLTransportFactory.getClientSocket(host, port, SOCKET_TIMEOUT, params);
        return new TFramedTransport(trans, DEFAULT_MAX_FRAME_SIZE);
    }

    @Override
    public void setOptions(Map<String, String> options)
    {
        if (options.containsKey(TRUSTSTORE))
            truststore = options.get(TRUSTSTORE);
        if (options.containsKey(TRUSTSTORE_PASSWORD))
            truststorePassword = options.get(TRUSTSTORE_PASSWORD);
        if (options.containsKey(KEYSTORE))
            keystore = options.get(KEYSTORE);
        if (options.containsKey(KEYSTORE_PASSWORD))
            keystorePassword = options.get(KEYSTORE_PASSWORD);
        if (options.containsKey(PROTOCOL))
            protocol = options.get(PROTOCOL);
        if (options.containsKey(CIPHER_SUITES))
            cipherSuites = options.get(CIPHER_SUITES).split(",");
    }

    @Override
    public Set<String> supportedOptions()
    {
        return SUPPORTED_OPTIONS;
    }
}
